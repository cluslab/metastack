/*****************************************************************************\
 *  acct_gather_profile_influxdb.c - slurm accounting plugin for influxdb
 *				     profiling.
 *****************************************************************************
 *  Author: Carlos Fenoy Garcia
 *  Copyright (C) 2016 F. Hoffmann - La Roche
 *
 *  Based on the HDF5 profiling plugin and Elasticsearch job completion plugin.
 *
 *  Portions Copyright (C) 2013 Bull S. A. S.
 *		Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois.
 *
 *  Portions Copyright (C) 2013 SchedMD LLC.
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <http://www.schedmd.com/slurmdocs/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
 *
 *  This file is patterned after jobcomp_linux.c, written by Morris Jette and
 *  Copyright (C) 2002 The Regents of the University of California.
 \*****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <inttypes.h>
#include <unistd.h>
#include <math.h>
#include <curl/curl.h>

#include "src/common/slurm_xlator.h"
#include "src/common/fd.h"
#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE
#include "src/plugins/jobacct_gather/common/common_jag.h"
#endif
#include "src/common/macros.h"
#include "src/common/slurm_acct_gather_profile.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/slurm_protocol_defs.h"
#include "src/common/slurm_time.h"
#include "src/common/timers.h"
#include "src/common/xstring.h"
#include "src/slurmd/common/proctrack.h"


/*
 * These variables are required by the generic plugin interface.  If they
 * are not found in the plugin, the plugin loader will ignore it.
 *
 * plugin_name - a string giving a human-readable description of the
 * plugin.  There is no maximum length, but the symbol must refer to
 * a valid string.
 *
 * plugin_type - a string suggesting the type of the plugin or its
 * applicability to a particular form of data or method of data handling.
 * If the low-level plugin API is used, the contents of this string are
 * unimportant and may be anything.  Slurm uses the higher-level plugin
 * interface which requires this string to be of the form
 *
 *	<application>/<method>
 *
 * where <application> is a description of the intended application of
 * the plugin (e.g., "jobacct" for Slurm job completion logging) and <method>
 * is a description of how this plugin satisfies that application.  Slurm will
 * only load job completion logging plugins if the plugin_type string has a
 * prefix of "jobacct/".
 *
 * plugin_version - an unsigned 32-bit integer containing the Slurm version
 * (major.minor.micro combined into a single number).
 */
const char plugin_name[] = "AcctGatherProfile influxdb plugin";
const char plugin_type[] = "acct_gather_profile/influxdb";
const uint32_t plugin_version = SLURM_VERSION_NUMBER;

typedef struct {
	char *host;
	char *database;
	uint32_t def;
	char *password;
	char *rt_policy;
	char *username;
#ifdef __METASTACK_LOAD_ABNORMAL
	char* workdir;
#endif	
#ifdef __METASTACK_JOB_USELESS_RUNNING_WARNING
	char* series_reduce;
#endif
} slurm_influxdb_conf_t;

typedef struct {
	char ** names;
	uint32_t *types;
	size_t size;
	char * name;
} table_t;

/* Type for handling HTTP responses */
struct http_response {
	char *message;
	size_t size;
};

union data_t{
	uint64_t u;
	double	 d;
};

static slurm_influxdb_conf_t influxdb_conf;
static uint32_t g_profile_running = ACCT_GATHER_PROFILE_NOT_SET;
static stepd_step_rec_t *g_job = NULL;

static char *datastr = NULL;
static int datastrlen = 0;

static table_t *tables = NULL;
static size_t tables_max_len = 0;
static size_t tables_cur_len = 0;

#ifdef __METASTACK_JOB_USELESS_RUNNING_WARNING
static char *stepd_datastr = NULL;  /* Save the data to send to the stepd retention policy */
static char *event_datastr = NULL;	/* Save the data to send to the event retention policy */
static char *buffer_file_stepd = NULL;
static char *buffer_file_event = NULL;
#define MAX_POLICY_NAME_LENGTH 256	/* Set the maximum length of the reservation policy name */
typedef enum {
	NATIVERP,
	STEPDRP,
	EVENTRP
} RPType;
/**
 *
 * This function extracts and returns the appropriate retention policy based on the input string `rt_policy`
 * and the specified retention policy type (`NATIVERP`, `STEPDRP`, or `EVENTRP`). 
 * If specific policies for `NATIVERP`, `STEPDRP`, or `EVENTRP` are found in the input string,
 * they will be used accordingly. If not, the function returns a fallback value from other policy types.
 * If no valid policy is specified, the function defaults to "autogen".
 *
 * rt_policy[in]	A string containing comma-separated key-value pairs that specify 
 *                 	the retention policies. For example: "NATIVERP=30d,STEPDRP=7d,EVENTRP=14d".
 *					If `NULL` or empty, the function defaults to "autogen".
 * type[in]			The retention policy type, which determines which policy to extract 
 *					(can be `NATIVERP`, `STEPDRP`, or `EVENTRP`).
 *
 * return 			A dynamically allocated string representing the retention policy for the given type.
 *         			The caller is responsible for freeing the returned string.
 *         			If no valid retention policy is found, the function returns "autogen".
 */
static char* _parse_rt_policy(const char *rt_policy, RPType type) {
    if(rt_policy == NULL || rt_policy[0] == '\0'){
        return xstrdup("autogen");
    }

    // Initialize the values
    char native_retention_policy[MAX_POLICY_NAME_LENGTH], stepd_retention_policy[MAX_POLICY_NAME_LENGTH], event_retention_policy[MAX_POLICY_NAME_LENGTH];
    char *token = NULL, *rest = NULL, *copy = NULL, *rval = NULL;
	bool native_set = false, stepd_set = false, event_set = false;
    copy = xstrdup(rt_policy);
	/*
		Compatible with the original way of setting up retention policies
	*/
    strncpy(native_retention_policy, copy, sizeof(native_retention_policy) - 1);
    native_retention_policy[sizeof(native_retention_policy) - 1] = '\0'; // Ensure null termination

    strncpy(stepd_retention_policy, copy, sizeof(stepd_retention_policy) - 1);
    stepd_retention_policy[sizeof(stepd_retention_policy) - 1] = '\0';

    strncpy(event_retention_policy, copy, sizeof(event_retention_policy) - 1);
    event_retention_policy[sizeof(event_retention_policy) - 1] = '\0';

    token = strtok_r(copy, ",", &rest);

    while (token != NULL) {
        // Extracting key and value
        char *key = strtok(token, "=");
        char *value = strtok(NULL, "=");

        if (key != NULL && value != NULL) {
            if (xstrcmp(key, "NATIVERP") == 0) {
				if(strlen(value) >= MAX_POLICY_NAME_LENGTH)
					debug3("NATIVERP Retention policy names(%s) too long, Maximum length allowed : %d", value, MAX_POLICY_NAME_LENGTH);
				native_set = true;
                strncpy(native_retention_policy, value, sizeof(native_retention_policy) - 1);
                native_retention_policy[sizeof(native_retention_policy) - 1] = '\0';
            } else if (xstrcmp(key, "STEPDRP") == 0) {
				if(strlen(value) >= MAX_POLICY_NAME_LENGTH)
					debug3("STEPDRP Retention policy names(%s) too long, Maximum length allowed : %d", value, MAX_POLICY_NAME_LENGTH);
				stepd_set = true;
				strncpy(stepd_retention_policy, value, sizeof(stepd_retention_policy) - 1);
                stepd_retention_policy[sizeof(stepd_retention_policy) - 1] = '\0';
			} else if (xstrcmp(key, "EVENTRP") == 0) {
				if(strlen(value) >= MAX_POLICY_NAME_LENGTH)
					debug3("EVENTRP Retention policy names(%s) too long, Maximum length allowed : %d", value, MAX_POLICY_NAME_LENGTH);
				event_set = true;
				strncpy(event_retention_policy, value, sizeof(event_retention_policy) - 1);
                event_retention_policy[sizeof(event_retention_policy) - 1] = '\0';
			}
        }
        token = strtok_r(NULL, ",", &rest);
    }
    xfree(copy);
    switch (type)
    {
    case NATIVERP:
		if(native_set)
			rval = xstrdup(native_retention_policy);
		else
			rval = event_set ? xstrdup(event_retention_policy) : xstrdup(stepd_retention_policy);
        break;
	case STEPDRP:
		if(stepd_set)
			rval = xstrdup(stepd_retention_policy);
		else
			rval = event_set ? xstrdup(event_retention_policy) : xstrdup(native_retention_policy);
        break;
	case EVENTRP:
		if(event_set)
			rval = xstrdup(event_retention_policy);
		else
			rval = stepd_set ? xstrdup(stepd_retention_policy) : xstrdup(native_retention_policy);
        break;
    default:
        rval = xstrdup("autogen");
        break;
    }
    return rval;
}
#endif

static void _free_tables(void)
{
	int i, j;

	debug3("%s %s called", plugin_type, __func__);

	if (!tables)
		return;

	for (i = 0; i < tables_cur_len; i++) {
		table_t *table = &(tables[i]);
		for (j = 0; j < table->size; j++)
			xfree(table->names[j]);
		xfree(table->name);
		xfree(table->names);
		xfree(table->types);
	}
	xfree(tables);
}

static uint32_t _determine_profile(void)
{
	uint32_t profile;

	debug3("%s %s called", plugin_type, __func__);
	xassert(g_job);

	if (g_profile_running != ACCT_GATHER_PROFILE_NOT_SET)
		profile = g_profile_running;
	else if (g_job->profile >= ACCT_GATHER_PROFILE_NONE)
		profile = g_job->profile;
	else
		profile = influxdb_conf.def;

	return profile;
}

/* Callback to handle the HTTP response */
static size_t _write_callback(void *contents, size_t size, size_t nmemb,
			      void *userp)
{
	size_t realsize = size * nmemb;
	struct http_response *mem = (struct http_response *) userp;

	debug3("%s %s called", plugin_type, __func__);

	mem->message = xrealloc(mem->message, mem->size + realsize + 1);

	memcpy(&(mem->message[mem->size]), contents, realsize);
	mem->size += realsize;
	mem->message[mem->size] = 0;

	return realsize;
}

#if defined(__METASTACK_LOAD_ABNORMAL) && defined(__METASTACK_JOB_USELESS_RUNNING_WARNING)
static int _send_data2(const char *data, int send_jobid ,int send_stepid, RPType type)
{
	CURL *curl_handle = NULL;
	CURLcode res;
	struct http_response chunk;
	int rc = SLURM_SUCCESS;
	long response_code;
	static int error_cnt = 0;
	char *url = NULL;
	//size_t length;
	char *rt_policy = NULL;
	char *tmp_datastr = (type == EVENTRP) ? event_datastr : stepd_datastr;

	debug3("%s %s called", plugin_type, __func__);

	/*
	 * Every compute node which is sampling data will try to establish a
	 * different connection to the influxdb server. The data will not be 
	 * cached and will be sent in real time at the head node of the job.
	 */
	if(send_jobid !=0)
		xstrcat(tmp_datastr, data);
	DEF_TIMERS;
	START_TIMER;
	if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
		error("%s %s: curl_global_init: %m", plugin_type, __func__);
		rc = SLURM_ERROR;
		goto cleanup_global_init;
	} else if ((curl_handle = curl_easy_init()) == NULL) {
		error("%s %s: curl_easy_init: %m", plugin_type, __func__);
		rc = SLURM_ERROR;
		goto cleanup_easy_init;
	}
	rt_policy = _parse_rt_policy(influxdb_conf.rt_policy, type);
	xstrfmtcat(url, "%s/write?db=%s&rp=%s&precision=s", influxdb_conf.host,
		   influxdb_conf.database, rt_policy);
	if(rt_policy) xfree(rt_policy);


	chunk.message = xmalloc(1);
	chunk.size = 0;

	curl_easy_setopt(curl_handle, CURLOPT_URL, url);
	if (influxdb_conf.password)
		curl_easy_setopt(curl_handle, CURLOPT_PASSWORD,
				 influxdb_conf.password);

	curl_easy_setopt(curl_handle, CURLOPT_POST, 1);
	curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, tmp_datastr);
	curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDSIZE, strlen(tmp_datastr));
	if (influxdb_conf.username)
		curl_easy_setopt(curl_handle, CURLOPT_USERNAME,
				 influxdb_conf.username);
	curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, _write_callback);
	curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *) &chunk);

	if ((res = curl_easy_perform(curl_handle)) != CURLE_OK) {
		if ((error_cnt++ % 100) == 0)
			error("%s %s: curl_easy_perform failed to send data (discarded). Reason: %s",
			      plugin_type, __func__, curl_easy_strerror(res));
		rc = SLURM_ERROR;
		goto cleanup;
	}

	if ((res = curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE,
				     &response_code)) != CURLE_OK) {
		error("%s %s: curl_easy_getinfo response code failed: %s",
		      plugin_type, __func__, curl_easy_strerror(res));
		rc = SLURM_ERROR;
		goto cleanup;
	}

	/* In general, status codes of the form 2xx indicate success,
	 * 4xx indicate that InfluxDB could not understand the request, and
	 * 5xx indicate that the system is overloaded or significantly impaired.
	 * Errors are returned in JSON.
	 * https://docs.influxdata.com/influxdb/v0.13/concepts/api/
	 */
	if (response_code >= 200 && response_code <= 205) {
		debug2("%s %s: data write success", plugin_type, __func__);
		if (error_cnt > 0)
			error_cnt = 0;
	} else {
		rc = SLURM_ERROR;
		debug2("%s %s: data write failed, response code: %ld",
		       plugin_type, __func__, response_code);
		if (slurm_conf.debug_flags & DEBUG_FLAG_PROFILE) {
			/* Strip any trailing newlines. */
			while (chunk.message[strlen(chunk.message) - 1] == '\n')
				chunk.message[strlen(chunk.message) - 1] = '\0';
			info("%s %s: JSON response body: %s", plugin_type,
			     __func__, chunk.message);
		}
	}

cleanup:
	xfree(chunk.message);
	xfree(url);
cleanup_easy_init:
	curl_easy_cleanup(curl_handle);
cleanup_global_init:
	curl_global_cleanup();
	END_TIMER;
	log_flag(PROFILE, "%s %s: took %s to send data ",
		 plugin_type, __func__, TIME_STR);
	struct stat st_tmp;
	bool influx_dir = true;
	if((rc == SLURM_ERROR) && (send_jobid > 0)) {

		if(influxdb_conf.workdir == NULL) {
			char tmp_dir[60]="/tmp/slurm_influxdb";
			if (stat(tmp_dir, &st_tmp) == -1) {
				if(mkdir(tmp_dir, 0700)==-1) {
					error("can't create directory /tmp/slurm_influxdb");
				}
			}
			influxdb_conf.workdir = xstrdup(tmp_dir);
		} else if(xstrcasecmp(influxdb_conf.workdir, "None") == 0) {
			influx_dir = false;
		}
	}


	if((send_jobid > 0) && (rc == SLURM_ERROR) && influx_dir) {
		char *influxdb_file = NULL;
		FILE *sys_file = NULL;
		xstrfmtcat(influxdb_file,"%s/job%d.%d.%s", influxdb_conf.workdir, send_jobid, send_stepid, (type == EVENTRP) ? "event" : "stepd");
		struct stat st;
		if(type == EVENTRP && buffer_file_event == NULL){
			buffer_file_event = xstrdup(influxdb_file);
		}else if(type == STEPDRP && buffer_file_stepd == NULL) {
			buffer_file_stepd = xstrdup(influxdb_file);
		}
		
		if (stat(influxdb_conf.workdir, &st) == -1) {
 			if(mkdir(influxdb_conf.workdir, 0700)==-1) {
				error("can't create directory influxdb_conf.workdir(%s)", influxdb_file);
			}
		}

		//slurm_mutex_lock(&file_lock);
		sys_file = fopen(influxdb_file, "a+");
		if (sys_file != NULL) {
			fprintf(sys_file, tmp_datastr);
			fclose(sys_file);
		} else {
			debug("Failed to write %s file. The file content is %s",influxdb_file ,tmp_datastr);
		}
			
		//slurm_mutex_unlock(&file_lock);

		if(influxdb_file)
			xfree(influxdb_file);
	}
	if (data) {
		tmp_datastr[0] = '\0';
	}
	return rc;	
}
#endif

#ifdef __METASTACK_LOAD_ABNORMAL
/*Get the total number of lines in a file*/
static int count_file_row(char *path)
{    
 int count = 0;
    char c;
    FILE *file;
    file = fopen(path, "r");
    if (file == NULL) {
        debug("Error opening file!");
    } else {
		while ((c = getc(file)) != EOF) {
			if (c == '\n') {
				count++;
			}
		}

	}
	return count;
    fclose(file);
}
#endif

/*At the end of the job, 
 *check whether the influxdb cache file is generated,and if so, 
 *try to send it again.*/
#if defined(__METASTACK_LOAD_ABNORMAL) && defined(__METASTACK_JOB_USELESS_RUNNING_WARNING)
static int _last_resend(const char *data, RPType type)
{
	struct stat st;
    int rc = SLURM_SUCCESS;
	int rc2 = SLURM_SUCCESS;
	bool send_buffer = false;
	char tmp_str[256] = {'0'};
	int all_row = 0;
	char *tmp_datastr = (type == EVENTRP) ? event_datastr : stepd_datastr;
	char *buffer_file = (type == EVENTRP) ? buffer_file_event : buffer_file_stepd; /* There is no need to specifically free the variable */
	char *tmp_copy = NULL;  /* send_data2 clears the data if it fails to send, so it needs to be saved before it is sent */
	bool send_flag = false; /* Used to mark whether to save failed data to temporary file */
  
	if(data || (!buffer_file) || (strlen(buffer_file) <= 0)) {
		return rc;
	}

	if((buffer_file != NULL) && (stat(buffer_file, &st) != -1)) {
       
	    all_row = count_file_row(buffer_file);
		if(all_row <= 0) {
			remove(buffer_file);
			return rc;
		}
			
		FILE *fp = NULL;
		FILE *fp2 = NULL;
		char *path_tmp = NULL;
		/* Temporary files are used to hold the data in the cache files that failed to be sent */
	    xstrfmtcat(path_tmp, "%s.tmp", buffer_file);
		fp = fopen(buffer_file, "r");
		if(fp == NULL) {
			rc = SLURM_ERROR;
			error("open %s failed!", buffer_file);
			if(path_tmp) xfree(path_tmp);
			return rc;
		}
		fp2 = fopen(path_tmp, "w+");
		if(fp2 == NULL) {
			/* Failure to create a temporary file does not affect the overall functionality of the function */
			rc = SLURM_ERROR;
			debug("open temp file failed : %s", path_tmp);
		}

		int line = 0;
		tmp_datastr[0] = '\0';
		int tmp_datastr_len = 0;

		while (fgets(tmp_str, 256, fp) != NULL)  {
			line++;
			/*
				If the cache file is not full, the next line of data is read, otherwise the data is sent, 
				and the data is backed up before sending, so that the data that fails to be sent can be 
				saved to the path_tmp file
			*/
			if((tmp_datastr_len + strlen(tmp_str)) <= BUF_SIZE) {
				xstrcat(tmp_datastr, tmp_str);
				tmp_datastr_len += strlen(tmp_str);
				send_buffer = true;
			} else {
				xfree(tmp_copy);
				tmp_copy = xstrdup(tmp_datastr);
				rc = _send_data2(tmp_datastr, 0, 0, type);
				if(rc != SLURM_SUCCESS && fp2 != NULL){
					fputs(tmp_copy, fp2);
					send_flag = true;
				}
				xstrcat(tmp_datastr, tmp_str);
				tmp_datastr_len = strlen(tmp_datastr);
			}
		}
		if(send_buffer == true) {
			xfree(tmp_copy);
			tmp_copy = xstrdup(tmp_datastr);
			rc = _send_data2(tmp_datastr, 0, 0, type);
			if(rc != SLURM_SUCCESS && fp2 != NULL){
				fputs(tmp_copy, fp2);
				send_flag = true;
			}
		}

		/* 
			1.If send_flag = true indicates that data transmission failed, delete the buffer_file and rename it
			2.If send_flag = false means no data transmission failed, only the buffer_file is deleted
		*/

		if(!send_flag && (rc == SLURM_SUCCESS) && (line >= all_row)) { 
			remove(buffer_file);
			remove(path_tmp);
		}else{
			remove(buffer_file);
			rename(path_tmp, buffer_file);
		}
		
		fclose(fp);
		fclose(fp2);
		xfree(tmp_copy);
		xfree(path_tmp);
	}

	if(((rc == SLURM_ERROR) && (rc2 == SLURM_ERROR)) || send_flag) {
		debug("Resend failed, file saved in %s",buffer_file);
	}
	return rc;

}
#endif

/* Try to send data to influxdb */
#if defined(__METASTACK_LOAD_ABNORMAL) && defined(__METASTACK_JOB_USELESS_RUNNING_WARNING)
static int _send_data(const char *data, RPType type)
#endif
{
	CURL *curl_handle = NULL;
	CURLcode res;
	struct http_response chunk;
	int rc = SLURM_SUCCESS;
	long response_code;
	static int error_cnt = 0;
	char *url = NULL;
	size_t length;
#ifdef __METASTACK_JOB_USELESS_RUNNING_WARNING
	char* rt_policy = NULL;
#endif

	debug3("%s %s called", plugin_type, __func__);

	/*
	 * Every compute node which is sampling data will try to establish a
	 * different connection to the influxdb server. In order to reduce the
	 * number of connections, every time a new sampled data comes in, it
	 * is saved in the 'datastr' buffer. Once this buffer is full, then we
	 * try to open the connection and send this buffer, instead of opening
	 * one per sample.
	 */
	if (data && ((datastrlen + strlen(data)) <= BUF_SIZE)) {
		xstrcat(datastr, data);
		length = strlen(data);
		datastrlen += length;
		log_flag(PROFILE, "%s %s: %zu bytes of data added to buffer. New buffer size: %d",
			 plugin_type, __func__, length, datastrlen);
		return rc;
	}

	DEF_TIMERS;
	START_TIMER;

	if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
		error("%s %s: curl_global_init: %m", plugin_type, __func__);
		rc = SLURM_ERROR;
		goto cleanup_global_init;
	} else if ((curl_handle = curl_easy_init()) == NULL) {
		error("%s %s: curl_easy_init: %m", plugin_type, __func__);
		rc = SLURM_ERROR;
		goto cleanup_easy_init;
	}
#ifdef __METASTACK_JOB_USELESS_RUNNING_WARNING
	rt_policy = _parse_rt_policy(influxdb_conf.rt_policy, type);
	xstrfmtcat(url, "%s/write?db=%s&rp=%s&precision=s", influxdb_conf.host,
		   influxdb_conf.database, rt_policy);
	xfree(rt_policy);
#endif

	chunk.message = xmalloc(1);
	chunk.size = 0;

	curl_easy_setopt(curl_handle, CURLOPT_URL, url);
	if (influxdb_conf.password)
		curl_easy_setopt(curl_handle, CURLOPT_PASSWORD,
				 influxdb_conf.password);
	curl_easy_setopt(curl_handle, CURLOPT_POST, 1);
	curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, datastr);
	curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDSIZE, strlen(datastr));
	if (influxdb_conf.username)
		curl_easy_setopt(curl_handle, CURLOPT_USERNAME,
				 influxdb_conf.username);
	curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, _write_callback);
	curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *) &chunk);

	if ((res = curl_easy_perform(curl_handle)) != CURLE_OK) {
		if ((error_cnt++ % 100) == 0)
			error("%s %s: curl_easy_perform failed to send data (discarded). Reason: %s",
			      plugin_type, __func__, curl_easy_strerror(res));
		rc = SLURM_ERROR;
		goto cleanup;
	}

	if ((res = curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE,
				     &response_code)) != CURLE_OK) {
		error("%s %s: curl_easy_getinfo response code failed: %s",
		      plugin_type, __func__, curl_easy_strerror(res));
		rc = SLURM_ERROR;
		goto cleanup;
	}

	/* In general, status codes of the form 2xx indicate success,
	 * 4xx indicate that InfluxDB could not understand the request, and
	 * 5xx indicate that the system is overloaded or significantly impaired.
	 * Errors are returned in JSON.
	 * https://docs.influxdata.com/influxdb/v0.13/concepts/api/
	 */
	if (response_code >= 200 && response_code <= 205) {
		debug2("%s %s: data write success", plugin_type, __func__);
		if (error_cnt > 0)
			error_cnt = 0;
	} else {
		rc = SLURM_ERROR;
		debug2("%s %s: data write failed, response code: %ld",
		       plugin_type, __func__, response_code);
		if (slurm_conf.debug_flags & DEBUG_FLAG_PROFILE) {
			/* Strip any trailing newlines. */
			while (chunk.message[strlen(chunk.message) - 1] == '\n')
				chunk.message[strlen(chunk.message) - 1] = '\0';
			info("%s %s: JSON response body: %s", plugin_type,
			     __func__, chunk.message);
		}
	}

cleanup:
	xfree(chunk.message);
	xfree(url);
cleanup_easy_init:
	curl_easy_cleanup(curl_handle);
cleanup_global_init:
	curl_global_cleanup();

	END_TIMER;
	log_flag(PROFILE, "%s %s: took %s to send data",
		 plugin_type, __func__, TIME_STR);

	if (data) {
#ifdef __METASTACK_LOAD_ABNORMAL
		xfree(datastr);
#endif
		datastr = xstrdup(data);
		datastrlen = strlen(data);
	} else {
		datastr[0] = '\0';
		datastrlen = 0;
	}

	return rc;
}

/*
 * init() is called when the plugin is loaded, before any other functions
 * are called. Put global initialization here.
 */
extern int init(void)
{
	debug3("%s %s called", plugin_type, __func__);

	if (!running_in_slurmstepd())
		return SLURM_SUCCESS;

	datastr = xmalloc(BUF_SIZE);
#ifdef __METASTACK_LOAD_ABNORMAL
	stepd_datastr = xmalloc(BUF_SIZE);
	event_datastr = xmalloc(BUF_SIZE);
#endif
#ifdef __METASTACK_JOB_USELESS_RUNNING_WARNING
	influxdb_conf.series_reduce = NULL;
	influxdb_conf.workdir = NULL;
#endif
	return SLURM_SUCCESS;
}

extern int fini(void)
{
	debug3("%s %s called", plugin_type, __func__);

	_free_tables();
	xfree(datastr);
	xfree(influxdb_conf.host);
	xfree(influxdb_conf.database);
	xfree(influxdb_conf.password);
	xfree(influxdb_conf.rt_policy);
	xfree(influxdb_conf.username);
#if defined(__METASTACK_LOAD_ABNORMAL) && defined(__METASTACK_JOB_USELESS_RUNNING_WARNING)
	xfree(influxdb_conf.workdir);
	xfree(influxdb_conf.series_reduce);
	xfree(stepd_datastr);
	xfree(event_datastr);
	xfree(buffer_file_stepd);
	xfree(buffer_file_event);
#endif
	return SLURM_SUCCESS;
}

extern void acct_gather_profile_p_conf_options(s_p_options_t **full_options,
					       int *full_options_cnt)
{
	debug3("%s %s called", plugin_type, __func__);

	s_p_options_t options[] = {
		{"ProfileInfluxDBHost", S_P_STRING},
		{"ProfileInfluxDBDatabase", S_P_STRING},
		{"ProfileInfluxDBDefault", S_P_STRING},
		{"ProfileInfluxDBPass", S_P_STRING},
		{"ProfileInfluxDBRTPolicy", S_P_STRING},
		{"ProfileInfluxDBUser", S_P_STRING},
#ifdef __METASTACK_LOAD_ABNORMAL
		{"ProfileInfluxDBWorkdir", S_P_STRING},
#endif	
#ifdef __METASTACK_JOB_USELESS_RUNNING_WARNING
		{"ProfileInfluxDBSeriesReduce", S_P_STRING},
#endif
		{NULL} };

	transfer_s_p_options(full_options, options, full_options_cnt);
	return;
}

extern void acct_gather_profile_p_conf_set(s_p_hashtbl_t *tbl)
{
	char *tmp = NULL;

	debug3("%s %s called", plugin_type, __func__);

	influxdb_conf.def = ACCT_GATHER_PROFILE_ALL;
	if (tbl) {
		s_p_get_string(&influxdb_conf.host, "ProfileInfluxDBHost", tbl);
		if (s_p_get_string(&tmp, "ProfileInfluxDBDefault", tbl)) {
			influxdb_conf.def =
				acct_gather_profile_from_string(tmp);
			if (influxdb_conf.def == ACCT_GATHER_PROFILE_NOT_SET)
				fatal("ProfileInfluxDBDefault can not be set to %s, please specify a valid option",
				      tmp);
			xfree(tmp);
		}
		s_p_get_string(&influxdb_conf.database,
			       "ProfileInfluxDBDatabase", tbl);
		s_p_get_string(&influxdb_conf.password,
			       "ProfileInfluxDBPass", tbl);
		s_p_get_string(&influxdb_conf.rt_policy,
			       "ProfileInfluxDBRTPolicy", tbl);
		s_p_get_string(&influxdb_conf.username,
			       "ProfileInfluxDBUser", tbl);
#ifdef __METASTACK_LOAD_ABNORMAL
		s_p_get_string(&influxdb_conf.workdir,
			       "ProfileInfluxDBWorkdir", tbl);
#endif
#ifdef __METASTACK_JOB_USELESS_RUNNING_WARNING
		s_p_get_string(&influxdb_conf.series_reduce,
			       "ProfileInfluxDBSeriesReduce", tbl);
#endif
	}

	if (!influxdb_conf.host)
		fatal("No ProfileInfluxDBHost in your acct_gather.conf file. This is required to use the %s plugin",
		      plugin_type);

	if (!influxdb_conf.database)
		fatal("No ProfileInfluxDBDatabase in your acct_gather.conf file. This is required to use the %s plugin",
		      plugin_type);

	if (influxdb_conf.password && !influxdb_conf.username)
		fatal("No ProfileInfluxDBUser in your acct_gather.conf file. This is required if ProfileInfluxDBPass is specified to use the %s plugin",
		      plugin_type);

	if (!influxdb_conf.rt_policy)
		fatal("No ProfileInfluxDBRTPolicy in your acct_gather.conf file. This is required to use the %s plugin",
		      plugin_type);

	debug("%s loaded", plugin_name);
}

extern void acct_gather_profile_p_get(enum acct_gather_profile_info info_type,
				      void *data)
{
	uint32_t *uint32 = (uint32_t *) data;
	char **tmp_char = (char **) data;

	debug3("%s %s called", plugin_type, __func__);

	switch (info_type) {
	case ACCT_GATHER_PROFILE_DIR:
		*tmp_char = xstrdup(influxdb_conf.host);
		break;
	case ACCT_GATHER_PROFILE_DEFAULT:
		*uint32 = influxdb_conf.def;
		break;
	case ACCT_GATHER_PROFILE_RUNNING:
		*uint32 = g_profile_running;
		break;
	default:
		debug2("%s %s: info_type %d invalid", plugin_type,
		       __func__, info_type);
	}
}

extern int acct_gather_profile_p_node_step_start(stepd_step_rec_t* job)
{
	int rc = SLURM_SUCCESS;
	char *profile_str;

	debug3("%s %s called", plugin_type, __func__);

	xassert(running_in_slurmstepd());

	g_job = job;
	profile_str = acct_gather_profile_to_string(g_job->profile);
	debug2("%s %s: option --profile=%s", plugin_type, __func__,
	       profile_str);
	g_profile_running = _determine_profile();
	return rc;
}

extern int acct_gather_profile_p_child_forked(void)
{
	debug3("%s %s called", plugin_type, __func__);
	return SLURM_SUCCESS;
}

extern int acct_gather_profile_p_node_step_end(void)
{
	int rc = SLURM_SUCCESS;
	debug3("%s %s called", plugin_type, __func__);

	xassert(running_in_slurmstepd());

	return rc;
}

extern int acct_gather_profile_p_task_start(uint32_t taskid)
{
	int rc = SLURM_SUCCESS;

	debug3("%s %s called with %d prof", plugin_type, __func__,
	       g_profile_running);

	xassert(running_in_slurmstepd());
	xassert(g_job);

	xassert(g_profile_running != ACCT_GATHER_PROFILE_NOT_SET);

	if (g_profile_running <= ACCT_GATHER_PROFILE_NONE)
		return rc;

	return rc;
}

extern int acct_gather_profile_p_task_end(pid_t taskpid)
{
	debug3("%s %s called", plugin_type, __func__);
#if defined(__METASTACK_LOAD_ABNORMAL) && defined(__METASTACK_JOB_USELESS_RUNNING_WARNING)
	_last_resend(NULL, EVENTRP);
	_last_resend(NULL, STEPDRP);
	_send_data(NULL, NATIVERP);
#endif
	return SLURM_SUCCESS;
}

extern int64_t acct_gather_profile_p_create_group(const char* name)
{
	debug3("%s %s called", plugin_type, __func__);

	return 0;
}

extern int acct_gather_profile_p_create_dataset(const char* name,
						int64_t parent,
						acct_gather_profile_dataset_t
						*dataset)
{
	table_t * table;
	acct_gather_profile_dataset_t *dataset_loc = dataset;

	debug3("%s %s called", plugin_type, __func__);

	if (g_profile_running <= ACCT_GATHER_PROFILE_NONE)
		return SLURM_ERROR;

	/* compute the size of the type needed to create the table */
	if (tables_cur_len == tables_max_len) {
		if (tables_max_len == 0)
			++tables_max_len;
		tables_max_len *= 2;
		tables = xrealloc(tables, tables_max_len * sizeof(table_t));
	}

	table = &(tables[tables_cur_len]);
	table->name = xstrdup(name);
	table->size = 0;

	while (dataset_loc && (dataset_loc->type != PROFILE_FIELD_NOT_SET)) {
		table->names = xrealloc(table->names,
					(table->size+1) * sizeof(char *));
		table->types = xrealloc(table->types,
					(table->size+1) * sizeof(char *));
		(table->names)[table->size] = xstrdup(dataset_loc->name);
		switch (dataset_loc->type) {
		case PROFILE_FIELD_UINT64:
			table->types[table->size] =
				PROFILE_FIELD_UINT64;
			break;
		case PROFILE_FIELD_DOUBLE:
			table->types[table->size] =
				PROFILE_FIELD_DOUBLE;
			break;
		case PROFILE_FIELD_NOT_SET:
			break;
		}
		table->size++;
		dataset_loc++;
	}
	++tables_cur_len;
	return tables_cur_len - 1;
}

extern int acct_gather_profile_p_add_sample_data(int table_id, void *data,
						 time_t sample_time)
{
	table_t *table = &tables[table_id];
	int i = 0;
	char *str = NULL;
#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE
	enum {
		FIELD_CPUFREQ,
		FIELD_CPUTIME,
		FIELD_CPUUTIL,
		FIELD_RSS,
		FIELD_VMSIZE,
		FIELD_PAGES,
		FIELD_READ,
		FIELD_WRITE,
		FIELD_CNT
	};
	struct data_pack{
			void *data;
			List process;
	};
	struct data_pack * pdata = (struct data_pack*)data;
#endif
	debug3("%s %s called", plugin_type, __func__);

	for(; i < table->size; i++) {
		switch (table->types[i]) {
		case PROFILE_FIELD_UINT64:
#if defined(__METASTACK_OPT_INFLUXDB_ENFORCE) && defined(__METASTACK_JOB_USELESS_RUNNING_WARNING)
			if(xstrncasecmp(influxdb_conf.series_reduce, "yes", 3) == 0){
				xstrfmtcat(str, "%s,host=%s,username=%s"
					" job=%d,step=%d,task=%s,value=%"PRIu64" "
					"%"PRIu64"\n", table->names[i], g_job->node_name, g_job->user_name,
					g_job->step_id.job_id, g_job->step_id.step_id,
					table->name, 
					((union data_t*)(pdata->data))[i].u,
					(uint64_t)sample_time);
			}else{
				xstrfmtcat(str, "%s,job=%d,step=%d,task=%s,"
					"host=%s,username=%s value=%"PRIu64" "
					"%"PRIu64"\n", table->names[i],
					g_job->step_id.job_id, g_job->step_id.step_id,
					table->name, g_job->node_name, g_job->user_name,
					((union data_t*)(pdata->data))[i].u,
					(uint64_t)sample_time);
			}
#endif
			break;
		case PROFILE_FIELD_DOUBLE:
#if defined(__METASTACK_OPT_INFLUXDB_ENFORCE) && defined(__METASTACK_JOB_USELESS_RUNNING_WARNING)
			if(xstrncasecmp(influxdb_conf.series_reduce, "yes", 3) == 0){
				xstrfmtcat(str, "%s,host=%s,username=%s "
				   " job=%d,step=%d,task=%s,value=%.2f %"PRIu64""
				   "\n", table->names[i],g_job->node_name, g_job->user_name,
				   g_job->step_id.job_id, g_job->step_id.step_id,
				   table->name, 
				   ((union data_t*)(pdata->data))[i].d,
				   (uint64_t)sample_time);
			}else{
				xstrfmtcat(str, "%s,job=%d,step=%d,task=%s,"
				   "host=%s,username=%s value=%.2f %"PRIu64""
				   "\n", table->names[i],
				   g_job->step_id.job_id, g_job->step_id.step_id,
				   table->name, g_job->node_name, g_job->user_name,
				   ((union data_t*)(pdata->data))[i].d,
				   (uint64_t)sample_time);
			}
#endif
			break;
		case PROFILE_FIELD_NOT_SET:
			break;
		}
	}
#if defined(__METASTACK_OPT_INFLUXDB_ENFORCE) && defined(__METASTACK_JOB_USELESS_RUNNING_WARNING)
	if(pdata->process != NULL  ) {
		ListIterator itr = list_iterator_create(pdata->process);
		jag_prec_t *prec;
		while((prec = list_next(itr))) {
			if(xstrncasecmp(influxdb_conf.series_reduce, "yes", 3) == 0){
				xstrfmtcat(str, "Command,host=%s,username=%s "
					"job=%d,step=%d,task=%s,pid=%d,ppid=%d,command=\"%s\","
					"rss=%"PRIu64",vmsize=%"PRIu64",value=%.2f %"PRIu64"\n",
					g_job->node_name,g_job->user_name,
					g_job->step_id.job_id, g_job->step_id.step_id,
					table->name,prec->pid, prec->ppid, prec->command, 
					((union data_t*)(pdata->data))[FIELD_RSS].u,
					((union data_t*)(pdata->data))[FIELD_VMSIZE].u,prec->cpu_util,
					(uint64_t)sample_time);
			}else{
				xstrfmtcat(str, "Command,job=%d,step=%d,username=%s,task=%s,"
					"host=%s pid=%d,ppid=%d,command=\"%s\","
					"rss=%"PRIu64",vmsize=%"PRIu64",value=%.2f %"PRIu64"\n",
					g_job->step_id.job_id, g_job->step_id.step_id,g_job->user_name,
					table->name, g_job->node_name,
					prec->pid, prec->ppid, prec->command,
					((union data_t*)(pdata->data))[FIELD_RSS].u,
					((union data_t*)(pdata->data))[FIELD_VMSIZE].u,prec->cpu_util,
					(uint64_t)sample_time);
			}
            
		}
		list_iterator_destroy(itr);
	}
	_send_data(str, NATIVERP);
#endif
	xfree(str);

	return SLURM_SUCCESS;
}

#ifdef __METASTACK_LOAD_ABNORMAL
extern int acct_gather_profile_p_add_sample_data_stepd(int dataset_id, void* data,
						 time_t sample_time)
{
    char *str = NULL;
	char *str1 = NULL;

	uint64_t event1 = 0x0000000000000001;
	uint64_t event2 = 0x0000000000000010;
	uint64_t event3 = 0x0000000000000100;

	enum {
		/*PROFILE*/
		FIELD_STEPCPU,
		FIELD_STEPCPUAVE,
		FIELD_STEPMEM,	
		FIELD_STEPVMEM,		
		FIELD_STEPPAGES,
#ifdef __METASTACK_JOB_USELESS_RUNNING_WARNING
		FIELD_TIMER,
#endif
		/*EVENT*/
		FIELD_FLAG,
		FIELD_CPUTHRESHOLD,
		FIELD_EVENTTYPE1,
		FIELD_EVENTTYPE2,
		FIELD_EVENTTYPE3,
		FIELD_EVENTTYPE1START,
		FIELD_EVENTTYPE2START,
		FIELD_EVENTTYPE3START,
		FIELD_EVENTTYPE1END,
		FIELD_EVENTTYPE2END,
		FIELD_EVENTTYPE3END,					
		FIELD_CNT
	};
	
	debug3("%s %s called", plugin_type, __func__);
	for(int i= 1; i <= 2; i++) {
		enum {
			SLUR_SEND_STEPD_TYPE = 1,
			SLUR_SEND_EVENT_TYPE = 2,
		};
		switch (i) {
			case SLUR_SEND_STEPD_TYPE:
				xstrfmtcat(str1,"Stepd,username=%s,jobid=%d,step=%d stepcpu=%.2f,"
#ifdef __METASTACK_JOB_USELESS_RUNNING_WARNING
				"stepcpuave=%.2f,stepmem=%.2f,stepvmem=%.2f,interval_time=%"PRIu64","
#endif
				"steppages=%"PRIu64" %"PRIu64"\n", 
				g_job->user_name,
				g_job->step_id.job_id, 
				g_job->step_id.step_id,
				((union data_t*)data)[FIELD_STEPCPU].d,
				((union data_t*)data)[FIELD_STEPCPUAVE].d,
				((union data_t*)data)[FIELD_STEPMEM].d,
				((union data_t*)data)[FIELD_STEPVMEM].d,
				((union data_t*)data)[FIELD_TIMER].u,
				((union data_t*)data)[FIELD_STEPPAGES].u,
				(uint64_t)sample_time);
#ifdef __METASTACK_JOB_USELESS_RUNNING_WARNING
				_send_data2(str1, g_job->step_id.job_id, g_job->step_id.step_id, STEPDRP);
#endif
				if(str1)
					xfree(str1);
				break;
			case SLUR_SEND_EVENT_TYPE:
#ifdef __METASTACK_JOB_USELESS_RUNNING_WARNING
				if(((union data_t*)data)[FIELD_FLAG].u == 0) break;	
				xstrfmtcat(str,"Event,username=%s,jobid=%d,step=%d,type1=%d,type2=%d,type3=%d "
				"cputhreshold=%.2f,stepcpu=%.2f,stepmem=%.2f,"
				"stepvmem=%.2f,steppages=%"PRIu64",start=%"PRIu64",end=%"PRIu64" %"PRIu64"\n",
				g_job->user_name,
				g_job->step_id.job_id, 
				g_job->step_id.step_id,
				((((union data_t*)data)[FIELD_FLAG].u & event1) ? 1 : 0),
				((((union data_t*)data)[FIELD_FLAG].u & event2) ? 1 : 0),
				((((union data_t*)data)[FIELD_FLAG].u & event3) ? 1 : 0),
				((union data_t*)data)[FIELD_CPUTHRESHOLD].d,
				((union data_t*)data)[FIELD_STEPCPU].d,
				((union data_t*)data)[FIELD_STEPMEM].d,
				((union data_t*)data)[FIELD_STEPVMEM].d,
				((union data_t*)data)[FIELD_STEPPAGES].u,
				((union data_t*)data)[FIELD_EVENTTYPE1START].u,
				((union data_t*)data)[FIELD_EVENTTYPE1END].u,
				(uint64_t)sample_time);
				_send_data2(str, g_job->step_id.job_id, g_job->step_id.step_id, EVENTRP);
#endif
				if(str)
					xfree(str);
				break;
		}

	}

	return SLURM_SUCCESS;
}
#endif

extern void acct_gather_profile_p_conf_values(List *data)
{
	config_key_pair_t *key_pair;

	debug3("%s %s called", plugin_type, __func__);

	xassert(*data);

	key_pair = xmalloc(sizeof(config_key_pair_t));
	key_pair->name = xstrdup("ProfileInfluxDBHost");
	key_pair->value = xstrdup(influxdb_conf.host);
	list_append(*data, key_pair);

	key_pair = xmalloc(sizeof(config_key_pair_t));
	key_pair->name = xstrdup("ProfileInfluxDBDatabase");
	key_pair->value = xstrdup(influxdb_conf.database);
	list_append(*data, key_pair);

	key_pair = xmalloc(sizeof(config_key_pair_t));
	key_pair->name = xstrdup("ProfileInfluxDBDefault");
	key_pair->value =
		xstrdup(acct_gather_profile_to_string(influxdb_conf.def));
	list_append(*data, key_pair);

	key_pair = xmalloc(sizeof(config_key_pair_t));
	key_pair->name = xstrdup("ProfileInfluxDBPass");
	key_pair->value = xstrdup(influxdb_conf.password);
	list_append(*data, key_pair);

	key_pair = xmalloc(sizeof(config_key_pair_t));
	key_pair->name = xstrdup("ProfileInfluxDBRTPolicy");
	key_pair->value = xstrdup(influxdb_conf.rt_policy);
	list_append(*data, key_pair);

	key_pair = xmalloc(sizeof(config_key_pair_t));
	key_pair->name = xstrdup("ProfileInfluxDBUser");
	key_pair->value = xstrdup(influxdb_conf.username);
	list_append(*data, key_pair);

#ifdef __METASTACK_LOAD_ABNORMAL
	key_pair = xmalloc(sizeof(config_key_pair_t));
	key_pair->name = xstrdup("ProfileInfluxDBWorkdir");
	key_pair->value = xstrdup(influxdb_conf.workdir);
	list_append(*data, key_pair);
#endif
#ifdef __METASTACK_JOB_USELESS_RUNNING_WARNING
	key_pair = xmalloc(sizeof(config_key_pair_t));
	key_pair->name = xstrdup("ProfileInfluxDBSeriesReduce");
	key_pair->value = xstrdup(influxdb_conf.series_reduce);
	list_append(*data, key_pair);
#endif
	return;

}

extern bool acct_gather_profile_p_is_active(uint32_t type)
{
	debug3("%s %s called", plugin_type, __func__);

	if (g_profile_running <= ACCT_GATHER_PROFILE_NONE)
		return false;

	return (type == ACCT_GATHER_PROFILE_NOT_SET) ||
		(g_profile_running & type);
}
