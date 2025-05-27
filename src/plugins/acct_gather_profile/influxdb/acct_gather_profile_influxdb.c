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
#ifdef __METASTACK_OPT_INFLUXDB_PERFORMANCE
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
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	char *str;
#endif
};

#ifdef __METASTACK_OPT_INFLUXDB_PERFORMANCE
typedef enum {
    EVENT_CPU = 0,
    EVENT_PROCESS,
    EVENT_NODE,
    EVENT_COUNT 
} EventType;

typedef struct {
    uint64_t flag;
    const char* name;
} EventConfig;

static const EventConfig EVENT_CONFIGS[] = {
    {0x0000000000000001, "cpu"},
    {0x0000000000000010, "process"},
    {0x0000000000000100, "node"}
};
#endif

static slurm_influxdb_conf_t influxdb_conf;
static uint32_t g_profile_running = ACCT_GATHER_PROFILE_NOT_SET;
static stepd_step_rec_t *g_job = NULL;

static char *datastr = NULL;
static int datastrlen = 0;

static table_t *tables = NULL;
static size_t tables_max_len = 0;
static size_t tables_cur_len = 0;

#ifdef __METASTACK_OPT_INFLUXDB_PERFORMANCE
static char *stepd_datastr = NULL;  /* Save the data to send to the stepd retention policy */
static char *event_datastr = NULL;	/* Save the data to send to the event retention policy */
static char *buffer_file_stepd = NULL;
static char *buffer_file_event = NULL;
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
static char *apptype_datastr = NULL;
static char *buffer_file_apptype = NULL;
#endif
#define MAX_POLICY_NAME_LENGTH 256	/* Set the maximum length of the reservation policy name */
typedef enum {
	NATIVERP,
	STEPDRP,
	EVENTRP,
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	APPTYPERP,
#endif
	RPCNT
} RPType;
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
static const char *RPTypeNames[] = {
	"NATIVERP",
    "STEPDRP",
    "EVENTRP",
    "APPTYPERP",
};
#endif
/**
 * @brief Parses the runtime policy string and extracts the policy value matching the specified type.
 *
 * This function takes a runtime policy string (rt_policy) and a target policy type (type), and parses the comma-separated
 * components of the policy string. If a matching type is found, it returns the associated value. If no matching policy is found,
 * it returns the default value "autogen". If the input rt_policy is NULL or an empty string, it directly returns "autogen".
 *
 * If the rt_policy string contains a standalone value (without any key-value pairs), it is treated as the default return value.
 * If no matching key is found, but a standalone value exists, the function returns that value.
 * If the string does not contain any valid key-value pairs, it is returned as is.
 *
 * @param rt_policy The runtime policy string, consisting of comma-separated key-value pairs (e.g., "type1=value1,type2=value2")
 *                  or a single default value.
 * @param type The target policy type, used to match a specific policy value. This is an enum representing different policy types.
 *
 * @return A string containing the policy value corresponding to the specified type.
 *         - If a matching key is found, returns its associated value.
 *         - If no matching key is found but a standalone default value exists, returns that value.
 *         - If no valid key-value pairs are found, returns the original rt_policy string.
 *         - If the input is NULL or empty, returns "autogen".
 *         - If at least one key is found but the requested type is missing, returns "autogen".
 */
static char* _parse_rt_policy(const char *rt_policy, RPType type) {
    int i = 0;
    if (rt_policy == NULL || rt_policy[0] == '\0') {
        return xstrdup("autogen");
    }

    // If rt_policy does not contain ',' or '=', return it directly
    if (strchr(rt_policy, ',') == NULL && strchr(rt_policy, '=') == NULL) {
        return xstrdup(rt_policy);
    }

    int found_any_keyword = 0;
    char *default_value = NULL; 
    char *policy_copy = xstrdup(rt_policy);
    if (!policy_copy) {
        return xstrdup("autogen");
    }

    char *saveptr = NULL;
    char *token = strtok_r(policy_copy, ",", &saveptr);

    while (token) {
        char *value = xstrchr(token, '=');
        if (value) {
            *value = '\0';  
            value++;        

            for (i = 0; i < RPCNT; i++) {
                if (strcmp(token, RPTypeNames[i]) == 0) {
                    found_any_keyword = 1;
                    if (i == (int)type) {
                        char *result = xstrdup(value);
                        xfree(default_value);
                        xfree(policy_copy);
                        return result;
                    }
                }
            }
        } else {
            // If no '=' is found, treat it as the default value
            xfree(default_value);
            default_value = xstrdup(token);
        }
        token = strtok_r(NULL, ",", &saveptr);
    }

    xfree(policy_copy);

    // Return the default value if no match is found
    if (default_value) {
        return default_value;
    }

    // If a key is found but no match for type, return "autogen"
    if (found_any_keyword) {
        return xstrdup("autogen");
    }

    // If no key-value structure is found, return the original string
    return xstrdup(rt_policy);
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

#if defined(__METASTACK_LOAD_ABNORMAL) && defined(__METASTACK_OPT_INFLUXDB_PERFORMANCE)
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
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	char *tmp_datastr = NULL;
	switch (type) {
		case EVENTRP:
			tmp_datastr = event_datastr;
			break;
		case STEPDRP:
			tmp_datastr = stepd_datastr;
			break;
		case APPTYPERP:
			tmp_datastr = apptype_datastr;
			break;
		default:
			error("Unknown Retention Policy");
			break;
	}
#endif

	debug3("%s %s called", plugin_type, __func__);

	/*
	 * Every compute node which is sampling data will try to establish a
	 * different connection to the influxdb server. The data will not be 
	 * cached and will be sent in real time at the head node of the job.
	 */
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	if (send_jobid != 0 && data)
		xstrcat(tmp_datastr, data);
#endif
	DEF_TIMERS;
	START_TIMER;
	// if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
	// 	error("%s %s: curl_global_init: %m", plugin_type, __func__);
	// 	rc = SLURM_ERROR;
	// 	goto cleanup_global_init;
	// } else 
	if ((curl_handle = curl_easy_init()) == NULL) {
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
// cleanup_global_init:
// 	curl_global_cleanup();
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
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
		switch (type) {
			case EVENTRP:
				xstrfmtcat(influxdb_file,"%s/job%d.%d.%s", influxdb_conf.workdir, send_jobid, send_stepid, "event");
				if (buffer_file_event == NULL) buffer_file_event = xstrdup(influxdb_file);
				break;
			case STEPDRP:
				xstrfmtcat(influxdb_file,"%s/job%d.%d.%s", influxdb_conf.workdir, send_jobid, send_stepid, "stepd");
				if (buffer_file_stepd == NULL) buffer_file_stepd = xstrdup(influxdb_file);
				break;
			case APPTYPERP:
				xstrfmtcat(influxdb_file,"%s/job%d.%d.%s", influxdb_conf.workdir, send_jobid, send_stepid, "apptype");
				if (buffer_file_apptype == NULL) buffer_file_apptype = xstrdup(influxdb_file);
				break;
			default:
				error("Unknown Retention Policy");
				break;
		}
#endif
		struct stat st;
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
#if defined(__METASTACK_LOAD_ABNORMAL) && defined(__METASTACK_OPT_INFLUXDB_PERFORMANCE)
static int _last_resend(const char *data, RPType type)
{
	struct stat st;
    int rc = SLURM_SUCCESS;
	int rc2 = SLURM_SUCCESS;
	bool send_buffer = false;
	char tmp_str[256] = {'0'};
	int all_row = 0;
	char *tmp_copy = NULL;  /* send_data2 clears the data if it fails to send, so it needs to be saved before it is sent */
	bool send_flag = false; /* Used to mark whether to save failed data to temporary file */
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	char *tmp_datastr = NULL;
	char *buffer_file = NULL;
	switch (type) {
		case EVENTRP:
			tmp_datastr = event_datastr;
			buffer_file = buffer_file_event;
			break;
		case STEPDRP:
			tmp_datastr = stepd_datastr;
			buffer_file = buffer_file_stepd;
			break;
		case APPTYPERP:
			apptype_recongn_count = 0;
			tmp_datastr = apptype_datastr;
			/*
				The difference between apptype and event data and stepd data is
					1. apptype always overwrites old data with new data, so each job step will send 
						data to apptype only once
					2. Since the data in apptype_datastr will only be sent once, by the time the 
						last_resend function is executed, the data in apptype_datastr may not have 
						been sent at all.
				As a result of the above differences, the last_resend function requires special handling 
				of the apptype data, which requires a send_data2 pass first and, if the send fails, also 
				saves the data to buffer_file. After send_data2 is executed, it is also necessary to 
				clear the data of apptype_datastr so that the data is not sent more than once.
			*/
			if (tmp_datastr != NULL && tmp_datastr[0] != '\0') {
				_send_data2(NULL, g_job->step_id.job_id, g_job->step_id.step_id, APPTYPERP);
				tmp_datastr[0] = '\0';
			}
			buffer_file = buffer_file_apptype;
			break;
		default:
			error("Retention Policy Count");
			break;
	}
#endif  
	if (data || (!buffer_file) || (strlen(buffer_file) <= 0)) {
		return rc;
	}

	if ((buffer_file != NULL) && (stat(buffer_file, &st) != -1)) {
       
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
		if (fp == NULL) {
			rc = SLURM_ERROR;
			error("open %s failed!", buffer_file);
			if(path_tmp) xfree(path_tmp);
			return rc;
		}
		fp2 = fopen(path_tmp, "w+");
		if (fp2 == NULL) {
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
			if ((tmp_datastr_len + strlen(tmp_str)) <= BUF_SIZE) {
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
		if (send_buffer == true) {
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

		if (!send_flag && (rc == SLURM_SUCCESS) && (line >= all_row)) { 
			remove(buffer_file);
			remove(path_tmp);
		} else {
			remove(buffer_file);
			rename(path_tmp, buffer_file);
		}
		
		fclose(fp);
		fclose(fp2);
		xfree(tmp_copy);
		xfree(path_tmp);
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
		buffer_file[0] = '\0';
#endif
	}

	if(((rc == SLURM_ERROR) && (rc2 == SLURM_ERROR)) || send_flag) {
		debug("Resend failed, file saved in %s",buffer_file);
	}
	return rc;

}
#endif

/* Try to send data to influxdb */
#if defined(__METASTACK_LOAD_ABNORMAL) && defined(__METASTACK_OPT_INFLUXDB_PERFORMANCE)
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
#ifdef __METASTACK_OPT_INFLUXDB_PERFORMANCE
	char* rt_policy = NULL;
#endif

#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
	if(data == NULL && datastr && strlen(datastr) <= 0)
		return rc;
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

#ifdef __METASTACK_NEW_CUSTOM_EXCEPTION
	if ((curl_handle = curl_easy_init()) == NULL) {
		error("%s %s: curl_easy_init: %m", plugin_type, __func__);
		rc = SLURM_ERROR;
		goto cleanup_easy_init;
	}
#else
	if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
		error("%s %s: curl_global_init: %m", plugin_type, __func__);
		rc = SLURM_ERROR;
		goto cleanup_global_init;
	} else if ((curl_handle = curl_easy_init()) == NULL) {
		error("%s %s: curl_easy_init: %m", plugin_type, __func__);
		rc = SLURM_ERROR;
		goto cleanup_easy_init;
	}
#endif

#ifdef __METASTACK_OPT_INFLUXDB_PERFORMANCE
	rt_policy = _parse_rt_policy(influxdb_conf.rt_policy, type);
	/* If open ProfileInfluxDBSeriesReduce, need to increase the time accuracy in order to avoid data overwrite */
	if(xstrncasecmp(influxdb_conf.series_reduce, "yes", 3) == 0) {
		xstrfmtcat(url, "%s/write?db=%s&rp=%s&precision=ns", influxdb_conf.host,
		   influxdb_conf.database, rt_policy);
	} else {
		xstrfmtcat(url, "%s/write?db=%s&rp=%s&precision=s", influxdb_conf.host,
		   influxdb_conf.database, rt_policy);
	}
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
#ifdef 	__METASTACK_LOAD_ABNORMAL
// cleanup_global_init:
// 	curl_global_cleanup();
#endif
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

#ifdef __METASTACK_LOAD_ABNORMAL
	if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
		error("%s %s: curl_global_init: %m", plugin_type, __func__);
		return SLURM_ERROR;
	}
#endif

	datastr = xmalloc(BUF_SIZE);
#ifdef __METASTACK_LOAD_ABNORMAL
	stepd_datastr = xmalloc(BUF_SIZE);
	event_datastr = xmalloc(BUF_SIZE);
#endif
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	apptype_datastr = xmalloc(BUF_SIZE);
#endif
#ifdef __METASTACK_OPT_INFLUXDB_PERFORMANCE
	influxdb_conf.series_reduce = NULL;
	influxdb_conf.workdir = NULL;
#endif
	return SLURM_SUCCESS;
}

extern int fini(void)
{
	debug3("%s %s called", plugin_type, __func__);
#ifdef __METASTACK_LOAD_ABNORMAL
	curl_global_cleanup();
#endif
	_free_tables();
	xfree(datastr);
	xfree(influxdb_conf.host);
	xfree(influxdb_conf.database);
	xfree(influxdb_conf.password);
	xfree(influxdb_conf.rt_policy);
	xfree(influxdb_conf.username);
#if defined(__METASTACK_LOAD_ABNORMAL) && defined(__METASTACK_OPT_INFLUXDB_PERFORMANCE)
	xfree(influxdb_conf.workdir);
	xfree(influxdb_conf.series_reduce);
	xfree(stepd_datastr);
	xfree(event_datastr);
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	xfree(apptype_datastr);
	xfree(buffer_file_apptype);	
#endif
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
#ifdef __METASTACK_OPT_INFLUXDB_PERFORMANCE
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
#ifdef __METASTACK_OPT_INFLUXDB_PERFORMANCE
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
#if defined(__METASTACK_LOAD_ABNORMAL) && defined(__METASTACK_OPT_INFLUXDB_PERFORMANCE)
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	/*
		Starting from 1, skip NATIVERP
	*/
	for (int i = 1 ; i < RPCNT ; i++) {
		_last_resend(NULL, i);
	}
#endif
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
#ifdef __METASTACK_OPT_INFLUXDB_PERFORMANCE
	time_t ct = 0, ct_ns = 0;
	struct timespec now;
#endif
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
#ifdef __METASTACK_OPT_INFLUXDB_PERFORMANCE
	if(xstrncasecmp(influxdb_conf.series_reduce, "yes", 3) == 0){
		clock_gettime(CLOCK_REALTIME, &now);
		ct_ns = now.tv_nsec;
		ct = now.tv_sec;
		sample_time = (uint64_t)(ct * 1000000000 + ct_ns);
	}
#endif
#endif

	debug3("%s %s called", plugin_type, __func__);

	for(; i < table->size; i++) {
		switch (table->types[i]) {
		case PROFILE_FIELD_UINT64:
#if defined(__METASTACK_OPT_INFLUXDB_ENFORCE) && defined(__METASTACK_OPT_INFLUXDB_PERFORMANCE)
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
#if defined(__METASTACK_OPT_INFLUXDB_ENFORCE) && defined(__METASTACK_OPT_INFLUXDB_PERFORMANCE)
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
#if defined(__METASTACK_OPT_INFLUXDB_ENFORCE) && defined(__METASTACK_OPT_INFLUXDB_PERFORMANCE)
	if(pdata->process != NULL  ) {
		ListIterator itr = list_iterator_create(pdata->process);
		jag_prec_t *prec;
		int i = 0;
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
					(uint64_t)sample_time + i);
				i++;
			}else{
				xstrfmtcat(str, "Command,job=%d,step=%d,username=%s,task=%s,"
					"host=%s,pid=%d,ppid=%d command=\"%s\","
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
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	char *str2 = NULL;
#endif

	enum {
		/*PROFILE*/
		FIELD_STEPCPU,
		FIELD_STEPCPUAVE,
		FIELD_STEPMEM,	
		FIELD_STEPVMEM,		
		FIELD_STEPPAGES,
#ifdef __METASTACK_OPT_INFLUXDB_PERFORMANCE
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
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
		/* APPTYPE */
		FIELD_SENDFLAG,
		FIELD_APPTYPESTEP,
		FIELD_APPTYPECLI,
		FIELD_HAVERECOGN,
		FIELD_CPUTIME,
#endif
		FIELD_CNT
	};
	
	debug3("%s %s called", plugin_type, __func__);
	for (int i= 1; i <= 3; i++) {
		enum {
			SLUR_SEND_STEPD_TYPE = 1,
			SLUR_SEND_EVENT_TYPE = 2,
			SLUR_SEND_APPTYPE_TYPE = 3,
		};
		switch (i) {
			case SLUR_SEND_STEPD_TYPE:
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
				if (!(((union data_t*)data)[FIELD_SENDFLAG].u & JOBACCT_GATHER_PROFILE_ABNORMAL)) 
					break;
#endif
				xstrfmtcat(str1,"Stepd,username=%s,jobid=%d,step=%d stepcpu=%.2f,"
#ifdef __METASTACK_OPT_INFLUXDB_PERFORMANCE
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
#ifdef __METASTACK_OPT_INFLUXDB_PERFORMANCE
				_send_data2(str1, g_job->step_id.job_id, g_job->step_id.step_id, STEPDRP);
#endif
				if (str1)
					xfree(str1);
				break;
			case SLUR_SEND_EVENT_TYPE:
#ifdef __METASTACK_OPT_INFLUXDB_PERFORMANCE
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
				if (((union data_t*)data)[FIELD_FLAG].u == 0 || 
					!(((union data_t*)data)[FIELD_SENDFLAG].u & JOBACCT_GATHER_PROFILE_ABNORMAL)) 
					break;	
#endif
				for (int i = 0; i < EVENT_COUNT; i++) {
					if (((union data_t*)data)[FIELD_FLAG].u & EVENT_CONFIGS[i].flag) {
						xstrfmtcat(str, "Event,username=%s,jobid=%d,step=%d,type=%s "
										"cputhreshold=%.2f,stepcpu=%.2f,stepmem=%.2f,"
										"stepvmem=%.2f,steppages=%"PRIu64",start=%"PRIu64",end=%"PRIu64" %"PRIu64"\n",
								g_job->user_name,
								g_job->step_id.job_id,
								g_job->step_id.step_id,
								EVENT_CONFIGS[i].name,
								((union data_t*)data)[FIELD_CPUTHRESHOLD].d,
								((union data_t*)data)[FIELD_STEPCPU].d,
								((union data_t*)data)[FIELD_STEPMEM].d,
								((union data_t*)data)[FIELD_STEPVMEM].d,
								((union data_t*)data)[FIELD_STEPPAGES].u,
								((union data_t*)data)[FIELD_EVENTTYPE1START].u,
								((union data_t*)data)[FIELD_EVENTTYPE1END].u,
								(uint64_t)sample_time);
					}
				}
				_send_data2(str, g_job->step_id.job_id, g_job->step_id.step_id, EVENTRP);
#endif
				if (str)
					xfree(str);
				break;
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
			case SLUR_SEND_APPTYPE_TYPE:
				if (!(((union data_t*)data)[FIELD_SENDFLAG].u & JOBACCT_GATHER_PROFILE_APPTYPE)) 
					break;
				char buffer[21];
				snprintf(buffer, sizeof(buffer), "%"PRIu64"", ((union data_t*)data)[FIELD_CPUTIME].u);
				/*
					curl does not support uint64 when sending data to influxdb, it will cause precision 
					loss in the conversion process, so it will be converted to a string type to send, 
					sjinfo will convert the string to uint64
				*/
				xstrfmtcat(str2,"Apptype,username=%s,jobid=%d,step=%d apptype_step=\"%s\",apptype_cli=\"%s\",cputime=\"%s\" %"PRIu64"\n",
				g_job->user_name,
				g_job->step_id.job_id,
				g_job->step_id.step_id,
				((union data_t*)data)[FIELD_APPTYPESTEP].str,
				((union data_t*)data)[FIELD_APPTYPECLI].str,
				buffer,
				(uint64_t)sample_time);
				if (!((union data_t*)data)[FIELD_HAVERECOGN].u) {
					if(apptype_datastr) 
						apptype_datastr[0] = '\0';
					memcpy(apptype_datastr, str2, strlen(str2) + 1);
				} else {
					if (apptype_datastr) 
						apptype_datastr[0] = '\0';
					apptype_recongn_count = 0;
					_send_data2(str2, g_job->step_id.job_id, g_job->step_id.step_id, APPTYPERP);
				}
				if (str2)
					xfree(str2);
				break;
#endif
		}

	}
#ifdef __METASTACK_NEW_APPTYPE_RECOGNITION
	if ((((union data_t*)data)[FIELD_SENDFLAG].u & JOBACCT_GATHER_PROFILE_APPTYPE)) {
		if (((union data_t*)data)[FIELD_APPTYPESTEP].str) 
			xfree(((union data_t*)data)[FIELD_APPTYPESTEP].str);
		if (((union data_t*)data)[FIELD_APPTYPECLI].str) 
			xfree(((union data_t*)data)[FIELD_APPTYPECLI].str);
	}
#endif
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
#ifdef __METASTACK_OPT_INFLUXDB_PERFORMANCE
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
