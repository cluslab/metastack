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
#ifdef __METASTACK_LOAD_ABNORMAL
static char *datastr2 = NULL;
static char *buffer_file = NULL;
#endif

static char *datastr = NULL;
static int datastrlen = 0;

static table_t *tables = NULL;
static size_t tables_max_len = 0;
static size_t tables_cur_len = 0;

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

#ifdef __METASTACK_LOAD_ABNORMAL
static int _send_data2(const char *data, int send_jobid ,int send_stepid)
{
	CURL *curl_handle = NULL;
	CURLcode res;
	struct http_response chunk;
	int rc = SLURM_SUCCESS;
	long response_code;
	static int error_cnt = 0;
	char *url = NULL;
	//size_t length;

	debug3("%s %s called", plugin_type, __func__);

	/*
	 * Every compute node which is sampling data will try to establish a
	 * different connection to the influxdb server. In order to reduce the
	 * number of connections, every time a new sampled data comes in, it
	 * is saved in the 'datastr2' buffer. Once this buffer is full, then we
	 * try to open the connection and send this buffer, instead of opening
	 * one per sample.
	 */
	if(send_jobid !=0)
		xstrcat(datastr2, data);
    debug("QINYH--TEST datastr2=%s",datastr2);

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
	xstrfmtcat(url, "%s/write?db=%s&rp=%s&precision=s", influxdb_conf.host,
		   influxdb_conf.database, influxdb_conf.rt_policy);

	chunk.message = xmalloc(1);
	chunk.size = 0;

	curl_easy_setopt(curl_handle, CURLOPT_URL, url);
	if (influxdb_conf.password)
		curl_easy_setopt(curl_handle, CURLOPT_PASSWORD,
				 influxdb_conf.password);

	curl_easy_setopt(curl_handle, CURLOPT_POST, 1);
	curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, datastr2);
	curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDSIZE, strlen(datastr2));
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
	struct stat st_tmp;
	bool influx_dir = true;
	if(rc == SLURM_ERROR) {

		if(influxdb_conf.workdir == NULL) {
			char tmp_dir[60]="/tmp/slurm_influxdb";
			if (stat(tmp_dir, &st_tmp) == -1) {
				if(mkdir(tmp_dir, 0700)==-1) {
					error("can't create directory /tmp/slurm_influxdb");
				}
				influxdb_conf.workdir = tmp_dir;
			}
		} else if(xstrcasecmp(influxdb_conf.workdir, "None") == 0) {
			influx_dir = false;
		}
	}


	if((send_jobid > 0) && (rc == SLURM_ERROR) && influx_dir) {
		char *influxdb_file = NULL;
		FILE *sys_file = NULL;
		xstrfmtcat(influxdb_file,"%s/job%d.%d", influxdb_conf.workdir, send_jobid, send_stepid);
		struct stat st;
        buffer_file = xstrdup(influxdb_file);
		if (stat(influxdb_conf.workdir, &st) == -1) {
 			if(mkdir(influxdb_conf.workdir, 0700)==-1) {
				error("can't create directory influxdb_conf.workdir");
			}
		}

		sys_file = fopen(influxdb_file, "a+");
		if (sys_file != NULL) {
			fprintf(sys_file, datastr2);
			fclose(sys_file);
		} else 
			debug("QINYH Error opening file of %s!",influxdb_file);
		if(influxdb_file)
			xfree(influxdb_file);
	}
	if (data) {
		datastr2[0] = '\0';
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

static int _remove_row(char *path_tmp,int have_send, char* tmp_str, FILE *fp)
{
	FILE *fp_out = NULL;
	fp_out = fopen(path_tmp, "a+"); 

	if (fp_out != NULL) {
		int tmpcount = 1;
		while (fgets(tmp_str, 200, fp) != NULL) {
			if (tmpcount > have_send ) {
				fwrite(tmp_str, 1, strlen(tmp_str), fp_out);
				memset(tmp_str, 0 , strlen(tmp_str) );
			}
			
			tmpcount++;
		}
		fclose(fp_out);
		return SLURM_SUCCESS;
	} else
	  return SLURM_ERROR;


}

/*At the end of the job, 
 *check whether the influxdb cache file is generated,and if so, 
 *try to send it again.*/

static int _last_resend(const char *data)
{
	struct stat st;
    int rc = SLURM_SUCCESS;
	int rc2 = SLURM_SUCCESS;
	bool send_buffer = false;
	char tmp_str[200] = {'0'};
	int all_row = 0;

  
	if(data || (!buffer_file) || (strlen(buffer_file) <= 0)) {
		return rc;
	}


	if((data == NULL) && (buffer_file != NULL) && (stat(buffer_file, &st) != -1)) {
       
	    all_row = count_file_row(buffer_file);
		if(all_row <= 0) {
			remove(buffer_file);
			return rc;
		}
			
		FILE *fp = NULL;
		char *path_tmp = NULL;
	    xstrfmtcat(path_tmp, "%s.tmp", buffer_file);
		fp = fopen(buffer_file, "r");
		if (fp == NULL) {
			rc = SLURM_ERROR;
            debug("open %s failed!", buffer_file);
        } else {

			int line = 0;
			datastr2[0] = '\0';
			int datastrlen2 = 0;
			int have_send = 0;

			while (fgets(tmp_str, 200, fp) != NULL)  {
				line++;
				if((datastrlen2 + strlen(tmp_str)) <= BUF_SIZE) {
					xstrcat(datastr2, tmp_str);
					datastrlen2 += strlen(tmp_str);
					send_buffer = true;
				} else  {
				   rc = _send_data2(datastr2, 0, 0);
				   xstrcat(datastr2, tmp_str);
				   datastrlen2 = strlen(datastr2);
				   if(rc == SLURM_SUCCESS)
				   		have_send = line;

                   if((rc == SLURM_ERROR) && (have_send > 0)) {
						/*
						 *If the transmission is not successful even once, 
						 *there is no need to regenerate the file.
						 */
						rc2 = _remove_row(path_tmp, have_send, tmp_str, fp);
				   }	
				}
			}

            
			if(send_buffer == true)
				rc = _send_data2(datastr2, 0, 0);

			if((rc ==SLURM_SUCCESS) &&  (line >= all_row)) {
				remove(buffer_file);
			} else {
				rc2 = _remove_row(path_tmp, have_send, tmp_str, fp);
			}
			
			fclose(fp);

			if(rc2 == SLURM_SUCCESS) {
				remove(buffer_file);
				rename(path_tmp, buffer_file);
			} 
		}
		debug("QINYH path_tmp =%s",path_tmp);
		if(path_tmp) 
			xfree(path_tmp);
		
	}
	if(buffer_file) 
		xfree(buffer_file);

	if((rc == SLURM_ERROR) && (rc2 == SLURM_ERROR)) {
		debug("QINYH Resend failed, file saved in %s",buffer_file);
	}
	return rc;

}
#endif

/* Try to send data to influxdb */
static int _send_data(const char *data)

{
	CURL *curl_handle = NULL;
	CURLcode res;
	struct http_response chunk;
	int rc = SLURM_SUCCESS;
	long response_code;
	static int error_cnt = 0;
	char *url = NULL;
	size_t length;

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

	xstrfmtcat(url, "%s/write?db=%s&rp=%s&precision=s", influxdb_conf.host,
		   influxdb_conf.database, influxdb_conf.rt_policy);

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
	datastr2 = xmalloc(BUF_SIZE);
    //buffer_file = xmalloc(1000);
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
#ifdef __METASTACK_LOAD_ABNORMAL
	xfree(influxdb_conf.workdir);
	xfree(datastr2);
	//xfree(buffer_file);
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
#ifdef __METASTACK_LOAD_ABNORMAL
	_last_resend(NULL);
#endif
	_send_data(NULL);
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
	debug3("%s %s called", plugin_type, __func__);

	for(; i < table->size; i++) {
		switch (table->types[i]) {
		case PROFILE_FIELD_UINT64:
			xstrfmtcat(str, "%s,job=%d,step=%d,task=%s,"
				   "host=%s value=%"PRIu64" "
				   "%"PRIu64"\n", table->names[i],
				   g_job->step_id.job_id,
				   g_job->step_id.step_id,
				   table->name, g_job->node_name,
				   ((union data_t*)data)[i].u,
				   (uint64_t)sample_time);
			break;
		case PROFILE_FIELD_DOUBLE:
			xstrfmtcat(str, "%s,job=%d,step=%d,task=%s,"
				   "host=%s value=%.2f %"PRIu64""
				   "\n", table->names[i],
				   g_job->step_id.job_id,
				   g_job->step_id.step_id,
				   table->name, g_job->node_name,
				   ((union data_t*)data)[i].d,
				   (uint64_t)sample_time);
			break;
		case PROFILE_FIELD_NOT_SET:
			break;
		}
	}

	_send_data(str);
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
				"stepcpuave=%.2f,stepmem=%.2f,stepvmem=%.2f,"
				"steppages=%"PRIu64"  %"PRIu64"\n", 
				g_job->user_name,
				g_job->step_id.job_id, 
				g_job->step_id.step_id,
				((union data_t*)data)[FIELD_STEPCPU].d,
				((union data_t*)data)[FIELD_STEPCPUAVE].d,
				((union data_t*)data)[FIELD_STEPMEM].d,
				((union data_t*)data)[FIELD_STEPVMEM].d,
				((union data_t*)data)[FIELD_STEPPAGES].u,
				(uint64_t)sample_time);
				
				_send_data2(str1, g_job->step_id.job_id, g_job->step_id.step_id);
				if(str1)
					xfree(str1);
				break;
			case SLUR_SEND_EVENT_TYPE:	
				xstrfmtcat(str,"Event,username=%s,jobid=%d,step=%d "
				"cputhreshold=%.2f,stepcpu=%.2f,stepmem=%.2f,"
				"stepvmem=%.2f,steppages=%"PRIu64"",
				g_job->user_name,
				g_job->step_id.job_id, 
				g_job->step_id.step_id,
				((union data_t*)data)[FIELD_CPUTHRESHOLD].d,
				((union data_t*)data)[FIELD_STEPCPU].d,
				((union data_t*)data)[FIELD_STEPMEM].d,
				((union data_t*)data)[FIELD_STEPVMEM].d,
				((union data_t*)data)[FIELD_STEPPAGES].u);	
				// xstrfmtcat(str1,"Event,");
				 if(((union data_t*)data)[FIELD_FLAG].u & event1) { 
					xstrfmtcat(str1,"%s,type=%"PRIu64",start=%"PRIu64",end=%"PRIu64""
					" %"PRIu64"\n",
					str,event1,
					((union data_t*)data)[FIELD_EVENTTYPE1START].u,
					((union data_t*)data)[FIELD_EVENTTYPE1END].u,
					(uint64_t)sample_time);
					_send_data2(str1, g_job->step_id.job_id, g_job->step_id.step_id);
					xfree(str1);
				}

				if(((union data_t*)data)[FIELD_FLAG].u & event2) { 
					xstrfmtcat(str1,"%s,type=%"PRIu64",start=%"PRIu64",end=%"PRIu64""
					" %"PRIu64"\n",
					str,event2,
					((union data_t*)data)[FIELD_EVENTTYPE2START].u,
					((union data_t*)data)[FIELD_EVENTTYPE2END].u,
					(uint64_t)sample_time);
					_send_data2(str1, g_job->step_id.job_id, g_job->step_id.step_id);
					xfree(str1);
				}

				if(((union data_t*)data)[FIELD_FLAG].u & event3) { 
					xstrfmtcat(str1,"%s,type=%"PRIu64",start=%"PRIu64",end=%"PRIu64" "
					"%"PRIu64"\n",
					str,event3,
					((union data_t*)data)[FIELD_EVENTTYPE3START].u,
					((union data_t*)data)[FIELD_EVENTTYPE3END].u,
					(uint64_t)sample_time);
					_send_data2(str1, g_job->step_id.job_id, g_job->step_id.step_id);
					xfree(str1);
				}	
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
