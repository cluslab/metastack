/****************************************************************************\
 *  opts.c - squeue command line option parsing
 *****************************************************************************
 *  Copyright (C) 2002-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2010 Lawrence Livermore National Security.
 *  Copyright (C) 2010-2013 SchedMD LLC.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Joey Ekstrom <ekstrom1@llnl.gov>, Morris Jette <jette1@llnl.gov>
 *  CODE-OCEC-09-009. All rights reserved.
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
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
\*****************************************************************************/

#define _GNU_SOURCE

#include <getopt.h>
#include <pwd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "src/common/data.h"
#include "src/common/read_config.h"
#include "src/common/xstring.h"
#include "src/common/proc_args.h"
#include "src/common/uid.h"

#include "src/squeue/squeue.h"

/* getopt_long options, integers but not characters */
#define OPT_LONG_HELP         0x100
#define OPT_LONG_USAGE        0x101
#define OPT_LONG_HIDE         0x102
#define OPT_LONG_START        0x103
#define OPT_LONG_NOCONVERT    0x104
#define OPT_LONG_ARRAY_UNIQUE 0x105
#define OPT_LONG_LOCAL        0x106
#define OPT_LONG_SIBLING      0x107
#define OPT_LONG_FEDR         0x108
#define OPT_LONG_ME           0x109
#define OPT_LONG_JSON         0x110
#define OPT_LONG_YAML         0x111
#ifdef __METASTACK_OPT_CACHE_QUERY
#define OPT_LONG_CACHE        0x112
#define OPT_LONG_NOCACHE        0x113
#endif

/* FUNCTIONS */
static List  _build_job_list( char* str );
static List  _build_str_list( char* str );
static List  _build_state_list( char* str );
static List  _build_step_list( char* str );
static List  _build_user_list( char* str );
static char *_get_prefix(char *token);
static void  _help( void );
static int   _parse_state( char* str, uint32_t* states );
static void  _parse_token( char *token, char *field, int *field_size,
			   bool *right_justify, char **suffix);
static void _parse_long_token( char *token, char *sep, int *field_size,
			       bool *right_justify, char **suffix);
static void  _print_options( void );
static void  _usage( void );
static void _filter_nodes(void);
static List _load_clusters_nodes(void);
static void _node_info_list_del(void *data);
static char *_map_node_name(List clusters_node_info, char *name);

#ifdef __METASTACK_OPT_PRINT_COMMAND
static void
/*parse character print format*/
_parse_env_token_extend( char *token, int *field_size, bool *right_format,
		   bool *symbol_flag)
{
	char *end_ptr = NULL, *ptr;

	//char *suffix = NULL;
	xassert(token);
	ptr = strchr(token, ':');
	if (ptr) {
		ptr[0] = '\0';
		if((ptr[1] == '-') || (ptr[2] == '-'))
			*symbol_flag = true;
		if (ptr[1] == '.') {
			*right_format = true;
			ptr++;
		} else {
			*right_format = false;
		}
		*field_size = strtol(ptr + 1, &end_ptr, 10);
	} else {
		*right_format = false;
		*field_size = 0;
	}
}
/*Analysis of squeue environment variables*/
static int _parse_squeue_format_extend (char* format_long, char* cmp, int* field_size, 
												 bool *right_justify) 
{
	char *tmp_format = NULL, *token_sub = NULL, *str_tmp = NULL;

	if (format_long == NULL) {
		error("Format option lacks specification");
		return SLURM_ERROR;
	}
	tmp_format = xstrdup(format_long);
	token_sub = strtok_r(tmp_format, ",",&str_tmp);
    while (token_sub) {
		bool right_format_tmp = false;
		bool symbol_tmp = false;
		bool right_justify_tmp = false;
		int field_size_tmp = 0;
        int command_len = 0;
		/*parse character print format*/
		_parse_env_token_extend(token_sub, &field_size_tmp, &right_format_tmp,
				   &symbol_tmp);
		command_len = strlen(token_sub);
		/*Parse fields of environment variables*/
		if (!xstrcasecmp(token_sub, "all")) {
			//*field_size = field_size_tmp;
            int abs_len = abs(field_size_tmp);

			if ((field_size_tmp == abs_len) && (!symbol_tmp)) 
				right_justify_tmp = true;
		    else	
				right_justify_tmp = false;
		    
			if(!params.format_flag ) {
				*right_justify = right_justify_tmp;	   
			} else if(params.format_flag && (!params.right_flag))
				*right_justify = right_justify_tmp;	   
			if(abs(field_size_tmp) >= 0) {
				if((!params.format_flag)) {
					*field_size = abs(field_size_tmp);	
				} 
				if(!params.format_field_flag && (params.format_flag)) {
					*field_size = abs(field_size_tmp);	
				}
			}

			if(right_format_tmp) {
				int tmp =  abs(*field_size);
				*field_size = - tmp;
			}
		}

		if (!xstrncasecmp(token_sub, cmp, command_len)) {
			int abs_len = abs(field_size_tmp);
			if ((field_size_tmp == abs_len) && (!symbol_tmp)) 
				right_justify_tmp = true;
		    else	
				right_justify_tmp = false;
		    
			if(!params.format_flag) {
				*right_justify = right_justify_tmp;	   
			} else if(params.format_flag && (!params.right_flag))
				*right_justify = right_justify_tmp;	 

			if(abs(field_size_tmp) >= 0) {
				if((!params.format_flag)) {
					*field_size = abs(field_size_tmp);	
				} else if(!params.format_field_flag && (params.format_flag)) {
					*field_size = abs(field_size_tmp);	
				}
			}
			if(right_format_tmp) {
				int tmp =  abs(*field_size);
				*field_size = - tmp;
			}
		}

		token_sub = strtok_r(NULL, ",", &str_tmp);	
	}

	if(tmp_format)
		xfree(tmp_format);
	return SLURM_SUCCESS;

}

static void _format_state_id(char *field,  int *field_size, 
										 bool *right_justify, bool format_all)
{
	bool all_flag = false;
    char *env_squeue = NULL;
	char *token = NULL;
	char *field_tmp = NULL;
	char *field_tmp2 = NULL;
	int   repeat_flag = 1;
	if (format_all) 
		all_flag = true;

    if((env_squeue = getenv("SQUEUE_EXTEND"))) {
		if (params.step_flag) {
		/*Job step corresponds to environment variable*/
			if      (field[0] == 'A')
				field_tmp = xstrdup("numtasks");
			else if (field[0] == 'b') {
				field_tmp = xstrdup("gres");
				field_tmp = xstrdup("tres-per-node");
				repeat_flag = 2;
			}
			else if (field[0] == 'i')
				field_tmp = xstrdup("stepid");
			else if (field[0] == 'j')
				field_tmp = xstrdup("stepname");
			else if (field[0] == 'l')
				field_tmp = xstrdup("timelimit");
			else if (field[0] == 'M')
				field_tmp = xstrdup("timeused");
			else if (field[0] == 'N')
				field_tmp = xstrdup("nodes");
			else if (field[0] == 'P')
				field_tmp = xstrdup("partition");
			else if (field[0] == 'S')
				field_tmp = xstrdup("starttime");
			else if (field[0] == 'U')
				field_tmp = xstrdup("userid");
			else if (field[0] == 'u')
				field_tmp = xstrdup("username");
		} else {
			/*Job corresponds to environment variable*/
			if (field[0] == 'a')
				field_tmp = xstrdup("account");
			else if (field[0] == 'A')
				field_tmp = xstrdup("jobid");
			else if (field[0] == 'b') {
				field_tmp = xstrdup("gres");
				field_tmp2 = xstrdup("tres-per-node");
				repeat_flag = 2;
			}
			else if (field[0] == 'B')
				field_tmp = xstrdup("batchhost");	
			else if (field[0] == 'c')
				field_tmp = xstrdup("mincpus");
			else if (field[0] == 'C')
				field_tmp = xstrdup("numcpus");
			else if (field[0] == 'd')
				field_tmp = xstrdup("mintmpdisk");
			else if (field[0] == 'D')
				field_tmp = xstrdup("numnodes");		
			else if (field[0] == 'e')
				field_tmp = xstrdup("endtime");
			else if (field[0] == 'E')
				field_tmp = xstrdup("dependency");
			else if (field[0] == 'f')
				field_tmp = xstrdup("feature");
			else if (field[0] == 'F')
				field_tmp = xstrdup("arrayjobid");
			else if (field[0] == 'g')
				field_tmp = xstrdup("groupname");
			else if (field[0] == 'G')
				field_tmp = xstrdup("groupid");
			else if (field[0] == 'h') {
				field_tmp = xstrdup("oversubscribe");
				field_tmp2 = xstrdup("shared");
				repeat_flag = 2;
			}
			else if (field[0] == 'H')
				field_tmp = xstrdup("sockets");
			else if (field[0] == 'i')
				field_tmp = xstrdup("jobarrayid");
			else if (field[0] == 'I')
				field_tmp = xstrdup("cores");
			else if (field[0] == 'j')
				field_tmp = xstrdup("name");
			else if (field[0] == 'J')
				field_tmp = xstrdup("threads");
			else if (field[0] == 'k')
				field_tmp = xstrdup("comment");
			else if (field[0] == 'K')
				field_tmp = xstrdup("arraytaskid");
			else if (field[0] == 'l')
				field_tmp = xstrdup("timelimit");
			else if (field[0] == 'L')
				field_tmp = xstrdup("timeleft");
			else if (field[0] == 'm')
				field_tmp = xstrdup("minmemory");
			else if (field[0] == 'M')
				field_tmp = xstrdup("timeused");
			else if (field[0] == 'n')
				field_tmp = xstrdup("reqnodes");
			else if (field[0] == 'N')
				field_tmp = xstrdup("nodelist");
			else if (field[0] == 'o')
				field_tmp = xstrdup("command");
			else if (field[0] == 'O')
				field_tmp = xstrdup("contiguous");
			else if (field[0] == 'p')
				field_tmp = xstrdup("priority");
			else if (field[0] == 'P')
				field_tmp = xstrdup("partition");
			else if (field[0] == 'q')
				field_tmp = xstrdup("qos");
			else if (field[0] == 'Q')
				field_tmp = xstrdup("prioritylong");
			else if (field[0] == 'r')
				field_tmp = xstrdup("reason");
			else if (field[0] == 'R')
				field_tmp = xstrdup("reasonlist");
			else if (field[0] == 's')
				field_tmp = xstrdup("selectjobinfo");
			else if (field[0] == 'S')
				field_tmp = xstrdup("starttime");
			else if (field[0] == 't')
				field_tmp = xstrdup("statecompact");
			else if (field[0] == 'T')
				field_tmp = xstrdup("state");
			else if (field[0] == 'U')
				field_tmp = xstrdup("userid");
			else if (field[0] == 'u')
				field_tmp = xstrdup("username");
			else if (field[0] == 'v')
				field_tmp = xstrdup("reservation");
			else if (field[0] == 'V')
				field_tmp = xstrdup("submittime");
			else if (field[0] == 'w')
				field_tmp = xstrdup("wckey");
			else if (field[0] == 'W')
				field_tmp = xstrdup("licenses");
			else if (field[0] == 'x')
				field_tmp = xstrdup(" ");
			else if (field[0] == 'X')
				field_tmp = xstrdup("corespec");
			else if (field[0] == 'y')
				field_tmp = xstrdup("nice");
			else if (field[0] == 'Y')
				field_tmp = xstrdup("schednodes");
			else if (field[0] == 'z')
				field_tmp = xstrdup("sct");
			else if (field[0] == 'Z')
				field_tmp = xstrdup("workdir");
			
		}
		
		for(;repeat_flag > 0; repeat_flag--) {
			if( (repeat_flag == 2) && field_tmp2)
				token = field_tmp2;
			else if(field_tmp != NULL) 
				token = field_tmp;

			if (token) {
				//*field_size = abs(*field_size);
				char *format_squeue = xstrdup(env_squeue);
				_parse_squeue_format_extend(format_squeue, token, field_size, right_justify);
				if(all_flag)
			 		*field_size = 0;
			if (format_squeue)
			xfree(format_squeue);
			}
		}
	}

	if (field_tmp)
		xfree(field_tmp);
	if (field_tmp2)
		xfree(field_tmp2);
	
}
#endif

/*
 * parse_command_line
 */
extern void
parse_command_line( int argc, char* *argv )
{
	char *env_val = NULL;
	bool override_format_env = false;
	int opt_char;
	int option_index;
	static struct option long_options[] = {
		{"accounts",   required_argument, 0, 'A'},
		{"all",        no_argument,       0, 'a'},
		{"array",      no_argument,       0, 'r'},
		{"array-unique",no_argument,      0, OPT_LONG_ARRAY_UNIQUE},
#ifdef __METASTACK_OPT_CACHE_QUERY	
		{"cache",	   no_argument, 	  0, OPT_LONG_CACHE},
#endif
		{"Format",     required_argument, 0, 'O'},
		{"format",     required_argument, 0, 'o'},
		{"federation", no_argument,       0, OPT_LONG_FEDR},
		{"help",       no_argument,       0, OPT_LONG_HELP},
		{"hide",       no_argument,       0, OPT_LONG_HIDE},
		{"iterate",    required_argument, 0, 'i'},
		{"jobs",       optional_argument, 0, 'j'},
		{"local",      no_argument,       0, OPT_LONG_LOCAL},
		{"long",       no_argument,       0, 'l'},
		{"licenses",   required_argument, 0, 'L'},
		{"cluster",    required_argument, 0, 'M'},
		{"clusters",   required_argument, 0, 'M'},
		{"me",         no_argument,       0, OPT_LONG_ME},
		{"name",       required_argument, 0, 'n'},
#ifdef __METASTACK_OPT_CACHE_QUERY	
		{"nocache",	   no_argument, 	  0, OPT_LONG_NOCACHE},
#endif
        {"noconvert",  no_argument,       0, OPT_LONG_NOCONVERT},
		{"node",       required_argument, 0, 'w'},
		{"nodes",      required_argument, 0, 'w'},
		{"nodelist",   required_argument, 0, 'w'},
		{"noheader",   no_argument,       0, 'h'},
		{"partitions", required_argument, 0, 'p'},
		{"priority",   no_argument,       0, 'P'},
		{"qos",        required_argument, 0, 'q'},
		{"reservation",required_argument, 0, 'R'},
		{"sib",        no_argument,       0, OPT_LONG_SIBLING},
		{"sibling",    no_argument,       0, OPT_LONG_SIBLING},
		{"sort",       required_argument, 0, 'S'},
		{"start",      no_argument,       0, OPT_LONG_START},
		{"steps",      optional_argument, 0, 's'},
		{"states",     required_argument, 0, 't'},
		{"usage",      no_argument,       0, OPT_LONG_USAGE},
		{"user",       required_argument, 0, 'u'},
		{"users",      required_argument, 0, 'u'},
		{"verbose",    no_argument,       0, 'v'},
		{"version",    no_argument,       0, 'V'},
		{"json", no_argument, 0, OPT_LONG_JSON},
		{"yaml", no_argument, 0, OPT_LONG_YAML},
		{NULL,         0,                 0, 0}
	};

	params.convert_flags = CONVERT_NUM_UNIT_EXACT;

	if (xstrstr(slurm_conf.fed_params, "fed_display"))
		params.federation_flag = true;

	if (getenv("SQUEUE_ALL"))
		params.all_flag = true;
	if (getenv("SQUEUE_ARRAY"))
		params.array_flag = true;
	if ( ( env_val = getenv("SQUEUE_SORT") ) )
		params.sort = xstrdup(env_val);
	if (getenv("SQUEUE_ARRAY_UNIQUE"))
		params.array_unique_flag = true;
	if ( ( env_val = getenv("SLURM_CLUSTERS") ) ) {
		if (!(params.clusters = slurmdb_get_info_cluster(env_val))) {
			print_db_notok(env_val, 1);
			exit(1);
		}
		working_cluster_rec = list_peek(params.clusters);
		params.local_flag = true;
	}
	if (getenv("SQUEUE_FEDERATION"))
		params.federation_flag = true;
	if (getenv("SQUEUE_LOCAL"))
		params.local_flag = true;
	if (getenv("SQUEUE_PRIORITY"))
		params.priority_flag = true;
	if (getenv("SQUEUE_SIB") || getenv("SQUEUE_SIBLING"))
		params.sibling_flag = true;
#ifdef __METASTACK_OPT_CACHE_QUERY
	if ((env_val = getenv("SQUEUE_CACHE_QUERY"))){
		if(!xstrcmp(env_val, "cache")){
			params.cache_query = true;
			params.nocache_query = false;
		}else if(!xstrcmp(env_val, "nocache")){
			params.nocache_query = true;
			params.cache_query = false;
		}
	}
#endif
	while ((opt_char = getopt_long(argc, argv,
					   "A:ahi:j::lL:n:M:O:o:p:Pq:R:rs::S:t:u:U:vVw:",
				       long_options, &option_index)) != -1) {
		switch (opt_char) {
		case (int)'?':
			fprintf(stderr, "Try \"squeue --help\" "
				"for more information\n");
			exit(1);
		case (int) 'A':
		case (int) 'U':	/* backwards compatibility */
			xfree(params.accounts);
			params.accounts = xstrdup(optarg);
			params.account_list =
				_build_str_list( params.accounts );
			break;
		case (int)'a':
			params.all_flag = true;
			break;
		case (int)'h':
			params.no_header = true;
			break;
		case (int) 'i':
			params.iterate= atoi(optarg);
			if (params.iterate <= 0) {
				error ("--iterate=%s\n", optarg);
				exit(1);
			}
			break;
		case (int) 'j':
			if (optarg) {
				params.jobs = xstrdup(optarg);
				params.job_list =
					_build_job_list(params.jobs);
			}
			params.job_flag = true;
			break;
		case (int) 'l':
			params.long_list = true;
			override_format_env = true;
			break;
		case (int) 'L':
			xfree(params.licenses);
			params.licenses = xstrdup(optarg);
			params.licenses_list = _build_str_list(params.licenses);
			break;
		case (int) 'M':
			if (params.clusters)
				FREE_NULL_LIST(params.clusters);
			if (!(params.clusters =
			      slurmdb_get_info_cluster(optarg))) {
				print_db_notok(optarg, 0);
				exit(1);
			}
			working_cluster_rec = list_peek(params.clusters);
			params.local_flag = true;
			break;
		case (int) 'n':
			xfree(params.names);
			params.names = xstrdup(optarg);
			params.name_list = _build_str_list( params.names );
			break;
		case (int) 'O':
			xfree(params.format_long);
#ifdef __METASTACK_OPT_PRINT_COMMAND
			params.format_flag = true;
#endif
			if (params.format == NULL) {
				params.format_long = xstrdup(optarg);
			} else {
				error ("-O (--Format) is incompatible with -o "
				       "(--format)");
				exit(1);
			}
			override_format_env = true;
			break;

		case (int) 'o':
			xfree(params.format);
#ifdef __METASTACK_OPT_PRINT_COMMAND
			params.format_flag = true;
#endif
			if (params.format_long == NULL) {
				params.format = xstrdup(optarg);
			} else {
				error ("-o (--format) is incompatible with -O "
				       "(--Format)");
				exit(1);
			}
			override_format_env = true;
			break;
		case (int) 'p':
			xfree(params.partitions);
			params.partitions = xstrdup(optarg);
			params.part_list =
				_build_str_list( params.partitions );
			params.all_flag = true;
			break;
		case (int) 'P':
			params.priority_flag = true;
			break;
		case (int) 'q':
			xfree(params.qoss);
			params.qoss = xstrdup(optarg);
			params.qos_list =
				_build_str_list( params.qoss );
			break;
		case (int) 'R':
			xfree(params.reservation);
			params.reservation = xstrdup(optarg);
			break;
		case (int)'r':
			params.array_flag = true;
			setenv("SLURM_BITSTR_LEN", "0", 1);
			break;
		case (int) 's':
			if (optarg) {
				params.steps = xstrdup(optarg);
				params.step_list =
					_build_step_list(params.steps);
			}
			params.step_flag = true;
			override_format_env = true;
			break;
		case (int) 'S':
			xfree(params.sort);
			params.sort = xstrdup(optarg);
			break;
		case (int) 't':
			xfree(params.states);
			params.states = xstrdup(optarg);
			params.state_list =
				_build_state_list( params.states );
			break;
		case (int) 'u':
			xfree(params.users);
			params.users = xstrdup(optarg);
			params.user_list =
				_build_user_list( params.users );
			break;
		case (int) 'v':
			params.verbose++;
			break;
		case (int) 'V':
			print_slurm_version();
			exit(0);
		case (int) 'w':
			if (params.nodes)
				hostset_destroy(params.nodes);

			params.nodes = hostset_create(optarg);
			if (params.nodes == NULL) {
				error("'%s' invalid entry for --nodelist",
				      optarg);
				exit(1);
			}
			break;
		case OPT_LONG_ARRAY_UNIQUE:
			params.array_unique_flag = true;
			break;
		case OPT_LONG_HELP:
			_help();
			exit(0);
		case OPT_LONG_FEDR:
			params.federation_flag = true;
			break;
		case OPT_LONG_HIDE:
			params.all_flag = false;
			break;
		case OPT_LONG_LOCAL:
			params.local_flag = true;
			break;
		case OPT_LONG_ME:
			xfree(params.users);
			xstrfmtcat(params.users, "%u", geteuid());
			params.user_list = _build_user_list(params.users);
			break;
		case OPT_LONG_SIBLING:
			params.sibling_flag = true;
			break;
		case OPT_LONG_START:
			params.start_flag = true;
			override_format_env = true;
			break;
		case OPT_LONG_NOCONVERT:
			params.convert_flags |= CONVERT_NUM_UNIT_NO;
			break;
		case OPT_LONG_USAGE:
			_usage();
			exit(0);
		case OPT_LONG_JSON:
			params.mimetype = MIME_TYPE_JSON;
			data_init(MIME_TYPE_JSON_PLUGIN, NULL);
			break;
		case OPT_LONG_YAML:
			params.mimetype = MIME_TYPE_YAML;
			data_init(MIME_TYPE_YAML_PLUGIN, NULL);
			break;
#ifdef __METASTACK_OPT_CACHE_QUERY	
		case OPT_LONG_CACHE:
			params.cache_query = true;
            params.nocache_query =false;
			break;
        case OPT_LONG_NOCACHE:
			params.nocache_query = true;
            params.cache_query = false;
			break;
#endif
		}
	}

	if (params.long_list && params.format)
		fatal("Options -o(--format) and -l(--long) are mutually exclusive. Please remove one and retry.");

	if (!override_format_env) {
		if ((env_val = getenv("SQUEUE_FORMAT")))
			params.format = xstrdup(env_val);
		else if ((env_val = getenv("SQUEUE_FORMAT2")))
			params.format_long = xstrdup(env_val);
	}

	params.cluster_flags = slurmdb_setup_cluster_flags();
	if (optind < argc) {
		if (params.job_flag) {
			params.jobs = xstrdup(argv[optind++]);
			params.job_list = _build_job_list(params.jobs);
		} else if (params.step_flag) {
			params.steps = xstrdup(argv[optind++]);
			params.step_list = _build_step_list(params.steps);
		}
		if (optind < argc) {
			error("Unrecognized option: %s",argv[optind]);
			_usage();
			exit(1);
		}
	}

	if ( params.job_flag && params.step_flag) {
		if (params.job_list) {
			verbose("Printing job steps with job filter");
			params.job_flag = false;
		} else {
			error("Incompatible options --jobs and --steps");
			exit(1);
		}
	}

	if ( params.nodes )
		_filter_nodes();

	if ( ( params.accounts == NULL ) &&
	     ( env_val = getenv("SQUEUE_ACCOUNT") ) ) {
		params.accounts = xstrdup(env_val);
		params.account_list = _build_str_list( params.accounts );
	}

	if ( ( params.names == NULL ) &&
	     ( env_val = getenv("SQUEUE_NAMES") ) ) {
		params.names = xstrdup(env_val);
		params.name_list = _build_str_list( params.names );
	}

	if ( ( params.partitions == NULL ) &&
	     ( env_val = getenv("SQUEUE_LICENSES") ) ) {
		params.licenses = xstrdup(env_val);
		params.licenses_list = _build_str_list( params.licenses );
	}

	if ( ( params.partitions == NULL ) &&
	     ( env_val = getenv("SQUEUE_PARTITION") ) ) {
		params.partitions = xstrdup(env_val);
		params.part_list = _build_str_list( params.partitions );
		params.all_flag = true;
	}

	if ( ( params.qoss == NULL ) &&
	     ( env_val = getenv("SQUEUE_QOS") ) ) {
		params.qoss = xstrdup(env_val);
		params.qos_list = _build_str_list( params.qoss );
	}

	if ( ( params.states == NULL ) &&
	     ( env_val = getenv("SQUEUE_STATES") ) ) {
		params.states = xstrdup(env_val);
		params.state_list = _build_state_list( params.states );
	}

	if ( ( params.users == NULL ) &&
	     ( env_val = getenv("SQUEUE_USERS") ) ) {
		params.users = xstrdup(env_val);
		params.user_list = _build_user_list( params.users );
	}

	if ( params.start_flag && !params.step_flag ) {
		/* Set more defaults */
		if (params.format == NULL)
			params.format = xstrdup("%.18i %.9P %.8j %.8u %.2t %.19S %.6D %20Y %R");
		if (params.sort == NULL)
			params.sort = xstrdup("S");
		if (params.states == NULL) {
			params.states = xstrdup("PD");
			params.state_list = _build_state_list( params.states );
		}
	}

	if (params.job_list && (list_count(params.job_list) == 1)) {
		squeue_job_step_t *job_step_ptr = list_peek(params.job_list);
		params.job_id = job_step_ptr->step_id.job_id;
	}
	if (params.user_list && (list_count(params.user_list) == 1)) {
		ListIterator iterator;
		uint32_t *uid_ptr;
		iterator = list_iterator_create(params.user_list);
		while ((uid_ptr = list_next(iterator))) {
			params.user_id = *uid_ptr;
			break;
		}
		list_iterator_destroy(iterator);
	}

	if ( params.verbose )
		_print_options();
}

/*
 * _parse_state - convert job state name string to numeric value
 * IN str - state name
 * OUT states - enum job_states value corresponding to str
 * RET 0 or error code
 */
static int
_parse_state( char* str, uint32_t* states )
{
	uint32_t i;
	char *state_names;

	if ((i = job_state_num(str)) != NO_VAL) {
		*states = i;
		return SLURM_SUCCESS;
	}

	error("Invalid job state specified: %s", str);
	state_names = xstrdup(job_state_string(0));
	for (i=1; i<JOB_END; i++) {
		xstrcat(state_names, ",");
		xstrcat(state_names, job_state_string(i));
	}
	xstrcat(state_names, ",");
	xstrcat(state_names, job_state_string(JOB_COMPLETING));
	xstrcat(state_names, ",");
	xstrcat(state_names, job_state_string(JOB_CONFIGURING));
	xstrcat(state_names, ",");
	xstrcat(state_names, job_state_string(JOB_RESIZING));
	xstrcat(state_names, ",");
	xstrcat(state_names, job_state_string(JOB_RESV_DEL_HOLD));
	xstrcat(state_names, ",");
	xstrcat(state_names, job_state_string(JOB_REQUEUE));
	xstrcat(state_names, ",");
	xstrcat(state_names, job_state_string(JOB_REQUEUE_FED));
	xstrcat(state_names, ",");
	xstrcat(state_names, job_state_string(JOB_REQUEUE_HOLD));
	xstrcat(state_names, ",");
	xstrcat(state_names, job_state_string(JOB_REVOKED));
	xstrcat(state_names, ",");
	xstrcat(state_names, job_state_string(JOB_SIGNALING));
	xstrcat(state_names, ",");
	xstrcat(state_names, job_state_string(JOB_SPECIAL_EXIT));
	xstrcat(state_names, ",");
	xstrcat(state_names, job_state_string(JOB_STAGE_OUT));
	xstrcat(state_names, ",");
	xstrcat(state_names, job_state_string(JOB_STOPPED));
	error("Valid job states include: %s\n", state_names);
	xfree (state_names);
	return SLURM_ERROR;
}

/*
 * parse_format - Take the user's format specification and use it to build
 *	build the format specifications (internalize it to print.c data
 *	structures)
 * IN format - user's format specification
 * RET zero or error code
 */
extern int parse_format( char* format )
{
	int field_size;
	bool right_justify;
	char *prefix = NULL, *suffix = NULL, *token = NULL;
	char *tmp_char = NULL, *tmp_format = NULL;
	char field[1];
	bool format_all = false;
	int i;

	if (format == NULL) {
		error ("Format option lacks specification.");
		exit( 1 );
	}

	params.format_list = list_create( NULL );
	if ((prefix = _get_prefix(format))) {
		if (params.step_flag) {
			step_format_add_prefix( params.format_list, 0, 0,
						prefix);
		} else {
			job_format_add_prefix( params.format_list, 0, 0,
					       prefix);
		}
	}

	if (!xstrcasecmp(format, "%all")) {
		xstrfmtcat(tmp_format, "%c%c", '%', 'a');
		for (i = 'b'; i <= 'z'; i++)
			xstrfmtcat(tmp_format, "|%c%c", '%', (char) i);
		for (i = 'A'; i <= 'Z'; i++)
			xstrfmtcat(tmp_format, "|%c%c", '%', (char) i);
		format_all = true;
	} else {
		tmp_format = xstrdup(format);
	}

	token = strtok_r( tmp_format, "%", &tmp_char);
	if (token && (format[0] != '%'))	/* toss header */
		token = strtok_r( NULL, "%", &tmp_char );
	while (token) {
		_parse_token( token, field, &field_size, &right_justify,
			      &suffix);
#ifdef __METASTACK_OPT_PRINT_COMMAND
		field_size = abs(field_size);
		 if(getenv("SQUEUE_EXTEND"))
			_format_state_id(field, &field_size, &right_justify, format_all);
#endif
		if (params.step_flag) {
			if      (field[0] == 'A')
				step_format_add_num_tasks( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (field[0] == 'b')
				/* Vestigial option */
				step_format_add_tres_per_node(
							params.format_list,
						  	field_size,
						  	right_justify, suffix );
			else if (field[0] == 'i')
				step_format_add_id( params.format_list,
						    field_size,
						    right_justify, suffix );
			else if (field[0] == 'j')
				step_format_add_name( params.format_list,
						      field_size,
						      right_justify,
						      suffix );
			else if (field[0] == 'l')
				step_format_add_time_limit( params.format_list,
							    field_size,
							    right_justify,
							    suffix );
			else if (field[0] == 'M')
				step_format_add_time_used( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (field[0] == 'N')
				step_format_add_nodes( params.format_list,
						       field_size,
						       right_justify, suffix );
			else if (field[0] == 'P')
				step_format_add_partition( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (field[0] == 'S')
				step_format_add_time_start( params.format_list,
							    field_size,
							    right_justify,
							    suffix );
			else if (field[0] == 'U')
				step_format_add_user_id( params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (field[0] == 'u')
				step_format_add_user_name( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (format_all)
				xfree(suffix);	/* ignore */
			else {
				prefix = xstrdup("%");
				xstrcat(prefix, token);
				xfree(suffix);
				suffix = prefix;

				step_format_add_invalid( params.format_list,
							 field_size,
							 right_justify,
							 suffix );
				error ( "Invalid job step format "
					"specification: %c",
					field[0] );
			}
		} else {
			if (field[0] == 'a')
				job_format_add_account( params.format_list,
							field_size,
							right_justify,
							suffix  );
			else if (field[0] == 'A')
				job_format_add_job_id2(params.format_list,
						       field_size,
						       right_justify,
						       suffix);
			else if (field[0] == 'b')
				/* Vestigial option */
				job_format_add_tres_per_node(params.format_list,
							     field_size,
							     right_justify,
							     suffix );
			else if (field[0] == 'B')
				job_format_add_batch_host( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (field[0] == 'c')
				job_format_add_min_cpus( params.format_list,
							 field_size,
							 right_justify,
							 suffix  );
			else if (field[0] == 'C')
				job_format_add_num_cpus( params.format_list,
							 field_size,
							 right_justify,
							 suffix  );
			else if (field[0] == 'd')
				job_format_add_min_tmp_disk(
					params.format_list,
					field_size,
					right_justify,
					suffix  );
			else if (field[0] == 'D')
				job_format_add_num_nodes( params.format_list,
							  field_size,
							  right_justify,
							  suffix  );
			else if (field[0] == 'e')
				job_format_add_time_end( params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (field[0] == 'E')
				job_format_add_dependency( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (field[0] == 'f')
				job_format_add_features( params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (field[0] == 'F')
				job_format_add_array_job_id(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (field[0] == 'g')
				job_format_add_group_name( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (field[0] == 'G')
				job_format_add_group_id( params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (field[0] == 'h')
				job_format_add_over_subscribe(
						       params.format_list,
						       field_size,
						       right_justify,
						       suffix );
			else if (field[0] == 'H')
				job_format_add_sockets( params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (field[0] == 'i')
				job_format_add_job_id( params.format_list,
						       field_size,
						       right_justify,
						       suffix );
			else if (field[0] == 'I')
				job_format_add_cores( params.format_list,
						      field_size,
						      right_justify, suffix );
			else if (field[0] == 'j')
				job_format_add_name( params.format_list,
						     field_size,
						     right_justify, suffix );
			else if (field[0] == 'J')
				job_format_add_threads( params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (field[0] == 'k')
				job_format_add_comment( params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (field[0] == 'K')
				job_format_add_array_task_id(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (field[0] == 'l')
				job_format_add_time_limit( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (field[0] == 'L')
				job_format_add_time_left( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (field[0] == 'm')
				job_format_add_min_memory( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (field[0] == 'M')
				job_format_add_time_used( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (field[0] == 'n')
				job_format_add_req_nodes( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (field[0] == 'N')
				job_format_add_nodes( params.format_list,
						      field_size,
						      right_justify, suffix );
			else if (field[0] == 'o')
#ifdef __METASTACK_NEW_PENDING_ORDER
			{
				char *field_o = NULL;
				if(suffix && (suffix[0] == 'd')){
					field_o = &suffix[1];
					job_format_add_order( params.format_list,
								field_size,
								right_justify, field_o);
				}else{
					job_format_add_command( params.format_list,
								field_size,
								right_justify, suffix);
				}
			}
#else
				job_format_add_command( params.format_list,
							field_size,
							right_justify, suffix);
#endif
			else if (field[0] == 'O')
				job_format_add_contiguous( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (field[0] == 'p')
				job_format_add_priority( params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (field[0] == 'P')
				job_format_add_partition( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (field[0] == 'q')
				job_format_add_qos( params.format_list,
						    field_size,
						    right_justify,
						    suffix );
			else if (field[0] == 'Q')
				job_format_add_priority_long(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (field[0] == 'r')
				job_format_add_reason( params.format_list,
						       field_size,
						       right_justify,
						       suffix );
			else if (field[0] == 'R')
				job_format_add_reason_list( params.format_list,
							    field_size,
							    right_justify,
							    suffix );
			else if (field[0] == 's')
				job_format_add_select_jobinfo(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (field[0] == 'S')
				job_format_add_time_start( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (field[0] == 't')
				job_format_add_job_state_compact(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (field[0] == 'T')
				job_format_add_job_state( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (field[0] == 'U')
				job_format_add_user_id( params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (field[0] == 'u')
				job_format_add_user_name( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (field[0] == 'v')
				job_format_add_reservation( params.format_list,
							    field_size,
							    right_justify,
							    suffix );
			else if (field[0] == 'V')
				job_format_add_time_submit( params.format_list,
							    field_size,
							    right_justify,
							    suffix );
			else if (field[0] == 'w')
				job_format_add_wckey( params.format_list,
						      field_size,
						      right_justify,
						      suffix );
			else if (field[0] == 'W')
				job_format_add_licenses( params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (field[0] == 'x')
				job_format_add_exc_nodes( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (field[0] == 'X')
				job_format_add_core_spec( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (field[0] == 'y')
				job_format_add_nice( params.format_list,
						     field_size, right_justify,
						     suffix );
			else if (field[0] == 'Y')
				job_format_add_schednodes( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (field[0] == 'z')
				job_format_add_num_sct( params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (field[0] == 'Z')
				job_format_add_work_dir( params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (format_all)
				xfree(suffix);	/* ignore */
			else {
				prefix = xstrdup("%");
				xstrcat(prefix, token);
				xfree(suffix);
				suffix = prefix;

				job_format_add_invalid( params.format_list,
							field_size,
							right_justify,
							suffix );
				error( "Invalid job format specification: %c",
				       field[0] );
			}
		}
		token = strtok_r( NULL, "%", &tmp_char);
	}

	xfree( tmp_format );
	return SLURM_SUCCESS;
}

extern int parse_long_format( char* format_long )
{
	int field_size;
	bool right_justify;
	char *tmp_format = NULL, *token = NULL, *str_tmp = NULL;
	char *sep = NULL;
	char* suffix = NULL;
#ifdef __METASTACK_OPT_PRINT_COMMAND
    char *env_squeue = NULL;
#endif

	if (format_long == NULL) {
		error("Format long option lacks specification");
		exit( 1 );
	}

	params.format_list = list_create(NULL);
	tmp_format = xstrdup(format_long);
	token = strtok_r(tmp_format, ",",&str_tmp);
	while (token) {
		_parse_long_token( token, sep, &field_size, &right_justify,
				   &suffix);

#ifdef __METASTACK_OPT_PRINT_COMMAND
			field_size = abs(field_size);
	   if ((env_squeue = getenv("SQUEUE_EXTEND"))) {
			char *format_sinfo = xstrdup(env_squeue);

			_parse_squeue_format_extend(format_sinfo, token, &field_size, &right_justify);
			if (format_sinfo)
				xfree(format_sinfo);
	    }
#endif
		if (params.step_flag) {

			if (!xstrcasecmp(token, "cluster"))
				step_format_add_cluster_name(params.format_list,
							     field_size,
							     right_justify,
							     suffix);
			else if (!xstrncasecmp(token, "container",
					       strlen("container")))
				step_format_add_container(params.format_list,
							  field_size,
							  right_justify,
							  suffix);
			else if (!xstrncasecmp(token, "numtasks",
					       strlen("numtask")))
				step_format_add_num_tasks( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (!xstrcasecmp(token, "gres"))
				/* Vestigial option */
				step_format_add_tres_per_node(
							params.format_list,
							field_size,
							right_justify, suffix );
			else if (!xstrcasecmp(token, "stepid"))
				step_format_add_id( params.format_list,
						    field_size,
						    right_justify, suffix );
			else if (!xstrcasecmp(token, "stepname"))
				step_format_add_name( params.format_list,
						      field_size,
						      right_justify,
						      suffix );
			else if (!xstrcasecmp(token, "timelimit"))
				step_format_add_time_limit( params.format_list,
							    field_size,
							    right_justify,
							    suffix );
			else if (!xstrcasecmp(token, "timeused"))
				step_format_add_time_used( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (!xstrcasecmp(token, "nodes"))
				step_format_add_nodes( params.format_list,
						       field_size,
						       right_justify,
						       suffix );
			else if (!xstrcasecmp(token, "partition"))
				step_format_add_partition( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (!xstrcasecmp(token, "starttime"))
				step_format_add_time_start( params.format_list,
							    field_size,
							    right_justify,
							    suffix );
			else if (!xstrcasecmp(token, "userid"))
				step_format_add_user_id( params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if  (!xstrcasecmp(token, "username"))
				step_format_add_user_name( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (!xstrcasecmp(token, "arrayjobid"))
				step_format_add_array_job_id(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (!xstrcasecmp(token, "arraytaskid"))
				step_format_add_array_task_id(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if ( !xstrcasecmp(token, "jobid"))
				step_format_add_job_id( params.format_list,
							field_size,
							right_justify,
							suffix );
			else if ( !xstrcasecmp(token, "network"))
				step_format_add_network( params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if ( !xstrcasecmp(token, "numcpus"))
				step_format_add_num_cpus( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if ( !xstrcasecmp(token, "cpufreq"))
				step_format_add_cpu_freq( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if ( !xstrcasecmp(token, "resvports"))
				step_format_add_resv_ports( params.format_list,
							    field_size,
							    right_justify,
							    suffix );
			else if ( !xstrcasecmp(token, "stepstate"))
				step_format_add_step_state( params.format_list,
							    field_size,
							    right_justify,
							    suffix );
			else if (!xstrcasecmp(token, "cpus-per-tres"))
				step_format_add_cpus_per_tres(params.format_list,
							      field_size,
							      right_justify,
							     suffix );
			else if (!xstrcasecmp(token, "mem-per-tres"))
				step_format_add_mem_per_tres(params.format_list,
							     field_size,
							     right_justify,
							     suffix );
			else if (!xstrcasecmp(token, "tres-bind"))
				step_format_add_tres_bind(params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "tres-freq"))
				step_format_add_tres_freq(params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "tres-per-job") ||
				 !xstrcasecmp(token, "tres-per-step"))
				step_format_add_tres_per_step(params.format_list,
							     field_size,
							     right_justify,
							      suffix );
			else if (!xstrcasecmp(token, "tres-per-node"))
				step_format_add_tres_per_node(
							params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (!xstrcasecmp(token, "tres-per-socket"))
				step_format_add_tres_per_socket(
							params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (!xstrcasecmp(token, "tres-per-task"))
				step_format_add_tres_per_task(
							params.format_list,
							field_size,
							right_justify,
							suffix );
			else {
				step_format_add_invalid( params.format_list,
							 field_size,
							 right_justify,
							 suffix );
				error ( "Invalid job step format "
					"specification: %s",
					token );
			}

		} else {

			if (!xstrcasecmp(token,"account"))
				job_format_add_account( params.format_list,
							field_size,
							right_justify,
							suffix  );
			else if (!xstrcasecmp(token, "container"))
				job_format_add_container(params.format_list,
						       field_size,
						       right_justify,
						       suffix );
			else if (!xstrcasecmp(token, "jobid"))
				job_format_add_job_id2(params.format_list,
						       field_size,
						       right_justify,
						       suffix );
			else if (!xstrcasecmp(token, "gres"))
				/* Vestigial option */
				job_format_add_tres_per_node(params.format_list,
							     field_size,
							     right_justify,
							     suffix );
			else if (!xstrcasecmp(token,"batchhost"))
				job_format_add_batch_host(params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "burstbuffer"))
				job_format_add_burst_buffer(params.format_list,
							    field_size,
							    right_justify,
							    suffix);
			else if (!xstrcasecmp(token, "burstbufferstate"))
				job_format_add_burst_buffer_state(
							params.format_list,
							field_size,
							right_justify,
							suffix);
			else if (!xstrcasecmp(token, "cluster"))
				job_format_add_cluster_name(params.format_list,
							    field_size,
							    right_justify,
							    suffix);
			else if (!xstrcasecmp(token, "delayboot"))
				job_format_add_delay_boot(params.format_list,
							  field_size,
							  right_justify,
							  suffix);
			else if (!xstrcasecmp(token,"mincpus"))
				job_format_add_min_cpus( params.format_list,
							 field_size,
							 right_justify,
							 suffix  );
			else if (!xstrcasecmp(token,"numcpus"))
				job_format_add_num_cpus( params.format_list,
							 field_size,
							 right_justify,
							 suffix  );
			else if (!xstrcasecmp(token, "mintmpdisk"))
				job_format_add_min_tmp_disk(
					params.format_list,
					field_size,
					right_justify,
					suffix  );
			else if (!xstrcasecmp(token, "numnodes"))
				job_format_add_num_nodes( params.format_list,
							  field_size,
							  right_justify,
							  suffix  );
			else if (!xstrncasecmp(token, "numtasks",
					       strlen("numtask")))
				job_format_add_num_tasks( params.format_list,
							  field_size,
							  right_justify,
							  suffix  );
			else if (!xstrcasecmp(token, "endtime"))
				job_format_add_time_end( params.format_list,
							 field_size,
							 right_justify,
							 suffix );

			else if (!xstrcasecmp(token, "dependency"))
				job_format_add_dependency( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (!xstrcasecmp(token, "feature"))
				job_format_add_features( params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (!xstrcasecmp(token, "clusterfeature"))
				job_format_add_cluster_features(
							params.format_list,
							field_size,
							right_justify, suffix);
			else if (!xstrcasecmp(token, "prefer"))
				job_format_add_prefer(params.format_list,
						      field_size,
						      right_justify,
						      suffix);
			else if (!xstrcasecmp(token, "arrayjobid"))
				job_format_add_array_job_id(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (!xstrcasecmp(token,"groupname"))
				job_format_add_group_name( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (!xstrcasecmp(token,"groupid"))
				job_format_add_group_id( params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (!xstrcasecmp(token, "oversubscribe") ||
				 !xstrcasecmp(token, "shared"))
				job_format_add_over_subscribe(
						       params.format_list,
						       field_size,
						       right_justify,
						       suffix );

			else if (!xstrcasecmp(token, "sockets"))
				job_format_add_sockets( params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (!xstrcasecmp(token,"jobarrayid"))
				job_format_add_job_id( params.format_list,
						       field_size,
						       right_justify,
						       suffix );
			else if (!xstrcasecmp(token, "cores"))
				job_format_add_cores( params.format_list,
						      field_size,
						      right_justify, suffix );
			else if (!xstrcasecmp(token, "name"))
				job_format_add_name( params.format_list,
						     field_size,
						     right_justify, suffix );
			else if (!xstrcasecmp(token, "threads"))
				job_format_add_threads( params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (!xstrcasecmp(token, "admin_comment"))
				job_format_add_admin_comment( params.format_list,
							      field_size,
							      right_justify,
							      suffix );
			else if (!xstrcasecmp(token, "system_comment"))
				job_format_add_system_comment(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (!xstrcasecmp(token, "comment"))
				job_format_add_comment( params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (!xstrcasecmp(token, "arraytaskid"))
				job_format_add_array_task_id(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (!xstrcasecmp(token, "timelimit"))
				job_format_add_time_limit( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (!xstrcasecmp(token, "timeleft"))
				job_format_add_time_left( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "minmemory"))
				job_format_add_min_memory( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (!xstrcasecmp(token,"timeused"))
				job_format_add_time_used( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "reqnodes"))
				job_format_add_req_nodes( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "nodelist"))
				job_format_add_nodes( params.format_list,
						      field_size,
						      right_justify, suffix );
#ifdef __METASTACK_NEW_PENDING_ORDER
			else if (!xstrcasecmp(token, "order"))
				job_format_add_order( params.format_list,
							field_size,
							right_justify, suffix);
#endif
			else if (!xstrcasecmp(token, "command"))
				job_format_add_command( params.format_list,
							field_size,
							right_justify, suffix);
			else if (!xstrcasecmp(token, "contiguous"))
				job_format_add_contiguous( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (!xstrcasecmp(token, "priority"))
				job_format_add_priority( params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (!xstrcasecmp(token, "partition"))
				job_format_add_partition( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "qos"))
				job_format_add_qos( params.format_list,
						    field_size,
						    right_justify,
						    suffix );
			else if (!xstrcasecmp(token, "prioritylong"))
				job_format_add_priority_long(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (!xstrcasecmp(token, "reason"))
				job_format_add_reason( params.format_list,
						       field_size,
						       right_justify,
						       suffix );
			else if (!xstrcasecmp(token, "reasonlist"))
				job_format_add_reason_list( params.format_list,
							    field_size,
							    right_justify,
							    suffix );
#ifdef __METASTACK_OPT_MSG_OUTPUT
            else if (!xstrcasecmp(token, "reasondetail"))
				job_format_add_reason_detail( params.format_list,
						       field_size,
						       right_justify,
						       suffix );
#endif
			else if (!xstrcasecmp(token, "selectjobinfo"))
				job_format_add_select_jobinfo(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (!xstrcasecmp(token, "starttime"))
				job_format_add_time_start( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (!xstrcasecmp(token, "lastschedeval"))
				job_format_add_job_last_sched_eval(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (!xstrcasecmp(token, "statecompact"))
				job_format_add_job_state_compact(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (!xstrcasecmp(token, "state"))
				job_format_add_job_state( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "userid"))
				job_format_add_user_id( params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (!xstrcasecmp(token, "username"))
				job_format_add_user_name( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "reservation"))
				job_format_add_reservation( params.format_list,
							    field_size,
							    right_justify,
							    suffix );
			else if (!xstrcasecmp(token, "submittime"))
				job_format_add_time_submit( params.format_list,
							    field_size,
							    right_justify,
							    suffix );
			else if (!xstrcasecmp(token, "pendingtime"))
				job_format_add_time_pending(params.format_list,
							    field_size,
							    right_justify,
							    suffix);
			else if (!xstrcasecmp(token, "wckey"))
				job_format_add_wckey( params.format_list,
						      field_size,
						      right_justify,
						      suffix );
			else if (!xstrcasecmp(token, "licenses"))
				job_format_add_licenses( params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (!xstrcasecmp(token, "corespec"))
				job_format_add_core_spec( params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "nice"))
				job_format_add_nice( params.format_list,
						     field_size, right_justify,
						     suffix );
			else if (!xstrcasecmp(token, "schednodes"))
				job_format_add_schednodes( params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (!xstrcasecmp(token, "sct"))
				job_format_add_num_sct( params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (!xstrcasecmp(token, "workdir"))
				job_format_add_work_dir( params.format_list,
							 field_size,
							 right_justify,
							 suffix );

			else if (!xstrcasecmp(token, "accruetime"))
				job_format_add_accrue_time(params.format_list,
							   field_size,
							   right_justify,
							   suffix);
			else if (!xstrcasecmp(token, "allocnodes"))
				job_format_add_alloc_nodes( params.format_list,
							    field_size,
							    right_justify,
							    suffix );
			else if (!xstrcasecmp(token, "allocsid"))
				job_format_add_alloc_sid(params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (!xstrcasecmp(token,"associd"))
				job_format_add_assoc_id(params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (!xstrcasecmp(token, "batchflag"))
				job_format_add_batch_flag(params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "boardspernode"))
				job_format_add_boards_per_node(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (!xstrcasecmp(token, "cpuspertask") ||
				 !xstrcasecmp(token, "cpus-per-task"))
				job_format_add_cpus_per_task(params.format_list,
							     field_size,
							     right_justify,
							     suffix );

			else if (!xstrcasecmp(token, "derivedec"))
				job_format_add_derived_ec(params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "eligibletime"))
				job_format_add_eligible_time(params.format_list,
							     field_size,
							     right_justify,
							     suffix );
			else if (!xstrcasecmp(token, "exit_code"))
				job_format_add_exit_code(params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (!xstrcasecmp(token, "origin"))
				job_format_add_fed_origin(params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "originraw"))
				job_format_add_fed_origin_raw(
							params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (!xstrcasecmp(token, "siblingsactive"))
				job_format_add_fed_siblings_active(
							params.format_list,
							field_size,
							right_justify, suffix );
			else if (!xstrcasecmp(token, "siblingsactiveraw"))
				job_format_add_fed_siblings_active_raw(
							params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (!xstrcasecmp(token, "siblingsviable"))
				job_format_add_fed_siblings_viable(
							params.format_list,
							field_size,
							right_justify, suffix );
			else if (!xstrcasecmp(token, "siblingsviableraw"))
				job_format_add_fed_siblings_viable_raw(
							params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (!xstrcasecmp(token, "maxcpus"))
				job_format_add_max_cpus(params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (!xstrcasecmp(token, "maxnodes"))
				job_format_add_max_nodes(params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (!xstrcasecmp(token, "network"))
				job_format_add_network(params.format_list,
						       field_size,
						       right_justify,
						       suffix );
			else if (!xstrcasecmp(token, "ntpercore"))
				job_format_add_ntasks_per_core(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (!xstrcasecmp(token, "ntpernode"))
				job_format_add_ntasks_per_node(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (!xstrcasecmp(token, "ntpersocket"))
				job_format_add_ntasks_per_socket(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (!xstrcasecmp(token, "ntperboard"))
				job_format_add_ntasks_per_board(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (!xstrcasecmp(token, "preempttime"))
				job_format_add_preempt_time(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (!xstrcasecmp(token, "profile"))
				job_format_add_profile(params.format_list,
						       field_size,
						       right_justify,
						       suffix );
			else if (!xstrcasecmp(token, "reboot"))
				job_format_add_reboot(params.format_list,
						      field_size,
						      right_justify,
						      suffix );
			else if (!xstrcasecmp(token, "reqswitch"))
				job_format_add_req_switch(params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "requeue"))
				job_format_add_requeue(params.format_list,
						       field_size,
						       right_justify,
						       suffix );
			else if (!xstrcasecmp(token, "resizetime"))
				job_format_add_resize_time(params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (!xstrcasecmp(token, "restartcnt"))
				job_format_add_restart_cnt(params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (!xstrcasecmp(token, "sperboard"))
				job_format_add_sockets_per_board(
					params.format_list,
					field_size,
					right_justify,
					suffix );
			else if (!xstrcasecmp(token, "stderr"))
				job_format_add_std_err(params.format_list,
						       field_size,
						       right_justify,
						       suffix );
			else if (!xstrcasecmp(token, "stdin"))
				job_format_add_std_in(params.format_list,
						      field_size,
						      right_justify,
						      suffix );
			else if (!xstrcasecmp(token, "stdout"))
				job_format_add_std_out(params.format_list,
						       field_size,
						       right_justify,
						       suffix );
			else if (!xstrcasecmp(token, "mintime"))
				job_format_add_min_time(params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (!xstrcasecmp(token, "wait4switch"))
				job_format_add_wait4switch(params.format_list,
							   field_size,
							   right_justify,
							   suffix );
			else if (!xstrcasecmp(token, "cpus-per-tres"))
				job_format_add_cpus_per_tres(params.format_list,
							     field_size,
							     right_justify,
							     suffix );
			else if (!xstrcasecmp(token, "mem-per-tres"))
				job_format_add_mem_per_tres(params.format_list,
							    field_size,
							    right_justify,
							    suffix );
			else if (!xstrcasecmp(token, "tres") ||
				 !xstrcasecmp(token, "tres-alloc"))
				job_format_add_tres_alloc(params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "tres-bind"))
				job_format_add_tres_bind(params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (!xstrcasecmp(token, "tres-freq"))
				job_format_add_tres_freq(params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (!xstrcasecmp(token, "tres-per-job"))
				job_format_add_tres_per_job(params.format_list,
							    field_size,
							    right_justify,
							    suffix );
			else if (!xstrcasecmp(token, "tres-per-node"))
				job_format_add_tres_per_node(params.format_list,
							     field_size,
							     right_justify,
							     suffix );
			else if (!xstrcasecmp(token, "tres-per-socket"))
				job_format_add_tres_per_socket(
							params.format_list,
							field_size,
							right_justify,
							suffix );
			else if (!xstrcasecmp(token, "tres-per-task"))
				job_format_add_tres_per_task(params.format_list,
							     field_size,
							     right_justify,
							     suffix );
			else if (!xstrcasecmp(token, "mcslabel"))
				job_format_add_mcs_label(params.format_list,
							 field_size,
							 right_justify,
							 suffix );
			else if (!xstrcasecmp(token, "deadline"))
				job_format_add_deadline(params.format_list,
							field_size,
							right_justify,
							suffix );
			/* Maintaining "pack*" for retrocompatibility */
			else if (!xstrcasecmp(token, "packjobid") ||
				 !xstrcasecmp(token, "hetjobid"))
				job_format_add_het_job_id(params.format_list,
							  field_size,
							  right_justify,
							  suffix );
			else if (!xstrcasecmp(token, "packjoboffset") ||
				 !xstrcasecmp(token, "hetjoboffset"))
				job_format_add_het_job_offset(params.format_list,
							      field_size,
							      right_justify,
							      suffix );
			else if (!xstrcasecmp(token, "packjobidset") ||
				 !xstrcasecmp(token, "hetjobidset"))
				job_format_add_het_job_id_set(params.format_list,
							      field_size,
							      right_justify,
							      suffix );
			else {
				job_format_add_invalid( params.format_list,
							field_size,
							right_justify,
							suffix );
				error( "Invalid job format specification: %s",
				       token );
			}
		}

		token = strtok_r(NULL, ",", &str_tmp);
	}

	xfree(tmp_format);
	return SLURM_SUCCESS;
}


/* Take a format specification and copy out it's prefix
 * IN/OUT token - input specification, everything before "%" is removed
 * RET - everything before "%" in the token
 */
static char *
_get_prefix( char *token )
{
	char *pos, *prefix;

	if (token == NULL)
		return NULL;

	pos = strchr(token, (int) '%');
	if (pos == NULL)	/* everything is prefix */
		return xstrdup(token);
	if (pos == token)	/* no prefix */
		return NULL;

	pos[0] = '\0';		/* some prefix */
	prefix = xstrdup(token);
	pos[0] = '%';
	memmove(token, pos, (strlen(pos)+1));
	return prefix;
}


/* Take a format specification and break it into its components
 * IN token - input specification without leading "%", eg. ".5u"
 * OUT field - the letter code for the data type
 * OUT field_size - byte count
 * OUT right_justify - true of field to be right justified
 * OUT suffix - string containing everything after the field specification
 */
static void
_parse_token( char *token, char *field, int *field_size, bool *right_justify,
	      char **suffix)
{
	int i = 0;
#ifdef __METASTACK_OPT_PRINT_COMMAND
	params.format_field_flag = false;
	params.right_flag = true;
#endif
	xassert(token);

	if (token[i] == '.') {
		*right_justify = true;
		i++;
	} else
		*right_justify = false;

	*field_size = 0;
	while ((token[i] >= '0') && (token[i] <= '9'))
#ifdef __METASTACK_OPT_PRINT_COMMAND
    {
		*field_size = (*field_size * 10) + (token[i++] - '0');
		if(params.format_flag && (*field_size > 0))
 	    	params.format_field_flag = true;
    }
#else
		*field_size = (*field_size * 10) + (token[i++] - '0');
#endif
	field[0] = token[i++];

	*suffix = xstrdup(&token[i]);
}

static void
_parse_long_token( char *token, char *sep, int *field_size, bool *right_justify,
		   char **suffix)
{
	char *end_ptr = NULL, *ptr;
#ifdef __METASTACK_OPT_PRINT_COMMAND
	params.format_field_flag = true;
	params.right_flag = false;
#endif
	*suffix = NULL;
	xassert(token);
	ptr = strchr(token, ':');
	if (ptr) {
#ifdef __METASTACK_OPT_PRINT_COMMAND
	params.right_flag = true;
#endif
		ptr[0] = '\0';
		if (ptr[1] == '.') {
			*right_justify = true;
			ptr++;
		} else {
			*right_justify = false;
		}
		*field_size = strtol(ptr + 1, &end_ptr, 10);
		if (end_ptr[0] != '\0')
			*suffix = xstrdup(end_ptr);
	} else {
		*right_justify = false;
		*field_size = 20;
#ifdef __METASTACK_OPT_PRINT_COMMAND
      	 params.format_field_flag = false;
#endif
	}
}

/* print the parameters specified */
static void
_print_options(void)
{
	ListIterator iterator;
	int i;
	char *license, *name, *part;
	uint32_t *user;
	uint32_t *state_id;
	squeue_job_step_t *job_step_id;
	char hostlist[8192];

	if (params.nodes) {
		hostset_ranged_string(params.nodes, sizeof(hostlist)-1,
				      hostlist);
	} else
		hostlist[0] = '\0';

	printf( "-----------------------------\n" );
	printf( "all         = %s\n", params.all_flag ? "true" : "false");
	printf( "array       = %s\n", params.array_flag ? "true" : "false");
	printf( "federation  = %s\n", params.federation_flag ? "true":"false");
	printf( "format      = %s\n", params.format );
	printf( "iterate     = %d\n", params.iterate );
	printf( "job_flag    = %d\n", params.job_flag );
	printf( "jobs        = %s\n", params.jobs );
	printf( "licenses    = %s\n", params.licenses );
	printf( "local       = %s\n", params.local_flag ? "true" : "false");
	printf( "names       = %s\n", params.names );
	printf( "nodes       = %s\n", hostlist ) ;
	printf( "partitions  = %s\n", params.partitions ) ;
	printf( "priority    = %s\n", params.priority_flag ? "true" : "false");
	printf( "reservation = %s\n", params.reservation ) ;
	printf( "sibling      = %s\n", params.sibling_flag ? "true" : "false");
	printf( "sort        = %s\n", params.sort ) ;
	printf( "start_flag  = %d\n", params.start_flag );
	printf( "states      = %s\n", params.states ) ;
	printf( "step_flag   = %d\n", params.step_flag );
	printf( "steps       = %s\n", params.steps );
	printf( "users       = %s\n", params.users );
	printf( "verbose     = %d\n", params.verbose );

	if (params.verbose <= 1)
		goto endit;
	if (params.job_list) {
		i = 0;
		iterator = list_iterator_create( params.job_list );
		while ( (job_step_id = list_next( iterator )) ) {
			if (job_step_id->array_id == NO_VAL) {
				printf( "job_list[%d] = %u\n", i++,
					job_step_id->step_id.job_id );
			} else {
				printf( "job_list[%d] = %u_%u\n", i++,
					job_step_id->step_id.job_id,
					job_step_id->array_id );
			}
		}
		list_iterator_destroy( iterator );
	}


	if (params.name_list) {
		i = 0;
		iterator = list_iterator_create( params.name_list );
		while ( (name = list_next( iterator )) ) {
			printf( "name_list[%d] = %u\n", i++, *name);
		}
		list_iterator_destroy( iterator );
	}

	if (params.licenses_list) {
		i = 0;
		iterator = list_iterator_create( params.licenses_list );
		while ( (license = list_next( iterator )) ) {
			printf( "licenses_list[%d] = %s\n", i++, license);
		}
		list_iterator_destroy( iterator );
	}

	if (params.part_list) {
		i = 0;
		iterator = list_iterator_create( params.part_list );
		while ( (part = list_next( iterator )) ) {
			printf( "part_list[%d] = %s\n", i++, part);
		}
		list_iterator_destroy( iterator );
	}

	if (params.all_states) {
		printf( "state_list = all\n");
	} else if (params.state_list) {
		i = 0;
		iterator = list_iterator_create( params.state_list );
		while ( (state_id = list_next( iterator )) ) {
			printf( "state_list[%d] = %s\n",
				i++, job_state_string( *state_id ));
		}
		list_iterator_destroy( iterator );
	}

	if (params.step_list) {
		char tmp_char[34];
		i = 0;
		iterator = list_iterator_create( params.step_list );
		while ( (job_step_id = list_next( iterator )) ) {
			if (job_step_id->array_id == NO_VAL) {
				log_build_step_id_str(&job_step_id->step_id,
						      tmp_char,
						      sizeof(tmp_char),
						      STEP_ID_FLAG_NO_PREFIX);
				printf( "step_list[%d] = %s\n", i++,
					tmp_char);
			} else {
				log_build_step_id_str(&job_step_id->step_id,
						      tmp_char,
						      sizeof(tmp_char),
						      (STEP_ID_FLAG_NO_PREFIX |
						       STEP_ID_FLAG_NO_JOB));
				printf( "step_list[%d] = %u_%u.%s\n", i++,
					job_step_id->step_id.job_id,
					job_step_id->array_id,
					tmp_char);
			}
		}
		list_iterator_destroy( iterator );
	}

	if (params.user_list) {
		i = 0;
		iterator = list_iterator_create( params.user_list );
		while ( (user = list_next( iterator )) ) {
			printf( "user_list[%d] = %u\n", i++, *user);
		}
		list_iterator_destroy( iterator );
	}
endit:
	printf( "-----------------------------\n\n\n" );
} ;


/*
 * _build_job_list- build a list of job_ids
 * IN str - comma separated list of job_ids
 * RET List of job_ids (uint32_t)
 */
static List
_build_job_list( char* str )
{
	List my_list;
	char *end_ptr = NULL, *job = NULL, *tmp_char = NULL;
	char *my_job_list = NULL;
	int job_id, array_id;
	squeue_job_step_t *job_step_id;

	if ( str == NULL )
		return NULL;
	my_list = list_create( NULL );
	my_job_list = xstrdup( str );
	job = strtok_r( my_job_list, ",", &tmp_char );
	while (job) {
		job_id = strtol( job, &end_ptr, 10 );
		if (end_ptr[0] == '_')
			array_id = strtol( end_ptr + 1, &end_ptr, 10 );
		else
			array_id = NO_VAL;
		if (job_id <= 0) {
			error( "Invalid job id: %s", job );
			exit( 1 );
		}

		job_step_id = xmalloc( sizeof( squeue_job_step_t ) );
		job_step_id->step_id.job_id   = job_id;
		job_step_id->array_id = array_id;
		list_append( my_list, job_step_id );
		job = strtok_r (NULL, ",", &tmp_char);
	}
	xfree(my_job_list);
	return my_list;
}

/*
 * _build_str_list - convert a string of comma-separated elements
 *		     into a list of strings
 * IN str - comma separated list of strings
 * RET List of strings
 */
static List
_build_str_list(char* str)
{
	List my_list;
	char *elem, *tok = NULL, *tmp_char = NULL, *my_str = NULL;

	if (str == NULL)
		return NULL;
	my_list = list_create(NULL);
	my_str = xstrdup(str);
	tok = strtok_r(my_str, ",", &tmp_char);
	while (tok) {
		elem = xstrdup(tok);
		list_append(my_list, elem);
		tok = strtok_r(NULL, ",", &tmp_char);
	}
	xfree(my_str);
	return my_list;
}

/*
 * _build_state_list - build a list of job states
 * IN str - comma separated list of job states
 * RET List of enum job_states values
 */
static List
_build_state_list( char* str )
{
	List my_list;
	char *state = NULL, *tmp_char = NULL, *my_state_list = NULL;
	uint32_t *state_id = NULL;

	if (str == NULL)
		return NULL;
	if (!xstrcasecmp(str, "all")) {
		params.all_states = true;
		return NULL;
	}
	params.all_states = false;

	my_list = list_create(NULL);
	my_state_list = xstrdup(str);
	state = strtok_r( my_state_list, ",", &tmp_char );
	while (state) {
		state_id = xmalloc(sizeof(uint32_t));
		if (_parse_state(state, state_id) != SLURM_SUCCESS)
			exit(1);
		list_append(my_list, state_id);
		state = strtok_r(NULL, ",", &tmp_char);
	}
	xfree(my_state_list);
	return my_list;

}

/*
 * _build_step_list- build a list of job/step_ids
 * IN str - comma separated list of job_id[array_id].step_id values
 * RET List of job/step_ids (structure of uint32_t's)
 */
static List
_build_step_list( char* str )
{
	List my_list;
	char *end_ptr = NULL, *step = NULL, *tmp_char = NULL, *tmps_char = NULL;
	char *job_name = NULL, *step_name = NULL, *my_step_list = NULL;
	int job_id, array_id, step_id;
	squeue_job_step_t *job_step_id = NULL;

	if (str == NULL)
		return NULL;

	my_list = list_create(NULL);
	my_step_list = xstrdup(str);
	step = strtok_r(my_step_list, ",", &tmp_char);
	while (step) {
		job_name = strtok_r(step, ".", &tmps_char);
		if (job_name == NULL)
			break;
		step_name = strtok_r(NULL, ".", &tmps_char);
		job_id = strtol(job_name, &end_ptr, 10);
		if (end_ptr[0] == '_')
			array_id = strtol(end_ptr + 1, &end_ptr, 10);
		else
			array_id = NO_VAL;
		if (step_name == NULL) {
			error("Invalid job_step id: %s.??", job_name);
			exit(1);
		}
		step_id = strtol( step_name, &end_ptr, 10 );
		if ((job_id <= 0) || (step_id < 0)) {
			error("Invalid job_step id: %s.%s",
			      job_name, step_name);
			exit(1);
		}
		job_step_id = xmalloc(sizeof(squeue_job_step_t));
		job_step_id->step_id.job_id   = job_id;
		job_step_id->array_id = array_id;
		job_step_id->step_id.step_id  = step_id;
		list_append(my_list, job_step_id);
		step = strtok_r(NULL, ",", &tmp_char);
	}
	xfree(my_step_list);
	return my_list;
}

/*
 * _build_user_list- build a list of UIDs
 * IN str - comma separated list of user names
 * RET List of UIDs (uint32_t)
 */
static List
_build_user_list( char* str )
{
	List my_list;
	char *user = NULL;
	char *tmp_char = NULL, *my_user_list = NULL;

	if (str == NULL)
		return NULL;

	my_list = list_create(NULL);
	my_user_list = xstrdup(str);
	user = strtok_r(my_user_list, ",", &tmp_char);
	while (user) {
		uid_t some_uid;
		if (uid_from_string(user, &some_uid) == 0) {
			uint32_t *user_id = NULL;
			user_id = xmalloc(sizeof(uint32_t));
			*user_id = (uint32_t) some_uid;
			list_append(my_list, user_id);
		} else {
			error("Invalid user: %s\n", user);
		}
		user = strtok_r(NULL, ",", &tmp_char);
	}
	xfree(my_user_list);
	return my_list;
}

static void _usage(void)
{
	printf("\
Usage: squeue [-A account] [--clusters names] [-i seconds] [--job jobid]\n\
              [-n name] [-o format] [-p partitions] [--qos qos]\n\
              [--reservation reservation] [--sort fields] [--start]\n\
              [--step step_id] [-t states] [-u user_name] [--usage]\n\
              [-L licenses] [-w nodes] [--federation] [--local] [--sibling]\n\
	      [-ahjlrsv]\n");
}

static void _help(void)
{
	printf("\
Usage: squeue [OPTIONS]\n\
  -A, --account=account(s)        comma separated list of accounts\n\
				  to view, default is all accounts\n\
  -a, --all                       display jobs in hidden partitions\n\
      --array-unique              display one unique pending job array\n\
                                  element per line\n\
      --cache                     retrieve job information from the cache\n\
      --federation                Report federated information if a member\n\
                                  of one\n\
  -h, --noheader                  no headers on output\n\
      --hide                      do not display jobs in hidden partitions\n\
  -i, --iterate=seconds           specify an interation period\n\
  -j, --job=job(s)                comma separated list of jobs IDs\n\
                                  to view, default is all\n\
      --json                      Produce JSON output\n\
      --local                     Report information only about jobs on the\n\
                                  local cluster. Overrides --federation.\n\
  -l, --long                      long report\n\
  -L, --licenses=(license names)  comma separated list of license names to view\n\
  -M, --clusters=cluster_name     cluster to issue commands to.  Default is\n\
                                  current cluster.  cluster with no name will\n\
                                  reset to default. Implies --local.\n\
  -n, --name=job_name(s)          comma separated list of job names to view\n\
      --nocache                   retrieve job information from the source data\n\
      --noconvert                 don't convert units from their original type\n\
                                  (e.g. 2048M won't be converted to 2G).\n\
  -o, --format=format             format specification\n\
  -O, --Format=format             format specification\n\
  -p, --partition=partition(s)    comma separated list of partitions\n\
				  to view, default is all partitions\n\
  -q, --qos=qos(s)                comma separated list of qos's\n\
				  to view, default is all qos's\n\
  -R, --reservation=name          reservation to view, default is all\n\
  -r, --array                     display one job array element per line\n\
      --sibling                   Report information about all sibling jobs\n\
                                  on a federated cluster. Implies --federation.\n\
  -s, --step=step(s)              comma separated list of job steps\n\
				  to view, default is all\n\
  -S, --sort=fields               comma separated list of fields to sort on\n\
      --start                     print expected start times of pending jobs\n\
  -t, --states=states             comma separated list of states to view,\n\
				  default is pending and running,\n\
				  '--states=all' reports all states\n\
  -u, --user=user_name(s)         comma separated list of users to view\n\
      --name=job_name(s)          comma separated list of job names to view\n\
  -v, --verbose                   verbosity level\n\
  -V, --version                   output version information and exit\n\
  -w, --nodelist=hostlist         list of nodes to view, default is \n\
				  all nodes\n\
      --yaml                      Produce YAML output\n\
\nHelp options:\n\
  --help                          show this help message\n\
  --usage                         display a brief summary of squeue options\n");
}

/*
 * Validate and assign filtered nodes to params.nodes.
 */
static void _filter_nodes(void)
{
	char *name = NULL, *nodename = NULL;
	hostset_t nodenames = hostset_create(NULL);
	List clusters_nodes = NULL;

	/* Retrieve node_info from controllers */
	if (!(clusters_nodes = _load_clusters_nodes()))
		exit(1);

	/* Map all node names specified with -w, if known to any controller. */
	while ((name = hostset_shift(params.nodes))) {
		if (!(nodename = _map_node_name(clusters_nodes, name))) {
			free(name);
			hostset_destroy(params.nodes);
			FREE_NULL_LIST(clusters_nodes);
			exit(1);
		}
		hostset_insert(nodenames, nodename);
		free(name);
		xfree(nodename);
	}
	FREE_NULL_LIST(clusters_nodes);

	/* Replace params.nodes with the new one */
	hostset_destroy(params.nodes);
	params.nodes = nodenames;
}

/*
 * ListDelF for a list of node_info_msg_t.
 */
static void _node_info_list_del(void *data)
{
	node_info_msg_t *node_info_ptr = data;

	slurm_free_node_info_msg(node_info_ptr);
}

/*
 * Retrieve node_info_msg_t for params.clusters or just local cluster.
 * RET: List of all needed node_info_msg_t or NULL if any fail
 *
 * NOTE: caller must free the returned list if not NULL.
 */
static List _load_clusters_nodes(void)
{
	List node_info_list = NULL;
	ListIterator iter = NULL;
	node_info_msg_t *node_info = NULL;

	node_info_list = list_create(_node_info_list_del);

	if (params.clusters)
		iter = list_iterator_create(params.clusters);

	do {
		if (slurm_load_node(0, &node_info, SHOW_ALL)) {
			slurm_perror("slurm_load_node error");
			FREE_NULL_LIST(node_info_list);
			break;
		}

		list_append(node_info_list, node_info);
	} while (params.clusters && (working_cluster_rec = list_next(iter)));

	/*
	 * Don't need to reset working_cluster_rec here. Nobody uses it in
	 * parse_command_line(), and it's already reset later in main().
	 */
	if (params.clusters)
		list_iterator_destroy(iter);

	return node_info_list;
}

/*
 * Map name into NodeName, and handle the special "localhost" case.
 * IN: pointer to an array of pointers to node_info_msg_t
 * IN: input node name
 * RET: mapped node name if valid, NULL otherwise
 *
 * NOTE: caller must xfree() the returned name.
 */
static char *_map_node_name(List clusters_node_info, char *name)
{
	char *nodename = NULL;
	node_info_msg_t *node_info;
	ListIterator node_info_itr;

	if (!name)
		return NULL;

	/* localhost = use current host name */
	if (!xstrcasecmp("localhost", name)) {
		nodename = xmalloc(128);
		gethostname_short(nodename, 128);
	} else
		nodename = xstrdup(name);

	node_info_itr = list_iterator_create(clusters_node_info);

	while ((node_info = list_next(node_info_itr))) {
		for (int cc = 0; cc < node_info->record_count; cc++) {
			/*
			 * This can happen if the host is removed from DNS but
			 * still in slurm.conf
			 */
			if (!node_info->node_array[cc].name)
				continue;
			if (!xstrcmp(nodename,
				     node_info->node_array[cc].name) ||
			    !xstrcmp(nodename,
				     node_info->node_array[cc].node_hostname)) {
				xfree(nodename);
				list_iterator_destroy(node_info_itr);
				return xstrdup(node_info->node_array[cc].name);
			}
		}
	}

	error("Invalid node name %s", name);
	xfree(nodename);
	list_iterator_destroy(node_info_itr);
	return NULL;
}
