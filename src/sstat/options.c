/*****************************************************************************\
 *  options.c - option functions for sstat
 *****************************************************************************
 *  Copyright (C) 2006 The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Danny Auble <da@llnl.gov>.
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

#include "src/common/read_config.h"
#include "src/common/proc_args.h"
#include "sstat.h"
#include <time.h>

/* getopt_long options, integers but not characters */
#define OPT_LONG_NOCONVERT 0x100

void _help_fields_msg(void);
void _help_msg(void);
void _usage(void);
void _init_params();

void _help_fields_msg(void)
{
	int i;

	for (i = 0; fields[i].name; i++) {
		if (i & 3)
			printf(" ");
		else if (i)
			printf("\n");
		printf("%-19s", fields[i].name);
	}
	printf("\n");
	return;
}

void _help_msg(void)
{
	printf("\
sstat [<OPTION>] -j <job(.stepid)>                                          \n\
    Valid <OPTION> values are:                                              \n\
      -a, --allsteps:                                                       \n\
                   Print all steps for the given job(s) when no step is     \n\
                   specified.                                               \n\
      -d,          The resource consumption information and abnormal events \n\
                   of each job step are displayed in units of job steps.	\n\
                   If no anomaly detection parameter is specified when the  \n\
                   job is submitted, or slurm.conf is not configured        \n\
                   (see --job-monitor for details), the data displayed      \n\
                   will be empty                                            \n\
      -e, --helpformat:                                                     \n\
	           Print a list of fields that can be specified with the    \n\
	           '--format' option                                        \n\
      -h, --help:   Print this description of use.                           \n\
      -i, --pidformat:                                                       \n\
                   Predefined format to list the pids running for each      \n\
                   job step.  (JobId,Nodes,Pids)                            \n\
      -j, --jobs:                                                            \n\
	           Format is <job(.step)>. Stat this job step               \n\
                   or comma-separated list of job steps. This option is     \n\
                   required.  The step portion will default to the lowest   \n\
                   numbered (not batch, extern, etc) step running if not    \n\
                   specified, unless the --allsteps flag is set where not   \n\
                   specifying a step will result in all running steps       \n\
                   to be displayed. A step id of 'batch' will display the   \n\
                   information about the batch step. A step id of 'extern'  \n\
                   will display the information about the extern step       \n\
                   when using PrologFlags=contain.                          \n\
      -n, --noheader:                                                        \n\
	           No header will be added to the beginning of output.      \n\
                   The default is to print a header.                        \n\
      --noconvert:  Don't convert units from their original type             \n\
		   (e.g. 2048M won't be converted to 2G).                   \n\
      -o, --format:                                                          \n\
	           Comma separated list of fields. (use \"--helpformat\"    \n\
                   for a list of available fields).                         \n\
      -p, --parsable: output will be '|' delimited with a '|' at the end     \n\
      -P, --parsable2: output will be '|' delimited without a '|' at the end \n\
      --usage:      Display brief usage message.                             \n\
      -v, --verbose:                                                         \n\
	           Primarily for debugging purposes, report the state of    \n\
                   various variables during processing.                     \n\
      -V, --version: Print version.                                          \n\
\n");

	return;
}

void _usage(void)
{
	printf("Usage: sstat [options] -j <job(.stepid)>\n"
	       "\tUse --help for help\n");
}


void _do_help(void)
{
	switch (params.opt_help) {
	case 1:
		_help_msg();
		break;
	case 2:
		_help_fields_msg();
		break;
	case 3:
		_usage();
		break;
	default:
		fprintf(stderr, "stats bug: params.opt_help=%d\n",
			params.opt_help);
	}
}

void _init_params()
{
	memset(&params, 0, sizeof(sstat_parameters_t));
	params.convert_flags = CONVERT_NUM_UNIT_EXACT;
	params.units = NO_VAL;
}

/* returns number of objects added to list */
static void _addto_job_list(char *names)
{
	if (!params.opt_job_list)
		params.opt_job_list = list_create(slurm_destroy_selected_step);

	(void)slurm_addto_step_list(params.opt_job_list, names);
}
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
int cpu_util_readable = 1;
#endif

int decode_state_char(char *state)
{
	if (!xstrcasecmp(state, "p"))
		return JOB_PENDING; 	/* we should never see this */
	else if (!xstrcasecmp(state, "r"))
		return JOB_RUNNING;
	else if (!xstrcasecmp(state, "su"))
		return JOB_SUSPENDED;
	else if (!xstrcasecmp(state, "cd"))
		return JOB_COMPLETE;
	else if (!xstrcasecmp(state, "ca"))
		return JOB_CANCELLED;
	else if (!xstrcasecmp(state, "f"))
		return JOB_FAILED;
	else if (!xstrcasecmp(state, "to"))
		return JOB_TIMEOUT;
	else if (!xstrcasecmp(state, "nf"))
		return JOB_NODE_FAIL;
	else if (!xstrcasecmp(state, "pr"))
		return JOB_PREEMPTED;
	else if (!xstrcasecmp(state, "dl"))
		return JOB_DEADLINE;
	else if (!xstrcasecmp(state, "oom"))
		return JOB_OOM;
	else
		return -1; // unknown
}

void parse_command_line(int argc, char **argv)
{
	extern int optind;
	int c, i, optionIndex = 0;
	char *end = NULL, *start = NULL;
	slurm_selected_step_t *selected_step = NULL;
	ListIterator itr = NULL;
	log_options_t logopt = LOG_OPTS_STDERR_ONLY;

	static struct option long_options[] = {
		{"allsteps", 0, 0, 'a'},
		{"helpformat", 0, 0, 'e'},
		{"help", 0, 0, 'h'},
		{"jobs", 1, 0, 'j'},
		{"noheader", 0, 0, 'n'},
		{"fields", 1, 0, 'o'},
		{"format", 1, 0, 'o'},
                {"noconvert",  no_argument, 0, OPT_LONG_NOCONVERT},
		{"pidformat", 0, 0, 'i'},
		{"parsable", 0, 0, 'p'},
		{"parsable2", 0, 0, 'P'},
		{"usage", 0, &params.opt_help, 3},
		{"verbose", 0, 0, 'v'},
		{"version", 0, 0, 'V'},
		{0, 0, 0, 0}};

	log_init(xbasename(argv[0]), logopt, 0, NULL);

	_init_params();

	opterr = 1;		/* Let getopt report problems to the user */

	while (1) {		/* now cycle through the command line */
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
#ifdef __METASTACK_LOAD_ABNORMAL
		c = getopt_long(argc, argv, "aehij:dno:pPvVmgr",
				long_options, &optionIndex);
#endif
#else
		c = getopt_long(argc, argv, "aehij:no:pPvV",
				long_options, &optionIndex);
#endif
		if (c == -1)
			break;
		switch (c) {
		case 'a':
			params.opt_all_steps = 1;
			break;
#ifdef __METASTACK_LOAD_ABNORMAL
		case 'd':
			params.opt_event = 1;
			break;
#endif
		case 'e':
			params.opt_help = 2;
			break;
		case 'h':
			params.opt_help = 1;
			break;
		case 'i':
			params.pid_format = 1;
			xstrfmtcat(params.opt_field_list, "%s,",
				   STAT_FIELDS_PID);
			break;
		case 'j':
			_addto_job_list(optarg);
 			break;
		case 'n':
			print_fields_have_header = 0;
			break;
		case OPT_LONG_NOCONVERT:
			params.convert_flags |= CONVERT_NUM_UNIT_NO;
			break;
		case 'o':
			xstrfmtcat(params.opt_field_list, "%s,", optarg);
			break;
		case 'p':
			print_fields_parsable_print =
				PRINT_FIELDS_PARSABLE_ENDING;
			break;
		case 'P':
			print_fields_parsable_print =
				PRINT_FIELDS_PARSABLE_NO_ENDING;
			break;
		case 'v':
			/* Handle -vvv thusly...
			 * 0 - report only normal messages and errors
			 * 1 - report options selected and major operations
			 * 2 - report data anomalies probably not errors
			 * 3 - blather on and on
			 */
			params.opt_verbose++;
			break;

		case 'V':
			print_slurm_version();
			exit(0);
			break;
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
	 	case 'm':
			params.units = UNIT_MEGA;
			break;
		case 'g':
			params.units = UNIT_GIGA;
			break;
		case 'r':
			cpu_util_readable = 0;
			break;
#endif
		case ':':
		case '?':	/* getopt() has explained it */
			exit(1);
		}
	}

	if (params.opt_help) {
		_do_help();
		exit(0);
	}

	if (optind < argc) {
		optarg = argv[optind];
		_addto_job_list(optarg);
	}

	if (!params.opt_field_list)
		xstrfmtcat(params.opt_field_list, "%s,", STAT_FIELDS);

	if (params.opt_verbose) {
		logopt.stderr_level += params.opt_verbose;
		logopt.prefix_level = 1;
		log_alter(logopt, 0, NULL);
	}

	/* specific jobs requested? */
	if (params.opt_verbose && params.opt_job_list
	    && list_count(params.opt_job_list)) {
		debug("Jobs requested:\n");
		itr = list_iterator_create(params.opt_job_list);
		while ((selected_step = list_next(itr))) {
			if (selected_step->step_id.step_id != NO_VAL)
				debug("\t: %ps\n", &selected_step->step_id);
			else
				debug("\t: All steps for job %u\n",
				      selected_step->step_id.job_id);
		}
		list_iterator_destroy(itr);
	}
#ifdef __METASTACK_OPT_PRINT_COMMAND
    char *env_sstat = NULL;
	char *format_tmp =NULL;
	if ((env_sstat = getenv("SSTAT_EXTEND"))) {
   		format_tmp = xstrdup(env_sstat);
		
		for (i = 0; fields[i].name; i++) {
			_parse_env_format_extend(format_tmp, &fields[i]);
		}
	}
	if (format_tmp)
		xfree(format_tmp);
#endif
	start = params.opt_field_list;
	while ((end = strstr(start, ","))) {
		char *tmp_char = NULL;
		int command_len = 0;
		int newlen = 0;

		*end = 0;
		while (isspace(*start))
			start++;	/* discard whitespace */
		if (!(int)*start)
			continue;

		if ((tmp_char = strstr(start, "\%"))) {
			newlen = atoi(tmp_char+1);
			tmp_char[0] = '\0';
		}

		command_len = strlen(start);

		for (i = 0; fields[i].name; i++) {
			if (!xstrncasecmp(fields[i].name, start, command_len))
				goto foundfield;
		}
		error("Invalid field requested: \"%s\"", start);
		exit(1);
	foundfield:
		if (newlen)
			fields[i].len = newlen;
		list_append(print_fields_list, &fields[i]);
		start = end + 1;
	}
	field_count = list_count(print_fields_list);


	return;
}
