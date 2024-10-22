/*****************************************************************************\
 *  common_jag.c - slurm job accounting gather common plugin functions.
 *****************************************************************************
 *  Copyright (C) 2013 SchedMD LLC
 *  Written by Danny Auble <da@schedmd.com>, who borrowed heavily
 *  from the original code in jobacct_gather/linux
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
 *
 *  This file is patterned after jobcomp_linux.c, written by Morris Jette and
 *  Copyright (C) 2002 The Regents of the University of California.
\*****************************************************************************/

#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <time.h>
#include <ctype.h>

#include "src/common/slurm_xlator.h"
#include "src/common/assoc_mgr.h"
#include "src/common/slurm_jobacct_gather.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/slurm_protocol_defs.h"
#include "src/common/slurm_acct_gather_energy.h"
#include "src/common/slurm_acct_gather_filesystem.h"
#include "src/common/slurm_acct_gather_interconnect.h"
#include "src/common/xstring.h"
#include "src/slurmd/common/proctrack.h"

#include "common_jag.h"

#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
#include <unistd.h>
#endif
/* These are defined here so when we link with something other than
 * the slurmstepd we will have these symbols defined.  They will get
 * overwritten when linking with the slurmstepd.
 */
#if defined (__APPLE__)
extern uint32_t g_tres_count __attribute__((weak_import));
extern char **assoc_mgr_tres_name_array __attribute__((weak_import));
#else
uint32_t g_tres_count;
char **assoc_mgr_tres_name_array;
#endif


static int cpunfo_frequency = 0;
static long conv_units = 0;
List prec_list = NULL;

static int my_pagesize = 0;
static int energy_profile = ENERGY_DATA_NODE_ENERGY_UP;

#ifdef __METASTACK_LOAD_ABNORMAL
static int _get_process_status_line(pid_t pid, jag_prec_t *prec) 
{
	char *filename = NULL;
	char buf[4096];
	int fd, attempts = 1;
	ssize_t n;
	char *status = NULL;
    int flag = 0 ;
	xstrfmtcat(filename, "/proc/%u/status", pid);

	fd = open(filename, O_RDONLY);
	if (fd < 0) {
		xfree(filename);
		return 1;
	}

again:
	n = read(fd, buf, sizeof(buf) - 1);
	if (n == -1 && (errno == EINTR || errno == EAGAIN) && attempts < 10) {
		attempts++;
		goto again;
	}
	if (n <= 0) {
		close(fd);
		xfree(filename);
		return 1;
	}
	buf[n] = '\0';
	close(fd);
	xfree(filename);

	status = xstrstr(buf, "State:");
	if(status) {
		status[9] = '\0';
		if(status[7] == 'D' ) {
			flag = 1;
		}
	} else {
		debug2("%s: status: string not found for pid=%u\n", __func__, pid);
	}
	prec->flag = flag;
	return 0;
}
#endif

static int _find_prec(void *x, void *key)
{
	jag_prec_t *prec = (jag_prec_t *) x;
	pid_t pid = *(pid_t *) key;

	if (prec->pid == pid)
		return 1;

	return 0;
}

/* return weighted frequency in mhz */
static uint32_t _update_weighted_freq(struct jobacctinfo *jobacct,
				      char * sbuf)
{
	uint32_t tot_cpu;
	int thisfreq = 0;

	if (cpunfo_frequency)
		/* scaling not enabled */
		thisfreq = cpunfo_frequency;
	else
		sscanf(sbuf, "%d", &thisfreq);

	jobacct->current_weighted_freq =
		jobacct->current_weighted_freq +
		(uint32_t)jobacct->this_sampled_cputime * thisfreq;
	tot_cpu = (uint32_t) jobacct->tres_usage_in_tot[TRES_ARRAY_CPU];
	if (tot_cpu) {
		return (uint32_t) (jobacct->current_weighted_freq / tot_cpu);
	} else
		return thisfreq;
}

/* Parse /proc/cpuinfo file for CPU frequency.
 * Store the value in global variable cpunfo_frequency
 * RET: True if read valid CPU frequency */
inline static bool _get_freq(char *str)
{
	char *sep = NULL;
	double cpufreq_value;
	int cpu_mult;

	if (strstr(str, "MHz"))
		cpu_mult = 1;
	else if (strstr(str, "GHz"))
		cpu_mult = 1000;	/* Scale to MHz */
	else
		return false;

	sep = strchr(str, ':');
	if (!sep)
		return false;

	if (sscanf(sep + 2, "%lf", &cpufreq_value) < 1)
		return false;

	cpunfo_frequency = cpufreq_value * cpu_mult;
	log_flag(JAG, "cpuinfo_frequency=%d", cpunfo_frequency);

	return true;
}

/*
 * collects the Pss value from /proc/<pid>/smaps
 */
static int _get_pss(char *proc_smaps_file, jag_prec_t *prec)
{
        uint64_t pss;
	uint64_t p;
        char line[128];
        FILE *fp;
	int i;

	fp = fopen(proc_smaps_file, "r");
        if (!fp) {
                return -1;
        }

	if (fcntl(fileno(fp), F_SETFD, FD_CLOEXEC) == -1)
		error("%s: fcntl(%s): %m", __func__, proc_smaps_file);
	pss = 0;

        while (fgets(line,sizeof(line),fp)) {

                if (xstrncmp(line, "Pss:", 4) != 0) {
                        continue;
                }

                for (i = 4; i < sizeof(line); i++) {

                        if (!isdigit(line[i])) {
                                continue;
                        }
                        if (sscanf(&line[i],"%"PRIu64"", &p) == 1) {
                                pss += p;
                        }
                        break;
                }
        }

	/* Check for error
	 */
	if (ferror(fp)) {
		fclose(fp);
		return -1;
	}

        fclose(fp);
        /* Sanity checks */

        if (pss > 0) {
		pss *= 1024; /* Scale KB to B */
		if (prec->tres_data[TRES_ARRAY_MEM].size_read > pss)
			prec->tres_data[TRES_ARRAY_MEM].size_read = pss;
        }

	log_flag(JAG, "%s read pss %"PRIu64" for process %s",
		 __func__, pss, proc_smaps_file);

        return 0;
}

static int _get_sys_interface_freq_line(uint32_t cpu, char *filename,
					char * sbuf)
{
	int num_read, fd;
	FILE *sys_fp = NULL;
	char freq_file[80];
	char cpunfo_line [128];

	if (cpunfo_frequency)
		/* scaling not enabled, static freq obtained */
		return 1;

	snprintf(freq_file, 79,
		 "/sys/devices/system/cpu/cpu%d/cpufreq/%s",
		 cpu, filename);
	log_flag(JAG, "filename = %s", freq_file);
	if ((sys_fp = fopen(freq_file, "r"))!= NULL) {
		/* frequency scaling enabled */
		fd = fileno(sys_fp);
		if (fcntl(fd, F_SETFD, FD_CLOEXEC) == -1)
			error("%s: fcntl(%s): %m", __func__, freq_file);
		num_read = read(fd, sbuf, (sizeof(sbuf) - 1));
		if (num_read > 0) {
			sbuf[num_read] = '\0';
			log_flag(JAG, "scaling enabled on cpu %d freq= %s",
				 cpu, sbuf);
		}
		fclose(sys_fp);
	} else {
		/* frequency scaling not enabled */
		if (!cpunfo_frequency) {
			snprintf(freq_file, 14, "/proc/cpuinfo");
			log_flag(JAG, "filename = %s (cpu scaling not enabled)",
			       freq_file);
			if ((sys_fp = fopen(freq_file, "r")) != NULL) {
				while (fgets(cpunfo_line, sizeof(cpunfo_line),
					     sys_fp) != NULL) {
					if (_get_freq(cpunfo_line))
						break;
				}
				fclose(sys_fp);
			}
		}
		return 1;
	}
	return 0;
}

static int _is_a_lwp(uint32_t pid)
{
	char *filename = NULL;
	char bf[4096];
	int fd, attempts = 1;
	ssize_t n;
	char *tgids = NULL;
	pid_t tgid = -1;

	xstrfmtcat(filename, "/proc/%u/status", pid);

	fd = open(filename, O_RDONLY);
	if (fd < 0) {
		xfree(filename);
		return SLURM_ERROR;
	}

again:
	n = read(fd, bf, sizeof(bf) - 1);
	if (n == -1 && (errno == EINTR || errno == EAGAIN) && attempts < 100) {
		attempts++;
		goto again;
	}
	if (n <= 0) {
		close(fd);
		xfree(filename);
		return SLURM_ERROR;
	}
	bf[n] = '\0';
	close(fd);
	xfree(filename);

	tgids = xstrstr(bf, "Tgid:");

	if (tgids) {
		tgids += 5; /* strlen("Tgid:") */
		tgid = atoi(tgids);
	} else
		error("%s: Tgid: string not found for pid=%u", __func__, pid);

	if (pid != (uint32_t)tgid) {
		log_flag(JAG, "pid=%u != tgid=%u is a lightweight process",
			 pid, tgid);
		return 1;
	} else {
		log_flag(JAG, "pid=%u == tgid=%u is the leader LWP",
			 pid, tgid);
		return 0;
	}
}


/* _get_process_data_line() - get line of data from /proc/<pid>/stat
 *
 * IN:	in - input file descriptor
 * OUT:	prec - the destination for the data
 *
 * RETVAL:	==0 - no valid data
 * 		!=0 - data are valid
 *
 * Based upon stat2proc() from the ps command. It can handle arbitrary
 * executable file basenames for `cmd', i.e. those with embedded whitespace or
 * embedded ')'s. Such names confuse %s (see scanf(3)), so the string is split
 * and %39c is used instead. (except for embedded ')' "(%[^)]c)" would work.
 */
static int _get_process_data_line(int in, jag_prec_t *prec) {
	char sbuf[512], *tmp;
	int num_read, nvals;
	char cmd[40], state[1];
	int ppid, pgrp, session, tty_nr, tpgid;
	long unsigned flags, minflt, cminflt, majflt, cmajflt;
	long unsigned utime, stime, starttime, vsize;
	long int cutime, cstime, priority, nice, timeout, itrealvalue, rss;
	long unsigned f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13;
	int exit_signal, last_cpu;

	num_read = read(in, sbuf, (sizeof(sbuf) - 1));
	if (num_read <= 0)
		return 0;
	sbuf[num_read] = '\0';

	/*
	 * split into "PID (cmd" and "<rest>" replace trailing ')' with NULL
	 */
	tmp = strrchr(sbuf, ')');
	if (!tmp)
		return 0;
	*tmp = '\0';

	/* parse these two strings separately, skipping the leading "(". */
#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE 
	memset(cmd, 0, sizeof(cmd));
#endif	
	nvals = sscanf(sbuf, "%d (%39c", &prec->pid, cmd);

	if (nvals < 2)
		return 0;

	nvals = sscanf(tmp + 2,	 /* skip space after ')' too */
		       "%c %d %d %d %d %d "
		       "%lu %lu %lu %lu %lu "
		       "%lu %lu %ld %ld %ld %ld "
		       "%ld %ld %lu %lu %ld "
		       "%lu %lu %lu %lu %lu "
		       "%lu %lu %lu %lu %lu "
		       "%lu %lu %lu %d %d ",
		       state, &ppid, &pgrp, &session, &tty_nr, &tpgid,
		       &flags, &minflt, &cminflt, &majflt, &cmajflt,
		       &utime, &stime, &cutime, &cstime, &priority, &nice,
		       &timeout, &itrealvalue, &starttime, &vsize, &rss,
		       &f1, &f2, &f3, &f4, &f5 ,&f6, &f7, &f8, &f9, &f10, &f11,
		       &f12, &f13, &exit_signal, &last_cpu);
	/* There are some additional fields, which we do not scan or use */
	if ((nvals < 37) || (rss < 0))
		return 0;

	/*
	 * If current pid corresponds to a Light Weight Process (Thread POSIX)
	 * or there was an error, skip it, we will only account the original
	 * process (pid==tgid).
	 */
	if (_is_a_lwp(prec->pid))
		return 0;
#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE 
	prec->command = xstrdup(cmd);
#endif
	/* Copy the values that slurm records into our data structure */
	prec->ppid  = ppid;

	prec->tres_data[TRES_ARRAY_PAGES].size_read = majflt;
	prec->tres_data[TRES_ARRAY_VMEM].size_read = vsize;
	prec->tres_data[TRES_ARRAY_MEM].size_read = rss * my_pagesize;

	/*
	 * Store unnormalized times, we will normalize in when
	 * transfering to a struct jobacctinfo in job_common_poll_data()
	 */
	prec->usec = (double)utime;
	prec->ssec = (double)stime;
	prec->last_cpu = last_cpu;
	return 1;
}

/* _get_process_memory_line() - get line of data from /proc/<pid>/statm
 *
 * IN:	in - input file descriptor
 * OUT:	prec - the destination for the data
 *
 * RETVAL:	==0 - no valid data
 * 		!=0 - data are valid
 *
 * The *prec will mostly be filled in. We need to simply subtract the
 * amount of shared memory used by the process (in KB) from *prec->rss
 * and return the updated struct.
 *
 */
static int _get_process_memory_line(int in, jag_prec_t *prec)
{
	char sbuf[256];
	int num_read, nvals;
	long int size, rss, share, text, lib, data, dt;

	num_read = read(in, sbuf, (sizeof(sbuf) - 1));
	if (num_read <= 0)
		return 0;
	sbuf[num_read] = '\0';

	nvals = sscanf(sbuf,
		       "%ld %ld %ld %ld %ld %ld %ld",
		       &size, &rss, &share, &text, &lib, &data, &dt);
	/* There are some additional fields, which we do not scan or use */
	if (nvals != 7)
		return 0;

	/* If shared > rss then there is a problem, give up... */
	if (share > rss) {
		log_flag(JAG, "share > rss - bail!");
		return 0;
	}

	/* Copy the values that slurm records into our data structure */
	prec->tres_data[TRES_ARRAY_MEM].size_read =
		(rss - share) * my_pagesize;;

	return 1;
}

static int _remove_share_data(char *proc_statm_file, jag_prec_t *prec)
{
	FILE *statm_fp = NULL;
	int rc = 0, fd;

	if (!(statm_fp = fopen(proc_statm_file, "r")))
		return rc;  /* Assume the process went away */
	fd = fileno(statm_fp);
	if (fcntl(fd, F_SETFD, FD_CLOEXEC) == -1)
		error("%s: fcntl(%s): %m", __func__, proc_statm_file);
	rc = _get_process_memory_line(fd, prec);
	fclose(statm_fp);
	return rc;
}

/* _get_process_io_data_line() - get line of data from /proc/<pid>/io
 *
 * IN:	in - input file descriptor
 * OUT:	prec - the destination for the data
 *
 * RETVAL:	==0 - no valid data
 * 		!=0 - data are valid
 *
 * /proc/<pid>/io content format is:
 * rchar: <# of characters read>
 * wrchar: <# of characters written>
 *   . . .
 */
static int _get_process_io_data_line(int in, jag_prec_t *prec) {
	char sbuf[256];
	char f1[7], f3[7];
	int num_read, nvals;
	uint64_t rchar, wchar;

	num_read = read(in, sbuf, (sizeof(sbuf) - 1));
	if (num_read <= 0)
		return 0;
	sbuf[num_read] = '\0';

	nvals = sscanf(sbuf, "%s %"PRIu64" %s %"PRIu64"",
		       f1, &rchar, f3, &wchar);
	if (nvals < 4)
		return 0;

	if (_is_a_lwp(prec->pid))
		return 0;

	/* keep real value here since we aren't doubles */
	prec->tres_data[TRES_ARRAY_FS_DISK].size_read = rchar;
	prec->tres_data[TRES_ARRAY_FS_DISK].size_write = wchar;

	return 1;
}

static int _init_tres(jag_prec_t *prec, void *empty)
{
	/* Initialize read/writes */
	for (int i = 0; i < prec->tres_count; i++) {
		prec->tres_data[i].num_reads = INFINITE64;
		prec->tres_data[i].num_writes = INFINITE64;
		prec->tres_data[i].size_read = INFINITE64;
		prec->tres_data[i].size_write = INFINITE64;
	}

	return SLURM_SUCCESS;
}

void _set_smaps_file(char **proc_smaps_file, pid_t pid)
{
	static int use_smaps_rollup = -1;

	if (use_smaps_rollup == -1) {
		xstrfmtcat(*proc_smaps_file, "/proc/%d/smaps_rollup", pid);
		FILE *fd = fopen(*proc_smaps_file, "r");
		if (fd) {
			fclose(fd);
			use_smaps_rollup = 1;
			return;
		}
		use_smaps_rollup = 0;
	}

	if (use_smaps_rollup)
		xstrfmtcat(*proc_smaps_file, "/proc/%d/smaps_rollup", pid);
	else
		xstrfmtcat(*proc_smaps_file, "/proc/%d/smaps", pid);
}

static void _handle_stats(pid_t pid, jag_callbacks_t *callbacks, int tres_count)
{
	static int no_share_data = -1;
	static int use_pss = -1;
	char *proc_file = NULL;
	FILE *stat_fp = NULL;
	FILE *io_fp = NULL;
	int fd, fd2;
	jag_prec_t *prec = NULL;
#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE
	time_t ct_pid;
#endif
	if (no_share_data == -1) {
		if (xstrcasestr(slurm_conf.job_acct_gather_params, "NoShare"))
			no_share_data = 1;
		else
			no_share_data = 0;

		if (xstrcasestr(slurm_conf.job_acct_gather_params, "UsePss"))
			use_pss = 1;
		else
			use_pss = 0;
	}

	xstrfmtcat(proc_file, "/proc/%u/stat", pid);
	if (!(stat_fp = fopen(proc_file, "r")))
		return;  /* Assume the process went away */
	/*
	 * Close the file on exec() of user tasks.
	 *
	 * NOTE: If we fork() slurmstepd after the
	 * fopen() above and before the fcntl() below,
	 * then the user task may have this extra file
	 * open, which can cause problems for
	 * checkpoint/restart, but this should be a very rare
	 * problem in practice.
	 */
	fd = fileno(stat_fp);
	if (fcntl(fd, F_SETFD, FD_CLOEXEC) == -1)
		error("%s: fcntl(%s): %m", __func__, proc_file);

	prec = xmalloc(sizeof(*prec));

	if (!tres_count) {
		assoc_mgr_lock_t locks = {
			NO_LOCK, NO_LOCK, NO_LOCK, NO_LOCK,
			READ_LOCK, NO_LOCK, NO_LOCK };
		assoc_mgr_lock(&locks);
		tres_count = g_tres_count;
		assoc_mgr_unlock(&locks);
	}

	prec->tres_count = tres_count;
	prec->tres_data = xcalloc(prec->tres_count,
				  sizeof(acct_gather_data_t));

	(void)_init_tres(prec, NULL);
#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE
	ct_pid = time(NULL);
	prec->command = NULL;
	prec->now_time = ct_pid;
#endif
	if (!_get_process_data_line(fd, prec)) {
		fclose(stat_fp);
		goto bail_out;
	}

	fclose(stat_fp);

	if (acct_gather_filesystem_g_get_data(prec->tres_data) < 0) {
		log_flag(JAG, "problem retrieving filesystem data");
	}

	if (acct_gather_interconnect_g_get_data(prec->tres_data) < 0) {
		log_flag(JAG, "problem retrieving interconnect data");
	}

	/* Remove shared data from rss */
	if (no_share_data) {
		xfree(proc_file);
		xstrfmtcat(proc_file, "/proc/%u/statm", pid);
		if (!_remove_share_data(proc_file, prec))
			goto bail_out;
	}

	/* Use PSS instead if RSS */
	if (use_pss) {
		xfree(proc_file);
		_set_smaps_file(&proc_file, pid);
		if (_get_pss(proc_file, prec) == -1)
			goto bail_out;
	}

	xfree(proc_file);
	xstrfmtcat(proc_file, "/proc/%u/io", pid);
	if ((io_fp = fopen(proc_file, "r"))) {
		fd2 = fileno(io_fp);
		if (fcntl(fd2, F_SETFD, FD_CLOEXEC) == -1)
			error("%s: fcntl: %m", __func__);
		if (!_get_process_io_data_line(fd2, prec)) {
			fclose(io_fp);
			goto bail_out;
		}
		fclose(io_fp);
	}
#ifdef __METASTACK_LOAD_ABNORMAL
	if(_get_process_status_line(pid, prec) == 1) {
		log_flag(JAG, "problem retrieving pid status data");
	} 
#endif

#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE
    if (acct_gather_profile_g_is_active(ACCT_GATHER_PROFILE_TASK) && prec_list) {
        jag_prec_t *prec_jobacct = NULL;
        if ((prec_jobacct = list_find_first(prec_list, _find_prec, &prec->pid))) {
            if (prec_jobacct->last_time > 0) {
                int et = prec->now_time - prec_jobacct->last_time;
                if (et > 1) {
                    if(conv_units <= 0)
                        conv_units = 100;
                    prec->cpu_util = ((prec->usec + prec->ssec) - prec_jobacct->last_total_calc) * 
                                                    100 /et/(double)conv_units ;
                }
            }
        }
        prec->last_time = prec->now_time;
        prec->last_total_calc = prec->usec + prec->ssec;
    }
#endif
	destroy_jag_prec(list_remove_first(prec_list, _find_prec, &prec->pid));
	list_append(prec_list, prec);
	xfree(proc_file);
	return;

bail_out:
	xfree(prec->tres_data);
#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE
    if(prec->command != NULL)
		xfree(prec->command);
#endif
	xfree(prec);
	return;
}


static List _get_precs(List task_list, uint64_t cont_id,
		       jag_callbacks_t *callbacks)
{
	int npids = 0;
	struct jobacctinfo *jobacct = NULL;
	pid_t *pids = NULL;

	xassert(task_list);

	jobacct = list_peek(task_list);

	/* get only the processes in the proctrack container */
	proctrack_g_get_pids(cont_id, &pids, &npids);
	if (npids) {
		for (int i = 0; i < npids; i++) {
			_handle_stats(pids[i], callbacks,
				      jobacct ? jobacct->tres_count : 0);
		}
		xfree(pids);
	} else {
		/* update consumed energy even if pids do not exist */
		if (jobacct) {
			acct_gather_energy_g_get_sum(energy_profile,
						     &jobacct->energy);
			jobacct->tres_usage_in_tot[TRES_ARRAY_ENERGY] =
				jobacct->energy.consumed_energy;
			jobacct->tres_usage_out_tot[TRES_ARRAY_ENERGY] =
				jobacct->energy.current_watts;
			log_flag(JAG, "energy = %"PRIu64" watts = %u",
				 jobacct->energy.consumed_energy,
				 jobacct->energy.current_watts);
		}
		log_flag(JAG, "no pids in this container %"PRIu64, cont_id);
	}

	return prec_list;
}

#ifdef __METASTACK_LOAD_ABNORMAL
static void _record_profile2(struct jobacctinfo *jobacct, write_t *send)
{
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

	acct_gather_profile_dataset_t dataset[] = {
		{ "STEPPROFILE", PROFILE_FIELD_DOUBLE },
		{ "STPEDEVENT", PROFILE_FIELD_DOUBLE },
		{ NULL, PROFILE_FIELD_NOT_SET }
	};
	static int64_t profile_gid = -1;
	//double et;

	union {
		double d;
		uint64_t u64;
	} data[FIELD_CNT];

	char str[256];

	if (profile_gid == -1)
		profile_gid = acct_gather_profile_g_create_group("Stepd");
	/* Create the dataset first */
	if (jobacct->dataset_id < 0) {
		char ds_name[32];
		snprintf(ds_name, sizeof(ds_name), "%u", jobacct->id.taskid);

		jobacct->dataset_id = acct_gather_profile_g_create_dataset(
			ds_name, profile_gid, dataset);
		if (jobacct->dataset_id == SLURM_ERROR) {
			error("JobAcct: Failed to create the dataset for "
			      "task %d",
			      jobacct->pid);
			return;
		}
	}

	if (jobacct->dataset_id < 0)
		return;
	/* Profile Mem and VMem as KB */
	if(jobacct->node_alloc_cpu > 0) {
		data[FIELD_CPUTHRESHOLD].d = send->cpu_threshold / jobacct->node_alloc_cpu;
		data[FIELD_STEPCPU].d = jobacct->cpu_step_real / jobacct->node_alloc_cpu;
		data[FIELD_STEPCPUAVE].d = jobacct->cpu_step_ave / jobacct->node_alloc_cpu;	
	} else {
		data[FIELD_CPUTHRESHOLD].d = 0;
		data[FIELD_STEPCPU].d = 0;
		data[FIELD_STEPCPUAVE].d = 0;			
	}

	data[FIELD_STEPMEM].d = jobacct->mem_step / 1024;
	data[FIELD_STEPVMEM].d = jobacct->vmem_step / 1024;	
	data[FIELD_STEPPAGES].u64 = jobacct->step_pages;

	data[FIELD_FLAG].u64 = send->load_flag;
#ifdef __METASTACK_JOB_USELESS_RUNNING_WARNING
	data[FIELD_TIMER].u64 = send->timer;
#endif
	if(send->load_flag & LOAD_LOW) { 
		data[FIELD_EVENTTYPE1START].u64 = send->cpu_start;
		data[FIELD_EVENTTYPE1END].u64 = send->cpu_end;
	}

	if(send->load_flag & PROC_AB) { 
		/*
			Since the acquisition period is a fixed interval, there is no need to record the start
			and end time of each anomaly, only one copy is needed.
		*/
		data[FIELD_EVENTTYPE1START].u64 = send->pid_start;
		data[FIELD_EVENTTYPE1END].u64 = send->pid_end;
	}

	if(send->load_flag & JNODE_STAT) { 
		data[FIELD_EVENTTYPE1START].u64 = send->node_start;
		data[FIELD_EVENTTYPE1END].u64 = send->node_end;
	}	


	log_flag(PROFILE, "PROFILE-Task: %s",
		 acct_gather_profile_dataset_str(dataset, data, str,
						 sizeof(str)));
	int influx_flag= acct_gather_profile_g_add_sample_data_stepd(jobacct->dataset_id,
	                                   (void *)data, jobacct->cur_time);
	if (influx_flag == SLURM_ERROR) {
		debug2("the influxdb plug is not enabled");
	}

}
#endif


static void _record_profile(struct jobacctinfo *jobacct)
{
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

	acct_gather_profile_dataset_t dataset[] = {
		{ "CPUFrequency", PROFILE_FIELD_UINT64 },
		{ "CPUTime", PROFILE_FIELD_DOUBLE },
		{ "CPUUtilization", PROFILE_FIELD_DOUBLE },
		{ "RSS", PROFILE_FIELD_UINT64 },
		{ "VMSize", PROFILE_FIELD_UINT64 },
		{ "Pages", PROFILE_FIELD_UINT64 },
		{ "ReadMB", PROFILE_FIELD_DOUBLE },
		{ "WriteMB", PROFILE_FIELD_DOUBLE },
		{ NULL, PROFILE_FIELD_NOT_SET }
	};

	static int64_t profile_gid = -1;
	double et;
	union {
		double d;
		uint64_t u64;
	} data[FIELD_CNT];
	char str[256];

	if (profile_gid == -1)
		profile_gid = acct_gather_profile_g_create_group("Tasks");

	/* Create the dataset first */
	if (jobacct->dataset_id < 0) {
		char ds_name[32];
		snprintf(ds_name, sizeof(ds_name), "%u", jobacct->id.taskid);

		jobacct->dataset_id = acct_gather_profile_g_create_dataset(
			ds_name, profile_gid, dataset);
		if (jobacct->dataset_id == SLURM_ERROR) {
			error("JobAcct: Failed to create the dataset for "
			      "task %d",
			      jobacct->pid);
			return;
		}
	}

	if (jobacct->dataset_id < 0)
		return;

	data[FIELD_CPUFREQ].u64 = jobacct->act_cpufreq;
	/* Profile Mem and VMem as KB */
	data[FIELD_RSS].u64 =
		jobacct->tres_usage_in_tot[TRES_ARRAY_MEM] / 1024;
	data[FIELD_VMSIZE].u64 =
		jobacct->tres_usage_in_tot[TRES_ARRAY_VMEM] / 1024;
	data[FIELD_PAGES].u64 = jobacct->tres_usage_in_tot[TRES_ARRAY_PAGES];

	/* delta from last snapshot */
	if (!jobacct->last_time) {
		data[FIELD_CPUTIME].d = 0;
		data[FIELD_CPUUTIL].d = 0.0;
		data[FIELD_READ].d = 0.0;
		data[FIELD_WRITE].d = 0.0;
	} else {
		data[FIELD_CPUTIME].d =
			((double)jobacct->tres_usage_in_tot[TRES_ARRAY_CPU] -
			 jobacct->last_total_cputime) / CPU_TIME_ADJ;

		if (data[FIELD_CPUTIME].d < 0)
			data[FIELD_CPUTIME].d =
				jobacct->tres_usage_in_tot[TRES_ARRAY_CPU] /
				CPU_TIME_ADJ;

		et = (jobacct->cur_time - jobacct->last_time);
		if (!et)
			data[FIELD_CPUUTIL].d = 0.0;
		else
			data[FIELD_CPUUTIL].d =
				(100.0 * (double)data[FIELD_CPUTIME].d) /
				((double) et);

		data[FIELD_READ].d = (double) jobacct->
			tres_usage_in_tot[TRES_ARRAY_FS_DISK] -
			jobacct->last_tres_usage_in_tot;

		if (data[FIELD_READ].d < 0)
			data[FIELD_READ].d =
				jobacct->tres_usage_in_tot[TRES_ARRAY_FS_DISK];

		data[FIELD_WRITE].d = (double) jobacct->
			tres_usage_out_tot[TRES_ARRAY_FS_DISK] -
			jobacct->last_tres_usage_out_tot;

		if (data[FIELD_WRITE].d < 0)
			data[FIELD_WRITE].d =
				jobacct->tres_usage_out_tot[TRES_ARRAY_FS_DISK];

		/* Profile disk as MB */
		data[FIELD_READ].d /= 1048576.0;
		data[FIELD_WRITE].d /= 1048576.0;
	}

	log_flag(PROFILE, "PROFILE-Task: %s",
		 acct_gather_profile_dataset_str(dataset, data, str,
						 sizeof(str)));
#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE
	struct data_pack{
			void *data;
			List process;
	} pdata;
	pdata.data = (void*) data;
	pdata.process = jobacct->pjobs;
	acct_gather_profile_g_add_sample_data(jobacct->dataset_id,
										  (void *)&pdata, jobacct->cur_time);
#endif
}

extern void jag_common_init(long plugin_units)
{
	uint32_t profile_opt;

	prec_list = list_create(destroy_jag_prec);

	acct_gather_profile_g_get(ACCT_GATHER_PROFILE_RUNNING,
				  &profile_opt);

	/* If we are profiling energy it will be checked at a
	   different rate, so just grab the last one.
	*/
	if (profile_opt & ACCT_GATHER_PROFILE_ENERGY)
		energy_profile = ENERGY_DATA_NODE_ENERGY;

	if (plugin_units < 1)
		fatal("Invalid units for statistics. Initialization failed.");

	/* Dividing the gathered data by this unit will give seconds. */
	conv_units = plugin_units;
	my_pagesize = getpagesize();
}

extern void jag_common_fini(void)
{
	FREE_NULL_LIST(prec_list);
}

extern void destroy_jag_prec(void *object)
{
	jag_prec_t *prec = (jag_prec_t *)object;

	if (!prec)
		return;
#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE
	xfree(prec->command);
#endif
	xfree(prec->tres_data);
	xfree(prec);
	return;
}

#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE 
static void _get_son_process(List prec_list,
                    jag_prec_t *ancestor, struct jobacctinfo *jobacct){
    jag_prec_t *prec = NULL;
    jag_prec_t *prec_tmp = NULL;
    List tmp_list = NULL;

    ListIterator itr = list_iterator_create(prec_list);
    while((prec = list_next(itr))){
        prec->ppid_flag = false;
    }
    list_iterator_destroy(itr);

    prec = ancestor;
    prec->ppid_flag = true;

    tmp_list = list_create(NULL);
    list_append(tmp_list, prec);

    jobacct->pjobs = list_create(NULL);
    list_append(jobacct->pjobs, prec);

    while ((prec_tmp = list_dequeue(tmp_list))) {
        ListIterator itrin = list_iterator_create(prec_list);
        while((prec = list_next(itrin))){
            if((prec->ppid == prec_tmp->pid) && (false == prec->ppid_flag)) {
                list_append(jobacct->pjobs, prec);
                list_append(tmp_list, prec);
                prec->ppid_flag = true;
            }
        }
        list_iterator_destroy(itrin);
    }
    FREE_NULL_LIST(tmp_list);
    return;
}
#endif

static void _print_jag_prec(jag_prec_t *prec)
{
	int i;
	assoc_mgr_lock_t locks = {
		NO_LOCK, NO_LOCK, NO_LOCK, NO_LOCK,
		READ_LOCK, NO_LOCK, NO_LOCK };

	if (!(slurm_conf.debug_flags & DEBUG_FLAG_JAG))
		return;

	log_flag(JAG, "pid %d (ppid %d)", prec->pid, prec->ppid);
	log_flag(JAG, "act_cpufreq\t%d", prec->act_cpufreq);
	log_flag(JAG, "ssec \t%f", prec->ssec);
	assoc_mgr_lock(&locks);
	for (i = 0; i < prec->tres_count; i++) {
		if (prec->tres_data[i].size_read == INFINITE64)
			continue;
		log_flag(JAG, "%s in/read \t%" PRIu64 "",
			 assoc_mgr_tres_name_array[i],
			 prec->tres_data[i].size_read);
		log_flag(JAG, "%s out/write \t%" PRIu64 "",
			 assoc_mgr_tres_name_array[i],
			 prec->tres_data[i].size_write);
	}
	assoc_mgr_unlock(&locks);
	log_flag(JAG, "usec \t%f", prec->usec);
}

#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
static void update_jobacct_ext( struct jobacctinfo *jobacct,
										double cpu_calc,bool profile) 
{
	debug3("calling %s, profile=%d", __func__, profile);
	if (jobacct->first_acct_time.tv_sec == 0) {
		gettimeofday(&jobacct->first_acct_time, NULL) ;
		jobacct->first_total_cputime = cpu_calc;
#ifdef __METASTACK_LOAD_ABNORMAL
		jobacct->flag = 0;
		jobacct->cpu_step_ave = 0.0;
		jobacct->cpu_step_real = 0.0;
		jobacct->cpu_step_max = 0.0;
		jobacct->cpu_step_min = INFINITE64;

		jobacct->mem_step = 0;
		jobacct->mem_step_max = 0;
		jobacct->mem_step_min = INFINITE64;

		jobacct->vmem_step = 0;
		jobacct->vmem_step_max = 0;
		jobacct->vmem_step_min = INFINITE64;
		jobacct->acct_flag = 0;	

		jobacct->node_alloc_cpu = 0;
		jobacct->timer = 0;
		jobacct->cpu_threshold = 100;
#endif
		jobacct->cpu_util = 0.0;
		jobacct->avg_cpu_util = 0.0;
		jobacct->min_cpu_util = INFINITE64;
		jobacct->max_cpu_util = 0.0;
		jobacct->pre1_acct_time = jobacct->first_acct_time;
		jobacct->pre1_total_cputime = cpu_calc;
	} else {
		struct timeval now_time;
		gettimeofday(&now_time, NULL);

		int deta_time = 0;
		deta_time += (now_time.tv_sec - jobacct->pre1_acct_time.tv_sec)*1000;
		deta_time += (now_time.tv_usec -  jobacct->pre1_acct_time.tv_usec) /1000;

		if(deta_time >= 1000) {
			double cpu_util = (cpu_calc - jobacct->pre1_total_cputime) * 100.0 * 1000 / deta_time / CPU_TIME_ADJ;
			jobacct->cpu_util = cpu_util;

			deta_time = 0;
			deta_time += (now_time.tv_sec - jobacct->first_acct_time.tv_sec) * 1000;
			deta_time += (now_time.tv_usec - jobacct->first_acct_time.tv_usec) / 1000;

			double avg_cpu_util = (cpu_calc - jobacct->first_total_cputime) *100* 1000 / deta_time / CPU_TIME_ADJ;
			jobacct->avg_cpu_util = avg_cpu_util;
			debug3("cpu_util:%.1f, av_cpu_util:%.1f", cpu_util, avg_cpu_util);
			if (jobacct->cpu_util > jobacct->max_cpu_util)
				jobacct->max_cpu_util = jobacct->cpu_util;

			if (jobacct->cpu_util < jobacct->min_cpu_util)
				jobacct->min_cpu_util = jobacct->cpu_util;
			gettimeofday(&jobacct->pre1_acct_time, NULL);
			jobacct->pre1_total_cputime = cpu_calc;
		}
	}
}
#endif

#ifdef __METASTACK_LOAD_ABNORMAL
extern void jag_common_poll_data(List task_list, uint64_t cont_id,
				 jag_callbacks_t *callbacks, bool profile, collection_t *collect, write_t *data)
#endif
{
	/* Update the data */
	uint64_t total_job_mem = 0, total_job_vsize = 0;
	uint32_t last_taskid = NO_VAL;
	ListIterator itr;
	jag_prec_t *prec = NULL, tmp_prec;
	struct jobacctinfo *jobacct = NULL;
	static int processing = 0;
	char sbuf[72];
	int energy_counted = 0;
	time_t ct;
	int i = 0;
#ifdef __METASTACK_LOAD_ABNORMAL
	double total_job_cpuutil = 0;
	double total_job_cpuutil_ave = 0;
	uint64_t pid_status = 0;
	uint64_t total_job_pages = 0;
	/* just write once to the jobacct structure */
	bool stamp = false; 
#endif

	xassert(callbacks);

	if (cont_id == NO_VAL64) {
		log_flag(JAG, "cont_id hasn't been set yet not running poll");
		return;
	}

	if (processing) {
		log_flag(JAG, "already running, returning");
		return;
	}
	processing = 1;

	if (!callbacks->get_precs)
		callbacks->get_precs = _get_precs;

	ct = time(NULL);

	(void)list_for_each(prec_list, (ListForF)_init_tres, NULL);
	(*(callbacks->get_precs))(task_list, cont_id, callbacks);

	if (!list_count(prec_list) || !task_list || !list_count(task_list))
		goto finished;	/* We have no business being here! */

	itr = list_iterator_create(task_list);
	while ((jobacct = list_next(itr))) {
		double cpu_calc;
		double last_total_cputime;
		if (!(prec = list_find_first(prec_list, _find_prec,
					     &jobacct->pid)))
			continue;
		/*
		 * We can't use the prec from the list as we need to keep it in
		 * the original state without offspring since we reuse this list
		 * keeping around precs after they end.
		 */
		memcpy(&tmp_prec, prec, sizeof(*prec));
		prec = &tmp_prec;

		/*
		 * Only jobacct_gather/cgroup uses prec_extra, and we want to
		 * make sure we call it once per task, so call it here as we
		 * iterate through the tasks instead of in get_precs.
		 */
		if (callbacks->prec_extra) {
			if (last_taskid == jobacct->id.taskid) {
				log_flag(JAG, "skipping prec_extra() call against nodeid:%u taskid:%u",
					 jobacct->id.nodeid,
					 jobacct->id.taskid);
				continue;
			} else {
				log_flag(JAG, "calling prec_extra() call against nodeid:%u taskid:%u",
					 jobacct->id.nodeid,
					 jobacct->id.taskid);
			}

			last_taskid = jobacct->id.taskid;

			(*(callbacks->prec_extra))(prec, jobacct->id.taskid);
			_print_jag_prec(prec);
		}

		log_flag(JAG, "pid:%u ppid:%u %s:%" PRIu64 " B",
			 prec->pid, prec->ppid,
			 (xstrcasestr(slurm_conf.job_acct_gather_params,
				      "UsePss") ?  "pss" : "rss"),
			 prec->tres_data[TRES_ARRAY_MEM].size_read);

		/* find all my descendents */
		if (callbacks->get_offspring_data)
			(*(callbacks->get_offspring_data))
				(prec_list, prec, prec->pid);

		last_total_cputime =
			(double)jobacct->tres_usage_in_tot[TRES_ARRAY_CPU];

		cpu_calc = (prec->ssec + prec->usec) / (double) conv_units;

		/*
		 * Since we are not storing things as a double anymore make it
		 * bigger so we don't loose precision.
		 */
		cpu_calc *= CPU_TIME_ADJ;

		prec->tres_data[TRES_ARRAY_CPU].size_read = (uint64_t)cpu_calc;
#ifdef __METASTACK_OPT_SSTAT_CPUUTIL
		update_jobacct_ext(jobacct, cpu_calc, profile);
#endif
		/* get energy consumption
		 * only once is enough since we
		 * report per node energy consumption.
		 * Energy is stored in read fields, while power is stored
		 * in write fields.*/
		log_flag(JAG, "energycounted = %d", energy_counted);
		if (energy_counted == 0) {
			acct_gather_energy_g_get_sum(
				energy_profile,
				&jobacct->energy);
			prec->tres_data[TRES_ARRAY_ENERGY].size_read =
				jobacct->energy.consumed_energy;
			prec->tres_data[TRES_ARRAY_ENERGY].size_write =
				jobacct->energy.current_watts;
			log_flag(JAG, "energy = %"PRIu64" watts = %"PRIu64" ave_watts = %u",
				 prec->tres_data[TRES_ARRAY_ENERGY].size_read,
				 prec->tres_data[TRES_ARRAY_ENERGY].size_write,
				 jobacct->energy.ave_watts);
			energy_counted = 1;
		}

		/* tally their usage */
		for (i = 0; i < jobacct->tres_count; i++) {
			if (prec->tres_data[i].size_read == INFINITE64)
				continue;
			if (jobacct->tres_usage_in_max[i] == INFINITE64)
				jobacct->tres_usage_in_max[i] =
					prec->tres_data[i].size_read;
			else
				jobacct->tres_usage_in_max[i] =
					MAX(jobacct->tres_usage_in_max[i],
					    prec->tres_data[i].size_read);
			/*
			 * Even with min we want to get the max as we are
			 * looking at a specific task.  We are always looking
			 * at the max that task had, not the min (or lots of
			 * things will be zero).  The min is from comparing
			 * ranks later when combining.  So here it will be the
			 * same as the max value set above.
			 * (same thing goes for the out)
			 */
			jobacct->tres_usage_in_min[i] =
				jobacct->tres_usage_in_max[i];
			jobacct->tres_usage_in_tot[i] =
				prec->tres_data[i].size_read;

			if (jobacct->tres_usage_out_max[i] == INFINITE64)
				jobacct->tres_usage_out_max[i] =
					prec->tres_data[i].size_write;
			else
				jobacct->tres_usage_out_max[i] =
					MAX(jobacct->tres_usage_out_max[i],
					    prec->tres_data[i].size_write);
			jobacct->tres_usage_out_min[i] =
				jobacct->tres_usage_out_max[i];
			jobacct->tres_usage_out_tot[i] =
				prec->tres_data[i].size_write;
		}

		total_job_mem += jobacct->tres_usage_in_tot[TRES_ARRAY_MEM];
		total_job_vsize += jobacct->tres_usage_in_tot[TRES_ARRAY_VMEM];
#ifdef __METASTACK_LOAD_ABNORMAL
		if(data != NULL) {
			if(stamp == false) {
				
				jobacct->node_alloc_cpu = data->node_alloc_cpu;
				jobacct->timer = data->timer;
				jobacct->cpu_threshold = data->cpu_threshold;

				if(data->load_flag & LOAD_LOW) { 
					jobacct->cpu_start[jobacct->cpu_count % JOBACCTINFO_START_END_ARRAY_SIZE] = data->cpu_start;
					jobacct->cpu_end[jobacct->cpu_count % JOBACCTINFO_START_END_ARRAY_SIZE] = data->cpu_end;
					jobacct->cpu_count++;
					jobacct->flag |= data->load_flag;
				}
				if(data->load_flag & PROC_AB) { 
					jobacct->pid_start[jobacct->pid_count % JOBACCTINFO_START_END_ARRAY_SIZE] = data->pid_start;
					jobacct->pid_end[jobacct->pid_count % JOBACCTINFO_START_END_ARRAY_SIZE] = data->pid_end;
					jobacct->pid_count++;
					jobacct->flag |= data->load_flag;
				}

				if(data->load_flag & JNODE_STAT) { 
					jobacct->node_start[jobacct->node_count % JOBACCTINFO_START_END_ARRAY_SIZE] = data->node_start;
					jobacct->node_end[jobacct->node_count % JOBACCTINFO_START_END_ARRAY_SIZE] = data->node_end;
					jobacct->node_count++;
					jobacct->flag |= data->load_flag;
				}

				jobacct->cpu_step_ave = data->cpu_step_ave;
				jobacct->cpu_step_real = data->cpu_step_real;
				jobacct->mem_step = data->mem_step;
				jobacct->vmem_step = data->vmem_step;
				jobacct->step_pages = data->step_pages;

				jobacct->cpu_step_max = jobacct->cpu_step_max < data->cpu_step_real ? data->cpu_step_real : jobacct->cpu_step_max;
				jobacct->cpu_step_min = jobacct->cpu_step_min > data->cpu_step_real ? data->cpu_step_real : jobacct->cpu_step_min;
				jobacct->mem_step_max = jobacct->mem_step_max < data->mem_step ? data->mem_step : jobacct->mem_step_max;
				jobacct->mem_step_min = jobacct->mem_step_min > data->mem_step ? data->mem_step : jobacct->mem_step_min;
				jobacct->vmem_step_max = jobacct->vmem_step_max < data->vmem_step ? data->vmem_step : jobacct->vmem_step_max;
				jobacct->vmem_step_min = jobacct->vmem_step_min > data->vmem_step ? data->vmem_step : jobacct->vmem_step_min;				
				jobacct->acct_flag = 1;
				if(data && profile &&  acct_gather_profile_g_is_active(ACCT_GATHER_PROFILE_STEPD)) {	
					jobacct->cur_time = ct;
					_record_profile2(jobacct, data);
				}
			}	
			stamp = true;
		}
		total_job_cpuutil += jobacct->cpu_util;
		total_job_cpuutil_ave += jobacct->avg_cpu_util;
		total_job_pages += jobacct->tres_usage_in_tot[TRES_ARRAY_PAGES];

		
#endif
		/* Update the cpu times */
		jobacct->user_cpu_sec = (uint64_t)(prec->usec /
						   (double)conv_units);
		jobacct->sys_cpu_sec = (uint64_t)(prec->ssec /
						  (double)conv_units);

		/* compute frequency */
		jobacct->this_sampled_cputime =
			cpu_calc - last_total_cputime;
		_get_sys_interface_freq_line(
			prec->last_cpu,
			"cpuinfo_cur_freq", sbuf);
		jobacct->act_cpufreq =
			_update_weighted_freq(jobacct, sbuf);

		log_flag(JAG, "Task %u pid %d ave_freq = %u mem size/max %"PRIu64"/%"PRIu64" vmem size/max %"PRIu64"/%"PRIu64", disk read size/max (%"PRIu64"/%"PRIu64"), disk write size/max (%"PRIu64"/%"PRIu64"), time %f(%"PRIu64"+%"PRIu64") Energy tot/max %"PRIu64"/%"PRIu64" TotPower %"PRIu64" MaxPower %"PRIu64" MinPower %"PRIu64,
			 jobacct->id.taskid,
			 jobacct->pid,
			 jobacct->act_cpufreq,
			 jobacct->tres_usage_in_tot[TRES_ARRAY_MEM],
			 jobacct->tres_usage_in_max[TRES_ARRAY_MEM],
			 jobacct->tres_usage_in_tot[TRES_ARRAY_VMEM],
			 jobacct->tres_usage_in_max[TRES_ARRAY_VMEM],
			 jobacct->tres_usage_in_tot[TRES_ARRAY_FS_DISK],
			 jobacct->tres_usage_in_max[TRES_ARRAY_FS_DISK],
			 jobacct->tres_usage_out_tot[TRES_ARRAY_FS_DISK],
			 jobacct->tres_usage_out_max[TRES_ARRAY_FS_DISK],
			 (double)(jobacct->tres_usage_in_tot[TRES_ARRAY_CPU] /
			          CPU_TIME_ADJ),
			 jobacct->user_cpu_sec,
			 jobacct->sys_cpu_sec,
			 jobacct->tres_usage_in_tot[TRES_ARRAY_ENERGY],
			 jobacct->tres_usage_in_max[TRES_ARRAY_ENERGY],
			 jobacct->tres_usage_out_tot[TRES_ARRAY_ENERGY],
			 jobacct->tres_usage_out_max[TRES_ARRAY_ENERGY],
			 jobacct->tres_usage_out_min[TRES_ARRAY_ENERGY]);

		if (profile &&
		    acct_gather_profile_g_is_active(ACCT_GATHER_PROFILE_TASK)) {
			jobacct->cur_time = ct;
#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE 
		    _get_son_process(prec_list, prec, jobacct);
#endif
			_record_profile(jobacct);

			jobacct->last_tres_usage_in_tot =
				jobacct->tres_usage_in_tot[TRES_ARRAY_FS_DISK];
			jobacct->last_tres_usage_out_tot =
				jobacct->tres_usage_out_tot[TRES_ARRAY_FS_DISK];
			jobacct->last_total_cputime =
				jobacct->tres_usage_in_tot[TRES_ARRAY_CPU];

			jobacct->last_time = jobacct->cur_time;
#ifdef __METASTACK_OPT_INFLUXDB_ENFORCE 
			if(jobacct->pjobs)
				FREE_NULL_LIST(jobacct->pjobs);
#endif
		}
	}
	list_iterator_destroy(itr);
#ifdef __METASTACK_LOAD_ABNORMAL
   if(collect && (collect->step)) {
		ListIterator itr1 = NULL;
		jag_prec_t *prec1 = NULL;
		itr1 = list_iterator_create(prec_list);

		while ((prec1 = list_next(itr1))) {
			if((prec1->flag) == 1) {
				log_flag(JAG,"pid = %d  abnormal process status",prec1->pid);
				pid_status = pid_status|PROC_AB;
			} 
		}
		list_iterator_destroy(itr1);
		collect->cpu_step_real = total_job_cpuutil;
		collect->cpu_step_ave = total_job_cpuutil_ave;
		collect->mem_step = total_job_mem;
		collect->vmem_step = total_job_vsize;
		collect->load_flag = collect->load_flag | pid_status;
	}
#endif
	if (slurm_conf.job_acct_oom_kill)
		jobacct_gather_handle_mem_limit(total_job_mem,
						total_job_vsize);
						
finished:
	processing = 0;
}
