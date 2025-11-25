/*****************************************************************************\
 *  as_kingbase_qos.c - functions dealing with qos.
 *****************************************************************************
 *  Copyright (C) 2004-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2010 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Danny Auble <da@llnl.gov>
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

#include "as_kingbase_qos.h"

#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
#include "as_kingbase_usage.h"
#endif


static char *mqos_req_inx[] = {
	"id",
	"name",
	"preempt",
	"grp_tres_mins",
	"grp_tres_run_mins",
	"grp_tres",
	"max_tres_mins_pj",
	"max_tres_run_mins_pa",
	"max_tres_run_mins_pu",
	"max_tres_pa",
	"max_tres_pj",
	"max_tres_pn",
	"max_tres_pu",
	"min_tres_pj",
};

enum {
	MQOS_ID,
	MQOS_NAME,
	MQOS_PREEMPT,
	MQOS_GTM,
	MQOS_GTRM,
	MQOS_GT,
	MQOS_MTMPJ,
	MQOS_MTRMA,
	MQOS_MTRM,
	MQOS_MTPA,
	MQOS_MTPJ,
	MQOS_MTPN,
	MQOS_MTPU,
	MQOS_MITPJ,
	MQOS_COUNT
};


bool checkIfResultIsNull(KCIResult *result, int num_row) {
    bool isNULL = false;
    int j = 0;
    int columnCount = KCIResultGetColumnCount(result);

    for (int i = 0; i < columnCount; i++) {
		char *temp = KCIResultGetColumnValue(result, num_row, i);
		if(*temp == '\0') {
            j++;
        }
    }

    if (j == columnCount) {
        isNULL = true;
    }

    return isNULL;
}

static int _preemption_loop(kingbase_conn_t *kingbase_conn, int begin_qosid,
			    bitstr_t *preempt_bitstr)
{
	slurmdb_qos_rec_t qos_rec;
	int rc = 0, i = 0;

	xassert(preempt_bitstr);

	if (bit_test(preempt_bitstr, begin_qosid)) {
		error("QOS ID %d has an internal loop", begin_qosid);
		return 1;
	}

	/* check in the preempt list for all qos's preempted */
	for (i = 0; i < bit_size(preempt_bitstr); i++) {
		if (!bit_test(preempt_bitstr, i))
			continue;

		memset(&qos_rec, 0, sizeof(qos_rec));
		qos_rec.id = i;
		if (assoc_mgr_fill_in_qos(kingbase_conn, &qos_rec,
					  ACCOUNTING_ENFORCE_QOS, NULL, 0) !=
		    SLURM_SUCCESS) {
			error("QOS ID %d not found", i);
			rc = 1;
			break;
		}
		/*
		 * check if the begin_qosid is preempted by this qos
		 * if so we have a loop
		 */
		if (qos_rec.preempt_bitstr
		    && (bit_test(qos_rec.preempt_bitstr, begin_qosid) ||
			bit_test(qos_rec.preempt_bitstr, i))) {
			error("QOS ID %d has a loop at QOS %s",
			      begin_qosid, qos_rec.name);
			rc = 1;
			break;
		} else if (qos_rec.preempt_bitstr) {
			/*
			 * check this qos' preempt list and make sure
			 * no loops exist there either
			 */
			if ((rc = _preemption_loop(kingbase_conn, begin_qosid,
						   qos_rec.preempt_bitstr)))
				break;
		}
	}
	return rc;
}

static int _setup_qos_cond_limits(slurmdb_qos_cond_t *qos_cond, char **extra)
{
	int set = 0;
	ListIterator itr = NULL;
	char *object = NULL;

	xassert(extra);
	xassert(*extra);

	if (!qos_cond)
		return 0;

	/*
	 * Don't handle with_deleted here, we don't want to delete or modify
	 * deleted things
	 */
	/* if (qos_cond->with_deleted) */
	/* 	xstrcat(extra, "where (deleted=0 || deleted=1)"); */
	/* else */
	/* 	xstrcat(extra, "where deleted=0"); */

	if (qos_cond->description_list &&
	    list_count(qos_cond->description_list)) {
		set = 0;
		xstrcat(*extra, " and (");
		itr = list_iterator_create(qos_cond->description_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "description='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (qos_cond->id_list &&
	    list_count(qos_cond->id_list)) {
		set = 0;
		xstrcat(*extra, " and (");
		itr = list_iterator_create(qos_cond->id_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "id='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (qos_cond->name_list &&
	    list_count(qos_cond->name_list)) {
		set = 0;
		xstrcat(*extra, " and (");
		itr = list_iterator_create(qos_cond->name_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcat(*extra, " or ");
			xstrfmtcat(*extra, "name='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if ((qos_cond->preempt_mode != NO_VAL16) && qos_cond->preempt_mode) {
		set = 1;
		xstrfmtcat(*extra, " and (preempt_mode&%d",
			   qos_cond->preempt_mode);
		if (qos_cond->preempt_mode & PREEMPT_MODE_COND_OFF)
			xstrcat(*extra, " or preempt_mode=0");
		xstrcat(*extra, ")");
	}


	return set;
}

static int _setup_qos_limits(slurmdb_qos_rec_t *qos,
			     char **cols, char **vals,
			     char **extra, char **added_preempt,
			     bool for_add)
{
	uint32_t tres_str_flags = TRES_STR_FLAG_REMOVE |
		TRES_STR_FLAG_SORT_ID | TRES_STR_FLAG_SIMPLE |
		TRES_STR_FLAG_NO_NULL;

	if (!qos)
		return SLURM_ERROR;

	if (for_add) {
		/* If we are adding we should make sure we don't get
		   old reside sitting around from a former life.
		*/
		if (!qos->description)
			qos->description = xstrdup("");
		if (qos->flags & QOS_FLAG_NOTSET)
			qos->flags = 0;
		if (qos->grace_time == NO_VAL)
			qos->grace_time = 0;
		if (qos->grp_jobs == NO_VAL)
			qos->grp_jobs = INFINITE;
		if (qos->grp_jobs_accrue == NO_VAL)
			qos->grp_jobs_accrue = INFINITE;
		if (qos->grp_submit_jobs == NO_VAL)
			qos->grp_submit_jobs = INFINITE;
		if (qos->grp_wall == NO_VAL)
			qos->grp_wall = INFINITE;
		if (qos->max_jobs_pa == NO_VAL)
			qos->max_jobs_pa = INFINITE;
		if (qos->max_jobs_pu == NO_VAL)
			qos->max_jobs_pu = INFINITE;
		if (qos->max_jobs_accrue_pa == NO_VAL)
			qos->max_jobs_accrue_pa = INFINITE;
		if (qos->max_jobs_accrue_pu == NO_VAL)
			qos->max_jobs_accrue_pu = INFINITE;
		if (qos->min_prio_thresh == NO_VAL)
			qos->min_prio_thresh = INFINITE;
		if (qos->max_submit_jobs_pa == NO_VAL)
			qos->max_submit_jobs_pa = INFINITE;
		if (qos->max_submit_jobs_pu == NO_VAL)
			qos->max_submit_jobs_pu = INFINITE;
		if (qos->max_wall_pj == NO_VAL)
			qos->max_wall_pj = INFINITE;
		if (qos->preempt_exempt_time == NO_VAL)
			qos->preempt_exempt_time = INFINITE;
		if (qos->preempt_mode == NO_VAL16)
			qos->preempt_mode = 0;
		if (qos->priority == NO_VAL)
			qos->priority = 0;
		if (fuzzy_equal(qos->usage_factor, NO_VAL))
			qos->usage_factor = 1;
		if (fuzzy_equal(qos->usage_thres, NO_VAL))
			qos->usage_thres = (double)INFINITE;
		if (fuzzy_equal(qos->limit_factor, NO_VAL))
			qos->limit_factor = INFINITE;
	}

	if (qos->description) {
		xstrcat(*cols, ", description");
		xstrfmtcat(*vals, ", '%s'", qos->description);
		xstrfmtcat(*extra, ", description='%s'",
			   qos->description);
	}

	if (!(qos->flags & QOS_FLAG_NOTSET)) {
		if (qos->flags & QOS_FLAG_REMOVE) {
			if (qos->flags)
				xstrfmtcat(*extra, ", flags=(flags & ~%u)",
					   qos->flags & ~QOS_FLAG_REMOVE);
		} else {
			/* If we are only removing there is no reason
			   to set up the cols and vals. */
			if (qos->flags & QOS_FLAG_ADD) {
				if (qos->flags) {
					xstrfmtcat(*extra,
						   ", flags=(flags | %u)",
						   qos->flags & ~QOS_FLAG_ADD);
				}
			} else
				xstrfmtcat(*extra, ", flags=%u", qos->flags);
			xstrcat(*cols, ", flags");
			xstrfmtcat(*vals, ", '%u'", qos->flags & ~QOS_FLAG_ADD);
		}
	}

	if (qos->grace_time == INFINITE) {
		xstrcat(*cols, ", grace_time");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", grace_time=NULL");
	} else if ((qos->grace_time != NO_VAL) &&
		   ((int32_t)qos->grace_time >= 0)) {
		xstrcat(*cols, ", grace_time");
		xstrfmtcat(*vals, ", %u", qos->grace_time);
		xstrfmtcat(*extra, ", grace_time=%u", qos->grace_time);
	}

	if (qos->grp_jobs == INFINITE) {
		xstrcat(*cols, ", grp_jobs");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", grp_jobs=NULL");
	} else if ((qos->grp_jobs != NO_VAL)
		   && ((int32_t)qos->grp_jobs >= 0)) {
		xstrcat(*cols, ", grp_jobs");
		xstrfmtcat(*vals, ", %u", qos->grp_jobs);
		xstrfmtcat(*extra, ", grp_jobs=%u", qos->grp_jobs);
	}

	if (qos->grp_jobs_accrue == INFINITE) {
		xstrcat(*cols, ", grp_jobs_accrue");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", grp_jobs_accrue=NULL");
	} else if ((qos->grp_jobs_accrue != NO_VAL)
		   && ((int32_t)qos->grp_jobs_accrue >= 0)) {
		xstrcat(*cols, ", grp_jobs_accrue");
		xstrfmtcat(*vals, ", %u", qos->grp_jobs_accrue);
		xstrfmtcat(*extra, ", grp_jobs_accrue=%u",
			   qos->grp_jobs_accrue);
	}

	if (qos->grp_submit_jobs == INFINITE) {
		xstrcat(*cols, ", grp_submit_jobs");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", grp_submit_jobs=NULL");
	} else if ((qos->grp_submit_jobs != NO_VAL)
		   && ((int32_t)qos->grp_submit_jobs >= 0)) {
		xstrcat(*cols, ", grp_submit_jobs");
		xstrfmtcat(*vals, ", %u", qos->grp_submit_jobs);
		xstrfmtcat(*extra, ", grp_submit_jobs=%u",
			   qos->grp_submit_jobs);
	}

	if (qos->grp_wall == INFINITE) {
		xstrcat(*cols, ", grp_wall");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", grp_wall=NULL");
	} else if ((qos->grp_wall != NO_VAL)
		   && ((int32_t)qos->grp_wall >= 0)) {
		xstrcat(*cols, ", grp_wall");
		xstrfmtcat(*vals, ", %u", qos->grp_wall);
		xstrfmtcat(*extra, ", grp_wall=%u", qos->grp_wall);
	}

	if (qos->max_jobs_pa == INFINITE) {
		xstrcat(*cols, ", max_jobs_pa");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", max_jobs_pa=NULL");
	} else if ((qos->max_jobs_pa != NO_VAL)
		   && ((int32_t)qos->max_jobs_pa >= 0)) {
		xstrcat(*cols, ", max_jobs_pa");
		xstrfmtcat(*vals, ", %u", qos->max_jobs_pa);
		xstrfmtcat(*extra, ", max_jobs_pa=%u", qos->max_jobs_pa);
	}

	if (qos->max_jobs_pu == INFINITE) {
		xstrcat(*cols, ", max_jobs_per_user");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", max_jobs_per_user=NULL");
	} else if ((qos->max_jobs_pu != NO_VAL)
		   && ((int32_t)qos->max_jobs_pu >= 0)) {
		xstrcat(*cols, ", max_jobs_per_user");
		xstrfmtcat(*vals, ", %u", qos->max_jobs_pu);
		xstrfmtcat(*extra, ", max_jobs_per_user=%u", qos->max_jobs_pu);
	}

	if (qos->max_jobs_accrue_pa == INFINITE) {
		xstrcat(*cols, ", max_jobs_accrue_pa");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", max_jobs_accrue_pa=NULL");
	} else if ((qos->max_jobs_accrue_pa != NO_VAL)
		   && ((int32_t)qos->max_jobs_accrue_pa >= 0)) {
		xstrcat(*cols, ", max_jobs_accrue_pa");
		xstrfmtcat(*vals, ", %u", qos->max_jobs_accrue_pa);
		xstrfmtcat(*extra, ", max_jobs_accrue_pa=%u",
			   qos->max_jobs_accrue_pa);
	}

	if (qos->max_jobs_accrue_pu == INFINITE) {
		xstrcat(*cols, ", max_jobs_accrue_pu");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", max_jobs_accrue_pu=NULL");
	} else if ((qos->max_jobs_accrue_pu != NO_VAL)
		   && ((int32_t)qos->max_jobs_accrue_pu >= 0)) {
		xstrcat(*cols, ", max_jobs_accrue_pu");
		xstrfmtcat(*vals, ", %u", qos->max_jobs_accrue_pu);
		xstrfmtcat(*extra, ", max_jobs_accrue_pu=%u",
			   qos->max_jobs_accrue_pu);
	}

	if (qos->min_prio_thresh == INFINITE) {
		xstrcat(*cols, ", min_prio_thresh");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", min_prio_thresh=NULL");
	} else if ((qos->min_prio_thresh != NO_VAL)
		   && ((int32_t)qos->min_prio_thresh >= 0)) {
		xstrcat(*cols, ", min_prio_thresh");
		xstrfmtcat(*vals, ", %u", qos->min_prio_thresh);
		xstrfmtcat(*extra, ", min_prio_thresh=%u",
			   qos->min_prio_thresh);
	}

	if (qos->max_submit_jobs_pa == INFINITE) {
		xstrcat(*cols, ", max_submit_jobs_pa");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", max_submit_jobs_pa=NULL");
	} else if ((qos->max_submit_jobs_pa != NO_VAL)
		   && ((int32_t)qos->max_submit_jobs_pa >= 0)) {
		xstrcat(*cols, ", max_submit_jobs_pa");
		xstrfmtcat(*vals, ", %u", qos->max_submit_jobs_pa);
		xstrfmtcat(*extra, ", max_submit_jobs_pa=%u",
			   qos->max_submit_jobs_pa);
	}

	if (qos->max_submit_jobs_pu == INFINITE) {
		xstrcat(*cols, ", max_submit_jobs_per_user");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", max_submit_jobs_per_user=NULL");
	} else if ((qos->max_submit_jobs_pu != NO_VAL)
		   && ((int32_t)qos->max_submit_jobs_pu >= 0)) {
		xstrcat(*cols, ", max_submit_jobs_per_user");
		xstrfmtcat(*vals, ", %u", qos->max_submit_jobs_pu);
		xstrfmtcat(*extra, ", max_submit_jobs_per_user=%u",
			   qos->max_submit_jobs_pu);
	}

	if ((qos->max_wall_pj != NO_VAL)
	    && ((int32_t)qos->max_wall_pj >= 0)) {
		xstrcat(*cols, ", max_wall_duration_per_job");
		xstrfmtcat(*vals, ", %u", qos->max_wall_pj);
		xstrfmtcat(*extra, ", max_wall_duration_per_job=%u",
			   qos->max_wall_pj);
	} else if (qos->max_wall_pj == INFINITE) {
		xstrcat(*cols, ", max_wall_duration_per_job");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", max_wall_duration_per_job=NULL");
	}

	if (qos->preempt_list && list_count(qos->preempt_list)) {
		char *preempt_val = NULL;
		char *tmp_char = NULL, *last_preempt = NULL;
		bool adding_straight = 0;
		ListIterator preempt_itr =
			list_iterator_create(qos->preempt_list);

		xstrcat(*cols, ", preempt");

		while ((tmp_char = list_next(preempt_itr))) {
			if (tmp_char[0] == '-') {
				preempt_val = xstrdup_printf(
					"replace(%s, ',%s,', ',')",
					last_preempt ? last_preempt : "preempt",
					tmp_char+1);
				xfree(last_preempt);
				last_preempt = preempt_val;
				preempt_val = NULL;
			} else if (tmp_char[0] == '+') {
				preempt_val = xstrdup_printf(
					"replace(concat(replace(%s, "
					"',%s,', ''), ',%s,'), ',,', ',')",
					last_preempt ? last_preempt : "preempt",
					tmp_char+1, tmp_char+1);
				if (added_preempt)
					xstrfmtcat(*added_preempt, ",%s",
						   tmp_char+1);
				xfree(last_preempt);
				last_preempt = preempt_val;
				preempt_val = NULL;
			} else if (tmp_char[0]) {
				xstrfmtcat(preempt_val, ",%s", tmp_char);
				if (added_preempt)
					xstrfmtcat(*added_preempt, ",%s",
						   tmp_char);
				adding_straight = 1;
			} else
				xstrcat(preempt_val, "");
		}
		list_iterator_destroy(preempt_itr);
		if (last_preempt) {
			preempt_val = last_preempt;
			last_preempt = NULL;
		}
		if (adding_straight) {
			xstrfmtcat(*vals, ", \'%s,\'", preempt_val);
			xstrfmtcat(*extra, ", preempt=\'%s,\'", preempt_val);
		} else if (preempt_val && preempt_val[0]) {
			xstrfmtcat(*vals, ", %s", preempt_val);
			xstrfmtcat(*extra, ", preempt=if(%s=',', '', %s)",
				   preempt_val, preempt_val);
		} else {
			xstrcat(*vals, ", ''");
			xstrcat(*extra, ", preempt=''");
		}
		xfree(preempt_val);
	}

	if (qos->preempt_exempt_time == INFINITE) {
		xstrcat(*cols, ", preempt_exempt_time");
		xstrfmtcat(*vals, ", NULL");
		xstrfmtcat(*extra, ", preempt_exempt_time=NULL");
	} else if (qos->preempt_exempt_time != NO_VAL) {
		xstrcat(*cols, ", preempt_exempt_time");
		xstrfmtcat(*vals, ", %u", qos->preempt_exempt_time);
		xstrfmtcat(*extra, ", preempt_exempt_time=%u",
			   qos->preempt_exempt_time);
	}

	if (qos->preempt_mode != NO_VAL16) {
		xstrcat(*cols, ", preempt_mode");
		xstrfmtcat(*vals, ", %u", qos->preempt_mode);
		xstrfmtcat(*extra, ", preempt_mode=%u", qos->preempt_mode);
	}

	if (qos->priority == INFINITE) {
		xstrcat(*cols, ", priority");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", priority=NULL");
	} else if ((qos->priority != NO_VAL)
		   && ((int32_t)qos->priority >= 0)) {
		xstrcat(*cols, ", priority");
		xstrfmtcat(*vals, ", %u", qos->priority);
		xstrfmtcat(*extra, ", priority=%u", qos->priority);
	}

	if (fuzzy_equal(qos->usage_factor, INFINITE)) {
		xstrcat(*cols, ", usage_factor");
		xstrcat(*vals, ", 1");
		xstrcat(*extra, ", usage_factor=1");
	} else if (!fuzzy_equal(qos->usage_factor, NO_VAL)
		   && (qos->usage_factor >= 0)) {
		xstrcat(*cols, ", usage_factor");
		xstrfmtcat(*vals, ", %f", qos->usage_factor);
		xstrfmtcat(*extra, ", usage_factor=%f", qos->usage_factor);
	}

	if (fuzzy_equal(qos->usage_thres, INFINITE)) {
		xstrcat(*cols, ", usage_thres");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", usage_thres=NULL");
	} else if (!fuzzy_equal(qos->usage_thres, NO_VAL)
		   && (qos->usage_thres >= 0)) {
		xstrcat(*cols, ", usage_thres");
		xstrfmtcat(*vals, ", %f", qos->usage_thres);
		xstrfmtcat(*extra, ", usage_thres=%f", qos->usage_thres);
	}

	if (fuzzy_equal(qos->limit_factor, INFINITE)) {
		xstrcat(*cols, ", limit_factor");
		xstrcat(*vals, ", NULL");
		xstrcat(*extra, ", limit_factor=NULL");
	} else if (!fuzzy_equal(qos->limit_factor, NO_VAL)
		   && (qos->limit_factor > 0)) {
		xstrcat(*cols, ", limit_factor");
		xstrfmtcat(*vals, ", %f", qos->limit_factor);
		xstrfmtcat(*extra, ", limit_factor=%f", qos->limit_factor);
	}

	/* When modifying anything below this comment it happens in
	 * the actual function since we have to wait until we hear
	 * about the original first.
	 * What we do to make it known something needs to be changed
	 * is we cat "" onto extra which will inform the caller
	 * something needs changing.
	 */

	if (qos->grp_tres) {
		if (!for_add) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", grp_tres");
		slurmdb_combine_tres_strings(
			&qos->grp_tres, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", qos->grp_tres);
		xstrfmtcat(*extra, ", grp_tres='%s'", qos->grp_tres);
	}

	if (qos->grp_tres_mins) {
		if (!for_add) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", grp_tres_mins");
		slurmdb_combine_tres_strings(
			&qos->grp_tres_mins, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", qos->grp_tres_mins);
		xstrfmtcat(*extra, ", grp_tres_mins='%s'",
			   qos->grp_tres_mins);
	}

	if (qos->grp_tres_run_mins) {
		if (!for_add) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", grp_tres_run_mins");
		slurmdb_combine_tres_strings(
			&qos->grp_tres_run_mins, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", qos->grp_tres_run_mins);
		xstrfmtcat(*extra, ", grp_tres_run_mins='%s'",
			   qos->grp_tres_run_mins);
	}

	if (qos->max_tres_pa) {
		if (!for_add) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", max_tres_pa");
		slurmdb_combine_tres_strings(
			&qos->max_tres_pa, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", qos->max_tres_pa);
		xstrfmtcat(*extra, ", max_tres_pa='%s'", qos->max_tres_pa);
	}

	if (qos->max_tres_pj) {
		if (!for_add) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", max_tres_pj");
		slurmdb_combine_tres_strings(
			&qos->max_tres_pj, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", qos->max_tres_pj);
		xstrfmtcat(*extra, ", max_tres_pj='%s'", qos->max_tres_pj);
	}

	if (qos->max_tres_pn) {
		if (!for_add) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", max_tres_pn");
		slurmdb_combine_tres_strings(
			&qos->max_tres_pn, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", qos->max_tres_pn);
		xstrfmtcat(*extra, ", max_tres_pn='%s'", qos->max_tres_pn);
	}

	if (qos->max_tres_pu) {
		if (!for_add) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", max_tres_pu");
		slurmdb_combine_tres_strings(
			&qos->max_tres_pu, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", qos->max_tres_pu);
		xstrfmtcat(*extra, ", max_tres_pu='%s'", qos->max_tres_pu);
	}

	if (qos->max_tres_mins_pj) {
		if (!for_add) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", max_tres_mins_pj");
		slurmdb_combine_tres_strings(
			&qos->max_tres_mins_pj, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", qos->max_tres_mins_pj);
		xstrfmtcat(*extra, ", max_tres_mins_pj='%s'",
			   qos->max_tres_mins_pj);
	}

	if (qos->max_tres_run_mins_pa) {
		if (!for_add) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", max_tres_run_mins_pa");
		slurmdb_combine_tres_strings(
			&qos->max_tres_run_mins_pa, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", qos->max_tres_run_mins_pa);
		xstrfmtcat(*extra, ", max_tres_run_mins_pa='%s'",
			   qos->max_tres_run_mins_pa);
	}

	if (qos->max_tres_run_mins_pu) {
		if (!for_add) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", max_tres_run_mins_pu");
		slurmdb_combine_tres_strings(
			&qos->max_tres_run_mins_pu, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", qos->max_tres_run_mins_pu);
		xstrfmtcat(*extra, ", max_tres_run_mins_pu='%s'",
			   qos->max_tres_run_mins_pu);
	}

	if (qos->min_tres_pj) {
		if (!for_add) {
			xstrcat(*extra, "");
			goto end_modify;
		}
		xstrcat(*cols, ", min_tres_pj");
		slurmdb_combine_tres_strings(
			&qos->min_tres_pj, NULL, tres_str_flags);
		xstrfmtcat(*vals, ", '%s'", qos->min_tres_pj);
		xstrfmtcat(*extra, ", min_tres_pj='%s'", qos->min_tres_pj);
	}

end_modify:

	return SLURM_SUCCESS;

}

extern int as_kingbase_add_qos(kingbase_conn_t *kingbase_conn, uint32_t uid,
			    List qos_list)
{
	ListIterator itr = NULL;
	int rc = SLURM_SUCCESS;
	slurmdb_qos_rec_t *object = NULL;
	char *cols = NULL, *extra = NULL, *vals = NULL, *query = NULL,
		*tmp_extra = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	int affect_rows = 0;
	int added = 0;
	char *added_preempt = NULL;
	uint32_t qos_cnt;
	assoc_mgr_lock_t locks = { NO_LOCK, NO_LOCK, READ_LOCK, NO_LOCK,
				   NO_LOCK, NO_LOCK, NO_LOCK };

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	if (!is_user_min_admin_level(kingbase_conn, uid, SLURMDB_ADMIN_SUPER_USER))
		return ESLURM_ACCESS_DENIED;

	assoc_mgr_lock(&locks);
	qos_cnt = g_qos_count;
	assoc_mgr_unlock(&locks);

	user_name = uid_to_string((uid_t) uid);
	itr = list_iterator_create(qos_list);

	while ((object = list_next(itr))) {
		if (!object->name || !object->name[0]) {
			error("We need a qos name to add.");
			rc = SLURM_ERROR;
			continue;
		}
		xstrcat(cols, "creation_time, mod_time, name");
		xstrfmtcat(vals, "%ld, %ld, '%s'",
			   now, now, object->name);
		xstrfmtcat(extra, ", mod_time=%ld", now);

		_setup_qos_limits(object, &cols, &vals,
				  &extra, &added_preempt, 1);
		if (added_preempt) {
			object->preempt_bitstr = bit_alloc(qos_cnt);
			bit_unfmt(object->preempt_bitstr, added_preempt+1);
			xfree(added_preempt);
		}

		xstrfmtcat(query,
			   "insert into %s (%s) values (%s) "
			   //"on duplicate key update deleted=0, "
			   //"id=nextval(id)%s;",
			   "on duplicate key update deleted=0 "
			   "%s;",
			   qos_table, cols, vals, extra);
        char *query2 = NULL;
		xstrfmtcat(query2,
				"insert into %s (%s) values (%s) "
				//"on duplicate key update deleted=0, "
				//"id=nextval(id)%s;",
				"on duplicate key update deleted=0 "
				"%s returning id;",
				qos_table, cols, vals, extra); 

		DB_DEBUG(DB_QOS, kingbase_conn->conn, "query\n%s", query);
		//error("[query] %d: %s", __LINE__, query);

		fetch_flag_t* fetch_flag = NULL;
		fetch_result_t* data_rt = NULL;
		fetch_flag = set_fetch_flag(true, false, true);
		data_rt = xmalloc(sizeof(fetch_result_t));
		rc = kingbase_for_fetch2(kingbase_conn, query, fetch_flag, data_rt,query2);
		object->id = (uint32_t)data_rt->insert_ret_id;
        

		xfree(query);
		xfree(query2);
		if (!object->id) {
			error("Couldn't add qos %s", object->name);
			added=0;
			xfree(cols);
			xfree(extra);
			xfree(vals);
			free_res_data(data_rt, fetch_flag); 
			break;
		}

		//KCIResult *res = NULL;

		affect_rows =data_rt->rows_affect;
		//KCIResultDealloc(res);

		if (!affect_rows) {
			debug2("nothing changed %d", affect_rows);
			xfree(cols);
			xfree(extra);
			xfree(vals);
			free_res_data(data_rt, fetch_flag); 
			continue;
		}
		free_res_data(data_rt, fetch_flag); 
		/* we always have a ', ' as the first 2 chars */
		tmp_extra = slurm_add_slash_to_quotes2(extra+2);

		xstrfmtcat(query,
			   "insert into %s "
			   "(timestamp, action, name, actor, info) "
			   "values (%ld, %u, '%s', '%s', '%s');",
			   txn_table,
			   now, DBD_ADD_QOS, object->name, user_name,
			   tmp_extra);

		xfree(tmp_extra);
		xfree(cols);
		xfree(extra);
		xfree(vals);
		debug4("query\n%s",query);
		//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

		fetch_flag = set_fetch_flag(false, false, false);
		data_rt = xmalloc(sizeof(fetch_result_t));
		rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
		free_res_data(data_rt, fetch_flag);
		xfree(query);
		if (rc == SLURM_ERROR) {
			error("Couldn't add txn");
		} else {
			if (addto_update_list(kingbase_conn->update_list,
					      SLURMDB_ADD_QOS,
					      object) == SLURM_SUCCESS)
				list_remove(itr);
			added++;
		}

	}
	list_iterator_destroy(itr);
	xfree(user_name);

	if (!added) {
		reset_kingbase_conn(kingbase_conn);
	}

	return rc;
}

extern List as_kingbase_modify_qos(kingbase_conn_t *kingbase_conn, uint32_t uid,
				slurmdb_qos_cond_t *qos_cond,
				slurmdb_qos_rec_t *qos)
{
	List ret_list = NULL;
	int rc = SLURM_SUCCESS;
	char *object = NULL;
	char *vals = NULL, *extra = NULL, *query = NULL, *name_char = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	int i;
	bool flag = false;
	KCIResult *result = NULL;
	char *tmp_char1=NULL, *tmp_char2=NULL;
	bitstr_t *preempt_bitstr = NULL;
	char *added_preempt = NULL;
	uint32_t qos_cnt;
	assoc_mgr_lock_t locks = { NO_LOCK, NO_LOCK, READ_LOCK, NO_LOCK,
				   NO_LOCK, NO_LOCK, NO_LOCK };

	if (!qos_cond || !qos) {
		error("we need something to change");
		return NULL;
	}

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	if (!is_user_min_admin_level(kingbase_conn, uid,
				     SLURMDB_ADMIN_SUPER_USER)) {
		errno = ESLURM_ACCESS_DENIED;
		return NULL;
	}

	xstrcat(extra, "where deleted=0");

	_setup_qos_cond_limits(qos_cond, &extra);

	_setup_qos_limits(qos, &tmp_char1, &tmp_char2,
			  &vals, &added_preempt, 0);

	assoc_mgr_lock(&locks);
	qos_cnt = g_qos_count;
	assoc_mgr_unlock(&locks);

	if (added_preempt) {
		preempt_bitstr = bit_alloc(qos_cnt);
		bit_unfmt(preempt_bitstr, added_preempt+1);
		xfree(added_preempt);
	}
	xfree(tmp_char1);
	xfree(tmp_char2);

	if (!extra || !vals) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		FREE_NULL_BITMAP(preempt_bitstr);
		error("Nothing to change");
		return NULL;
	}

	object = xstrdup(mqos_req_inx[0]);
	for (i = 1; i < MQOS_COUNT; i++)
		xstrfmtcat(object, ", %s", mqos_req_inx[i]);

	query = xstrdup_printf("select %s from %s %s;",
			       object, qos_table, extra);
	xfree(extra);
	xfree(object);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		FREE_NULL_BITMAP(preempt_bitstr);
		return NULL;
	}

	rc = 0;
	ret_list = list_create(xfree_ptr);
	int row = KCIResultGetRowCount(result);
	for(int i = 0; i < row; i++) {
		slurmdb_qos_rec_t *qos_rec = NULL;
	    uint32_t id = 0;
		char *tmp_count = KCIResultGetColumnValue(result, i, MQOS_ID);
		if(tmp_count)
	    	id= slurm_atoul(tmp_count);
		if (preempt_bitstr) {
			if (_preemption_loop(kingbase_conn, id, preempt_bitstr)){
				//flag = checkIfResultIsNull(result, i);
				flag = true;
				break;
			}				
		}
		object = xstrdup(KCIResultGetColumnValue(result, i, MQOS_NAME));
		list_append(ret_list, object);
		if (!rc) {
			xstrfmtcat(name_char, "(name='%s'", object);
			rc = 1;
		} else  {
			xstrfmtcat(name_char, " or name='%s'", object);
		}

		qos_rec = xmalloc(sizeof(slurmdb_qos_rec_t));
		qos_rec->name = xstrdup(object);
		qos_rec->id = id;
		qos_rec->flags = qos->flags;

		qos_rec->grace_time = qos->grace_time;

		mod_tres_str(&qos_rec->grp_tres,
			     qos->grp_tres, KCIResultGetColumnValue(result, i, MQOS_GT),
			     NULL, "grp_tres", &vals, qos_rec->id, 0);
		mod_tres_str(&qos_rec->grp_tres_mins,
			     qos->grp_tres_mins, KCIResultGetColumnValue(result, i, MQOS_GTM),
			     NULL, "grp_tres_mins", &vals, qos_rec->id, 0);
		mod_tres_str(&qos_rec->grp_tres_run_mins,
			     qos->grp_tres_run_mins, KCIResultGetColumnValue(result, i, MQOS_GTRM),
			     NULL, "grp_tres_run_mins", &vals,
			     qos_rec->id, 0);

		qos_rec->grp_jobs = qos->grp_jobs;
		qos_rec->grp_jobs_accrue = qos->grp_jobs_accrue;
		qos_rec->grp_submit_jobs = qos->grp_submit_jobs;
		qos_rec->grp_wall = qos->grp_wall;

		mod_tres_str(&qos_rec->max_tres_pa,
			     qos->max_tres_pa, KCIResultGetColumnValue(result, i, MQOS_MTPA),
			     NULL, "max_tres_pa", &vals, qos_rec->id, 0);
		mod_tres_str(&qos_rec->max_tres_pj,
			     qos->max_tres_pj, KCIResultGetColumnValue(result, i, MQOS_MTPJ),
			     NULL, "max_tres_pj", &vals, qos_rec->id, 0);
		mod_tres_str(&qos_rec->max_tres_pn,
			     qos->max_tres_pn, KCIResultGetColumnValue(result, i, MQOS_MTPN),
			     NULL, "max_tres_pn", &vals, qos_rec->id, 0);
		mod_tres_str(&qos_rec->max_tres_pu,
			     qos->max_tres_pu, KCIResultGetColumnValue(result, i, MQOS_MTPU),
			     NULL, "max_tres_pu", &vals, qos_rec->id, 0);
		mod_tres_str(&qos_rec->max_tres_mins_pj,
			     qos->max_tres_mins_pj, KCIResultGetColumnValue(result, i, MQOS_MTMPJ),
			     NULL, "max_tres_mins_pj", &vals, qos_rec->id, 0);
		mod_tres_str(&qos_rec->max_tres_run_mins_pa,
			     qos->max_tres_run_mins_pa, KCIResultGetColumnValue(result, i, MQOS_MTRM),
			     NULL, "max_tres_run_mins_pa", &vals,
			     qos_rec->id, 0);
		mod_tres_str(&qos_rec->max_tres_run_mins_pu,
			     qos->max_tres_run_mins_pu, KCIResultGetColumnValue(result, i, MQOS_MTRM),
			     NULL, "max_tres_run_mins_pu", &vals,
			     qos_rec->id, 0);

		qos_rec->max_jobs_pa  = qos->max_jobs_pa;
		qos_rec->max_jobs_pu  = qos->max_jobs_pu;
		qos_rec->max_jobs_accrue_pa  = qos->max_jobs_accrue_pa;
		qos_rec->max_jobs_accrue_pu  = qos->max_jobs_accrue_pu;
		qos_rec->min_prio_thresh  = qos->min_prio_thresh;
		qos_rec->max_submit_jobs_pa  = qos->max_submit_jobs_pa;
		qos_rec->max_submit_jobs_pu  = qos->max_submit_jobs_pu;
		qos_rec->max_wall_pj = qos->max_wall_pj;

		mod_tres_str(&qos_rec->min_tres_pj,
			     qos->min_tres_pj, KCIResultGetColumnValue(result, i, MQOS_MITPJ),
			     NULL, "min_tres_pj", &vals, qos_rec->id, 0);

		qos_rec->preempt_mode = qos->preempt_mode;
		qos_rec->priority = qos->priority;

		if (qos->preempt_list) {
			ListIterator new_preempt_itr =
				list_iterator_create(qos->preempt_list);
			char *new_preempt = NULL;
			bool cleared = 0;

			qos_rec->preempt_bitstr = bit_alloc(qos_cnt);
            char *tmp_str = KCIResultGetColumnValue(result, i, MQOS_PREEMPT);
			if (tmp_str&& tmp_str[0])
				bit_unfmt(qos_rec->preempt_bitstr,tmp_str+1);

			while ((new_preempt = list_next(new_preempt_itr))) {
				if (new_preempt[0] == '-') {
					bit_clear(qos_rec->preempt_bitstr,
						  atol(new_preempt+1));
				} else if (new_preempt[0] == '+') {
					bit_set(qos_rec->preempt_bitstr,
						atol(new_preempt+1));
				} else {
					if (!cleared) {
						cleared = 1;
						bit_nclear(
							qos_rec->preempt_bitstr,
							0,
							qos_cnt-1);
					}

					bit_set(qos_rec->preempt_bitstr,
						atol(new_preempt));
				}
			}
			list_iterator_destroy(new_preempt_itr);
		}

		qos_rec->preempt_exempt_time = qos->preempt_exempt_time;

		qos_rec->usage_factor = qos->usage_factor;
		qos_rec->usage_thres = qos->usage_thres;
		qos_rec->limit_factor = qos->limit_factor;

		if (addto_update_list(kingbase_conn->update_list,
				      SLURMDB_MODIFY_QOS, qos_rec)
		    != SLURM_SUCCESS)
			slurmdb_destroy_qos_rec(qos_rec);
	}
	KCIResultDealloc(result);

	FREE_NULL_BITMAP(preempt_bitstr);

	if (flag) {
		xfree(vals);
		xfree(name_char);
		xfree(query);
		FREE_NULL_LIST(ret_list);
		ret_list = NULL;
		errno = ESLURM_QOS_PREEMPTION_LOOP;
		return ret_list;
	}

	if (!list_count(ret_list)) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		DB_DEBUG(DB_QOS, kingbase_conn->conn,
		         "didn't affect anything\n%s", query);
		xfree(vals);
		xfree(query);
		return ret_list;
	}
	xfree(query);
	xstrcat(name_char, ")");

	user_name = uid_to_string((uid_t) uid);
	rc = modify_common(kingbase_conn, DBD_MODIFY_QOS, now,
			   user_name, qos_table, name_char, vals, NULL);
	xfree(user_name);
	xfree(name_char);
	xfree(vals);
	if (rc == SLURM_ERROR) {
		error("Couldn't modify qos");
		FREE_NULL_LIST(ret_list);
		ret_list = NULL;
	}

	return ret_list;
}

extern List as_kingbase_remove_qos(kingbase_conn_t *kingbase_conn, uint32_t uid,
				slurmdb_qos_cond_t *qos_cond)
{
	ListIterator itr = NULL;
	List ret_list = NULL;
	int rc = SLURM_SUCCESS;
	char *object = NULL;
	char *extra = NULL, *query = NULL,
		*name_char = NULL, *assoc_char = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	KCIResult *result = NULL;
	List cluster_list_tmp = NULL;

	if (!qos_cond) {
		error("we need something to change");
		return NULL;
	}

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	if (!is_user_min_admin_level(
		    kingbase_conn, uid, SLURMDB_ADMIN_SUPER_USER)) {
		errno = ESLURM_ACCESS_DENIED;
		return NULL;
	}

	xstrcat(extra, "where deleted=0");

	_setup_qos_cond_limits(qos_cond, &extra);

	if (!extra) {
		error("Nothing to remove");
		return NULL;
	}

	query = xstrdup_printf("select id, name from %s %s;", qos_table, extra);
	xfree(extra);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return NULL;
	}

	name_char = NULL;
	ret_list = list_create(xfree_ptr);
	for(int i = 0; i < KCIResultGetRowCount(result); i++) {
		slurmdb_qos_rec_t *qos_rec = NULL;

		list_append(ret_list, xstrdup(KCIResultGetColumnValue(result, i, 1)));
		if (!name_char)
			xstrfmtcat(name_char, "id='%s'", KCIResultGetColumnValue(result, i, 0));
		else
			xstrfmtcat(name_char, " or id='%s'", KCIResultGetColumnValue(result, i, 0));
		if (!assoc_char)
			xstrfmtcat(assoc_char, "id_qos='%s'", KCIResultGetColumnValue(result, i, 0));
		else
			xstrfmtcat(assoc_char, " or id_qos='%s'", KCIResultGetColumnValue(result, i, 0));
		xstrfmtcat(extra,
			   ", qos=replace(qos, ',%s,', '')"
			   ", delta_qos=replace(replace(delta_qos, ',+%s,', ''), ',-%s,', '')",
			   KCIResultGetColumnValue(result, i, 0), KCIResultGetColumnValue(result, i, 0), KCIResultGetColumnValue(result, i, 0));

		qos_rec = xmalloc(sizeof(slurmdb_qos_rec_t));
		/* we only need id when removing no real need to init */
		qos_rec->id = slurm_atoul(KCIResultGetColumnValue(result, i, 0));
		if (addto_update_list(kingbase_conn->update_list,
				      SLURMDB_REMOVE_QOS, qos_rec)
		    != SLURM_SUCCESS)
			slurmdb_destroy_qos_rec(qos_rec);
	}
	KCIResultDealloc(result);

	if (!list_count(ret_list)) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		DB_DEBUG(DB_QOS, kingbase_conn->conn,
		         "didn't affect anything\n%s", query);
		xfree(query);
		return ret_list;
	}
	xfree(query);

	user_name = uid_to_string((uid_t) uid);

	slurm_rwlock_rdlock(&as_kingbase_cluster_list_lock);
	cluster_list_tmp = list_shallow_copy(as_kingbase_cluster_list);
	if (list_count(cluster_list_tmp)) {
#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
		slurm_mutex_lock(&assoc_lock);
#endif
		itr = list_iterator_create(cluster_list_tmp);
		while ((object = list_next(itr))) {
			/*
			 * remove this qos from all the associations
			 * that have it
			 */
			query = xstrdup_printf("update `%s_%s` set mod_time=%ld %s where deleted=0;",
					       object, assoc_table,
					       now, extra);
			DB_DEBUG(DB_QOS, kingbase_conn->conn, "query\n%s", query);
			//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);

			fetch_flag_t *fetch_flag = NULL;
			fetch_result_t *data_rt = NULL;
			fetch_flag = set_fetch_flag(false, false, false);
			data_rt = xmalloc(sizeof(fetch_result_t));
			rc = kingbase_for_fetch(kingbase_conn, query, fetch_flag, data_rt);
			free_res_data(data_rt, fetch_flag);
			xfree(query);
			if (rc == SLURM_ERROR) {
				reset_kingbase_conn(kingbase_conn);
				break;
			}

			if ((rc = remove_common(kingbase_conn, DBD_REMOVE_QOS, now,
						user_name, qos_table, name_char,
						assoc_char, object, NULL, NULL, 
						NULL))
			    != SLURM_SUCCESS)
				break;
		}
		list_iterator_destroy(itr);
#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
		slurm_mutex_unlock(&assoc_lock);
#endif
	} else
		rc = remove_common(kingbase_conn, DBD_REMOVE_QOS, now,
				   user_name, qos_table, name_char,
				   assoc_char, NULL, NULL, NULL, NULL);

	FREE_NULL_LIST(cluster_list_tmp);
	slurm_rwlock_unlock(&as_kingbase_cluster_list_lock);

	xfree(extra);
	xfree(assoc_char);
	xfree(name_char);
	xfree(user_name);
	if (rc != SLURM_SUCCESS) {
		FREE_NULL_LIST(ret_list);
		return NULL;
	}

	return ret_list;
}

extern List as_kingbase_get_qos(kingbase_conn_t *kingbase_conn, uid_t uid,
			     slurmdb_qos_cond_t *qos_cond)
{
	char *query = NULL;
	char *extra = NULL;
	char *tmp = NULL;
	List qos_list = NULL;
	int i=0;
	KCIResult *result = NULL;
	uint32_t qos_cnt;
	assoc_mgr_lock_t locks = { NO_LOCK, NO_LOCK, READ_LOCK, NO_LOCK,
				   NO_LOCK, NO_LOCK, NO_LOCK };

	/* if this changes you will need to edit the corresponding enum */
	char *qos_req_inx[] = {
		"name",
		"description",
		"id",
		"flags",
		"grace_time",
		"grp_tres_mins",
		"grp_tres_run_mins",
		"grp_tres",
		"grp_jobs",
		"grp_jobs_accrue",
		"grp_submit_jobs",
		"grp_wall",
		"max_tres_mins_pj",
		"max_tres_run_mins_pa",
		"max_tres_run_mins_pu",
		"max_tres_pa",
		"max_tres_pj",
		"max_tres_pn",
		"max_tres_pu",
		"max_jobs_pa",
		"max_jobs_per_user",
		"max_jobs_accrue_pa",
		"max_jobs_accrue_pu",
		"min_prio_thresh",
		"max_submit_jobs_pa",
		"max_submit_jobs_per_user",
		"max_wall_duration_per_job",
		"substr(preempt, 1, length(preempt))",
		"preempt_mode",
		"preempt_exempt_time",
		"priority",
		"usage_factor",
		"usage_thres",
		"min_tres_pj",
		"limit_factor",
	};
	enum {
		QOS_REQ_NAME,
		QOS_REQ_DESC,
		QOS_REQ_ID,
		QOS_REQ_FLAGS,
		QOS_REQ_GRACE,
		QOS_REQ_GTM,
		QOS_REQ_GTRM,
		QOS_REQ_GT,
		QOS_REQ_GJ,
		QOS_REQ_GJA,
		QOS_REQ_GSJ,
		QOS_REQ_GW,
		QOS_REQ_MTMPJ,
		QOS_REQ_MTRMA,
		QOS_REQ_MTRM,
		QOS_REQ_MTPA,
		QOS_REQ_MTPJ,
		QOS_REQ_MTPN,
		QOS_REQ_MTPU,
		QOS_REQ_MJPA,
		QOS_REQ_MJPU,
		QOS_REQ_MJAPA,
		QOS_REQ_MJAPU,
		QOS_REQ_MPT,
		QOS_REQ_MSJPA,
		QOS_REQ_MSJPU,
		QOS_REQ_MWPJ,
		QOS_REQ_PREE,
		QOS_REQ_PREEM,
		QOS_REQ_PREXMPT,
		QOS_REQ_PRIO,
		QOS_REQ_UF,
		QOS_REQ_UT,
		QOS_REQ_MITPJ,
		QOS_REQ_LF,
		QOS_REQ_COUNT
	};

	if (check_connection(kingbase_conn) != SLURM_SUCCESS)
		return NULL;

	if (!qos_cond) {
		xstrcat(extra, "where deleted=0");
		goto empty;
	}

	if (qos_cond->with_deleted)
		xstrcat(extra, "where (deleted=0 or deleted=1)");
	else
		xstrcat(extra, "where deleted=0");

	_setup_qos_cond_limits(qos_cond, &extra);

empty:

	xfree(tmp);
	xstrfmtcat(tmp, "%s", qos_req_inx[i]);
	for(i=1; i<QOS_REQ_COUNT; i++) {
		xstrfmtcat(tmp, ", %s", qos_req_inx[i]);
	}

	query = xstrdup_printf("select %s from %s %s", tmp, qos_table, extra);
	xfree(tmp);
	xfree(extra);

	DB_DEBUG(DB_QOS, kingbase_conn->conn, "query\n%s", query);
	//info("[query] line %d, %s: query: %s", __LINE__, __func__, query);
	result = kingbase_db_query_ret(kingbase_conn, query, 0);
	if (KCIResultGetStatusCode(result) != EXECUTE_TUPLES_OK) {
		KCIResultDealloc(result);
		xfree(query);
		return NULL;
	}
	xfree(query);

	qos_list = list_create(slurmdb_destroy_qos_rec);

	assoc_mgr_lock(&locks);
	qos_cnt = g_qos_count;
	assoc_mgr_unlock(&locks);

	for(int i = 0; i < KCIResultGetRowCount(result); i++) {
		slurmdb_qos_rec_t *qos = xmalloc(sizeof(slurmdb_qos_rec_t));
		list_append(qos_list, qos);
         char *tmp_qos = NULL;
		 tmp_qos = KCIResultGetColumnValue(result, i, QOS_REQ_DESC) ;
		if (*tmp_qos != '\0')
			qos->description = xstrdup(KCIResultGetColumnValue(result, i, QOS_REQ_DESC));

		qos->id = slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_ID));

		qos->flags = slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_FLAGS));
		tmp_qos = KCIResultGetColumnValue(result, i, QOS_REQ_NAME);
		if (*tmp_qos != '\0')
			qos->name = xstrdup(KCIResultGetColumnValue(result, i, QOS_REQ_NAME));
		
		tmp_qos = KCIResultGetColumnValue(result, i, QOS_REQ_GRACE);
		if (*tmp_qos != '\0')////////////////////////////
			qos->grace_time = slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_GRACE));

		tmp_qos = KCIResultGetColumnValue(result, i, QOS_REQ_GT);
		if (*tmp_qos != '\0')
			qos->grp_tres = xstrdup(KCIResultGetColumnValue(result, i, QOS_REQ_GT));
	
	    tmp_qos =  KCIResultGetColumnValue(result, i, QOS_REQ_GTM);
		if (*tmp_qos != '\0')
			qos->grp_tres_mins = xstrdup(KCIResultGetColumnValue(result, i, QOS_REQ_GTM));

		tmp_qos = KCIResultGetColumnValue(result, i, QOS_REQ_GTRM);
		if (*tmp_qos != '\0')
			qos->grp_tres_run_mins = xstrdup(KCIResultGetColumnValue(result, i, QOS_REQ_GTRM));

		tmp_qos = KCIResultGetColumnValue(result, i, QOS_REQ_GJ);
		if (*tmp_qos != '\0')
			qos->grp_jobs = slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_GJ));
		else
			qos->grp_jobs = INFINITE;
		
		tmp_qos = KCIResultGetColumnValue(result, i, QOS_REQ_GJA);
		if (*tmp_qos != '\0')
			qos->grp_jobs_accrue = slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_GJA));
		else
			qos->grp_jobs_accrue = INFINITE;

		tmp_qos = KCIResultGetColumnValue(result, i, QOS_REQ_GSJ);
		if (*tmp_qos != '\0')
			qos->grp_submit_jobs = slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_GSJ));
		else
			qos->grp_submit_jobs = INFINITE;

		tmp_qos = KCIResultGetColumnValue(result, i, QOS_REQ_GW);
		if (*tmp_qos != '\0')
			qos->grp_wall = slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_GW));
		else
			qos->grp_wall = INFINITE;

		tmp_qos= KCIResultGetColumnValue(result, i, QOS_REQ_MJPA);
		if (*tmp_qos != '\0')
			qos->max_jobs_pa = slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_MJPA));
		else
			qos->max_jobs_pa = INFINITE;

		tmp_qos= KCIResultGetColumnValue(result, i, QOS_REQ_MJPU);
		if (*tmp_qos != '\0')
			qos->max_jobs_pu = slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_MJPU));
		else
			qos->max_jobs_pu = INFINITE;

		tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_MJAPA);
		if (*tmp_qos != '\0')
			qos->max_jobs_accrue_pa =
				slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_MJAPA));
		else
			qos->max_jobs_accrue_pa = INFINITE;
		tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_MJAPU);
		if (*tmp_qos != '\0')
			qos->max_jobs_accrue_pu =
				slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_MJAPU));
		else
			qos->max_jobs_accrue_pu = INFINITE;

		tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_MPT);
		if (*tmp_qos != '\0')
			qos->min_prio_thresh = slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_MPT));
		else
			qos->min_prio_thresh = INFINITE;

		tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_MSJPA);

		if (*tmp_qos != '\0')
			qos->max_submit_jobs_pa =
				slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_MSJPA));
		else
			qos->max_submit_jobs_pa = INFINITE;

		tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_MSJPU);
		if (*tmp_qos != '\0')
			qos->max_submit_jobs_pu =
				slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_MSJPU));
		else
			qos->max_submit_jobs_pu = INFINITE;

		tmp_qos= KCIResultGetColumnValue(result, i, QOS_REQ_MTPA);
		if (*tmp_qos != '\0')
			qos->max_tres_pa = xstrdup(KCIResultGetColumnValue(result, i, QOS_REQ_MTPA));

		tmp_qos= KCIResultGetColumnValue(result, i, QOS_REQ_MTPJ);
		if (*tmp_qos != '\0')
			qos->max_tres_pj = xstrdup(KCIResultGetColumnValue(result, i, QOS_REQ_MTPJ));
         
		tmp_qos= KCIResultGetColumnValue(result, i, QOS_REQ_MTPN);
		if (*tmp_qos != '\0')
			qos->max_tres_pn = xstrdup(KCIResultGetColumnValue(result, i, QOS_REQ_MTPN));

		tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_MTPU);
		if (*tmp_qos != '\0')
			qos->max_tres_pu = xstrdup(KCIResultGetColumnValue(result, i, QOS_REQ_MTPU));

        tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_MTMPJ);
		if (*tmp_qos != '\0')
			qos->max_tres_mins_pj = xstrdup(KCIResultGetColumnValue(result, i, QOS_REQ_MTMPJ));

        tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_MTRMA);
		if (*tmp_qos != '\0')
			qos->max_tres_run_mins_pa = xstrdup(KCIResultGetColumnValue(result, i, QOS_REQ_MTRMA));
		tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_MTRM);
		if (*tmp_qos != '\0')
			qos->max_tres_run_mins_pu = xstrdup(KCIResultGetColumnValue(result, i, QOS_REQ_MTRM));

        tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_MWPJ);
		if (*tmp_qos != '\0')
			qos->max_wall_pj = slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_MWPJ));
		else
			qos->max_wall_pj = INFINITE;

        tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_PREE);
		if (*tmp_qos != '\0') {
			if (!qos->preempt_bitstr)
				qos->preempt_bitstr = bit_alloc(qos_cnt);
			char *columnValue = KCIResultGetColumnValue(result, i, QOS_REQ_PREE);
			if (strlen(columnValue) >= 1) {
        		columnValue[strlen(columnValue) - 1] = '\0';
    		}
			bit_unfmt(qos->preempt_bitstr, columnValue+1);
		}
		tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_PREEM);
		if (*tmp_qos != '\0') 
			qos->preempt_mode = slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_PREEM));
		tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_PREXMPT);
		if (*tmp_qos != '\0') 
			qos->preempt_exempt_time =
				slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_PREXMPT));
		else
			qos->preempt_exempt_time = INFINITE;
		tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_PRIO);
		if (*tmp_qos != '\0') 
			qos->priority = slurm_atoul(KCIResultGetColumnValue(result, i, QOS_REQ_PRIO));

        tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_UF);
		if (*tmp_qos != '\0') 
			qos->usage_factor = atof(KCIResultGetColumnValue(result, i, QOS_REQ_UF));
		tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_UT);
		if (*tmp_qos != '\0') 
			qos->usage_thres = atof(KCIResultGetColumnValue(result, i, QOS_REQ_UT));
		else
			qos->usage_thres = (double)INFINITE;
		tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_MITPJ);
		if (*tmp_qos != '\0') 
			qos->min_tres_pj = xstrdup(KCIResultGetColumnValue(result, i, QOS_REQ_MITPJ));
		tmp_qos=KCIResultGetColumnValue(result, i, QOS_REQ_LF);
		if (*tmp_qos != '\0') 
			qos->limit_factor = atof(KCIResultGetColumnValue(result, i, QOS_REQ_LF));
		else
			qos->limit_factor = (double)INFINITE;
	}
	KCIResultDealloc(result);

	return qos_list;
}
