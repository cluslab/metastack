/*****************************************************************************\
 *  partition_mgr.c - manage the partition information of slurm
 *	Note: there is a global partition list (part_list) and
 *	time stamp (last_part_update)
 *****************************************************************************
 *  Copyright (C) 2002-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2010 Lawrence Livermore National Security.
 *  Portions Copyright (C) 2010-2016 SchedMD <https://www.schedmd.com>.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Morris Jette <jette@llnl.gov> et. al.
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

#include "config.h"

#include <ctype.h>
#include <errno.h>
#include <grp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "src/common/assoc_mgr.h"
#include "src/common/fd.h"
#include "src/common/hostlist.h"
#include "src/common/list.h"
#include "src/common/pack.h"
#include "src/common/select.h"
#include "src/common/slurm_protocol_pack.h"
#include "src/common/slurm_resource_info.h"
#include "src/common/uid.h"
#include "src/common/xstring.h"

#include "src/slurmctld/burst_buffer.h"
#include "src/slurmctld/gang.h"
#include "src/slurmctld/groups.h"
#include "src/slurmctld/licenses.h"
#include "src/slurmctld/locks.h"
#include "src/slurmctld/power_save.h"
#include "src/slurmctld/proc_req.h"
#include "src/slurmctld/read_config.h"
#include "src/slurmctld/reservation.h"
#include "src/slurmctld/slurmctld.h"
#include "src/slurmctld/state_save.h"

/* No need to change we always pack SLURM_PROTOCOL_VERSION */
#define PART_STATE_VERSION        "PROTOCOL_VERSION"

typedef struct {
	buf_t *buffer;
	uint32_t parts_packed;
	bool privileged;
	uint16_t protocol_version;
	uint16_t show_flags;
	uid_t uid;
	part_record_t **visible_parts;
#ifdef __METASTACK_OPT_PART_VISIBLE
	slurmdb_user_rec_t user_rec;
#endif
#ifdef __METASTACK_OPT_CACHE_QUERY
	bool pack_cache;
#endif

} _foreach_pack_part_info_t;

#ifdef __METASTACK_OPT_CACHE_QUERY
List   copy_part_list = NULL;		/* copy partition list */
List   cache_part_list = NULL;      /* cache partition list */
char *default_cache_part_name = NULL;		/* name of default partition */
char *default_copy_part_name = NULL;		/* name of default partition */
#endif

/* Global variables */
List part_list = NULL;			/* partition list */
char *default_part_name = NULL;		/* name of default partition */
part_record_t *default_part_loc = NULL;	/* default partition location */
time_t last_part_update = (time_t) 0;	/* time of last update to partition records */
uint16_t part_max_priority = DEF_PART_MAX_PRIORITY;

static int    _dump_part_state(void *x, void *arg);
static void   _list_delete_part(void *part_entry);
static int    _match_part_ptr(void *part_ptr, void *key);
static buf_t *_open_part_state_file(char **state_file);
static void   _unlink_free_nodes(bitstr_t *old_bitmap, part_record_t *part_ptr);

static int _calc_part_tres(void *x, void *arg)
{
	int i, j;
	node_record_t *node_ptr;
	uint64_t *tres_cnt;
	part_record_t *part_ptr = (part_record_t *) x;

	xfree(part_ptr->tres_cnt);
	xfree(part_ptr->tres_fmt_str);
	part_ptr->tres_cnt = xcalloc(slurmctld_tres_cnt, sizeof(uint64_t));
	tres_cnt = part_ptr->tres_cnt;

	/* sum up nodes' tres in the partition. */
	for (i = 0; (node_ptr = next_node(&i)); i++) {
		if (!bit_test(part_ptr->node_bitmap, node_ptr->index))
			continue;
		for (j = 0; j < slurmctld_tres_cnt; j++)
			tres_cnt[j] += node_ptr->tres_cnt[j];
	}

	/* Just to be safe, lets do this after the node TRES ;) */
	tres_cnt[TRES_ARRAY_NODE] = part_ptr->total_nodes;

	/* grab the global tres and stick in partition for easy reference. */
	for(i = 0; i < slurmctld_tres_cnt; i++) {
		slurmdb_tres_rec_t *tres_rec = assoc_mgr_tres_array[i];

		if (!xstrcasecmp(tres_rec->type, "bb") ||
		    !xstrcasecmp(tres_rec->type, "license"))
			tres_cnt[i] = tres_rec->count;
	}

	/*
	 * Now figure out the total billing of the partition as the node_ptrs
	 * are configured with the max of all partitions they are in instead of
	 * what is configured on this partition.
	 */
	tres_cnt[TRES_ARRAY_BILLING] = assoc_mgr_tres_weighted(
		tres_cnt, part_ptr->billing_weights,
		slurm_conf.priority_flags, true);

	part_ptr->tres_fmt_str =
		assoc_mgr_make_tres_str_from_array(part_ptr->tres_cnt,
						   TRES_STR_CONVERT_UNITS,
						   true);
#ifdef __METASTACK_OPT_CACHE_QUERY
	_add_part_state_to_queue(part_ptr);
#endif
	return 0;
}

/*
 * Calculate and populate the number of tres' for all partitions.
 */
extern void set_partition_tres()
{
	xassert(verify_lock(PART_LOCK, WRITE_LOCK));
	xassert(verify_lock(NODE_LOCK, READ_LOCK));

	list_for_each(part_list, _calc_part_tres, NULL);
}

#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
/* 
 * _add_node_to_parts -  add node to new partition
 * IN node_ptr - pointer to the node will be returned
 * IN part_ptr - pointer to the partition  standby_node_bitmap of the partition
 */
extern void _add_node_to_parts(node_record_t *node_ptr, part_record_t *part_ptr)
{
	part_record_t *p_ptr = NULL;

	/* return node to orig partitions */
	if (part_ptr == NULL) {
		node_ptr->part_cnt = 0;
		if ((node_ptr->orig_parts == NULL) || node_ptr->orig_parts[0] == '\0') {
			xfree(node_ptr->part_pptr);
		} else {
			char *parts = NULL, *part = NULL, *save_ptr = NULL;
			parts = xstrdup(node_ptr->orig_parts);
			part = strtok_r(parts, ",", &save_ptr);
			while (part != NULL) {
				if ((p_ptr = find_part_record(part))) {
					/* Associate node to partitions */
					node_ptr->part_cnt++;
					xrecalloc(node_ptr->part_pptr, node_ptr->part_cnt,
						sizeof(part_record_t *));
					node_ptr->part_pptr[node_ptr->part_cnt-1] = p_ptr;
					bit_set(p_ptr->node_bitmap, node_ptr->index);
					/* Update partition related resource attributes */
					p_ptr->total_nodes++;
					p_ptr->total_cpus += node_ptr->cpus;
					p_ptr->max_cpu_cnt = MAX(p_ptr->max_cpu_cnt,
									node_ptr->cpus);
					p_ptr->max_core_cnt = MAX(p_ptr->max_core_cnt,
									node_ptr->tot_cores);									
					xfree(p_ptr->nodes);
					xfree(p_ptr->orig_nodes);
					p_ptr->nodes = bitmap2node_name(p_ptr->node_bitmap);
					p_ptr->orig_nodes = xstrdup(p_ptr->nodes);
					debug("%s: add node %s to partition %s", __func__, node_ptr->name, p_ptr->name);
#ifdef __METASTACK_OPT_CACHE_QUERY
                    _add_node_state_to_queue(node_ptr, false);
	                _add_part_state_to_queue(p_ptr);
#endif
				}
				part = strtok_r(NULL, ",", &save_ptr);
			}
			xfree(parts);	
		}	
	} else { 	/* borrow node to new partition */
		/* Associate node to partitions */
		bit_set(part_ptr->node_bitmap, node_ptr->index);
		bit_set(part_ptr->standby_nodes->borrowed_node_bitmap, node_ptr->index);
		xfree(part_ptr->nodes);
		xfree(part_ptr->orig_nodes);
		part_ptr->nodes = bitmap2node_name(part_ptr->node_bitmap);
		part_ptr->orig_nodes = xstrdup(part_ptr->nodes);
		/* Update borrowed nodes in partition */
		xfree(part_ptr->standby_nodes->borrowed_nodes);
		if (bit_set_count(part_ptr->standby_nodes->borrowed_node_bitmap) == 0) {
			part_ptr->standby_nodes->borrowed_nodes = NULL;
		} else {
			part_ptr->standby_nodes->borrowed_nodes = bitmap2node_name(part_ptr->standby_nodes->borrowed_node_bitmap);
		}	
		part_ptr->standby_nodes->nodes_borrowed = bit_set_count(part_ptr->standby_nodes->borrowed_node_bitmap);
		/* Update partition related resource attributes */
 		part_ptr->total_nodes++;
		part_ptr->total_cpus += node_ptr->cpus;
		part_ptr->max_cpu_cnt = MAX(part_ptr->max_cpu_cnt,
					    node_ptr->cpus);
		part_ptr->max_core_cnt = MAX(part_ptr->max_core_cnt,
					     node_ptr->tot_cores);					 		
		xfree(node_ptr->part_pptr);
		node_ptr->part_cnt = 1;
		node_ptr->part_pptr = xcalloc(1, sizeof(part_record_t *));
		node_ptr->part_pptr[0] = part_ptr;
#ifdef __METASTACK_OPT_CACHE_QUERY
        _add_node_state_to_queue(node_ptr, false);
        _add_part_state_to_queue(part_ptr);
#endif
		debug("%s: borrow node %s to partition %s", __func__, node_ptr->name, part_ptr->name);
		debug("%s: partition %s: max_nodes_can_borrow: %u, nodes_already_borrowed: %u", 
			__func__, part_ptr->name, part_ptr->standby_nodes->nodes_can_borrow, part_ptr->standby_nodes->nodes_borrowed);
	}
}

/* 
 * build_part_standby_nodes_bitmap -  Constructing standby_node_bitmap based on configuration
 * IN part_ptr - pointer to the partition be constructing standby_node_bitmap
 * OUT - 0: construction successful, -1: construction failed
 */
extern int build_part_standby_nodes_bitmap(part_record_t *part_ptr)
{
	int rc = SLURM_SUCCESS;
	char *this_node_name = NULL;
	node_record_t *node_ptr = NULL;
	bitstr_t *old_standby_bitmap = NULL;
	hostlist_t standby_node_host_list = NULL;

	if (!(part_ptr->standby_nodes)) {
		part_ptr->standby_nodes = xcalloc(1, sizeof(standby_nodes_t));
	}

	if (part_ptr->standby_nodes->standby_node_bitmap) {
		old_standby_bitmap = bit_copy(part_ptr->standby_nodes->standby_node_bitmap);
		bit_nclear(part_ptr->standby_nodes->standby_node_bitmap, 0,
			   node_record_count - 1);		
	} else {
		part_ptr->standby_nodes->standby_node_bitmap = bit_alloc(node_record_count);
	}

	if (!part_ptr->standby_nodes->nodes) {
		FREE_NULL_BITMAP(old_standby_bitmap);
		return SLURM_ERROR;
	}
	/* parse standby nodes */
	standby_node_host_list = nodespec_to_hostlist(part_ptr->standby_nodes->nodes, true, NULL);
	if ((!standby_node_host_list) || (!hostlist_count(standby_node_host_list))) {
		error("%s: Invalid nodes in partition %s standby_nodes %s", 
			__func__, part_ptr->name, part_ptr->standby_nodes->nodes);
		FREE_NULL_BITMAP(part_ptr->standby_nodes->standby_node_bitmap);
		part_ptr->standby_nodes->standby_node_bitmap = old_standby_bitmap;
		old_standby_bitmap = NULL;
		rc = ESLURM_INVALID_NODE_NAME;
	}

	if (rc == SLURM_SUCCESS) {
		while ((this_node_name = hostlist_shift(standby_node_host_list))) {
			node_ptr = find_node_record_no_alias(this_node_name);
			if (node_ptr == NULL) {
				error("%s: Invalid nodes in partition %s standby_nodes %s", 
					__func__, part_ptr->name, this_node_name);
				FREE_NULL_BITMAP(part_ptr->standby_nodes->standby_node_bitmap);
				part_ptr->standby_nodes->standby_node_bitmap = old_standby_bitmap;
				old_standby_bitmap = NULL;
				free(this_node_name);
				rc = ESLURM_INVALID_NODE_NAME;
				break;
			}

			bit_set(part_ptr->standby_nodes->standby_node_bitmap, node_ptr->index);
			free(this_node_name);
		}
	}
	xfree(part_ptr->standby_nodes->nodes);
	if (bit_set_count(part_ptr->standby_nodes->standby_node_bitmap) == 0) {
		part_ptr->standby_nodes->nodes = NULL;
	} else {
		part_ptr->standby_nodes->nodes = bitmap2node_name(part_ptr->standby_nodes->standby_node_bitmap);
	}
	FREE_NULL_HOSTLIST(standby_node_host_list);
	FREE_NULL_BITMAP(old_standby_bitmap);

	return rc;
}

/* 
 * _standby_node_avail -  Determine if a node can be borrowed immediately
 * IN node_ptr - pointer to the node under consideration
 * OUT - true: this node can be borrowed; false: this node cannot be borrowed
 */
extern bool _standby_node_avail(node_record_t *node_ptr)
{
	int i;
	bool avail = false;
	part_record_t *part_ptr;

	if (!node_ptr) {
		return avail;
	}

	if (IS_NODE_IDLE(node_ptr) &&  /* Idle nodes that meet the following conditions */
		(!IS_NODE_DRAIN(node_ptr)) && 
		(!IS_NODE_NO_RESPOND(node_ptr)) &&
		(!IS_NODE_RES(node_ptr)) && (!IS_NODE_MAINT(node_ptr)) && 
		(!IS_NODE_POWER_DOWN(node_ptr)) &&
		(!IS_NODE_COMPLETING(node_ptr)) && 
		(!IS_NODE_INVALID_REG(node_ptr))) {
		avail = true;
		debug("%s: standby node %s state idle, consider borrowing", __func__, node_ptr->name);
	} else {
		debug2("%s: standby node %s state not avail, consider other nodes", __func__, node_ptr->name);		
	}

	if (avail) {
		for (i = 0; i < node_ptr->part_cnt; i++) {
			part_ptr = node_ptr->part_pptr[i];
			if (part_ptr && part_ptr->standby_nodes && part_ptr->standby_nodes->enable) {
				avail = false;
				debug2("%s: standby node %s already in standby enable part %s, consider other nodes", 
						__func__, node_ptr->name, part_ptr->name);
				break;
			}
		}
	}
	return avail;
}

static void _build_borrow_nodes_partitions(part_record_t *part_ptr)
{
	int i;
	node_record_t *node_ptr = NULL;
	for (i = 0; (node_ptr = next_node(&i)); i++) {
		if (!IS_NODE_BORROWED(node_ptr)) {
			continue;
		}
		_build_node_partitions(node_ptr, part_ptr, false);
	}
}

/* 
 * _build_node_partitions -  recoed the original partition of a node
 * IN node_ptr - pointer to the node will be record
 * IN part_ptr - pointer to the partition which the node belongs
 * IN add_part - true:  add part_name to node_ptr->orig_parts
 * 				 false：remove part_name from node_ptr->orig_parts
 */
extern void _build_node_partitions(node_record_t *node_ptr, part_record_t *part_ptr, bool add_part)
{
	char *sep = "";
	if (add_part) {
		if (node_ptr->orig_parts && node_ptr->orig_parts[0] == '\0') {
			xfree(node_ptr->orig_parts);
		}
		/* part_ptr->name already exist in node_ptr->orig_parts */
		if (xstrcasestr(node_ptr->orig_parts, part_ptr->name)) {
			return;
		}
		 /* add part_ptr->name to node_ptr->orig_parts */
		if (node_ptr->orig_parts) {
			sep = ",";
		}
		xstrfmtcat(node_ptr->orig_parts, "%s%s", sep,
				part_ptr->name);
		debug3("%s: record node %s in partitions %s", __func__, node_ptr->name, node_ptr->orig_parts);
	} else { /* remove part_ptr->name to node_ptr->orig_parts */
		if ((node_ptr->orig_parts == NULL) || node_ptr->orig_parts[0] == '\0' || (part_ptr->name == NULL)) {
			return;
		} else {
			char *parts = NULL, *pos = NULL;
			parts = xstrdup(node_ptr->orig_parts);
			pos = xstrstr(parts, part_ptr->name);
			if (pos != NULL) {
				int len = strlen(part_ptr->name);
				char *end = pos + len;
				char *next_comma = strchr(end, ',');

				if (next_comma) {
					/* There is more content to follow, remove 'part_name,' */
					memmove(pos, next_comma + 1, strlen(next_comma + 1) + 1);
				} else if (pos > parts && *(pos - 1) == ',') {
					/* There is no content at the end but there is content at the beginning, remove ',part_name' */
					*(pos - 1) = '\0';
				} else {
					/* There is no content in front or behind */
					*pos = '\0';
				}
				xfree(node_ptr->orig_parts);
				node_ptr->orig_parts = parts;				
			} else {
				xfree(parts);
			}
			debug3("%s: record node %s in partitions %s", __func__, node_ptr->name, node_ptr->orig_parts);			
		}	
	}
}

/* 
 * _remove_node_from_parts -  Remove nodes from the partition
 * IN node_ptr - pointer to the node willl be removed
 * IN clear_flag - true: remove node from borrowed partition;
 * 				   false: remove node from original partition
 */
extern void _remove_node_from_parts(node_record_t *node_ptr, bool clear_flag)
{
	int i, j, part_cnt;
	node_record_t *node_record = NULL;
	part_record_t *part_ptr = NULL;

 	part_cnt = node_ptr->part_cnt;
	if (part_cnt < 1) {
		return;
	}

	node_ptr->part_cnt = 0;
	for (i = 0; i < part_cnt; i++) {
		if (!(part_ptr = node_ptr->part_pptr[i])) {
			continue;
		}
		/* Disassociate nodes from partitions */
		bit_clear(part_ptr->node_bitmap, node_ptr->index);
		xfree(part_ptr->nodes);
		xfree(part_ptr->orig_nodes);
		part_ptr->nodes = bitmap2node_name(part_ptr->node_bitmap);
		part_ptr->orig_nodes = xstrdup(part_ptr->nodes);
		part_ptr->total_nodes--;
		part_ptr->total_cpus -= node_ptr->cpus;
		/* Update partition related resource attributes */
		for (j = 0; (node_record = next_node(&j)); j++) {
			if (!bit_test(part_ptr->node_bitmap, node_record->index)) {
				continue;
			}
			part_ptr->max_cpu_cnt = MAX(part_ptr->max_cpu_cnt,
							node_record->cpus);
			part_ptr->max_core_cnt = MAX(part_ptr->max_core_cnt,
							node_record->tot_cores);				
		}					
		debug("%s: remove node %s from partition %s", __func__, node_ptr->name, part_ptr->name);
		if (clear_flag && part_ptr->standby_nodes && part_ptr->standby_nodes->borrowed_node_bitmap) {
			/* Remove nodes from partition borrowing */
			bit_clear(part_ptr->standby_nodes->borrowed_node_bitmap, node_ptr->index);
			xfree(part_ptr->standby_nodes->borrowed_nodes);
			uint32_t nodes_borrowed = bit_set_count(part_ptr->standby_nodes->borrowed_node_bitmap);
			if (nodes_borrowed == 0) {
				part_ptr->standby_nodes->borrowed_nodes = NULL;
			} else {
				part_ptr->standby_nodes->borrowed_nodes = bitmap2node_name(part_ptr->standby_nodes->borrowed_node_bitmap);
			}					
			part_ptr->standby_nodes->nodes_borrowed = nodes_borrowed;
			debug("%s: nodes_borrowed for partition %s: %u", __func__, part_ptr->name, nodes_borrowed);
		}
	}
}

/* 
 * is_met_borrow_condition - when node offline determine whether can borrow other node to corresponding partitions
 * IN node_ptr - pointer to the offline node
 * IN part_ptr - pointer to the partition in which the offline node belongs
 * OUT - true: meet borrowing condition, false: not meeting borrowing condition
 */
bool is_met_borrow_condition(node_record_t *node_ptr, part_record_t *part_ptr)
{
	bool rc = false;
	int node_state = part_ptr->standby_nodes->offline_node_state;
	/* Determine whether the node offline state meets the supplementary conditions */
	switch (node_state) {
	case 1:
		if (IS_NODE_DRAIN(node_ptr) && (xstrcasecmp(node_ptr->reason, offline_reason))) {
			rc = true;
		}
		break;			
	case 2:
		if (IS_NODE_DOWN(node_ptr) && IS_NODE_NO_RESPOND(node_ptr)) {
			rc = true;
		}
		break;
	case 3:
		if ((IS_NODE_DRAIN(node_ptr) && (xstrcasecmp(node_ptr->reason, offline_reason))) ||
			(IS_NODE_DOWN(node_ptr) && IS_NODE_NO_RESPOND(node_ptr))) {
			rc = true;
		}
		break;
	default:
		error("%s: Invalid offline_node_state %d", __func__, node_state);
		break;
	}

	return rc;
}

bool return_over_borrowed_nodes(part_record_t *part_ptr, uint32_t nodes_over_borrowed, 
		int *wait_return_borrow_nodes, int *nodes_borrow_wait_return)
{
	bool part_change = false;
	int i, wait_nodes = 0, *wait_return_nodes = NULL;
	node_record_t *node_ptr = NULL;

	wait_return_nodes = xcalloc(node_record_count, sizeof(int));
	/* First, return the idle nodes */
	for (i = 0; (i < node_record_count) && (nodes_over_borrowed > 0); i++) {
		if (!bit_test(part_ptr->standby_nodes->borrowed_node_bitmap, i)) {
			continue;
		}
		node_ptr = node_record_table_ptr[i];
		if (!node_ptr) {
			continue;
		}
		if (IS_NODE_IDLE(node_ptr)) {
			part_change = true;
			_return_borrowed_node(node_ptr);
			nodes_over_borrowed--;
		} else {
			wait_return_nodes[wait_nodes] = node_ptr->index;
			wait_nodes++;
		}
	}
	/* Next, drain mix/alloc nodes */
	for (i = 0; (i < wait_nodes) && (nodes_over_borrowed > 0); i++) {
		node_ptr = node_record_table_ptr[wait_return_nodes[i]];
		if (!node_ptr) {
			continue;
		}
		if (!IS_NODE_DRAIN(node_ptr)) {
			drain_nodes(node_ptr->name, offline_reason, slurm_conf.slurm_user_id);
			nodes_over_borrowed--;
			wait_return_borrow_nodes[*nodes_borrow_wait_return] = node_ptr->index;
			(*nodes_borrow_wait_return)++;
		}
	}
	xfree(wait_return_nodes);
	return part_change;
}

extern void update_all_parts_resource(bool update_resv)
{
	part_record_t *part_ptr = NULL;
	assoc_mgr_lock_t assoc_tres_read_lock = { .tres = READ_LOCK };
	DEF_TIMERS;

	START_TIMER;
	ListIterator part_iterator = list_iterator_create(part_list);
	while ((part_ptr = list_next(part_iterator))) {
		if (update_resv) {
			update_part_nodes_in_resv(part_ptr);
		}

		assoc_mgr_lock(&assoc_tres_read_lock);
		_calc_part_tres(part_ptr, NULL);
		assoc_mgr_unlock(&assoc_tres_read_lock);
	}
	list_iterator_destroy(part_iterator);

	schedule_part_save();
#ifdef __METASTACK_NEW_PART_PARA_SCHED
	build_sched_resource();
#endif
	power_save_set_timeouts(NULL);
	END_TIMER;
	debug("%s: %s", __func__, TIME_STR);
}

extern bool validate_partition_borrow_nodes(part_record_t *part_ptr)
{
	bool part_change = false;
	int i, new_borrowed_nodes;
	node_record_t *node_ptr = NULL;
	int *wait_return_borrow_nodes = NULL, *work_borrow_nodes = NULL, *idle_borrow_nodes = NULL;
	int nodes_need_borrow = 0, nodes_borrow_wait_return = 0, nodes_borrowed_unavail = 0;
	int nodes_borrowed = 0, max_can_borrow = 0, nodes_offline = 0, nodes_borrowed_worked = 0, node_borrowed_idle = 0;

	if (!part_ptr->standby_nodes || !part_ptr->standby_nodes->borrowed_node_bitmap ||
		!part_ptr->standby_nodes->standby_node_bitmap) {
		/* This should never happen */
		return part_change;
	}

	nodes_borrowed = part_ptr->standby_nodes->nodes_borrowed;
	max_can_borrow = part_ptr->standby_nodes->nodes_can_borrow;
	if (!(part_ptr->standby_nodes->enable)) {
		/* Return borrowed nodes when disable standby nodes */
		if (nodes_borrowed > 0) {
			for (i = 0; i < node_record_count; i++) {
				if (!bit_test(part_ptr->standby_nodes->borrowed_node_bitmap, i)) {
					continue;
				}
				node_ptr = node_record_table_ptr[i];
				if (!node_ptr || !IS_NODE_BORROWED(node_ptr)) {
					continue;
				}
				/* There are no jobs on the node */
				if (IS_NODE_IDLE(node_ptr) || (IS_NODE_DOWN(node_ptr))) {
					part_change = true;
					_return_borrowed_node(node_ptr);
				} else {
					if (!IS_NODE_DRAIN(node_ptr)) {
						drain_nodes(node_ptr->name, offline_reason, slurm_conf.slurm_user_id);
					}
				}
			}
		}
		return part_change;
	}

	/* Return borrowed nodes or drain those nodes which are removed form standby_nodes or in DOWN* state or have no partition */
	if (nodes_borrowed > 0) {
		for (i = 0; i < node_record_count; i++) {
			if (!bit_test(part_ptr->standby_nodes->borrowed_node_bitmap, i)) {
				continue;
			}
			
			node_ptr = node_record_table_ptr[i];
			if (!node_ptr || !IS_NODE_BORROWED(node_ptr)) {
				continue;
			}

			/* borrowed nodes not in standby_nodes will be return */
			/* borrowed nodes in standby_nodes drain or DOWN* will be return */
			if (bit_test(part_ptr->standby_nodes->standby_node_bitmap, i) && 
				(!IS_NODE_DRAIN(node_ptr) && !IS_NODE_DOWN(node_ptr))) {
				continue;
			}
			/* The IDLE/DOWN* or have no partition nodes will be returned immediately, and mix/alloc nodes will be taken offline */
			if (IS_NODE_IDLE(node_ptr) || (IS_NODE_DOWN(node_ptr) && IS_NODE_NO_RESPOND(node_ptr)) || (node_ptr->part_cnt == 0)) {
				part_change = true;
				_return_borrowed_node(node_ptr);
			} else {
				if ((!IS_NODE_DRAIN(node_ptr)) && (!IS_NODE_DOWN(node_ptr))) {
					drain_nodes(node_ptr->name, offline_reason, slurm_conf.slurm_user_id);
				}
			}	
		}	
	}

	wait_return_borrow_nodes = xcalloc(node_record_count, sizeof(int));
	work_borrow_nodes        = xcalloc(node_record_count, sizeof(int));
	idle_borrow_nodes        = xcalloc(node_record_count, sizeof(int));

	/* Calculate offline nodes */
	part_ptr->standby_nodes->nodes_offline = 0;
	for (i = 0; i < node_record_count; i++) {
		if (!bit_test(part_ptr->node_bitmap, i)) {
			continue;
		}
		node_ptr = node_record_table_ptr[i];
		if (!node_ptr) {
			continue;
		}
		/* Exclude drain nodes waiting for return */
		if (is_met_borrow_condition(node_ptr, part_ptr)) {
			if (IS_NODE_BORROWED(node_ptr)) {
				nodes_borrowed_unavail++;
			} else {
				part_ptr->standby_nodes->nodes_offline++;
			}
		}

		/* Record the detailed status of borrowing nodes */
		if (IS_NODE_DRAIN(node_ptr) && IS_NODE_BORROWED(node_ptr) && !xstrcasecmp(node_ptr->reason, offline_reason)
			&& bit_test(part_ptr->standby_nodes->standby_node_bitmap, i)) {
				/* Wait for return, can be borrowed again */
				wait_return_borrow_nodes[nodes_borrow_wait_return] = i;
				nodes_borrow_wait_return++;
		}

		if (IS_NODE_BORROWED(node_ptr) && !IS_NODE_DRAIN(node_ptr) && !IS_NODE_DOWN(node_ptr)) {
			if (IS_NODE_IDLE(node_ptr)) {
				idle_borrow_nodes[node_borrowed_idle] = i;
				node_borrowed_idle++;
			} else {
				work_borrow_nodes[nodes_borrowed_worked] = i;
				nodes_borrowed_worked++;
			}
		}
	}

	nodes_offline  = part_ptr->standby_nodes->nodes_offline;
	nodes_borrowed = part_ptr->standby_nodes->nodes_borrowed - nodes_borrowed_unavail;

	/* Return additional borrowed nodes */
	if (nodes_borrowed > max_can_borrow) {
		part_change |= return_over_borrowed_nodes(part_ptr, nodes_borrowed - max_can_borrow, 
				wait_return_borrow_nodes, &nodes_borrow_wait_return);
	}

	nodes_need_borrow = nodes_offline;
	if (nodes_need_borrow >= max_can_borrow) {
		nodes_need_borrow = max_can_borrow;
	}

	/* Is the number of mix/alloc borrowed nodes sufficient */
	if (nodes_need_borrow >= nodes_borrowed_worked) {
		nodes_need_borrow -= nodes_borrowed_worked;
	} else {
		part_change |= return_over_borrowed_nodes(part_ptr, nodes_borrowed_worked - nodes_need_borrow,
				wait_return_borrow_nodes, &nodes_borrow_wait_return);
		xfree(wait_return_borrow_nodes);
		xfree(work_borrow_nodes);
		xfree(idle_borrow_nodes);				
		return part_change;
	}

	/* Is the number of idle borrowed nodes sufficient */
	if (nodes_need_borrow >= node_borrowed_idle) {
		nodes_need_borrow -= node_borrowed_idle;
	} else {
		part_change |= return_over_borrowed_nodes(part_ptr, node_borrowed_idle - nodes_need_borrow,
				wait_return_borrow_nodes, &nodes_borrow_wait_return);
		xfree(wait_return_borrow_nodes);
		xfree(work_borrow_nodes);
		xfree(idle_borrow_nodes);		
		return part_change;		
	}

	/* Is the number of wait for return borrowed nodes sufficient */
	if (nodes_need_borrow >= nodes_borrow_wait_return) {
		time_t now = time(NULL);
		for (i = 0; i < nodes_borrow_wait_return; i++) {
			node_ptr = node_record_table_ptr[wait_return_borrow_nodes[i]];
			if (!node_ptr) {
				continue;
			}
			node_ptr->node_state &= (~NODE_STATE_DRAIN);
				clusteracct_storage_g_node_up(
					acct_db_conn,
					node_ptr,
					now);
			debug("%s: node %s remove drain state", __func__, node_ptr->name);							
			nodes_need_borrow--;	
#ifdef __METASTACK_OPT_CACHE_QUERY
            _add_node_state_to_queue(node_ptr, true);
#endif

		}
	} else {
		time_t now = time(NULL);
		uint32_t tmp_nodes_need_borrow = nodes_need_borrow;
		for (i = 0; i < tmp_nodes_need_borrow; i++) {
			node_ptr = node_record_table_ptr[wait_return_borrow_nodes[i]];
			if (!node_ptr) {
				continue;
			}
			node_ptr->node_state &= (~NODE_STATE_DRAIN);
			clusteracct_storage_g_node_up(
				acct_db_conn,
				node_ptr,
				now);
			debug("%s: node %s remove drain state", __func__, node_ptr->name);				
			nodes_need_borrow--;
#ifdef __METASTACK_OPT_CACHE_QUERY
            _add_node_state_to_queue(node_ptr, true);
#endif

		}		
	}

	/* The current borrowing nodes are insufficient, new nodes need to be borrowed */
	new_borrowed_nodes = _select_node_to_borrow(part_ptr, nodes_need_borrow);
	if (new_borrowed_nodes > 0) {
		part_change = true;
	}

	xfree(wait_return_borrow_nodes);
	xfree(work_borrow_nodes);
	xfree(idle_borrow_nodes);
	return part_change;
}

extern bool validate_all_partitions_borrow_nodes(List part_list)
{
	bool rebuild = false;
	part_record_t *part_ptr = NULL;

	if (!part_list) {
		return rebuild;
	}

	DEF_TIMERS;
	START_TIMER;
	
	ListIterator part_iterator = list_iterator_create(part_list);
	while ((part_ptr = list_next(part_iterator))) {
		rebuild |= validate_partition_borrow_nodes(part_ptr);
	}
	list_iterator_destroy(part_iterator);

	if (rebuild) {
		update_all_parts_resource(true);
	}
	
	END_TIMER;
	debug("%s: %s", __func__, TIME_STR);

	return rebuild;
}
#endif

/*
 * build_part_bitmap - update the total_cpus, total_nodes, and node_bitmap
 *	for the specified partition, also reset the partition pointers in
 *	the node back to this partition.
 * IN part_ptr - pointer to the partition
 * RET 0 if no error, errno otherwise
 * global: node_record_table_ptr - pointer to global node table
 * NOTE: this does not report nodes defined in more than one partition. this
 *	is checked only upon reading the configuration file, not on an update
 */
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
extern int build_part_bitmap(part_record_t *part_ptr, bool update_part)
#else
extern int build_part_bitmap(part_record_t *part_ptr)
#endif
{
	int rc = SLURM_SUCCESS;
	char *this_node_name;
	bitstr_t *old_bitmap;
	node_record_t *node_ptr;
	hostlist_t host_list, missing_hostlist = NULL;
	int i;

	part_ptr->total_cpus = 0;
	part_ptr->total_nodes = 0;
	part_ptr->max_cpu_cnt = 0;
	part_ptr->max_core_cnt = 0;

	if (part_ptr->node_bitmap == NULL) {
		part_ptr->node_bitmap = bit_alloc(node_record_count);
		old_bitmap = NULL;
	} else {
		old_bitmap = bit_copy(part_ptr->node_bitmap);
		bit_nclear(part_ptr->node_bitmap, 0,
			   node_record_count - 1);
	}

	if (!(host_list = nodespec_to_hostlist(part_ptr->orig_nodes, true,
					       &part_ptr->nodesets))) {
		/* Error, restore original bitmap */
		FREE_NULL_BITMAP(part_ptr->node_bitmap);
		part_ptr->node_bitmap = old_bitmap;
		return ESLURM_INVALID_NODE_NAME;
	} else if (!hostlist_count(host_list)) {
		info("%s: No nodes in partition %s", __func__, part_ptr->name);
		/*
		 * Clear "nodes" but leave "orig_nodes" intact.
		 * e.g.
		 * orig_nodes="nodeset1" and all of the nodes in "nodeset1" are
		 * removed. "nodes" should be cleared to show that there are no
		 * nodes in the partition right now. "orig_nodes" needs to stay
		 * intact so that when "nodeset1" nodes come back they are added
		 * to the partition.
		 */
		xfree(part_ptr->nodes);
		_unlink_free_nodes(old_bitmap, part_ptr);
		FREE_NULL_BITMAP(old_bitmap);
		FREE_NULL_HOSTLIST(host_list);
		return 0;
	}

	while ((this_node_name = hostlist_shift(host_list))) {
		node_ptr = find_node_record_no_alias(this_node_name);
		if (node_ptr == NULL) {
			if (!missing_hostlist)
				missing_hostlist =
					hostlist_create(this_node_name);
			else
				hostlist_push_host(missing_hostlist,
						   this_node_name);
			info("%s: invalid node name %s in partition",
			     __func__, this_node_name);
			free(this_node_name);
			rc = ESLURM_INVALID_NODE_NAME;
			continue;
		}
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
		if (update_part) {
			if (part_ptr->standby_nodes && part_ptr->standby_nodes->borrowed_node_bitmap
				&& bit_test(part_ptr->standby_nodes->standby_node_bitmap, node_ptr->index)) {
				error("%s: node %s is already in StandbyNodes and cannot be updated to this partitions", __func__, node_ptr->name);
				continue;
			}
			if (IS_NODE_BORROWED(node_ptr)) {
				error("%s: node %s is already in borrowed state and cannot be updated to other partitions", __func__, node_ptr->name);
				continue;
			}
		}
#endif
		part_ptr->total_nodes++;
		part_ptr->total_cpus += node_ptr->cpus;
		part_ptr->max_cpu_cnt = MAX(part_ptr->max_cpu_cnt,
					    node_ptr->cpus);
		part_ptr->max_core_cnt = MAX(part_ptr->max_core_cnt,
					     node_ptr->tot_cores);

		for (i = 0; i < node_ptr->part_cnt; i++) {
			if (node_ptr->part_pptr[i] == part_ptr)
				break;
		}
		if (i == node_ptr->part_cnt) { /* Node in new partition */
			node_ptr->part_cnt++;
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
			_build_node_partitions(node_ptr, part_ptr, true);
#endif
			xrecalloc(node_ptr->part_pptr, node_ptr->part_cnt,
				  sizeof(part_record_t *));
			node_ptr->part_pptr[node_ptr->part_cnt-1] = part_ptr;
#ifdef __METASTACK_OPT_CACHE_QUERY
            _add_node_state_to_queue(node_ptr, false);
#endif
		}
#ifdef __METASTACK_OPT_CACHE_QUERY
        _add_part_state_to_queue(part_ptr);
#endif
		if (old_bitmap)
			bit_clear(old_bitmap, node_ptr->index);

		bit_set(part_ptr->node_bitmap, node_ptr->index);
		free(this_node_name);
	}
	hostlist_destroy(host_list);

	if ((rc == ESLURM_INVALID_NODE_NAME) && missing_hostlist) {
		/*
		 * Remove missing node from partition nodes so we don't keep
		 * trying to remove them.
		 */
		hostlist_t hl;
		char *missing_nodes;

		hl = hostlist_create(part_ptr->orig_nodes);
		missing_nodes =
			hostlist_ranged_string_xmalloc(missing_hostlist);
		hostlist_delete(hl, missing_nodes);
		xfree(missing_nodes);
		xfree(part_ptr->orig_nodes);
		part_ptr->orig_nodes = hostlist_ranged_string_xmalloc(hl);
		hostlist_destroy(hl);

	}
	hostlist_destroy(missing_hostlist);
	xfree(part_ptr->nodes);
	part_ptr->nodes = bitmap2node_name(part_ptr->node_bitmap);

	_unlink_free_nodes(old_bitmap, part_ptr);
	last_node_update = time(NULL);
	FREE_NULL_BITMAP(old_bitmap);
	return rc;
}

/* unlink nodes removed from a partition */
static void _unlink_free_nodes(bitstr_t *old_bitmap, part_record_t *part_ptr)
{
	int i, j, k, update_nodes = 0;
	node_record_t *node_ptr;

	if (old_bitmap == NULL)
		return;

	for (i = 0; (node_ptr = next_node(&i)); i++) {
		if (bit_test(old_bitmap, node_ptr->index) == 0)
			continue;
		for (j=0; j<node_ptr->part_cnt; j++) {
			if (node_ptr->part_pptr[j] != part_ptr)
				continue;
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
			/* remove part_ptr->name from node_ptr->orig_parts */
			_build_node_partitions(node_ptr, part_ptr, false);
			/* return borrowed_node */
			if (IS_NODE_BORROWED(node_ptr)) {		
				if (IS_NODE_IDLE(node_ptr) || (IS_NODE_DOWN(node_ptr) && IS_NODE_NO_RESPOND(node_ptr))) {
					/* Returning nodes will subtract node resources from the partition, but in reality,
						* node resources are no longer included in the partition resources. Therefore, we will
						* increase the resources first
						*/
					part_ptr->total_nodes++;
					part_ptr->total_cpus += node_ptr->cpus;							
					_return_borrowed_node(node_ptr);
#ifdef __METASTACK_OPT_CACHE_QUERY
                    _add_part_state_to_queue(part_ptr);
#endif
				} else if (!IS_NODE_DRAIN(node_ptr)) {
					/* borrowed node remove from part before, add back here */
					debug("%s: add the borrowed node %s back to the partition %s, wait for the job finish then remove it",
						 __func__, node_ptr->name, part_ptr->name);
					_add_node_to_parts(node_ptr, part_ptr);
					drain_nodes(node_ptr->name, offline_reason, slurm_conf.slurm_user_id);
				}
				continue;
			}
#endif					
			node_ptr->part_cnt--;
			for (k=j; k<node_ptr->part_cnt; k++) {
				node_ptr->part_pptr[k] =
					node_ptr->part_pptr[k+1];
			}
#ifdef __METASTACK_OPT_CACHE_QUERY
            _add_node_state_to_queue(node_ptr, false);
#endif
			break;
		}
		update_nodes = 1;
	}

	if (update_nodes)
		last_node_update = time(NULL);
}

/*
 * Sync with _init_conf_part().
 *
 * _init_conf_part() initializes default values from slurm.conf parameters.
 * After parsing slurm.conf, _build_single_partitionline_info() copies
 * slurm_conf_partition_t to part_record_t. Default values between
 * slurm_conf_partition_t and part_record_t should stay in sync in case a
 * part_record_t is created outside of slurm.conf parsing.
 */
static void _init_part_record(part_record_t *part_ptr)
{
	part_ptr->magic = PART_MAGIC;
	if (slurm_conf.conf_flags & CTL_CONF_DRJ)
		part_ptr->flags |= PART_FLAG_NO_ROOT;
	part_ptr->max_nodes_orig = INFINITE;
	part_ptr->min_nodes = 1;
	part_ptr->min_nodes_orig = 1;

	/* sync with slurm_conf_partition_t */
	part_ptr->default_time = NO_VAL;
	part_ptr->max_cpus_per_node = INFINITE;
	part_ptr->max_nodes = INFINITE;
	part_ptr->max_share = 1;
	part_ptr->max_time = INFINITE;
	part_ptr->over_time_limit = NO_VAL16;
	part_ptr->preempt_mode = NO_VAL16;
	part_ptr->priority_job_factor = 1;
	part_ptr->priority_tier = 1;
	part_ptr->resume_timeout = NO_VAL16;
	part_ptr->state_up = PARTITION_UP;
	part_ptr->suspend_time = NO_VAL;
	part_ptr->suspend_timeout = NO_VAL16;
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
    part_ptr->suspend_idle		= NO_VAL;
#endif
#if (defined __METASTACK_NEW_HETPART_SUPPORT) || (defined __METASTACK_NEW_PART_RBN)
    part_ptr->meta_flags			= 0;
#endif
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
	part_ptr->standby_nodes = xcalloc(1, sizeof(standby_nodes_t));
	part_ptr->standby_nodes->borrowed_node_bitmap = bit_alloc(node_record_count);
	part_ptr->standby_nodes->standby_node_bitmap  = bit_alloc(node_record_count);
#endif	
}

/*
 * create_part_record - create a partition record
 * RET a pointer to the record or NULL if error
 * global: part_list - global partition list
 */
part_record_t *create_part_record(const char *name)
{
	part_record_t *part_ptr = xmalloc(sizeof(*part_ptr));

	last_part_update = time(NULL);

	_init_part_record(part_ptr);
	part_ptr->name = xstrdup(name);

	(void) list_append(part_list, part_ptr);

	return part_ptr;
}

/* dump_all_part_state - save the state of all partitions to file */
int dump_all_part_state(void)
{
	/* Save high-water mark to avoid buffer growth with copies */
	static int high_buffer_size = BUF_SIZE;
	int error_code = 0, log_fd;
	char *old_file, *new_file, *reg_file;
	/* Locks: Read partition */
	slurmctld_lock_t part_read_lock =
	    { READ_LOCK, NO_LOCK, NO_LOCK, READ_LOCK, NO_LOCK };
	buf_t *buffer = init_buf(high_buffer_size);
	DEF_TIMERS;

	START_TIMER;
	/* write header: time */
	packstr(PART_STATE_VERSION, buffer);
	pack16(SLURM_PROTOCOL_VERSION, buffer);
	pack_time(time(NULL), buffer);

	/* write partition records to buffer */
	lock_slurmctld(part_read_lock);
	list_for_each_ro(part_list, _dump_part_state, buffer);

	old_file = xstrdup(slurm_conf.state_save_location);
	xstrcat(old_file, "/part_state.old");
	reg_file = xstrdup(slurm_conf.state_save_location);
	xstrcat(reg_file, "/part_state");
	new_file = xstrdup(slurm_conf.state_save_location);
	xstrcat(new_file, "/part_state.new");
	unlock_slurmctld(part_read_lock);

	/* write the buffer to file */
	lock_state_files();
	log_fd = creat(new_file, 0600);
	if (log_fd < 0) {
		error("Can't save state, error creating file %s, %m",
		      new_file);
		error_code = errno;
	} else {
		int pos = 0, nwrite = get_buf_offset(buffer), amount, rc;
		char *data = (char *)get_buf_data(buffer);
		high_buffer_size = MAX(nwrite, high_buffer_size);
		while (nwrite > 0) {
			amount = write(log_fd, &data[pos], nwrite);
			if ((amount < 0) && (errno != EINTR)) {
				error("Error writing file %s, %m", new_file);
				error_code = errno;
				break;
			}
			nwrite -= amount;
			pos    += amount;
		}

		rc = fsync_and_close(log_fd, "partition");
		if (rc && !error_code)
			error_code = rc;
	}
	if (error_code)
		(void) unlink(new_file);
	else {			/* file shuffle */
		(void) unlink(old_file);
		if (link(reg_file, old_file)) {
			debug4("unable to create link for %s -> %s: %m",
			       reg_file, old_file);
		}
		(void) unlink(reg_file);
		if (link(new_file, reg_file)) {
			debug4("unable to create link for %s -> %s: %m",
			       new_file, reg_file);
		}
		(void) unlink(new_file);
	}
	xfree(old_file);
	xfree(reg_file);
	xfree(new_file);
	unlock_state_files();

	free_buf(buffer);
	END_TIMER2("dump_all_part_state");
	return 0;
}

/*
 * _dump_part_state - dump the state of a specific partition to a buffer
 * IN part_ptr - pointer to partition for which information
 *	is requested
 * IN/OUT buffer - location to store data, pointers automatically advanced
 *
 * Note: read by load_all_part_state().
 */
static int _dump_part_state(void *x, void *arg)
{
	part_record_t *part_ptr = (part_record_t *) x;
	buf_t *buffer = (buf_t *) arg;

	xassert(part_ptr);
	xassert(part_ptr->magic == PART_MAGIC);

	if (default_part_loc == part_ptr)
		part_ptr->flags |= PART_FLAG_DEFAULT;
	else
		part_ptr->flags &= (~PART_FLAG_DEFAULT);

	pack32(part_ptr->cpu_bind,	 buffer);
	packstr(part_ptr->name,          buffer);
	pack32(part_ptr->grace_time,	 buffer);
	pack32(part_ptr->max_time,       buffer);
	pack32(part_ptr->default_time,   buffer);
	pack32(part_ptr->max_cpus_per_node, buffer);
	pack32(part_ptr->max_nodes_orig, buffer);
	pack32(part_ptr->min_nodes_orig, buffer);

	pack16(part_ptr->flags,          buffer);
	pack16(part_ptr->max_share,      buffer);
	pack16(part_ptr->over_time_limit,buffer);
	pack16(part_ptr->preempt_mode,   buffer);
	pack16(part_ptr->priority_job_factor, buffer);
	pack16(part_ptr->priority_tier,  buffer);

	pack16(part_ptr->state_up,       buffer);

	pack16(part_ptr->cr_type,        buffer);

	packstr(part_ptr->allow_accounts, buffer);
	packstr(part_ptr->allow_groups,  buffer);
	packstr(part_ptr->allow_qos,     buffer);
	packstr(part_ptr->qos_char,      buffer);
	packstr(part_ptr->allow_alloc_nodes, buffer);
	packstr(part_ptr->alternate,     buffer);
	packstr(part_ptr->deny_accounts, buffer);
	packstr(part_ptr->deny_qos,      buffer);
	/* Save orig_nodes as nodes will be built from orig_nodes */
	packstr(part_ptr->orig_nodes, buffer);
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
	pack32(part_ptr->suspend_idle,	 buffer);
#endif
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
	packstr(part_ptr->standby_nodes->borrowed_nodes, buffer);
#endif

	return 0;
}

/* Open the partition state save file, or backup if necessary.
 * state_file IN - the name of the state save file used
 * RET the file description to read from or error code
 */
static buf_t *_open_part_state_file(char **state_file)
{
	buf_t *buf;

	*state_file = xstrdup(slurm_conf.state_save_location);
	xstrcat(*state_file, "/part_state");
	buf = create_mmap_buf(*state_file);
	if (!buf) {
		error("Could not open partition state file %s: %m",
		      *state_file);
	} else 	/* Success */
		return buf;

	error("NOTE: Trying backup partition state save file. Information may be lost!");
	xstrcat(*state_file, ".old");
	buf = create_mmap_buf(*state_file);
	return buf;
}

/*
 * load_all_part_state - load the partition state from file, recover on
 *	slurmctld restart. execute this after loading the configuration
 *	file data.
 *
 * Note: reads dump from _dump_part_state().
 */
int load_all_part_state(void)
{
	char *part_name = NULL, *nodes = NULL;
	char *allow_accounts = NULL, *allow_groups = NULL, *allow_qos = NULL;
	char *deny_accounts = NULL, *deny_qos = NULL, *qos_char = NULL;
	char *state_file = NULL;
	uint32_t max_time, default_time, max_nodes, min_nodes;
	uint32_t max_cpus_per_node = INFINITE, cpu_bind = 0, grace_time = 0;
	time_t time;
	uint16_t flags, priority_job_factor, priority_tier;
	uint16_t max_share, over_time_limit = NO_VAL16, preempt_mode;
	uint16_t state_up, cr_type;
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
	uint32_t suspend_idle;
#endif
	part_record_t *part_ptr;
	uint32_t name_len;
	int error_code = 0, part_cnt = 0;
	buf_t *buffer;
	char *ver_str = NULL;
	char* allow_alloc_nodes = NULL;
	uint16_t protocol_version = NO_VAL16;
	char* alternate = NULL;
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
	char *borrowed_nodes = NULL;
#endif
	xassert(verify_lock(CONF_LOCK, READ_LOCK));

	/* read the file */
	lock_state_files();
	buffer = _open_part_state_file(&state_file);
	if (!buffer) {
		info("No partition state file (%s) to recover",
		     state_file);
		xfree(state_file);
		unlock_state_files();
		return ENOENT;
	}
	xfree(state_file);
	unlock_state_files();

	safe_unpackstr_xmalloc(&ver_str, &name_len, buffer);
	debug3("Version string in part_state header is %s", ver_str);
	if (ver_str && !xstrcmp(ver_str, PART_STATE_VERSION))
		safe_unpack16(&protocol_version, buffer);

	if (protocol_version == NO_VAL16) {
		if (!ignore_state_errors)
			fatal("Can not recover partition state, data version incompatible, start with '-i' to ignore this. Warning: using -i will lose the data that can't be recovered.");
		error("**********************************************************");
		error("Can not recover partition state, data version incompatible");
		error("**********************************************************");
		xfree(ver_str);
		free_buf(buffer);
		return EFAULT;
	}
	xfree(ver_str);
	safe_unpack_time(&time, buffer);

	while (remaining_buf(buffer) > 0) {
#ifdef __META_PROTOCOL
        if (protocol_version >= SLURM_22_05_PROTOCOL_VERSION) {
            if (protocol_version >= META_2_1_PROTOCOL_VERSION) {
                safe_unpack32(&cpu_bind, buffer);
                safe_unpackstr_xmalloc(&part_name, &name_len, buffer);
                safe_unpack32(&grace_time, buffer);
                safe_unpack32(&max_time, buffer);
                safe_unpack32(&default_time, buffer);
                safe_unpack32(&max_cpus_per_node, buffer);
                safe_unpack32(&max_nodes, buffer);
                safe_unpack32(&min_nodes, buffer);

                safe_unpack16(&flags,        buffer);
                safe_unpack16(&max_share,    buffer);
                safe_unpack16(&over_time_limit, buffer);
                safe_unpack16(&preempt_mode, buffer);

                safe_unpack16(&priority_job_factor, buffer);
                safe_unpack16(&priority_tier, buffer);
                if (priority_job_factor > part_max_priority)
                    part_max_priority = priority_job_factor;

                safe_unpack16(&state_up, buffer);
                safe_unpack16(&cr_type, buffer);

                safe_unpackstr_xmalloc(&allow_accounts,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_groups,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_qos,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&qos_char,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_alloc_nodes, &name_len,
                            buffer);
                safe_unpackstr_xmalloc(&alternate, &name_len, buffer);
                safe_unpackstr_xmalloc(&deny_accounts,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&deny_qos,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&nodes, &name_len, buffer);
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
                safe_unpack32(&suspend_idle, buffer);
#endif
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
			    safe_unpackstr_xmalloc(&borrowed_nodes, &name_len, buffer);
#endif	
                if ((flags & PART_FLAG_DEFAULT_CLR)   ||
                    (flags & PART_FLAG_EXC_USER_CLR)  ||
                    (flags & PART_FLAG_HIDDEN_CLR)    ||
                    (flags & PART_FLAG_NO_ROOT_CLR)   ||
                    (flags & PART_FLAG_ROOT_ONLY_CLR) ||
                    (flags & PART_FLAG_REQ_RESV_CLR)  ||
                    (flags & PART_FLAG_LLN_CLR)) {
                    error("Invalid data for partition %s: flags=%u",
                        part_name, flags);
                    error_code = EINVAL;
                }
            } else if (protocol_version >= META_2_0_PROTOCOL_VERSION) {
                safe_unpack32(&cpu_bind, buffer);
                safe_unpackstr_xmalloc(&part_name, &name_len, buffer);
                safe_unpack32(&grace_time, buffer);
                safe_unpack32(&max_time, buffer);
                safe_unpack32(&default_time, buffer);
                safe_unpack32(&max_cpus_per_node, buffer);
                safe_unpack32(&max_nodes, buffer);
                safe_unpack32(&min_nodes, buffer);

                safe_unpack16(&flags,        buffer);
                safe_unpack16(&max_share,    buffer);
                safe_unpack16(&over_time_limit, buffer);
                safe_unpack16(&preempt_mode, buffer);

                safe_unpack16(&priority_job_factor, buffer);
                safe_unpack16(&priority_tier, buffer);
                if (priority_job_factor > part_max_priority)
                    part_max_priority = priority_job_factor;

                safe_unpack16(&state_up, buffer);
                safe_unpack16(&cr_type, buffer);

                safe_unpackstr_xmalloc(&allow_accounts,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_groups,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_qos,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&qos_char,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_alloc_nodes, &name_len,
                            buffer);
                safe_unpackstr_xmalloc(&alternate, &name_len, buffer);
                safe_unpackstr_xmalloc(&deny_accounts,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&deny_qos,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&nodes, &name_len, buffer);
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
                safe_unpack32(&suspend_idle, buffer);
#endif
                if ((flags & PART_FLAG_DEFAULT_CLR)   ||
                    (flags & PART_FLAG_EXC_USER_CLR)  ||
                    (flags & PART_FLAG_HIDDEN_CLR)    ||
                    (flags & PART_FLAG_NO_ROOT_CLR)   ||
                    (flags & PART_FLAG_ROOT_ONLY_CLR) ||
                    (flags & PART_FLAG_REQ_RESV_CLR)  ||
                    (flags & PART_FLAG_LLN_CLR)) {
                    error("Invalid data for partition %s: flags=%u",
                        part_name, flags);
                    error_code = EINVAL;
                }
            } else {
                safe_unpack32(&cpu_bind, buffer);
                safe_unpackstr_xmalloc(&part_name, &name_len, buffer);
                safe_unpack32(&grace_time, buffer);
                safe_unpack32(&max_time, buffer);
                safe_unpack32(&default_time, buffer);
                safe_unpack32(&max_cpus_per_node, buffer);
                safe_unpack32(&max_nodes, buffer);
                safe_unpack32(&min_nodes, buffer);

                safe_unpack16(&flags,        buffer);
                safe_unpack16(&max_share,    buffer);
                safe_unpack16(&over_time_limit, buffer);
                safe_unpack16(&preempt_mode, buffer);

                safe_unpack16(&priority_job_factor, buffer);
                safe_unpack16(&priority_tier, buffer);
                if (priority_job_factor > part_max_priority)
                    part_max_priority = priority_job_factor;

                safe_unpack16(&state_up, buffer);
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
                safe_unpack32(&suspend_idle, buffer);
#endif
                safe_unpack16(&cr_type, buffer);

                safe_unpackstr_xmalloc(&allow_accounts,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_groups,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_qos,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&qos_char,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_alloc_nodes, &name_len,
                            buffer);
                safe_unpackstr_xmalloc(&alternate, &name_len, buffer);
                safe_unpackstr_xmalloc(&deny_accounts,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&deny_qos,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&nodes, &name_len, buffer);
                if ((flags & PART_FLAG_DEFAULT_CLR)   ||
                    (flags & PART_FLAG_EXC_USER_CLR)  ||
                    (flags & PART_FLAG_HIDDEN_CLR)    ||
                    (flags & PART_FLAG_NO_ROOT_CLR)   ||
                    (flags & PART_FLAG_ROOT_ONLY_CLR) ||
                    (flags & PART_FLAG_REQ_RESV_CLR)  ||
                    (flags & PART_FLAG_LLN_CLR)) {
                    error("Invalid data for partition %s: flags=%u",
                        part_name, flags);
                    error_code = EINVAL;
                }
            }
        } else if (protocol_version >= SLURM_21_08_PROTOCOL_VERSION) {
            safe_unpack32(&cpu_bind, buffer);
			safe_unpackstr_xmalloc(&part_name, &name_len, buffer);
			safe_unpack32(&grace_time, buffer);
			safe_unpack32(&max_time, buffer);
			safe_unpack32(&default_time, buffer);
			safe_unpack32(&max_cpus_per_node, buffer);
			safe_unpack32(&max_nodes, buffer);
			safe_unpack32(&min_nodes, buffer);

			safe_unpack16(&flags,        buffer);
			safe_unpack16(&max_share,    buffer);
			safe_unpack16(&over_time_limit, buffer);
			safe_unpack16(&preempt_mode, buffer);

			safe_unpack16(&priority_job_factor, buffer);
			safe_unpack16(&priority_tier, buffer);
			if (priority_job_factor > part_max_priority)
				part_max_priority = priority_job_factor;

			safe_unpack16(&state_up, buffer);
			safe_unpack16(&cr_type, buffer);

			safe_unpackstr_xmalloc(&allow_accounts,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&allow_groups,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&allow_qos,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&qos_char,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&allow_alloc_nodes, &name_len,
					       buffer);
			safe_unpackstr_xmalloc(&alternate, &name_len, buffer);
			safe_unpackstr_xmalloc(&deny_accounts,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&deny_qos,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&nodes, &name_len, buffer);
			if ((flags & PART_FLAG_DEFAULT_CLR)   ||
			    (flags & PART_FLAG_EXC_USER_CLR)  ||
			    (flags & PART_FLAG_HIDDEN_CLR)    ||
			    (flags & PART_FLAG_NO_ROOT_CLR)   ||
			    (flags & PART_FLAG_ROOT_ONLY_CLR) ||
			    (flags & PART_FLAG_REQ_RESV_CLR)  ||
			    (flags & PART_FLAG_LLN_CLR)) {
				error("Invalid data for partition %s: flags=%u",
				      part_name, flags);
				error_code = EINVAL;
			}
        } else if (protocol_version >= SLURM_MIN_PROTOCOL_VERSION) {
			safe_unpack32(&cpu_bind, buffer);
			safe_unpackstr_xmalloc(&part_name, &name_len, buffer);
			safe_unpack32(&grace_time, buffer);
			safe_unpack32(&max_time, buffer);
			safe_unpack32(&default_time, buffer);
			safe_unpack32(&max_cpus_per_node, buffer);
			safe_unpack32(&max_nodes, buffer);
			safe_unpack32(&min_nodes, buffer);

			safe_unpack16(&flags,        buffer);
			safe_unpack16(&max_share,    buffer);
			safe_unpack16(&over_time_limit, buffer);
			safe_unpack16(&preempt_mode, buffer);

			safe_unpack16(&priority_job_factor, buffer);
			safe_unpack16(&priority_tier, buffer);
			if (priority_job_factor > part_max_priority)
				part_max_priority = priority_job_factor;

			safe_unpack16(&state_up, buffer);
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
            safe_unpack32(&suspend_idle, buffer);
#endif
			safe_unpack16(&cr_type, buffer);

			safe_unpackstr_xmalloc(&allow_accounts,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&allow_groups,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&allow_qos,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&qos_char,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&allow_alloc_nodes, &name_len,
					       buffer);
			safe_unpackstr_xmalloc(&alternate, &name_len, buffer);
			safe_unpackstr_xmalloc(&deny_accounts,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&deny_qos,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&nodes, &name_len, buffer);
			if ((flags & PART_FLAG_DEFAULT_CLR)   ||
			    (flags & PART_FLAG_EXC_USER_CLR)  ||
			    (flags & PART_FLAG_HIDDEN_CLR)    ||
			    (flags & PART_FLAG_NO_ROOT_CLR)   ||
			    (flags & PART_FLAG_ROOT_ONLY_CLR) ||
			    (flags & PART_FLAG_REQ_RESV_CLR)  ||
			    (flags & PART_FLAG_LLN_CLR)) {
				error("Invalid data for partition %s: flags=%u",
				      part_name, flags);
				error_code = EINVAL;
			}
        }
#endif 
        else {
			error("%s: protocol_version %hu not supported",
			      __func__, protocol_version);
			goto unpack_error;
		}
		/* validity test as possible */
		if (state_up > PARTITION_UP) {
			error("Invalid data for partition %s: state_up=%u",
			      part_name, state_up);
			error_code = EINVAL;
		}
		if (error_code) {
			error("No more partition data will be processed from "
			      "the checkpoint file");
			xfree(allow_accounts);
			xfree(allow_groups);
			xfree(allow_qos);
			xfree(qos_char);
			xfree(allow_alloc_nodes);
			xfree(alternate);
			xfree(deny_accounts);
			xfree(deny_qos);
			xfree(part_name);
			xfree(nodes);
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
			xfree(borrowed_nodes);
#endif			
			error_code = EINVAL;
			break;
		}

		/* find record and perform update */
		part_ptr = list_find_first(part_list, &list_find_part,
					   part_name);
		part_cnt++;
		if (part_ptr == NULL) {
			info("%s: partition %s missing from configuration file",
			     __func__, part_name);
			part_ptr = create_part_record(part_name);
		}

		part_ptr->cpu_bind       = cpu_bind;
		part_ptr->flags          = flags;
		if (part_ptr->flags & PART_FLAG_DEFAULT) {
			xfree(default_part_name);
			default_part_name = xstrdup(part_name);
			default_part_loc = part_ptr;
		}
		part_ptr->max_time       = max_time;
		part_ptr->default_time   = default_time;
		part_ptr->max_cpus_per_node = max_cpus_per_node;
		part_ptr->max_nodes      = max_nodes;
		part_ptr->max_nodes_orig = max_nodes;
		part_ptr->min_nodes      = min_nodes;
		part_ptr->min_nodes_orig = min_nodes;
		part_ptr->max_share      = max_share;
		part_ptr->grace_time     = grace_time;
		part_ptr->over_time_limit = over_time_limit;
		if (preempt_mode != NO_VAL16)
			part_ptr->preempt_mode   = preempt_mode;
		part_ptr->priority_job_factor = priority_job_factor;
		part_ptr->priority_tier  = priority_tier;
		part_ptr->state_up       = state_up;
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
        part_ptr->suspend_idle  = suspend_idle;
#endif
		part_ptr->cr_type	 = cr_type;
		xfree(part_ptr->allow_accounts);
		part_ptr->allow_accounts = allow_accounts;
		xfree(part_ptr->allow_groups);
		accounts_list_build(part_ptr->allow_accounts,
				    &part_ptr->allow_account_array);
		part_ptr->allow_groups   = allow_groups;
		xfree(part_ptr->allow_qos);
		part_ptr->allow_qos      = allow_qos;
		qos_list_build(part_ptr->allow_qos,&part_ptr->allow_qos_bitstr);

		if (qos_char) {
			slurmdb_qos_rec_t qos_rec;
			xfree(part_ptr->qos_char);
			part_ptr->qos_char = qos_char;

			memset(&qos_rec, 0, sizeof(slurmdb_qos_rec_t));
			qos_rec.name = part_ptr->qos_char;
			if (assoc_mgr_fill_in_qos(
				    acct_db_conn, &qos_rec, accounting_enforce,
				    (slurmdb_qos_rec_t **)&part_ptr->qos_ptr, 0)
			    != SLURM_SUCCESS) {
				error("Partition %s has an invalid qos (%s), "
				      "please check your configuration",
				      part_ptr->name, qos_rec.name);
				xfree(part_ptr->qos_char);
			}
		}

		xfree(part_ptr->allow_alloc_nodes);
		part_ptr->allow_alloc_nodes   = allow_alloc_nodes;
		xfree(part_ptr->alternate);
		part_ptr->alternate      = alternate;
		xfree(part_ptr->deny_accounts);
		part_ptr->deny_accounts  = deny_accounts;
		accounts_list_build(part_ptr->deny_accounts,
				    &part_ptr->deny_account_array);
		xfree(part_ptr->deny_qos);
		part_ptr->deny_qos       = deny_qos;
		qos_list_build(part_ptr->deny_qos, &part_ptr->deny_qos_bitstr);

		/*
		 * Store saved nodelist in orig_nodes. nodes will be regenerated
		 * from orig_nodes.
		 */
		xfree(part_ptr->nodes);
		xfree(part_ptr->orig_nodes);
		part_ptr->orig_nodes = nodes;
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
		if (part_ptr->standby_nodes) {
			xfree(part_ptr->standby_nodes->borrowed_nodes);
		} else {
			part_ptr->standby_nodes = xcalloc(1, sizeof(standby_nodes_t));
		}
		part_ptr->standby_nodes->borrowed_nodes = borrowed_nodes;
#endif	
		xfree(part_name);
	}

	info("Recovered state of %d partitions", part_cnt);
	free_buf(buffer);
	return error_code;

unpack_error:
	if (!ignore_state_errors)
		fatal("Incomplete partition data checkpoint file, start with '-i' to ignore this. Warning: using -i will lose the data that can't be recovered.");
	error("Incomplete partition data checkpoint file");
	info("Recovered state of %d partitions", part_cnt);
	free_buf(buffer);
	return EFAULT;
}

#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
void standby_nodes_free(standby_nodes_t *standby_nodes)
{
	if (standby_nodes) {
		xfree(standby_nodes->borrowed_nodes);
		xfree(standby_nodes->nodes);
		xfree(standby_nodes->parameters);
		FREE_NULL_BITMAP(standby_nodes->borrowed_node_bitmap);
		FREE_NULL_BITMAP(standby_nodes->standby_node_bitmap);
		xfree(standby_nodes);
	}
}

/*
 * load_all_part_borrow_nodes - load the partition borrowed nodes from file, recover on
 *	slurmctld restart. execute this after loading the configuration
 *	file data.
 *
 * Note: reads dump from _dump_part_state().
 */
extern int load_all_part_borrow_nodes(void)
{
	char *part_name = NULL, *nodes = NULL;
	char *allow_accounts = NULL, *allow_groups = NULL, *allow_qos = NULL;
	char *deny_accounts = NULL, *deny_qos = NULL, *qos_char = NULL;
	char *state_file = NULL;
	uint32_t max_time, default_time, max_nodes, min_nodes;
	uint32_t max_cpus_per_node = INFINITE, cpu_bind = 0, grace_time = 0;
	time_t time;
	uint16_t flags, priority_job_factor, priority_tier;
	uint16_t max_share, over_time_limit = NO_VAL16, preempt_mode;
	uint16_t state_up, cr_type;
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
	uint32_t suspend_idle;
#endif
	part_record_t *part_ptr = NULL;
	uint32_t name_len;
	int error_code = 0, part_cnt = 0;
	buf_t *buffer;
	char *ver_str = NULL;
	char* allow_alloc_nodes = NULL;
	uint16_t protocol_version = NO_VAL16;
	char* alternate = NULL;
	char *borrowed_nodes = NULL;

	xassert(verify_lock(CONF_LOCK, READ_LOCK));

	/* read the file */
	lock_state_files();
	buffer = _open_part_state_file(&state_file);
	if (!buffer) {
		info("No partition state file (%s) to recover",
		     state_file);
		xfree(state_file);
		unlock_state_files();
		return ENOENT;
	}
	xfree(state_file);
	unlock_state_files();

	safe_unpackstr_xmalloc(&ver_str, &name_len, buffer);
	debug3("Version string in part_state header is %s", ver_str);
	if (ver_str && !xstrcmp(ver_str, PART_STATE_VERSION))
		safe_unpack16(&protocol_version, buffer);

	if (protocol_version == NO_VAL16) {
		if (!ignore_state_errors)
			fatal("Can not recover partition state, data version incompatible, start with '-i' to ignore this. Warning: using -i will lose the data that can't be recovered.");
		error("**********************************************************");
		error("Can not recover partition state, data version incompatible");
		error("**********************************************************");
		xfree(ver_str);
		free_buf(buffer);
		return EFAULT;
	}
	xfree(ver_str);
	safe_unpack_time(&time, buffer);

	while (remaining_buf(buffer) > 0) {
#ifdef __META_PROTOCOL
        if (protocol_version >= SLURM_22_05_PROTOCOL_VERSION) {
            if (protocol_version >= META_2_1_PROTOCOL_VERSION) {
                safe_unpack32(&cpu_bind, buffer);
                safe_unpackstr_xmalloc(&part_name, &name_len, buffer);
                safe_unpack32(&grace_time, buffer);
                safe_unpack32(&max_time, buffer);
                safe_unpack32(&default_time, buffer);
                safe_unpack32(&max_cpus_per_node, buffer);
                safe_unpack32(&max_nodes, buffer);
                safe_unpack32(&min_nodes, buffer);

                safe_unpack16(&flags,        buffer);
                safe_unpack16(&max_share,    buffer);
                safe_unpack16(&over_time_limit, buffer);
                safe_unpack16(&preempt_mode, buffer);

                safe_unpack16(&priority_job_factor, buffer);
                safe_unpack16(&priority_tier, buffer);
                if (priority_job_factor > part_max_priority)
                    part_max_priority = priority_job_factor;

                safe_unpack16(&state_up, buffer);
                safe_unpack16(&cr_type, buffer);

                safe_unpackstr_xmalloc(&allow_accounts,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_groups,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_qos,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&qos_char,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_alloc_nodes, &name_len,
                            buffer);
                safe_unpackstr_xmalloc(&alternate, &name_len, buffer);
                safe_unpackstr_xmalloc(&deny_accounts,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&deny_qos,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&nodes, &name_len, buffer);
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
                safe_unpack32(&suspend_idle, buffer);
#endif
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
			    safe_unpackstr_xmalloc(&borrowed_nodes, &name_len, buffer);
#endif	
                if ((flags & PART_FLAG_DEFAULT_CLR)   ||
                    (flags & PART_FLAG_EXC_USER_CLR)  ||
                    (flags & PART_FLAG_HIDDEN_CLR)    ||
                    (flags & PART_FLAG_NO_ROOT_CLR)   ||
                    (flags & PART_FLAG_ROOT_ONLY_CLR) ||
                    (flags & PART_FLAG_REQ_RESV_CLR)  ||
                    (flags & PART_FLAG_LLN_CLR)) {
                    error("Invalid data for partition %s: flags=%u",
                        part_name, flags);
                    error_code = EINVAL;
                }
            } else if (protocol_version >= META_2_0_PROTOCOL_VERSION) {
                safe_unpack32(&cpu_bind, buffer);
                safe_unpackstr_xmalloc(&part_name, &name_len, buffer);
                safe_unpack32(&grace_time, buffer);
                safe_unpack32(&max_time, buffer);
                safe_unpack32(&default_time, buffer);
                safe_unpack32(&max_cpus_per_node, buffer);
                safe_unpack32(&max_nodes, buffer);
                safe_unpack32(&min_nodes, buffer);

                safe_unpack16(&flags,        buffer);
                safe_unpack16(&max_share,    buffer);
                safe_unpack16(&over_time_limit, buffer);
                safe_unpack16(&preempt_mode, buffer);

                safe_unpack16(&priority_job_factor, buffer);
                safe_unpack16(&priority_tier, buffer);
                if (priority_job_factor > part_max_priority)
                    part_max_priority = priority_job_factor;

                safe_unpack16(&state_up, buffer);
                safe_unpack16(&cr_type, buffer);

                safe_unpackstr_xmalloc(&allow_accounts,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_groups,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_qos,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&qos_char,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_alloc_nodes, &name_len,
                            buffer);
                safe_unpackstr_xmalloc(&alternate, &name_len, buffer);
                safe_unpackstr_xmalloc(&deny_accounts,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&deny_qos,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&nodes, &name_len, buffer);
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
                safe_unpack32(&suspend_idle, buffer);
#endif
                if ((flags & PART_FLAG_DEFAULT_CLR)   ||
                    (flags & PART_FLAG_EXC_USER_CLR)  ||
                    (flags & PART_FLAG_HIDDEN_CLR)    ||
                    (flags & PART_FLAG_NO_ROOT_CLR)   ||
                    (flags & PART_FLAG_ROOT_ONLY_CLR) ||
                    (flags & PART_FLAG_REQ_RESV_CLR)  ||
                    (flags & PART_FLAG_LLN_CLR)) {
                    error("Invalid data for partition %s: flags=%u",
                        part_name, flags);
                    error_code = EINVAL;
                }
            } else {
                safe_unpack32(&cpu_bind, buffer);
                safe_unpackstr_xmalloc(&part_name, &name_len, buffer);
                safe_unpack32(&grace_time, buffer);
                safe_unpack32(&max_time, buffer);
                safe_unpack32(&default_time, buffer);
                safe_unpack32(&max_cpus_per_node, buffer);
                safe_unpack32(&max_nodes, buffer);
                safe_unpack32(&min_nodes, buffer);

                safe_unpack16(&flags,        buffer);
                safe_unpack16(&max_share,    buffer);
                safe_unpack16(&over_time_limit, buffer);
                safe_unpack16(&preempt_mode, buffer);

                safe_unpack16(&priority_job_factor, buffer);
                safe_unpack16(&priority_tier, buffer);
                if (priority_job_factor > part_max_priority)
                    part_max_priority = priority_job_factor;

                safe_unpack16(&state_up, buffer);
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
                safe_unpack32(&suspend_idle, buffer);
#endif
                safe_unpack16(&cr_type, buffer);

                safe_unpackstr_xmalloc(&allow_accounts,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_groups,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_qos,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&qos_char,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&allow_alloc_nodes, &name_len,
                            buffer);
                safe_unpackstr_xmalloc(&alternate, &name_len, buffer);
                safe_unpackstr_xmalloc(&deny_accounts,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&deny_qos,
                            &name_len, buffer);
                safe_unpackstr_xmalloc(&nodes, &name_len, buffer);
                if ((flags & PART_FLAG_DEFAULT_CLR)   ||
                    (flags & PART_FLAG_EXC_USER_CLR)  ||
                    (flags & PART_FLAG_HIDDEN_CLR)    ||
                    (flags & PART_FLAG_NO_ROOT_CLR)   ||
                    (flags & PART_FLAG_ROOT_ONLY_CLR) ||
                    (flags & PART_FLAG_REQ_RESV_CLR)  ||
                    (flags & PART_FLAG_LLN_CLR)) {
                    error("Invalid data for partition %s: flags=%u",
                        part_name, flags);
                    error_code = EINVAL;
                }
            }
        } else if (protocol_version >= SLURM_21_08_PROTOCOL_VERSION) {
            safe_unpack32(&cpu_bind, buffer);
			safe_unpackstr_xmalloc(&part_name, &name_len, buffer);
			safe_unpack32(&grace_time, buffer);
			safe_unpack32(&max_time, buffer);
			safe_unpack32(&default_time, buffer);
			safe_unpack32(&max_cpus_per_node, buffer);
			safe_unpack32(&max_nodes, buffer);
			safe_unpack32(&min_nodes, buffer);

			safe_unpack16(&flags,        buffer);
			safe_unpack16(&max_share,    buffer);
			safe_unpack16(&over_time_limit, buffer);
			safe_unpack16(&preempt_mode, buffer);

			safe_unpack16(&priority_job_factor, buffer);
			safe_unpack16(&priority_tier, buffer);
			if (priority_job_factor > part_max_priority)
				part_max_priority = priority_job_factor;

			safe_unpack16(&state_up, buffer);
			safe_unpack16(&cr_type, buffer);

			safe_unpackstr_xmalloc(&allow_accounts,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&allow_groups,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&allow_qos,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&qos_char,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&allow_alloc_nodes, &name_len,
					       buffer);
			safe_unpackstr_xmalloc(&alternate, &name_len, buffer);
			safe_unpackstr_xmalloc(&deny_accounts,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&deny_qos,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&nodes, &name_len, buffer);
			if ((flags & PART_FLAG_DEFAULT_CLR)   ||
			    (flags & PART_FLAG_EXC_USER_CLR)  ||
			    (flags & PART_FLAG_HIDDEN_CLR)    ||
			    (flags & PART_FLAG_NO_ROOT_CLR)   ||
			    (flags & PART_FLAG_ROOT_ONLY_CLR) ||
			    (flags & PART_FLAG_REQ_RESV_CLR)  ||
			    (flags & PART_FLAG_LLN_CLR)) {
				error("Invalid data for partition %s: flags=%u",
				      part_name, flags);
				error_code = EINVAL;
			}
        } else if (protocol_version >= SLURM_MIN_PROTOCOL_VERSION) {
			safe_unpack32(&cpu_bind, buffer);
			safe_unpackstr_xmalloc(&part_name, &name_len, buffer);
			safe_unpack32(&grace_time, buffer);
			safe_unpack32(&max_time, buffer);
			safe_unpack32(&default_time, buffer);
			safe_unpack32(&max_cpus_per_node, buffer);
			safe_unpack32(&max_nodes, buffer);
			safe_unpack32(&min_nodes, buffer);

			safe_unpack16(&flags,        buffer);
			safe_unpack16(&max_share,    buffer);
			safe_unpack16(&over_time_limit, buffer);
			safe_unpack16(&preempt_mode, buffer);

			safe_unpack16(&priority_job_factor, buffer);
			safe_unpack16(&priority_tier, buffer);
			if (priority_job_factor > part_max_priority)
				part_max_priority = priority_job_factor;

			safe_unpack16(&state_up, buffer);
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
            safe_unpack32(&suspend_idle, buffer);
#endif
			safe_unpack16(&cr_type, buffer);

			safe_unpackstr_xmalloc(&allow_accounts,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&allow_groups,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&allow_qos,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&qos_char,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&allow_alloc_nodes, &name_len,
					       buffer);
			safe_unpackstr_xmalloc(&alternate, &name_len, buffer);
			safe_unpackstr_xmalloc(&deny_accounts,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&deny_qos,
					       &name_len, buffer);
			safe_unpackstr_xmalloc(&nodes, &name_len, buffer);
			if ((flags & PART_FLAG_DEFAULT_CLR)   ||
			    (flags & PART_FLAG_EXC_USER_CLR)  ||
			    (flags & PART_FLAG_HIDDEN_CLR)    ||
			    (flags & PART_FLAG_NO_ROOT_CLR)   ||
			    (flags & PART_FLAG_ROOT_ONLY_CLR) ||
			    (flags & PART_FLAG_REQ_RESV_CLR)  ||
			    (flags & PART_FLAG_LLN_CLR)) {
				error("Invalid data for partition %s: flags=%u",
				      part_name, flags);
				error_code = EINVAL;
			}
        } else {
			error("%s: protocol_version %hu not supported",
			      __func__, protocol_version);
			goto unpack_error;
		}
#endif

		/* Release unnecessary field memory space */
		xfree(allow_accounts);
		xfree(allow_groups);
		xfree(allow_qos);
		xfree(allow_alloc_nodes);
		xfree(alternate);		
		xfree(deny_accounts);
		xfree(deny_qos);
		//xfree(part_name);
		xfree(qos_char);
		xfree(nodes);

		/* validity test as possible */
		if (state_up > PARTITION_UP) {
			error("Invalid data for partition %s: state_up=%u",
			      part_name, state_up);
			error_code = EINVAL;
		}
		if (error_code) {
			error("No more partition data will be processed from "
			      "the checkpoint file");
			xfree(part_name);	  
			xfree(borrowed_nodes);
			break;
		}

		/* find record and perform update */
		part_ptr = list_find_first(part_list, &list_find_part,
					   part_name);
		part_cnt++;
		if (part_ptr == NULL) {
			info("%s: partition %s removed from configuration file, skip",
			     __func__, part_name);
			xfree(part_name);	 
			xfree(borrowed_nodes);
			continue;
		}

		xfree(part_name);
		xfree(part_ptr->standby_nodes->borrowed_nodes);
		if (borrowed_nodes) {
			part_ptr->standby_nodes->borrowed_nodes = borrowed_nodes;
			debug("%s: Recovered borrowed nodes %s of partition %s", __func__, borrowed_nodes, part_ptr->name);
		} else {
			debug("%s: No borrowed nodes recovered of partition %s", __func__, part_ptr->name);
		}	
	}

	info("Recovered borrowed nodes of %d partitions", part_cnt);
	free_buf(buffer);
	return error_code;

unpack_error:
	if (!ignore_state_errors)
		fatal("Incomplete partition data checkpoint file, start with '-i' to ignore this. Warning: using -i will lose the data that can't be recovered.");
	error("Incomplete partition data checkpoint file");
	info("Recovered borrowed nodes of %d partitions", part_cnt);
	free_buf(buffer);
	return EFAULT;
}
#endif
/*
 * find_part_record - find a record for partition with specified name
 * IN name - name of the desired partition
 * RET pointer to partition or NULL if not found
 */
part_record_t *find_part_record(char *name)
{
	if (!part_list) {
		error("part_list is NULL");
		return NULL;
	}
	return list_find_first(part_list, &list_find_part, name);
}

/*
 * Create a copy of a job's part_list *partition list
 * IN part_list_src - a job's part_list
 * RET copy of part_list_src, must be freed by caller
 */
extern List part_list_copy(List part_list_src)
{
	part_record_t *part_ptr;
	ListIterator iter;
	List part_list_dest = NULL;

	if (!part_list_src)
		return part_list_dest;

	part_list_dest = list_create(NULL);
	iter = list_iterator_create(part_list_src);
	while ((part_ptr = list_next(iter))) {
		list_append(part_list_dest, part_ptr);
	}
	list_iterator_destroy(iter);

	return part_list_dest;
}

/*
 * get_part_list - find record for named partition(s)
 * IN name - partition name(s) in a comma separated list
 * OUT err_part - The first invalid partition name.
 * RET List of pointers to the partitions or NULL if not found
 * NOTE: Caller must free the returned list
 * NOTE: Caller must free err_part
 */
extern List get_part_list(char *name, char **err_part)
{
	part_record_t *part_ptr;
	List job_part_list = NULL;
	char *token, *last = NULL, *tmp_name;

	if (name == NULL)
		return job_part_list;

	tmp_name = xstrdup(name);
	token = strtok_r(tmp_name, ",", &last);
	while (token) {
		part_ptr = list_find_first(part_list, &list_find_part, token);
		if (part_ptr) {
			if (job_part_list == NULL) {
				job_part_list = list_create(NULL);
			}
			if (!list_find_first(job_part_list, &_match_part_ptr,
					     part_ptr)) {
				list_append(job_part_list, part_ptr);
			}
		} else {
			FREE_NULL_LIST(job_part_list);
			if (err_part) {
				xfree(*err_part);
				*err_part = xstrdup(token);
			}
			break;
		}
		token = strtok_r(NULL, ",", &last);
	}
	xfree(tmp_name);
	return job_part_list;
}

/*
 * Create a global partition list.
 *
 * This should be called before creating any partition entries.
 */
void init_part_conf(void)
{
	last_part_update = time(NULL);

	if (part_list)		/* delete defunct partitions */
		list_flush(part_list);
	else
		part_list = list_create(_list_delete_part);
	
#ifdef __METASTACK_OPT_CACHE_QUERY	
	if (!cache_part_list)		/* delete defunct partitions */
		cache_part_list = list_create(_list_delete_part);
#endif
	xfree(default_part_name);
	default_part_loc = NULL;
}

/*
 * Free memory for cached backfill data in partition record
 */
static void _bf_data_free(bf_part_data_t **datap)
{
	bf_part_data_t *data;
	if (!datap || !*datap)
		return;

	data = *datap;

	slurmdb_destroy_bf_usage(data->job_usage);
        slurmdb_destroy_bf_usage(data->resv_usage);
	xhash_free(data->user_usage);
	xfree(data);

	*datap = NULL;
	return;
}

/*
 * _list_delete_part - delete an entry from the global partition list,
 *	see common/list.h for documentation
 * global: node_record_count - count of nodes in the system
 *         node_record_table_ptr - pointer to global node table
 */
static void _list_delete_part(void *part_entry)
{
	part_record_t *part_ptr;
	node_record_t *node_ptr;
	int i, j, k;

	part_ptr = (part_record_t *) part_entry;
	for (i = 0; (node_ptr = next_node(&i)); i++) {
		for (j=0; j<node_ptr->part_cnt; j++) {
			if (node_ptr->part_pptr[j] != part_ptr)
				continue;
			node_ptr->part_cnt--;
			for (k=j; k<node_ptr->part_cnt; k++) {
				node_ptr->part_pptr[k] =
					node_ptr->part_pptr[k+1];
			}
			break;
		}
	}

	xfree(part_ptr->allow_accounts);
	accounts_list_free(&part_ptr->allow_account_array);
	xfree(part_ptr->allow_alloc_nodes);
	xfree(part_ptr->allow_groups);
	xfree(part_ptr->allow_uids);
	xfree(part_ptr->allow_qos);
	FREE_NULL_BITMAP(part_ptr->allow_qos_bitstr);
	xfree(part_ptr->alternate);
	xfree(part_ptr->billing_weights_str);
	xfree(part_ptr->billing_weights);
	xfree(part_ptr->deny_accounts);
	accounts_list_free(&part_ptr->deny_account_array);
	xfree(part_ptr->deny_qos);
	FREE_NULL_BITMAP(part_ptr->deny_qos_bitstr);
	FREE_NULL_LIST(part_ptr->job_defaults_list);
	xfree(part_ptr->name);
	xfree(part_ptr->orig_nodes);
	xfree(part_ptr->nodes);
	xfree(part_ptr->nodesets);
	FREE_NULL_BITMAP(part_ptr->node_bitmap);
	xfree(part_ptr->qos_char);
	xfree(part_ptr->tres_cnt);
	xfree(part_ptr->tres_fmt_str);
	_bf_data_free(&part_ptr->bf_data);
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES	
	standby_nodes_free(part_ptr->standby_nodes);
	part_ptr->standby_nodes = NULL;
#endif

	xfree(part_entry);
}


/*
 * list_find_part - find an entry in the partition list, see common/list.h
 *	for documentation
 * IN key - partition name
 * RET 1 if matches key, 0 otherwise
 * global- part_list - the global partition list
 */
int list_find_part(void *x, void *key)
{
	part_record_t *part_ptr = (part_record_t *) x;
	char *part = (char *)key;

	return (!xstrcmp(part_ptr->name, part));
}

/*
 * _match_part_ptr - find an entry in the partition list, see common/list.h
 *	for documentation
 * IN key - partition pointer
 * RET 1 if partition pointer matches, 0 otherwise
 */
static int _match_part_ptr(void *part_ptr, void *key)
{
	if (part_ptr == key)
		return 1;

	return 0;
}

#ifdef __METASTACK_OPT_PART_VISIBLE
/** 
 * validate_qos
 * In: part_ptr, assoc_rec
 * Out: 1 0r 0
 * return 1 if assoc's qos matches part's allow or deny, 0 otherwise
 */
static int validate_qos(part_record_t *part_ptr, slurmdb_assoc_rec_t *assoc) 
{
	xassert(assoc);
	int rc = 0;
	ListIterator itr_assoc_qos = list_iterator_create(assoc->qos_list);

	if (part_ptr->allow_qos == NULL) {
		if (part_ptr->deny_qos == NULL) {
			// allow_qos == NULL && deny_qos == NULL
			rc = 1;
			goto clean;
		}
		
		// allow_qos == NULL && deny_qos != NULL
		char *tmp_qos = NULL;
		uint32_t qos_id;
		while ((tmp_qos = list_next(itr_assoc_qos))) {
			qos_id = atoi(tmp_qos);
			if ((qos_id < bit_size(part_ptr->deny_qos_bitstr)) && 
								!bit_test(part_ptr->deny_qos_bitstr, qos_id)) { 
				// at least one qos not be denied.
				rc = 1;
				goto clean;
			}
		}

		debug3("%s, all qos associated with user:%s are denied by partition:%s", 
				__func__, assoc->user, part_ptr->name);
		goto clean;
	} else {
		// allow_qos have been set, ignore deny. 
		char *tmp_qos = NULL;
		uint32_t qos_id;
		while ((tmp_qos = list_next(itr_assoc_qos))) {
			qos_id = atoi(tmp_qos);
			if ((qos_id < bit_size(part_ptr->allow_qos_bitstr)) && 
								bit_test(part_ptr->allow_qos_bitstr, qos_id)) {
				/* one qos have been allowed */
				rc = 1;
				break;
			}
		}

		if (!rc) {
			debug3("%s, all qos associated with user:%s are not allowed by partition:%s", 
					__func__, assoc->user, part_ptr->name);
		}
	}

clean:
	list_iterator_destroy(itr_assoc_qos);

	return rc;
}

/** 
 * validate_account
 * In: part_ptr, assoc_rec
 * Out: 1 0r 0
 * return 1 if assoc's acct matches part's allow or deny, 0 otherwise
 */
static int validate_account(part_record_t *part_ptr, slurmdb_assoc_rec_t *assoc_rec)
{
	int rc = 0;
	xassert(assoc_rec);

	if (assoc_rec->partition != NULL) {
		/* do not check part's allow or deny, when assoc's part is not null */
		if (!xstrcasecmp(assoc_rec->partition, part_ptr->name))
			rc = 1;
		
		goto cleanup;
	}

	if (part_ptr->allow_accounts == NULL) {
		if (part_ptr->deny_accounts == NULL) {
			// allow==null && deny==null, no limit
			rc = 1;
			goto cleanup;
		}
		
		// allow == null && deny != null. deny is on work.
		int is_deny = 0;
		for (int i=0; part_ptr->deny_account_array[i]; i++) {
			if (xstrcasecmp(assoc_rec->acct, part_ptr->deny_account_array[i]) == 0) {
				is_deny = 1; 
				break;
			}
		}

		if (is_deny == 0)
			rc = 1;

		goto cleanup;
	} else {
		// allow != null, ignore deny. 
		for (int i=0; part_ptr->allow_account_array[i]; i++) {
			if (xstrcasecmp(part_ptr->allow_account_array[i], assoc_rec->acct) == 0) {
				rc = 1;
				break;
			}
		}
	}

cleanup:
	if (rc)
		debug3("%s, user:%s (acct:%s) is allowed by partition:%s", 
				__func__, assoc_rec->user, assoc_rec->acct, part_ptr->name);
	
	return rc;
}

/** 
 * _part_is_visible_assoc: if part is visible to all user's account 
 * In: part_ptr, assoc_rec
 * Out: 1 0r 0
 * return 1 if visible, 0 otherwise
 */
static bool _part_is_visible_assoc(part_record_t *part_ptr, 
			slurmdb_user_rec_t *user)
{
#ifdef __METASTACK_OPT_CACHE_QUERY
    xassert(verify_lock(PART_LOCK, READ_LOCK) || cache_verify_lock(PART_LOCK, READ_LOCK));
#else
    xassert(verify_lock(PART_LOCK, READ_LOCK));
#endif
	xassert(user->uid != 0);

	if (part_ptr->flags & PART_FLAG_HIDDEN)
		return false;
	if (validate_group(part_ptr, user->uid) == 0)
		return false;

	// only consider the case if the "AccountingStorageType=accounting_storage/slurmdbd" 
	if (!xstrcasecmp(slurm_conf.accounting_storage_type, "accounting_storage/slurmdbd") && 
			 (slurm_conf.private_data & PRIVATE_DATA_PART_EXTEND)) {
		bool rc = false;
		slurmdb_assoc_rec_t *assoc_rec;
		ListIterator itr_assoc = list_iterator_create(user->assoc_list);
		while ((assoc_rec = list_next(itr_assoc))) {
			if ((!(slurm_conf.accounting_storage_enforce & ACCOUNTING_ENFORCE_ASSOCS) || 
					validate_account(part_ptr, assoc_rec)) &&
					(!(slurm_conf.accounting_storage_enforce & ACCOUNTING_ENFORCE_QOS) || 
					validate_qos(part_ptr, assoc_rec))) {
				rc = true;
				break;
			}
		}
		list_iterator_destroy(itr_assoc);
		return rc;
	}

	return true;
}

extern List fill_assoc_list(uid_t uid, bool locked) 
{
	/* should be free in who calls this */
	List assoc_list = NULL;
	slurmdb_assoc_rec_t *assoc_rec;
	assoc_mgr_lock_t locks = { .assoc = READ_LOCK, .user = READ_LOCK};

	if (!assoc_mgr_assoc_list)
		return NULL;
	
	if (!locked)
		assoc_mgr_lock(&locks);

    assoc_list = list_create(NULL);
	ListIterator itr = list_iterator_create(assoc_mgr_assoc_list);
	while ((assoc_rec = list_next(itr))) {
		if (assoc_rec->uid == uid)
			list_append(assoc_list, assoc_rec);
	}
	list_iterator_destroy(itr);

	if (!locked)
		assoc_mgr_unlock(&locks);

	return assoc_list;
}
#else
/* partition is visible to the user */
static bool _part_is_visible(part_record_t *part_ptr, uid_t uid)
{
#ifdef __METASTACK_OPT_CACHE_QUERY
	xassert(verify_lock(PART_LOCK, READ_LOCK) || cache_verify_lock(PART_LOCK, READ_LOCK));
#else
	xassert(verify_lock(PART_LOCK, READ_LOCK));
#endif
	xassert(uid != 0);

	if (part_ptr->flags & PART_FLAG_HIDDEN)
		return false;
	if (validate_group(part_ptr, uid) == 0)
		return false;

	return true;
}
#endif

typedef struct {
	uid_t uid;
	part_record_t **visible_parts;
#ifdef __METASTACK_OPT_PART_VISIBLE
	slurmdb_user_rec_t user_rec;
#endif
} build_visible_parts_arg_t;

static int _build_visible_parts_foreach(void *elem, void *x)
{
	part_record_t *part_ptr = elem;
	build_visible_parts_arg_t *arg = x;

#ifdef __METASTACK_OPT_PART_VISIBLE
	if (_part_is_visible_assoc(part_ptr, &arg->user_rec)) {
#else
	if (_part_is_visible(part_ptr, arg->uid)) {
#endif
		*(arg->visible_parts) = part_ptr;
		arg->visible_parts++;
		if (get_log_level() >= LOG_LEVEL_DEBUG3) {
			char *tmp_str = NULL;
			for (int i = 0; arg->visible_parts[i]; i++)
				xstrfmtcat(tmp_str, "%s%s", tmp_str ? "," : "",
					   arg->visible_parts[i]->name);
			debug3("%s: uid:%u visible_parts:%s",
			       __func__, arg->uid, tmp_str);
			xfree(tmp_str);
		}
	}

	return SLURM_SUCCESS;
}

/** 
 * 
*/
#ifdef __METASTACK_OPT_PART_VISIBLE
extern part_record_t **build_visible_parts_user(slurmdb_user_rec_t *user_ret, 
				bool skip, bool locked)
{
	part_record_t **visible_parts_save;
	part_record_t **visible_parts;
	build_visible_parts_arg_t args = {0};

	/*
	 * if return, user_ret->assoc_list == NULL;
	 */
	if (skip)
		return NULL;

	visible_parts = xcalloc(list_count(part_list) + 1,
				sizeof(part_record_t *));
	args.uid = user_ret->uid;
	args.visible_parts = visible_parts;
	args.user_rec.uid = user_ret->uid;

	assoc_mgr_lock_t locks = { .assoc = READ_LOCK, .user = READ_LOCK };
	if (!locked)
		assoc_mgr_lock(&locks);
	
	args.user_rec.assoc_list = fill_assoc_list(user_ret->uid, true);

	/*
	 * Save start pointer to start of the list so can point to start
	 * after appending to the list.
	 */
	visible_parts_save = visible_parts;
	list_for_each(part_list, _build_visible_parts_foreach, &args);

	/* user_ret->assoc_list */
	// list_destroy(args.user_rec.assoc_list);
	user_ret->assoc_list = args.user_rec.assoc_list;
	args.user_rec.assoc_list = NULL;

	if (!locked)
		assoc_mgr_unlock(&locks);

	return visible_parts_save;
}
#endif

extern part_record_t **build_visible_parts(uid_t uid, bool skip)
{
	part_record_t **visible_parts_save;
	part_record_t **visible_parts;
	build_visible_parts_arg_t args = {0};

	/*
	 * The array of visible parts isn't used for privileged (i.e. operators)
	 * users or when SHOW_ALL is requested, so no need to create list.
	 */
	if (skip)
		return NULL;

	visible_parts = xcalloc(list_count(part_list) + 1,
				sizeof(part_record_t *));
	args.uid = uid;
	args.visible_parts = visible_parts;

	/*
	 * Save start pointer to start of the list so can point to start
	 * after appending to the list.
	 */
	visible_parts_save = visible_parts;
	list_for_each(part_list, _build_visible_parts_foreach, &args);

	return visible_parts_save;
}

extern int part_not_on_list(part_record_t **parts, part_record_t *x)
{
	for (int i = 0; parts[i]; i++) {
		if (parts[i] == x) {
			debug3("%s: partition: %s on visible part list",
			       __func__, x->name);
			return false;
		} else
			debug3("%s: partition: %s not on visible part list",
			       __func__, x->name);
	}
	return true;
}

static int _pack_part(void *object, void *arg)
{
	part_record_t *part_ptr = object;
	_foreach_pack_part_info_t *pack_info = arg;

	xassert(part_ptr->magic == PART_MAGIC);

	if (!(pack_info->show_flags & SHOW_ALL) &&
	    !pack_info->privileged &&
	    part_not_on_list(pack_info->visible_parts, part_ptr))
		return SLURM_SUCCESS;
#ifdef __METASTACK_OPT_CACHE_QUERY		
    pack_part(part_ptr, pack_info->buffer, pack_info->protocol_version, pack_info->pack_cache);
#else
    pack_part(part_ptr, pack_info->buffer, pack_info->protocol_version);
#endif
	pack_info->parts_packed++;

	return SLURM_SUCCESS;
}

/*
 * pack_all_part - dump all partition information for all partitions in
 *	machine independent form (for network transmission)
 * OUT buffer_ptr - the pointer is set to the allocated buffer.
 * OUT buffer_size - set to size of the buffer in bytes
 * IN show_flags - partition filtering options
 * IN uid - uid of user making request (for partition filtering)
 * global: part_list - global list of partition records
 * NOTE: the buffer at *buffer_ptr must be xfreed by the caller
 * NOTE: change slurm_load_part() in api/part_info.c if data format changes
 */
extern void pack_all_part(char **buffer_ptr, int *buffer_size,
			  uint16_t show_flags, uid_t uid,
			  uint16_t protocol_version)
{
	int tmp_offset;
	time_t now = time(NULL);
	bool privileged = validate_operator(uid);
#ifdef __METASTACK_OPT_PART_VISIBLE
	_foreach_pack_part_info_t pack_info = {
		.buffer = init_buf(BUF_SIZE),
		.parts_packed = 0,
		.privileged = privileged,
		.protocol_version = protocol_version,
		.show_flags = show_flags,
		.uid = uid,
		.user_rec.uid = (uint32_t)uid,
#ifdef __METASTACK_OPT_CACHE_QUERY		
		.pack_cache = false,
#endif
	};
	
    pack_info.user_rec.assoc_list = NULL;
	/* also return user_rec.assoc_list */
	pack_info.visible_parts = build_visible_parts_user(&pack_info.user_rec, privileged, false);
#else
	_foreach_pack_part_info_t pack_info = {
		.buffer = init_buf(BUF_SIZE),
		.parts_packed = 0,
		.privileged = privileged,
		.protocol_version = protocol_version,
		.show_flags = show_flags,
		.uid = uid,
		.visible_parts = build_visible_parts(uid, privileged),
#ifdef __METASTACK_OPT_CACHE_QUERY		
        .pack_cache = false,
#endif
	};
#endif

	buffer_ptr[0] = NULL;
	*buffer_size = 0;

	/* write header: version and time */
	pack32(0, pack_info.buffer);
	pack_time(now, pack_info.buffer);

	list_for_each_ro(part_list, _pack_part, &pack_info);
#ifdef __METASTACK_OPT_PART_VISIBLE
	/* if (privileged) 
	 * assoc_list == NULL */
	if (pack_info.user_rec.assoc_list)
		list_destroy(pack_info.user_rec.assoc_list);
#endif

	/* put the real record count in the message body header */
	tmp_offset = get_buf_offset(pack_info.buffer);
	set_buf_offset(pack_info.buffer, 0);
	pack32(pack_info.parts_packed, pack_info.buffer);
	set_buf_offset(pack_info.buffer, tmp_offset);

	*buffer_size = get_buf_offset(pack_info.buffer);
	buffer_ptr[0] = xfer_buf_data(pack_info.buffer);
	xfree(pack_info.visible_parts);
}


/*
 * pack_part - dump all configuration information about a specific partition
 *	in machine independent form (for network transmission)
 * IN part_ptr - pointer to partition for which information is requested
 * IN/OUT buffer - buffer in which data is placed, pointers automatically
 *	updated
 * global: default_part_loc - pointer to the default partition
 * NOTE: if you make any changes here be sure to make the corresponding changes
 *	to _unpack_partition_info_members() in common/slurm_protocol_pack.c
 */
 #ifdef __METASTACK_OPT_CACHE_QUERY
void pack_part(part_record_t *part_ptr, buf_t *buffer, uint16_t protocol_version, bool pack_cache)
#else
void pack_part(part_record_t *part_ptr, buf_t *buffer, uint16_t protocol_version)
#endif
{
#ifdef __META_PROTOCOL
	if (protocol_version >= SLURM_22_05_PROTOCOL_VERSION) {
        if (protocol_version >= META_2_1_PROTOCOL_VERSION) {
            
#ifdef __METASTACK_OPT_CACHE_QUERY
            if(pack_cache){
                if (default_cache_part_name && !xstrcmp(default_cache_part_name, part_ptr->name))
                    part_ptr->flags |= PART_FLAG_DEFAULT;
                else
                    part_ptr->flags &= (~PART_FLAG_DEFAULT);
            }else{
                if (default_part_loc == part_ptr)
                    part_ptr->flags |= PART_FLAG_DEFAULT;
                else
                    part_ptr->flags &= (~PART_FLAG_DEFAULT);
            }
#else
            if (default_part_loc == part_ptr)
                part_ptr->flags |= PART_FLAG_DEFAULT;
            else
                part_ptr->flags &= (~PART_FLAG_DEFAULT);
#endif
            packstr(part_ptr->name, buffer);
            pack32(part_ptr->cpu_bind, buffer);
            pack32(part_ptr->grace_time, buffer);
            pack32(part_ptr->max_time, buffer);
            pack32(part_ptr->default_time, buffer);
            pack32(part_ptr->max_nodes_orig, buffer);
            pack32(part_ptr->min_nodes_orig, buffer);
            pack32(part_ptr->total_nodes, buffer);
            pack32(part_ptr->total_cpus, buffer);
            pack64(part_ptr->def_mem_per_cpu, buffer);
            pack32(part_ptr->max_cpus_per_node, buffer);
            pack64(part_ptr->max_mem_per_cpu, buffer);

            pack16(part_ptr->flags,      buffer);
            pack16(part_ptr->max_share,  buffer);
            pack16(part_ptr->over_time_limit, buffer);
            pack16(part_ptr->preempt_mode, buffer);
            pack16(part_ptr->priority_job_factor, buffer);
            pack16(part_ptr->priority_tier, buffer);
            pack16(part_ptr->state_up, buffer);

            pack16(part_ptr->cr_type, buffer);
            pack16(part_ptr->resume_timeout, buffer);
            pack16(part_ptr->suspend_timeout, buffer);
            pack32(part_ptr->suspend_time, buffer);

            packstr(part_ptr->allow_accounts, buffer);
            packstr(part_ptr->allow_groups, buffer);
            packstr(part_ptr->allow_alloc_nodes, buffer);
            packstr(part_ptr->allow_qos, buffer);
            packstr(part_ptr->qos_char, buffer);
            packstr(part_ptr->alternate, buffer);
            packstr(part_ptr->deny_accounts, buffer);
            packstr(part_ptr->deny_qos, buffer);
            packstr(part_ptr->nodes, buffer);
            packstr(part_ptr->nodesets, buffer);
            pack_bit_str_hex(part_ptr->node_bitmap, buffer);
            packstr(part_ptr->billing_weights_str, buffer);
            packstr(part_ptr->tres_fmt_str, buffer);
            (void)slurm_pack_list(part_ptr->job_defaults_list,
                        job_defaults_pack, buffer,
                        protocol_version);
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
            pack32(part_ptr->suspend_idle, buffer);
#endif
#if (defined __METASTACK_NEW_HETPART_SUPPORT) || (defined __METASTACK_NEW_PART_RBN)
		pack16(part_ptr->meta_flags,      buffer);
#endif
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
            packstr(part_ptr->standby_nodes->borrowed_nodes, buffer);
            packstr(part_ptr->standby_nodes->nodes, buffer);
            packstr(part_ptr->standby_nodes->parameters, buffer);
#endif	
        } else if (protocol_version >= META_2_0_PROTOCOL_VERSION) {
            if (default_part_loc == part_ptr)
                part_ptr->flags |= PART_FLAG_DEFAULT;
            else
                part_ptr->flags &= (~PART_FLAG_DEFAULT);

            packstr(part_ptr->name, buffer);
            pack32(part_ptr->cpu_bind, buffer);
            pack32(part_ptr->grace_time, buffer);
            pack32(part_ptr->max_time, buffer);
            pack32(part_ptr->default_time, buffer);
            pack32(part_ptr->max_nodes_orig, buffer);
            pack32(part_ptr->min_nodes_orig, buffer);
            pack32(part_ptr->total_nodes, buffer);
            pack32(part_ptr->total_cpus, buffer);
            pack64(part_ptr->def_mem_per_cpu, buffer);
            pack32(part_ptr->max_cpus_per_node, buffer);
            pack64(part_ptr->max_mem_per_cpu, buffer);

            pack16(part_ptr->flags,      buffer);
            pack16(part_ptr->max_share,  buffer);
            pack16(part_ptr->over_time_limit, buffer);
            pack16(part_ptr->preempt_mode, buffer);
            pack16(part_ptr->priority_job_factor, buffer);
            pack16(part_ptr->priority_tier, buffer);
            pack16(part_ptr->state_up, buffer);

            pack16(part_ptr->cr_type, buffer);
            pack16(part_ptr->resume_timeout, buffer);
            pack16(part_ptr->suspend_timeout, buffer);
            pack32(part_ptr->suspend_time, buffer);

            packstr(part_ptr->allow_accounts, buffer);
            packstr(part_ptr->allow_groups, buffer);
            packstr(part_ptr->allow_alloc_nodes, buffer);
            packstr(part_ptr->allow_qos, buffer);
            packstr(part_ptr->qos_char, buffer);
            packstr(part_ptr->alternate, buffer);
            packstr(part_ptr->deny_accounts, buffer);
            packstr(part_ptr->deny_qos, buffer);
            packstr(part_ptr->nodes, buffer);
            packstr(part_ptr->nodesets, buffer);
            pack_bit_str_hex(part_ptr->node_bitmap, buffer);
            packstr(part_ptr->billing_weights_str, buffer);
            packstr(part_ptr->tres_fmt_str, buffer);
            (void)slurm_pack_list(part_ptr->job_defaults_list,
                        job_defaults_pack, buffer,
                        protocol_version);
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
            pack32(part_ptr->suspend_idle, buffer);
#endif
        } else {
            if (default_part_loc == part_ptr)
                part_ptr->flags |= PART_FLAG_DEFAULT;
            else
                part_ptr->flags &= (~PART_FLAG_DEFAULT);

            packstr(part_ptr->name, buffer);
            pack32(part_ptr->cpu_bind, buffer);
            pack32(part_ptr->grace_time, buffer);
            pack32(part_ptr->max_time, buffer);
            pack32(part_ptr->default_time, buffer);
            pack32(part_ptr->max_nodes_orig, buffer);
            pack32(part_ptr->min_nodes_orig, buffer);
            pack32(part_ptr->total_nodes, buffer);
            pack32(part_ptr->total_cpus, buffer);
            pack64(part_ptr->def_mem_per_cpu, buffer);
            pack32(part_ptr->max_cpus_per_node, buffer);
            pack64(part_ptr->max_mem_per_cpu, buffer);

            pack16(part_ptr->flags,      buffer);
            pack16(part_ptr->max_share,  buffer);
            pack16(part_ptr->over_time_limit, buffer);
            pack16(part_ptr->preempt_mode, buffer);
            pack16(part_ptr->priority_job_factor, buffer);
            pack16(part_ptr->priority_tier, buffer);
            pack16(part_ptr->state_up, buffer);
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
            pack32(part_ptr->suspend_idle, buffer);
#endif

            pack16(part_ptr->cr_type, buffer);
            pack16(part_ptr->resume_timeout, buffer);
            pack16(part_ptr->suspend_timeout, buffer);
            pack32(part_ptr->suspend_time, buffer);

            packstr(part_ptr->allow_accounts, buffer);
            packstr(part_ptr->allow_groups, buffer);
            packstr(part_ptr->allow_alloc_nodes, buffer);
            packstr(part_ptr->allow_qos, buffer);
            packstr(part_ptr->qos_char, buffer);
            packstr(part_ptr->alternate, buffer);
            packstr(part_ptr->deny_accounts, buffer);
            packstr(part_ptr->deny_qos, buffer);
            packstr(part_ptr->nodes, buffer);
            packstr(part_ptr->nodesets, buffer);
            pack_bit_str_hex(part_ptr->node_bitmap, buffer);
            packstr(part_ptr->billing_weights_str, buffer);
            packstr(part_ptr->tres_fmt_str, buffer);
            (void)slurm_pack_list(part_ptr->job_defaults_list,
                        job_defaults_pack, buffer,
                        protocol_version);
        }
    } else if (protocol_version >= SLURM_21_08_PROTOCOL_VERSION) {
        if (default_part_loc == part_ptr)
            part_ptr->flags |= PART_FLAG_DEFAULT;
        else
            part_ptr->flags &= (~PART_FLAG_DEFAULT);

        packstr(part_ptr->name, buffer);
        pack32(part_ptr->cpu_bind, buffer);
        pack32(part_ptr->grace_time, buffer);
        pack32(part_ptr->max_time, buffer);
        pack32(part_ptr->default_time, buffer);
        pack32(part_ptr->max_nodes_orig, buffer);
        pack32(part_ptr->min_nodes_orig, buffer);
        pack32(part_ptr->total_nodes, buffer);
        pack32(part_ptr->total_cpus, buffer);
        pack64(part_ptr->def_mem_per_cpu, buffer);
        pack32(part_ptr->max_cpus_per_node, buffer);
        pack64(part_ptr->max_mem_per_cpu, buffer);

        pack16(part_ptr->flags,      buffer);
        pack16(part_ptr->max_share,  buffer);
        pack16(part_ptr->over_time_limit, buffer);
        pack16(part_ptr->preempt_mode, buffer);
        pack16(part_ptr->priority_job_factor, buffer);
        pack16(part_ptr->priority_tier, buffer);
        pack16(part_ptr->state_up, buffer);
        
        pack16(part_ptr->cr_type, buffer);
        pack16(part_ptr->resume_timeout, buffer);
        pack16(part_ptr->suspend_timeout, buffer);
        pack32(part_ptr->suspend_time, buffer);

        packstr(part_ptr->allow_accounts, buffer);
        packstr(part_ptr->allow_groups, buffer);
        packstr(part_ptr->allow_alloc_nodes, buffer);
        packstr(part_ptr->allow_qos, buffer);
        packstr(part_ptr->qos_char, buffer);
        packstr(part_ptr->alternate, buffer);
        packstr(part_ptr->deny_accounts, buffer);
        packstr(part_ptr->deny_qos, buffer);
        packstr(part_ptr->nodes, buffer);
        pack_bit_str_hex(part_ptr->node_bitmap, buffer);
        packstr(part_ptr->billing_weights_str, buffer);
        packstr(part_ptr->tres_fmt_str, buffer);
        (void)slurm_pack_list(part_ptr->job_defaults_list,
                    job_defaults_pack, buffer,
                    protocol_version);
    } else if (protocol_version >= SLURM_MIN_PROTOCOL_VERSION) {
        if (default_part_loc == part_ptr)
            part_ptr->flags |= PART_FLAG_DEFAULT;
        else
            part_ptr->flags &= (~PART_FLAG_DEFAULT);

        packstr(part_ptr->name, buffer);
        pack32(part_ptr->cpu_bind, buffer);
        pack32(part_ptr->grace_time, buffer);
        pack32(part_ptr->max_time, buffer);
        pack32(part_ptr->default_time, buffer);
        pack32(part_ptr->max_nodes_orig, buffer);
        pack32(part_ptr->min_nodes_orig, buffer);
        pack32(part_ptr->total_nodes, buffer);
        pack32(part_ptr->total_cpus, buffer);
        pack64(part_ptr->def_mem_per_cpu, buffer);
        pack32(part_ptr->max_cpus_per_node, buffer);
        pack64(part_ptr->max_mem_per_cpu, buffer);

        pack16(part_ptr->flags,      buffer);
        pack16(part_ptr->max_share,  buffer);
        pack16(part_ptr->over_time_limit, buffer);
        pack16(part_ptr->preempt_mode, buffer);
        pack16(part_ptr->priority_job_factor, buffer);
        pack16(part_ptr->priority_tier, buffer);
        pack16(part_ptr->state_up, buffer);
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
        pack32(part_ptr->suspend_idle, buffer);
#endif
        pack16(part_ptr->cr_type, buffer);

        packstr(part_ptr->allow_accounts, buffer);
        packstr(part_ptr->allow_groups, buffer);
        packstr(part_ptr->allow_alloc_nodes, buffer);
        packstr(part_ptr->allow_qos, buffer);
        packstr(part_ptr->qos_char, buffer);
        packstr(part_ptr->alternate, buffer);
        packstr(part_ptr->deny_accounts, buffer);
        packstr(part_ptr->deny_qos, buffer);
        packstr(part_ptr->nodes, buffer);
        pack_bit_str_hex(part_ptr->node_bitmap, buffer);
        packstr(part_ptr->billing_weights_str, buffer);
        packstr(part_ptr->tres_fmt_str, buffer);
        (void)slurm_pack_list(part_ptr->job_defaults_list,
                    job_defaults_pack, buffer,
                    protocol_version);
    }
#endif
	else {
		error("%s: protocol_version %hu not supported",
		      __func__, protocol_version);
	}
}

/*
 * Process string and set partition fields to appropriate values if valid
 *
 * IN billing_weights_str - suggested billing weights
 * IN part_ptr - pointer to partition
 * IN fail - whether the inner function should fatal if the string is invalid.
 * RET return SLURM_ERROR on error, SLURM_SUCCESS otherwise.
 */
extern int set_partition_billing_weights(char *billing_weights_str,
					 part_record_t *part_ptr, bool fail)
{
	double *tmp = NULL;

	if (!billing_weights_str || *billing_weights_str == '\0') {
		/* Clear the weights */
		xfree(part_ptr->billing_weights_str);
		xfree(part_ptr->billing_weights);
	} else {
		if (!(tmp = slurm_get_tres_weight_array(billing_weights_str,
							slurmctld_tres_cnt,
							fail)))
		    return SLURM_ERROR;

		xfree(part_ptr->billing_weights_str);
		xfree(part_ptr->billing_weights);
		part_ptr->billing_weights_str =
			xstrdup(billing_weights_str);
		part_ptr->billing_weights = tmp;
	}

	return SLURM_SUCCESS;
}

/*
 * update_part - create or update a partition's configuration data
 * IN part_desc - description of partition changes
 * IN create_flag - create a new partition
 * RET 0 or an error code
 * global: part_list - list of partition entries
 *	last_part_update - update time of partition records
 */
extern int update_part(update_part_msg_t * part_desc, bool create_flag)
{
	int error_code;
	part_record_t *part_ptr;
#ifdef __METASTACK_NEW_PART_PARA_SCHED
	bool update_part_node = false;
#endif
	if (part_desc->name == NULL) {
		info("%s: invalid partition name, NULL", __func__);
		return ESLURM_INVALID_PARTITION_NAME;
	}

	error_code = SLURM_SUCCESS;
	part_ptr = list_find_first(part_list, &list_find_part,
				   part_desc->name);

	if (create_flag) {
		if (part_ptr) {
			verbose("%s: Duplicate partition name for create (%s)",
				__func__, part_desc->name);
			return ESLURM_INVALID_PARTITION_NAME;
		}
		info("%s: partition %s being created", __func__,
		     part_desc->name);
		part_ptr = create_part_record(part_desc->name);
	} else {
		if (!part_ptr) {
			verbose("%s: Update for partition not found (%s)",
				__func__, part_desc->name);
			return ESLURM_INVALID_PARTITION_NAME;
		}
	}

	last_part_update = time(NULL);

	if (part_desc->billing_weights_str &&
	    set_partition_billing_weights(part_desc->billing_weights_str,
					  part_ptr, false)) {

		if (create_flag)
			list_delete_all(part_list, &list_find_part,
					part_desc->name);

		return ESLURM_INVALID_TRES_BILLING_WEIGHTS;
	}

	if (part_desc->cpu_bind) {
		char tmp_str[128];
		slurm_sprint_cpu_bind_type(tmp_str, part_desc->cpu_bind);
		info("%s: setting CpuBind to %s for partition %s", __func__,
		     tmp_str, part_desc->name);
		if (part_desc->cpu_bind == CPU_BIND_OFF)
			part_ptr->cpu_bind = 0;
		else
			part_ptr->cpu_bind = part_desc->cpu_bind;
	}

	if (part_desc->max_cpus_per_node != NO_VAL) {
		info("%s: setting MaxCPUsPerNode to %u for partition %s",
		     __func__, part_desc->max_cpus_per_node, part_desc->name);
		part_ptr->max_cpus_per_node = part_desc->max_cpus_per_node;
	}

	if (part_desc->max_time != NO_VAL) {
		info("%s: setting max_time to %u for partition %s", __func__,
		     part_desc->max_time, part_desc->name);
		part_ptr->max_time = part_desc->max_time;
	}

	if ((part_desc->default_time != NO_VAL) &&
	    (part_desc->default_time > part_ptr->max_time)) {
		info("%s: DefaultTime would exceed MaxTime for partition %s",
		     __func__, part_desc->name);
	} else if (part_desc->default_time != NO_VAL) {
		info("%s: setting default_time to %u for partition %s",
		     __func__, part_desc->default_time, part_desc->name);
		part_ptr->default_time = part_desc->default_time;
	}

	if (part_desc->max_nodes != NO_VAL) {
		info("%s: setting max_nodes to %u for partition %s", __func__,
		     part_desc->max_nodes, part_desc->name);
		part_ptr->max_nodes      = part_desc->max_nodes;
		part_ptr->max_nodes_orig = part_desc->max_nodes;
	}

	if (part_desc->min_nodes != NO_VAL) {
		info("%s: setting min_nodes to %u for partition %s", __func__,
		     part_desc->min_nodes, part_desc->name);
		part_ptr->min_nodes      = part_desc->min_nodes;
		part_ptr->min_nodes_orig = part_desc->min_nodes;
	}

	if (part_desc->grace_time != NO_VAL) {
		info("%s: setting grace_time to %u for partition %s", __func__,
		     part_desc->grace_time, part_desc->name);
		part_ptr->grace_time = part_desc->grace_time;
	}

	if (part_desc->flags & PART_FLAG_HIDDEN) {
		info("%s: setting hidden for partition %s", __func__,
		     part_desc->name);
		part_ptr->flags |= PART_FLAG_HIDDEN;
	} else if (part_desc->flags & PART_FLAG_HIDDEN_CLR) {
		info("%s: clearing hidden for partition %s", __func__,
		     part_desc->name);
		part_ptr->flags &= (~PART_FLAG_HIDDEN);
	}

	if (part_desc->flags & PART_FLAG_REQ_RESV) {
		info("%s: setting req_resv for partition %s", __func__,
		     part_desc->name);
		part_ptr->flags |= PART_FLAG_REQ_RESV;
	} else if (part_desc->flags & PART_FLAG_REQ_RESV_CLR) {
		info("%s: clearing req_resv for partition %s", __func__,
		     part_desc->name);
		part_ptr->flags &= (~PART_FLAG_REQ_RESV);
	}

	if (part_desc->flags & PART_FLAG_ROOT_ONLY) {
		info("%s: setting root_only for partition %s", __func__,
		     part_desc->name);
		part_ptr->flags |= PART_FLAG_ROOT_ONLY;
	} else if (part_desc->flags & PART_FLAG_ROOT_ONLY_CLR) {
		info("%s: clearing root_only for partition %s", __func__,
		     part_desc->name);
		part_ptr->flags &= (~PART_FLAG_ROOT_ONLY);
	}

	if (part_desc->flags & PART_FLAG_NO_ROOT) {
		info("%s: setting no_root for partition %s", __func__,
		     part_desc->name);
		part_ptr->flags |= PART_FLAG_NO_ROOT;
	} else if (part_desc->flags & PART_FLAG_NO_ROOT_CLR) {
		info("%s: clearing no_root for partition %s", __func__,
		     part_desc->name);
		part_ptr->flags &= (~PART_FLAG_NO_ROOT);
	}

	if (part_desc->flags & PART_FLAG_EXCLUSIVE_USER) {
		info("%s: setting exclusive_user for partition %s", __func__,
		     part_desc->name);
		part_ptr->flags |= PART_FLAG_EXCLUSIVE_USER;
	} else if (part_desc->flags & PART_FLAG_EXC_USER_CLR) {
		info("%s: clearing exclusive_user for partition %s", __func__,
		     part_desc->name);
		part_ptr->flags &= (~PART_FLAG_EXCLUSIVE_USER);
	}

	if (part_desc->flags & PART_FLAG_DEFAULT) {
		if (default_part_name == NULL) {
			info("%s: setting default partition to %s", __func__,
			     part_desc->name);
		} else if (xstrcmp(default_part_name, part_desc->name) != 0) {
			info("%s: changing default partition from %s to %s",
			     __func__, default_part_name, part_desc->name);
		}
		xfree(default_part_name);
		default_part_name = xstrdup(part_desc->name);
		default_part_loc = part_ptr;
		part_ptr->flags |= PART_FLAG_DEFAULT;
	} else if ((part_desc->flags & PART_FLAG_DEFAULT_CLR) &&
		   (default_part_loc == part_ptr)) {
		info("%s: clearing default partition from %s", __func__,
		     part_desc->name);
		xfree(default_part_name);
		default_part_loc = NULL;
		part_ptr->flags &= (~PART_FLAG_DEFAULT);
	}

	if (part_desc->flags & PART_FLAG_LLN) {
		info("%s: setting LLN for partition %s", __func__,
		     part_desc->name);
		part_ptr->flags |= PART_FLAG_LLN;
	} else if (part_desc->flags & PART_FLAG_LLN_CLR) {
		info("%s: clearing LLN for partition %s", __func__,
		     part_desc->name);
		part_ptr->flags &= (~PART_FLAG_LLN);
	}

#ifdef __METASTACK_NEW_PART_LLS
	if (part_desc->flags & PART_FLAG_LLS) {
		info("%s: setting LLS for partition %s", __func__,
		     part_desc->name);
		part_ptr->flags |= PART_FLAG_LLS;
	} else if (part_desc->flags & PART_FLAG_LLS_CLR) {
		info("%s: clearing LLS for partition %s", __func__,
		     part_desc->name);
		part_ptr->flags &= (~PART_FLAG_LLS);
	}
#endif
#ifdef __METASTACK_NEW_HETPART_SUPPORT
	if (part_desc->meta_flags & PART_METAFLAG_HETPART) {
		info("%s: setting HetPart for partition %s", __func__,
		     part_desc->name);
		part_ptr->meta_flags |= PART_METAFLAG_HETPART;
	} else if (part_desc->meta_flags & PART_METAFLAG_HETPART_CLR) {
		info("%s: clearing HetPart for partition %s", __func__,
		     part_desc->name);
		part_ptr->meta_flags &= (~PART_METAFLAG_HETPART);
	}
#endif
#ifdef __METASTACK_NEW_PART_RBN
	if (part_desc->meta_flags & PART_METAFLAG_RBN) {
		info("%s: setting RBN for partition %s", __func__,
		     part_desc->name);
		part_ptr->meta_flags |= PART_METAFLAG_RBN;
	} else if (part_desc->meta_flags & PART_METAFLAG_RBN_CLR) {
		info("%s: clearing RBN for partition %s", __func__,
		     part_desc->name);
		part_ptr->meta_flags &= (~PART_METAFLAG_RBN);
	}
#endif
	if (part_desc->state_up != NO_VAL16) {
		info("%s: setting state_up to %u for partition %s", __func__,
		     part_desc->state_up, part_desc->name);
		part_ptr->state_up = part_desc->state_up;
	}

	if (part_desc->max_share != NO_VAL16) {
		uint16_t force = part_desc->max_share & SHARED_FORCE;
		uint16_t val = part_desc->max_share & (~SHARED_FORCE);
		char tmp_str[24];
		if (val == 0)
			snprintf(tmp_str, sizeof(tmp_str), "EXCLUSIVE");
		else if (force)
			snprintf(tmp_str, sizeof(tmp_str), "FORCE:%u", val);
		else if (val == 1)
			snprintf(tmp_str, sizeof(tmp_str), "NO");
		else
			snprintf(tmp_str, sizeof(tmp_str), "YES:%u", val);
		info("%s: setting share to %s for partition %s", __func__,
		     tmp_str, part_desc->name);
		part_ptr->max_share = part_desc->max_share;
	}

	if (part_desc->over_time_limit != NO_VAL16) {
		info("%s: setting OverTimeLimit to %u for partition %s",
		     __func__, part_desc->over_time_limit, part_desc->name);
		part_ptr->over_time_limit = part_desc->over_time_limit;
	}

	if (part_desc->preempt_mode != NO_VAL16) {
		uint16_t new_mode;
		new_mode = part_desc->preempt_mode & (~PREEMPT_MODE_GANG);
		if (new_mode <= PREEMPT_MODE_CANCEL) {
			info("%s: setting preempt_mode to %s for partition %s",
			     __func__, preempt_mode_string(new_mode),
			     part_desc->name);
			part_ptr->preempt_mode = new_mode;
		} else {
			info("%s: invalid preempt_mode %u", __func__, new_mode);
		}
	}

	if (part_desc->priority_tier != NO_VAL16) {
		info("%s: setting PriorityTier to %u for partition %s",
		     __func__, part_desc->priority_tier, part_desc->name);
		part_ptr->priority_tier = part_desc->priority_tier;
	}

	if (part_desc->priority_job_factor != NO_VAL16) {
		int redo_prio = 0;
		info("%s: setting PriorityJobFactor to %u for partition %s",
		     __func__, part_desc->priority_job_factor, part_desc->name);

		if ((part_ptr->priority_job_factor == part_max_priority) &&
		    (part_desc->priority_job_factor < part_max_priority))
			redo_prio = 2;
		else if (part_desc->priority_job_factor > part_max_priority)
			redo_prio = 1;

		part_ptr->priority_job_factor = part_desc->priority_job_factor;

		/* If the max_priority changes we need to change all
		 * the normalized priorities of all the other
		 * partitions. If not then just set this partition.
		 */
		if (redo_prio) {
			ListIterator itr = list_iterator_create(part_list);
			part_record_t *part2 = NULL;

			if (redo_prio == 2) {
				part_max_priority = DEF_PART_MAX_PRIORITY;
				while ((part2 = list_next(itr))) {
					if (part2->priority_job_factor >
					    part_max_priority)
						part_max_priority =
							part2->priority_job_factor;
				}
				list_iterator_reset(itr);
			} else
				part_max_priority = part_ptr->priority_job_factor;

			while ((part2 = list_next(itr))) {
				part2->norm_priority =
					(double)part2->priority_job_factor /
					(double)part_max_priority;
			}
			list_iterator_destroy(itr);
		} else {
			part_ptr->norm_priority =
				(double)part_ptr->priority_job_factor /
				(double)part_max_priority;
		}
	}
#ifdef __METASTACK_NEW_SUSPEND_KEEP_IDLE
    if (part_desc->suspend_idle != NO_VAL) {
		info("%s: setting SuspendKeepIdle to %u for partition %s",
		     __func__, part_desc->suspend_idle, part_desc->name);
		part_ptr->suspend_idle = part_desc->suspend_idle;
	}
#endif
	if (part_desc->allow_accounts != NULL) {
		xfree(part_ptr->allow_accounts);
		if ((xstrcasecmp(part_desc->allow_accounts, "ALL") == 0) ||
		    (part_desc->allow_accounts[0] == '\0')) {
			info("%s: setting AllowAccounts to ALL for partition %s",
			     __func__, part_desc->name);
		} else {
			part_ptr->allow_accounts = part_desc->allow_accounts;
			part_desc->allow_accounts = NULL;
			info("%s: setting AllowAccounts to %s for partition %s",
			     __func__, part_ptr->allow_accounts,
			     part_desc->name);
		}
		accounts_list_build(part_ptr->allow_accounts,
				    &part_ptr->allow_account_array);
	}

	if (part_desc->allow_groups != NULL) {
		xfree(part_ptr->allow_groups);
		xfree(part_ptr->allow_uids);
		if ((xstrcasecmp(part_desc->allow_groups, "ALL") == 0) ||
		    (part_desc->allow_groups[0] == '\0')) {
			info("%s: setting allow_groups to ALL for partition %s",
			     __func__, part_desc->name);
		} else {
			part_ptr->allow_groups = part_desc->allow_groups;
			part_desc->allow_groups = NULL;
			info("%s: setting allow_groups to %s for partition %s",
			     __func__, part_ptr->allow_groups, part_desc->name);
			part_ptr->allow_uids =
				get_groups_members(part_ptr->allow_groups);
			clear_group_cache();
		}
	}

	if (part_desc->allow_qos != NULL) {
		xfree(part_ptr->allow_qos);
		if ((xstrcasecmp(part_desc->allow_qos, "ALL") == 0) ||
		    (part_desc->allow_qos[0] == '\0')) {
			info("%s: setting AllowQOS to ALL for partition %s",
			     __func__, part_desc->name);
		} else {
			part_ptr->allow_qos = part_desc->allow_qos;
			part_desc->allow_qos = NULL;
			info("%s: setting AllowQOS to %s for partition %s",
			     __func__, part_ptr->allow_qos, part_desc->name);
		}
		qos_list_build(part_ptr->allow_qos,&part_ptr->allow_qos_bitstr);
	}

	if (part_desc->qos_char && part_desc->qos_char[0] == '\0') {
		info("%s: removing partition QOS %s from partition %s",
		     __func__, part_ptr->qos_char, part_ptr->name);
		xfree(part_ptr->qos_char);
		part_ptr->qos_ptr = NULL;
	} else if (part_desc->qos_char) {
		slurmdb_qos_rec_t qos_rec, *backup_qos_ptr = part_ptr->qos_ptr;

		memset(&qos_rec, 0, sizeof(slurmdb_qos_rec_t));
		qos_rec.name = part_desc->qos_char;
		if (assoc_mgr_fill_in_qos(
			    acct_db_conn, &qos_rec, accounting_enforce,
			    (slurmdb_qos_rec_t **)&part_ptr->qos_ptr, 0)
		    != SLURM_SUCCESS) {
			error("%s: invalid qos (%s) given",
			      __func__, qos_rec.name);
			error_code = ESLURM_INVALID_QOS;
			part_ptr->qos_ptr = backup_qos_ptr;
		} else {
			info("%s: changing partition QOS from "
			     "%s to %s for partition %s",
			     __func__, part_ptr->qos_char, part_desc->qos_char,
			     part_ptr->name);

			xfree(part_ptr->qos_char);
			part_ptr->qos_char = xstrdup(part_desc->qos_char);
		}
	}

	if (part_desc->allow_alloc_nodes != NULL) {
		xfree(part_ptr->allow_alloc_nodes);
		if ((part_desc->allow_alloc_nodes[0] == '\0') ||
		    (xstrcasecmp(part_desc->allow_alloc_nodes, "ALL") == 0)) {
			part_ptr->allow_alloc_nodes = NULL;
			info("%s: setting allow_alloc_nodes to ALL for partition %s",
			     __func__, part_desc->name);
		}
		else {
			part_ptr->allow_alloc_nodes = part_desc->
						      allow_alloc_nodes;
			part_desc->allow_alloc_nodes = NULL;
			info("%s: setting allow_alloc_nodes to %s for partition %s",
			     __func__, part_ptr->allow_alloc_nodes,
			     part_desc->name);
		}
	}
	if (part_desc->alternate != NULL) {
		xfree(part_ptr->alternate);
		if ((xstrcasecmp(part_desc->alternate, "NONE") == 0) ||
		    (part_desc->alternate[0] == '\0'))
			part_ptr->alternate = NULL;
		else
			part_ptr->alternate = xstrdup(part_desc->alternate);
		part_desc->alternate = NULL;
		info("%s: setting alternate to %s for partition %s",
		     __func__, part_ptr->alternate, part_desc->name);
	}

	if (part_desc->def_mem_per_cpu != NO_VAL64) {
		char *key;
		uint32_t value;
		if (part_desc->def_mem_per_cpu & MEM_PER_CPU) {
			key = "DefMemPerCpu";
			value = part_desc->def_mem_per_cpu & (~MEM_PER_CPU);
		} else {
			key = "DefMemPerNode";
			value = part_desc->def_mem_per_cpu;
		}
		info("%s: setting %s to %u for partition %s", __func__,
		     key, value, part_desc->name);
		part_ptr->def_mem_per_cpu = part_desc->def_mem_per_cpu;
	}

	if (part_desc->deny_accounts != NULL) {
		xfree(part_ptr->deny_accounts);
		if (part_desc->deny_accounts[0] == '\0')
			xfree(part_desc->deny_accounts);
		part_ptr->deny_accounts = part_desc->deny_accounts;
		part_desc->deny_accounts = NULL;
		info("%s: setting DenyAccounts to %s for partition %s",
		     __func__, part_ptr->deny_accounts, part_desc->name);
		accounts_list_build(part_ptr->deny_accounts,
				    &part_ptr->deny_account_array);
	}
	if (part_desc->allow_accounts && part_desc->deny_accounts) {
		error("%s: Both AllowAccounts and DenyAccounts are defined, DenyAccounts will be ignored",
		      __func__);
	}

	if (part_desc->deny_qos != NULL) {
		xfree(part_ptr->deny_qos);
		if (part_desc->deny_qos[0] == '\0')
			xfree(part_ptr->deny_qos);
		part_ptr->deny_qos = part_desc->deny_qos;
		part_desc->deny_qos = NULL;
		info("%s: setting DenyQOS to %s for partition %s", __func__,
		     part_ptr->deny_qos, part_desc->name);
		qos_list_build(part_ptr->deny_qos, &part_ptr->deny_qos_bitstr);
	}
	if (part_desc->allow_qos && part_desc->deny_qos) {
		error("%s: Both AllowQOS and DenyQOS are defined, DenyQOS will be ignored",
		      __func__);
	}

	if (part_desc->max_mem_per_cpu != NO_VAL64) {
		char *key;
		uint32_t value;
		if (part_desc->max_mem_per_cpu & MEM_PER_CPU) {
			key = "MaxMemPerCpu";
			value = part_desc->max_mem_per_cpu & (~MEM_PER_CPU);
		} else {
			key = "MaxMemPerNode";
			value = part_desc->max_mem_per_cpu;
		}
		info("%s: setting %s to %u for partition %s", __func__,
		     key, value, part_desc->name);
		part_ptr->max_mem_per_cpu = part_desc->max_mem_per_cpu;
	}

	if (part_desc->job_defaults_str) {
		List new_job_def_list = NULL;
		if (part_desc->job_defaults_str[0] == '\0') {
			FREE_NULL_LIST(part_ptr->job_defaults_list);
		} else if (job_defaults_list(part_desc->job_defaults_str,
					     &new_job_def_list)
					!= SLURM_SUCCESS) {
			error("%s: Invalid JobDefaults(%s) given",
			      __func__, part_desc->job_defaults_str);
			error_code = ESLURM_INVALID_JOB_DEFAULTS;
		} else {	/* New list successfully built */
			FREE_NULL_LIST(part_ptr->job_defaults_list);
			part_ptr->job_defaults_list = new_job_def_list;
			info("%s: Setting JobDefaults to %s for partition %s",
			      __func__, part_desc->job_defaults_str,
			      part_desc->name);
		}
	}

	if (part_desc->nodes != NULL) {
		assoc_mgr_lock_t assoc_tres_read_lock = { .tres = READ_LOCK };
		char *backup_node_list = part_ptr->nodes;

		if (part_desc->nodes[0] == '\0')
			part_ptr->nodes = NULL;	/* avoid empty string */
		else {
			int i;		
			part_ptr->nodes = xstrdup(part_desc->nodes);
			for (i = 0; part_ptr->nodes[i]; i++) {
				if (isspace(part_ptr->nodes[i]))
					part_ptr->nodes[i] = ',';
			}
		}
		xfree(part_ptr->orig_nodes);
		part_ptr->orig_nodes = xstrdup(part_ptr->nodes);
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
		error_code = build_part_bitmap(part_ptr, true);
#else
		error_code = build_part_bitmap(part_ptr);
#endif		
		if (error_code) {
			xfree(part_ptr->nodes);
			part_ptr->nodes = backup_node_list;
		} else {
			info("%s: setting nodes to %s for partition %s",
			     __func__, part_ptr->nodes, part_desc->name);
			xfree(backup_node_list);
#ifdef __METASTACK_NEW_PART_PARA_SCHED
			update_part_node = true;
#endif				
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
			/* update borrowed node's orig partitions */
			_build_borrow_nodes_partitions(part_ptr);
#endif
		}
		update_part_nodes_in_resv(part_ptr);
		power_save_set_timeouts(NULL);

		assoc_mgr_lock(&assoc_tres_read_lock);
		_calc_part_tres(part_ptr, NULL);
		assoc_mgr_unlock(&assoc_tres_read_lock);
	} else if (part_ptr->node_bitmap == NULL) {
		/* Newly created partition needs a bitmap, even if empty */
		part_ptr->node_bitmap = bit_alloc(node_record_count);
	}

#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
	bool nodes_enable = true, parameters_enable = true, standby_changed = false;
	char *backup_standby_nodes = NULL, *backup_standby_parameters = NULL;

	if (part_desc->standby_nodes) {
		if (!(part_ptr->standby_nodes)) {
			part_ptr->standby_nodes = xcalloc(1, sizeof(standby_nodes_t));
		}
		backup_standby_nodes = part_ptr->standby_nodes->nodes;
		if (part_desc->standby_nodes[0] == '\0') {
			part_ptr->standby_nodes->nodes = NULL;	/* avoid empty string */
		} else {
			part_ptr->standby_nodes->nodes = xstrdup(part_desc->standby_nodes);
		}

		if (!build_part_standby_nodes_bitmap(part_ptr)) {
			xfree(backup_standby_nodes);
			standby_changed = true;
			info("%s: setting standby_nodes to %s for partition %s",
				     __func__, part_ptr->standby_nodes->nodes, part_desc->name);
		} else {
			nodes_enable = false;
			xfree(part_ptr->standby_nodes->nodes);
			part_ptr->standby_nodes->nodes = backup_standby_nodes;
			backup_standby_nodes = NULL;
			error_code = ESLURM_INVALID_NODE_NAME;
		}		 
	}

	if (nodes_enable && (part_desc->standby_node_parameters)) {
		if (!(part_ptr->standby_nodes)) {
			part_ptr->standby_nodes = xcalloc(1, sizeof(standby_nodes_t));
		}		
		backup_standby_parameters = part_ptr->standby_nodes->parameters;
		if (part_desc->standby_node_parameters[0] == '\0') {
			part_ptr->standby_nodes->parameters = NULL;
		} else {
			part_ptr->standby_nodes->parameters = xstrdup(part_desc->standby_node_parameters);
		}
			
		if (!valid_standby_node_parameters(part_ptr)) {
			xfree(backup_standby_parameters);
			standby_changed = true;
			info("%s: setting standby_node_parameters to %s for partition %s",
					__func__, part_ptr->standby_nodes->parameters, part_desc->name);
		} else {
			parameters_enable = false;
			xfree(part_ptr->standby_nodes->parameters);
			part_ptr->standby_nodes->parameters = backup_standby_parameters;
			backup_standby_parameters = NULL;
			error_code = ESLURM_INVALID_STANDBY_NODE_PARAMETERS;
		}
	}

	if (nodes_enable && parameters_enable && standby_changed) {
		if ((part_ptr->standby_nodes->nodes_can_borrow > 0) &&
				(part_ptr->standby_nodes->offline_node_state > 0) &&
					part_ptr->standby_nodes->standby_node_bitmap && 
					(bit_set_count(part_ptr->standby_nodes->standby_node_bitmap) > 0)) {
			part_ptr->standby_nodes->enable = true;
			debug("partition %s standby_nodes enable", part_ptr->name);
		} else {
			part_ptr->standby_nodes->enable = false;
			debug("partition %s standby_nodes disable", part_ptr->name);
		}
		/* validate partition borrow nodes */
		if (validate_partition_borrow_nodes(part_ptr)) {
			update_all_parts_resource(true);
#ifdef __METASTACK_NEW_PART_PARA_SCHED
			update_part_node = false;
#endif
		}
	} 
#endif

#ifdef __METASTACK_OPT_CACHE_QUERY
	if (create_flag && part_cachedup_realtime == 1) {
		_add_cache_part(part_ptr);
        xfree(default_cache_part_name);
        if(default_part_name)
            default_cache_part_name = xstrdup(default_part_name);
	}else if(create_flag && part_cachedup_realtime == 2 && cache_queue){
		slurm_cache_date_t *cache_msg = NULL;
		cache_msg = xmalloc(sizeof(slurm_cache_date_t));
		cache_msg->msg_type = CREATE_CACHE_PART_RECORD;
		cache_msg->part_ptr = _add_part_to_queue(part_ptr);
        cache_msg->default_part_name = xstrdup(default_part_name);
		cache_enqueue(cache_msg);
	}else if(!create_flag){
       _add_part_state_to_queue(part_ptr);
	}
#endif
	if (error_code == SLURM_SUCCESS) {
		gs_reconfig();
		select_g_reconfigure();		/* notify select plugin too */
#ifdef __METASTACK_NEW_PART_PARA_SCHED
		/* The nodes in the partition have changed or a partition has been created */
		if (update_part_node || create_flag)
			build_sched_resource();
#endif
	}

	return error_code;
}


/*
 * validate_group - validate that the uid is authorized to access the partition
 * IN part_ptr - pointer to a partition
 * IN run_uid - user to run the job as
 * RET 1 if permitted to run, 0 otherwise
 */
extern int validate_group(part_record_t *part_ptr, uid_t run_uid)
{
	static uid_t last_fail_uid = 0;
	static part_record_t *last_fail_part_ptr = NULL;
	static time_t last_fail_time = 0;
	time_t now;
#if defined(_SC_GETPW_R_SIZE_MAX)
	long ii;
#endif
	int i = 0, res, uid_array_len;
	size_t buflen;
	struct passwd pwd, *pwd_result;
	char *buf;
	char *grp_buffer;
	struct group grp, *grp_result;
	char *groups, *saveptr = NULL, *one_group_name;
	int ret = 0;

	if (part_ptr->allow_groups == NULL)
		return 1;	/* all users allowed */
	if (validate_slurm_user(run_uid))
		return 1;	/* super-user can run anywhere */
	if (part_ptr->allow_uids == NULL)
		return 0;	/* no non-super-users in the list */

	for (i = 0; part_ptr->allow_uids[i]; i++) {
		if (part_ptr->allow_uids[i] == run_uid)
			return 1;
	}
	uid_array_len = i;

	/* If this user has failed AllowGroups permission check on this
	 * partition in past 5 seconds, then do not test again for performance
	 * reasons. */
	now = time(NULL);
	if ((run_uid == last_fail_uid) &&
	    (part_ptr == last_fail_part_ptr) &&
	    (difftime(now, last_fail_time) < 5)) {
		return 0;
	}

	/* The allow_uids list is built from the allow_groups list,
	 * and if user/group enumeration has been disabled, it's
	 * possible that the users primary group is not returned as a
	 * member of a group.  Enumeration is problematic if the
	 * user/group database is large (think university-wide central
	 * account database or such), as in such environments
	 * enumeration would load the directory servers a lot, so the
	 * recommendation is to have it disabled (e.g. enumerate=False
	 * in sssd.conf).  So check explicitly whether the primary
	 * group is allowed as a final resort.  This should
	 * (hopefully) not happen that often, and anyway the
	 * getpwuid_r and getgrgid_r calls should be cached by
	 * sssd/nscd/etc. so should be fast.  */

	/* First figure out the primary GID.  */
	buflen = PW_BUF_SIZE;
#if defined(_SC_GETPW_R_SIZE_MAX)
	ii = sysconf(_SC_GETPW_R_SIZE_MAX);
	if ((ii >= 0) && (ii > buflen))
		buflen = ii;
#endif
	buf = xmalloc(buflen);
	while (1) {
		slurm_seterrno(0);
		res = getpwuid_r(run_uid, &pwd, buf, buflen, &pwd_result);
		/* We need to check for !pwd_result, since it appears some
		 * versions of this function do not return an error on
		 * failure.
		 */
		if (res != 0 || !pwd_result) {
			if (errno == ERANGE) {
				buflen *= 2;
				xrealloc(buf, buflen);
				continue;
			}
			error("%s: Could not find passwd entry for uid %ld",
			      __func__, (long) run_uid);
			xfree(buf);
			goto fini;
		}
		break;
	}

	/* Then use the primary GID to figure out the name of the
	 * group with that GID.  */
#ifdef _SC_GETGR_R_SIZE_MAX
	ii = sysconf(_SC_GETGR_R_SIZE_MAX);
	buflen = PW_BUF_SIZE;
	if ((ii >= 0) && (ii > buflen))
		buflen = ii;
#endif
	grp_buffer = xmalloc(buflen);
	while (1) {
		slurm_seterrno(0);
		res = getgrgid_r(pwd.pw_gid, &grp, grp_buffer, buflen,
				 &grp_result);

		/* We need to check for !grp_result, since it appears some
		 * versions of this function do not return an error on
		 * failure.
		 */
		if (res != 0 || !grp_result) {
			if (errno == ERANGE) {
				buflen *= 2;
				xrealloc(grp_buffer, buflen);
				continue;
			}
			error("%s: Could not find group with gid %ld",
			      __func__, (long) pwd.pw_gid);
			xfree(buf);
			xfree(grp_buffer);
			goto fini;
		}
		break;
	}

	/* And finally check the name of the primary group against the
	 * list of allowed group names.  */
	groups = xstrdup(part_ptr->allow_groups);
	one_group_name = strtok_r(groups, ",", &saveptr);
	while (one_group_name) {
		if (xstrcmp (one_group_name, grp.gr_name) == 0) {
			ret = 1;
			break;
		}
		one_group_name = strtok_r(NULL, ",", &saveptr);
	}
	xfree(groups);

	if (ret == 1) {
		debug("UID %ld added to AllowGroup %s of partition %s",
		      (long) run_uid, grp.gr_name, part_ptr->name);
		part_ptr->allow_uids =
			xrealloc(part_ptr->allow_uids,
				 (sizeof(uid_t) * (uid_array_len + 2)));
		part_ptr->allow_uids[uid_array_len] = run_uid;
	}

    xfree(buf);
	xfree(grp_buffer);

fini:	if (ret == 0) {
		last_fail_uid = run_uid;
		last_fail_part_ptr = part_ptr;
		last_fail_time = now;
	}
	return ret;
}

/*
 * validate_alloc_node - validate that the allocating node
 * is allowed to use this partition
 * IN part_ptr - pointer to a partition
 * IN alloc_node - allocting node of the request
 * RET 1 if permitted to run, 0 otherwise
 */
extern int validate_alloc_node(part_record_t *part_ptr, char *alloc_node)
{
	int status;

 	if (part_ptr->allow_alloc_nodes == NULL)
 		return 1;	/* all allocating nodes allowed */
 	if (alloc_node == NULL)
		return 0;	/* if no allocating node deny */

 	hostlist_t hl = hostlist_create(part_ptr->allow_alloc_nodes);
 	status=hostlist_find(hl,alloc_node);
 	hostlist_destroy(hl);

 	if (status == -1)
		status = 0;
 	else
		status = 1;

 	return status;
}

static int _update_part_uid_access_list(void *x, void *arg)
{
	part_record_t *part_ptr = (part_record_t *)x;
	int *updated = (int *)arg;
	int i = 0;
	uid_t *tmp_uids;

	tmp_uids = part_ptr->allow_uids;
	part_ptr->allow_uids = get_groups_members(part_ptr->allow_groups);

	if ((!part_ptr->allow_uids) && (!tmp_uids)) {
		/* no changes, and no arrays to compare */
	} else if ((!part_ptr->allow_uids) || (!tmp_uids)) {
		/* one is set when it wasn't before */
		*updated = 1;
	} else {
		/* step through arrays and compare item by item */
		/* uid_t arrays are terminated with a zero */
		for (i = 0; part_ptr->allow_uids[i]; i++) {
			if (tmp_uids[i] != part_ptr->allow_uids[i]) {
				*updated = 1;
				break;
			}
		}
	}

	xfree(tmp_uids);
	return 0;
}

/*
 * load_part_uid_allow_list - reload the allow_uid list of partitions
 *	if required (updated group file or force set)
 * IN force - if set then always reload the allow_uid list
 */
void load_part_uid_allow_list(int force)
{
	static time_t last_update_time;
	int updated = 0;
	time_t temp_time;
	DEF_TIMERS;

	START_TIMER;
	temp_time = get_group_tlm();
	if ((force == 0) && (temp_time == last_update_time))
		return;
	debug("Updating partition uid access list");
	last_update_time = temp_time;

	list_for_each(part_list, _update_part_uid_access_list, &updated);

	/* only update last_part_update when changes made to avoid restarting
	 * backfill scheduler unnecessarily */
	if (updated) {
		debug2("%s: list updated, resetting last_part_update time",
		       __func__);
		last_part_update = time(NULL);
	}

	clear_group_cache();
	END_TIMER2("load_part_uid_allow_list");
}

/* part_fini - free all memory associated with partition records */
void part_fini (void)
{
	FREE_NULL_LIST(part_list);
	default_part_loc = NULL;
}

/*
 * delete_partition - delete the specified partition
 * IN job_specs - job specification from RPC
 */
extern int delete_partition(delete_part_msg_t *part_desc_ptr)
{
	part_record_t *part_ptr;

	part_ptr = find_part_record (part_desc_ptr->name);
	if (part_ptr == NULL)	/* No such partition */
		return ESLURM_INVALID_PARTITION_NAME;

	if (partition_in_use(part_desc_ptr->name))
		return ESLURM_PARTITION_IN_USE;

	if (default_part_loc == part_ptr) {
		error("Deleting default partition %s", part_ptr->name);
		default_part_loc = NULL;
	}
	(void) kill_job_by_part_name(part_desc_ptr->name);
	list_delete_all(part_list, list_find_part, part_desc_ptr->name);
	last_part_update = time(NULL);

#ifdef __METASTACK_OPT_CACHE_QUERY
	if(part_cachedup_realtime == 1){
		_del_cache_part(part_desc_ptr->name);
        xfree(default_cache_part_name);
        if(default_part_name)
            default_cache_part_name = xstrdup(default_part_name);
	}else if(part_cachedup_realtime == 2 && cache_queue){
		slurm_cache_date_t *cache_msg = NULL;
		cache_msg = xmalloc(sizeof(slurm_cache_date_t));
		cache_msg->msg_type = DELETE_CACHE_PART_RECORD;
		cache_msg->part_name = xstrdup(part_desc_ptr->name);
        cache_msg->default_part_name = xstrdup(default_part_name);
		cache_enqueue(cache_msg);
	}
#endif

	gs_reconfig();
	select_g_reconfigure();		/* notify select plugin too */
#ifdef __METASTACK_NEW_PART_PARA_SCHED
    build_sched_resource();
#endif

	return SLURM_SUCCESS;
}

/*
 * Determine of the specified job can execute right now or is currently
 * blocked by a miscellaneous limit. This does not re-validate job state,
 * but relies upon schedule() in src/slurmctld/job_scheduler.c to do so.
 */
extern bool misc_policy_job_runnable_state(job_record_t *job_ptr)
{
	if ((job_ptr->state_reason == FAIL_ACCOUNT) ||
	    (job_ptr->state_reason == FAIL_QOS) ||
	    (job_ptr->state_reason == WAIT_NODE_NOT_AVAIL)) {
		return false;
	}

	return true;
}

/*
 * Determine of the specified job can execute right now or is currently
 * blocked by a partition state or limit. These job states should match the
 * reason values returned by job_limits_check().
 */
extern bool part_policy_job_runnable_state(job_record_t *job_ptr)
{
	if ((job_ptr->state_reason == WAIT_PART_DOWN) ||
	    (job_ptr->state_reason == WAIT_PART_INACTIVE) ||
	    (job_ptr->state_reason == WAIT_PART_NODE_LIMIT) ||
	    (job_ptr->state_reason == WAIT_PART_TIME_LIMIT) ||
	    (job_ptr->state_reason == WAIT_QOS_THRES)) {
		return false;
	}

	return true;
}

/*
 * Validate a job's account against the partition's AllowAccounts or
 *	DenyAccounts parameters.
 * IN part_ptr - Partition pointer
 * IN acct - account name
 * in job_ptr - Job pointer or NULL. If set and job can not run, then set the
 *		job's state_desc and state_reason fields
 * RET SLURM_SUCCESS or error code
 */
extern int part_policy_valid_acct(part_record_t *part_ptr, char *acct,
				  job_record_t *job_ptr)
{
	char *tmp_err = NULL;
	int i;

	if (part_ptr->allow_account_array && part_ptr->allow_account_array[0]) {
		int match = 0;
		if (!acct) {
			xstrfmtcat(tmp_err,
				   "Job's account not known, so it can't use this partition "
				   "(%s allows %s)",
				   part_ptr->name, part_ptr->allow_accounts);
			info("%s: %s", __func__, tmp_err);
			if (job_ptr) {
				xfree(job_ptr->state_desc);
				job_ptr->state_desc = tmp_err;
				job_ptr->state_reason = WAIT_ACCOUNT;
				last_job_update = time(NULL);
			} else {
				xfree(tmp_err);
			}
			return ESLURM_INVALID_ACCOUNT;
		}

		for (i = 0; part_ptr->allow_account_array[i]; i++) {
			if (xstrcmp(part_ptr->allow_account_array[i], acct))
				continue;
			match = 1;
			break;
		}
		if (match == 0) {
			xstrfmtcat(tmp_err,
				   "Job's account not permitted to use this partition "
				   "(%s allows %s not %s)",
				   part_ptr->name, part_ptr->allow_accounts,
				   acct);
			info("%s: %s", __func__, tmp_err);
			if (job_ptr) {
				xfree(job_ptr->state_desc);
				job_ptr->state_desc = tmp_err;
				job_ptr->state_reason = WAIT_ACCOUNT;
				last_job_update = time(NULL);
			} else {
				xfree(tmp_err);
			}
			return ESLURM_INVALID_ACCOUNT;
		}
	} else if (part_ptr->deny_account_array &&
		   part_ptr->deny_account_array[0]) {
		int match = 0;
		if (!acct) {
			debug2("%s: job's account not known, so couldn't check if it was denied or not",
			       __func__);
			return SLURM_SUCCESS;
		}
		for (i = 0; part_ptr->deny_account_array[i]; i++) {
			if (xstrcmp(part_ptr->deny_account_array[i], acct))
				continue;
			match = 1;
			break;
		}
		if (match == 1) {
			xstrfmtcat(tmp_err,
				   "Job's account not permitted to use this partition "
				   "(%s denies %s including %s)",
				   part_ptr->name, part_ptr->deny_accounts,
				   acct);
			info("%s: %s", __func__, tmp_err);
			if (job_ptr) {
				xfree(job_ptr->state_desc);
				job_ptr->state_desc = tmp_err;
				job_ptr->state_reason = WAIT_ACCOUNT;
				last_job_update = time(NULL);
			} else {
				xfree(tmp_err);
			}
			return ESLURM_INVALID_ACCOUNT;
		}
	}

	return SLURM_SUCCESS;
}

/*
 * Validate a job's QOS against the partition's AllowQOS or DenyQOS parameters.
 * IN part_ptr - Partition pointer
 * IN qos_ptr - QOS pointer
 * in job_ptr - Job pointer or NULL. If set and job can not run, then set the
 *		job's state_desc and state_reason fields
 * RET SLURM_SUCCESS or error code
 */
extern int part_policy_valid_qos(part_record_t *part_ptr,
				 slurmdb_qos_rec_t *qos_ptr,
				 job_record_t *job_ptr)
{
	char *tmp_err = NULL;

	if (part_ptr->allow_qos_bitstr) {
		int match = 0;
		if (!qos_ptr) {
			xstrfmtcat(tmp_err,
				   "Job's QOS not known, so it can't use this partition (%s allows %s)",
				   part_ptr->name, part_ptr->allow_qos);
			info("%s: %s", __func__, tmp_err);
			if (job_ptr) {
				xfree(job_ptr->state_desc);
				job_ptr->state_desc = tmp_err;
				job_ptr->state_reason = WAIT_QOS;
				last_job_update = time(NULL);
			} else {
				xfree(tmp_err);
			}
			return ESLURM_INVALID_QOS;
		}
		if ((qos_ptr->id < bit_size(part_ptr->allow_qos_bitstr)) &&
		    bit_test(part_ptr->allow_qos_bitstr, qos_ptr->id))
			match = 1;
		if (match == 0) {
			xstrfmtcat(tmp_err,
				   "Job's QOS not permitted to use this partition (%s allows %s not %s)",
				   part_ptr->name, part_ptr->allow_qos,
				   qos_ptr->name);
			info("%s: %s", __func__, tmp_err);
			if (job_ptr) {
				xfree(job_ptr->state_desc);
				job_ptr->state_desc = tmp_err;
				job_ptr->state_reason = WAIT_QOS;
				last_job_update = time(NULL);
			} else {
				xfree(tmp_err);
			}
			return ESLURM_INVALID_QOS;
		}
	} else if (part_ptr->deny_qos_bitstr) {
		int match = 0;
		if (!qos_ptr) {
			debug2("%s: Job's QOS not known, so couldn't check if it was denied or not",
			       __func__);
			return SLURM_SUCCESS;
		}
		if ((qos_ptr->id < bit_size(part_ptr->deny_qos_bitstr)) &&
		    bit_test(part_ptr->deny_qos_bitstr, qos_ptr->id))
			match = 1;
		if (match == 1) {
			xstrfmtcat(tmp_err,
				   "Job's QOS not permitted to use this partition (%s denies %s including %s)",
				   part_ptr->name, part_ptr->deny_qos,
				   qos_ptr->name);
			info("%s: %s", __func__, tmp_err);
			if (job_ptr) {
				xfree(job_ptr->state_desc);
				job_ptr->state_desc = tmp_err;
				job_ptr->state_reason = WAIT_QOS;
				last_job_update = time(NULL);
			} else {
				xfree(tmp_err);
			}
			return ESLURM_INVALID_QOS;
		}
	}

	return SLURM_SUCCESS;
}

#ifdef __METASTACK_OPT_CACHE_QUERY

/*
 * get_part_list - find record for named partition(s)
 * IN name - partition name(s) in a comma separated list
 * OUT err_part - The first invalid partition name.
 * RET List of pointers to the partitions or NULL if not found
 * NOTE: Caller must free the returned list
 * NOTE: Caller must free err_part
 */
extern List get_cache_part_list(char *name)
{
	part_record_t *part_ptr = NULL;
	List job_part_list = NULL;
	char *token, *last = NULL, *tmp_name;

	if (name == NULL)
		return job_part_list;

	tmp_name = xstrdup(name);
	token = strtok_r(tmp_name, ",", &last);
	while (token) {
		part_ptr = list_find_first(cache_part_list, &list_find_part, token);
		if (part_ptr) {
			if (job_part_list == NULL) {
				job_part_list = list_create(NULL);
			}
			if (!list_find_first(job_part_list, &_match_part_ptr,
					     part_ptr)) {
				list_append(job_part_list, part_ptr);
			}
		} else {
			FREE_NULL_LIST(job_part_list);
			break;
		}
		token = strtok_r(NULL, ",", &last);
	}
	xfree(tmp_name);
	return job_part_list;
}


/*
 * find_copy_part_record - find a record for partition with specified name
 * IN name - name of the desired partition
 * RET pointer to partition or NULL if not found
 */
extern part_record_t *find_copy_part_record(char *name, List part_list)
{
	if (!part_list) {
		error("part_list is NULL");
		return NULL;
	}
	return list_find_first(part_list, &list_find_part, name);
}


static int copy_job_defaults_list(List src_send_list,   List des_send_list)
{
	uint32_t count = 0;
	int rc = SLURM_SUCCESS;
	job_defaults_t *src_object = NULL, *des_object = NULL;

	count = list_count(src_send_list);

	if (count) {
			ListIterator itr = list_iterator_create(src_send_list);
			while ((src_object = list_next(itr))) {
					des_object = xmalloc(sizeof(job_defaults_t));
					des_object->type  = src_object->type;
					des_object->value = src_object->value;
					list_append(des_send_list, des_object);
			}
			list_iterator_destroy(itr);
	}
	return rc;
}

static void _copy_list_delete_part(void *part_entry)
{
	part_record_t *part_ptr = NULL;
	node_record_t *node_ptr = NULL;
	int i, j, k;

	part_ptr = (part_record_t *) part_entry;
	for (i = 0; (node_ptr = next_cache_node(&i, cache_node_record_count, cache_node_record_table_ptr)); i++) {
		for (j=0; j<node_ptr->part_cnt; j++) {
			if (node_ptr->part_pptr[j] != part_ptr)
				continue;
			node_ptr->part_cnt--;
			for (k=j; k<node_ptr->part_cnt; k++) {
				node_ptr->part_pptr[k] =
					node_ptr->part_pptr[k+1];
			}
			break;
		}
	}

	xfree(part_ptr->allow_accounts);
	xfree(part_ptr->allow_alloc_nodes);
	xfree(part_ptr->allow_groups);
	xfree(part_ptr->allow_qos);
	xfree(part_ptr->allow_uids);
	FREE_NULL_BITMAP(part_ptr->allow_qos_bitstr);
	FREE_NULL_BITMAP(part_ptr->deny_qos_bitstr);
	accounts_list_free(&part_ptr->allow_account_array);
	xfree(part_ptr->alternate);
	xfree(part_ptr->orig_nodes);
	xfree(part_ptr->billing_weights_str);
	xfree(part_ptr->deny_accounts);
	accounts_list_free(&part_ptr->deny_account_array);
	xfree(part_ptr->deny_qos);
	FREE_NULL_LIST(part_ptr->job_defaults_list);
	xfree(part_ptr->name);
	xfree(part_ptr->nodes);
	xfree(part_ptr->nodesets);
	FREE_NULL_BITMAP(part_ptr->node_bitmap);
	xfree(part_ptr->qos_char);
	xfree(part_ptr->tres_fmt_str);
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES	
	standby_nodes_free(part_ptr->standby_nodes);
	part_ptr->standby_nodes = NULL;
#endif
	xfree(part_ptr);
}

extern void _list_delete_cache_part(part_record_t *part_ptr)
{
	if(!part_ptr)
		return;
	
	xfree(part_ptr->allow_accounts);
	xfree(part_ptr->allow_alloc_nodes);
	xfree(part_ptr->allow_groups);
	xfree(part_ptr->allow_qos);
	xfree(part_ptr->allow_uids);
	FREE_NULL_BITMAP(part_ptr->allow_qos_bitstr);
	FREE_NULL_BITMAP(part_ptr->deny_qos_bitstr);
	accounts_list_free(&part_ptr->allow_account_array);
	xfree(part_ptr->alternate);
	xfree(part_ptr->orig_nodes);
	xfree(part_ptr->billing_weights_str);
	xfree(part_ptr->deny_accounts);
	accounts_list_free(&part_ptr->deny_account_array);
	xfree(part_ptr->deny_qos);
	FREE_NULL_LIST(part_ptr->job_defaults_list);
	xfree(part_ptr->name);
	xfree(part_ptr->nodes);
	xfree(part_ptr->nodesets);
	FREE_NULL_BITMAP(part_ptr->node_bitmap);
	xfree(part_ptr->qos_char);
	xfree(part_ptr->tres_fmt_str);
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES	
	standby_nodes_free(part_ptr->standby_nodes);
	part_ptr->standby_nodes = NULL;
#endif
	xfree(part_ptr);
}


static int copy_part(part_record_t *src_part_ptr, part_record_t *des_part_ptr)
{
	if(!src_part_ptr || !des_part_ptr)
		return 0;
	memcpy(des_part_ptr, src_part_ptr, sizeof(part_record_t));

	des_part_ptr->name = xstrdup(src_part_ptr->name);
	des_part_ptr->allow_accounts = xstrdup(src_part_ptr->allow_accounts);
	des_part_ptr->allow_groups = xstrdup(src_part_ptr->allow_groups);
	des_part_ptr->allow_alloc_nodes = xstrdup(src_part_ptr->allow_alloc_nodes);
	des_part_ptr->allow_qos = xstrdup(src_part_ptr->allow_qos);
	des_part_ptr->qos_char = xstrdup(src_part_ptr->qos_char);
	des_part_ptr->alternate = xstrdup(src_part_ptr->alternate);
	des_part_ptr->deny_accounts = xstrdup(src_part_ptr->deny_accounts);
	des_part_ptr->deny_qos = xstrdup(src_part_ptr->deny_qos);
	des_part_ptr->nodes = xstrdup(src_part_ptr->nodes);
	des_part_ptr->nodesets = xstrdup(src_part_ptr->nodesets);
	des_part_ptr->orig_nodes = xstrdup(src_part_ptr->orig_nodes);
	if(src_part_ptr->node_bitmap)
		des_part_ptr->node_bitmap = bit_copy(src_part_ptr->node_bitmap);
	if(src_part_ptr->allow_qos_bitstr)
		des_part_ptr->allow_qos_bitstr = bit_copy(src_part_ptr->allow_qos_bitstr);
	if(src_part_ptr->deny_qos_bitstr)
		des_part_ptr->deny_qos_bitstr = bit_copy(src_part_ptr->deny_qos_bitstr);
	des_part_ptr->billing_weights_str = xstrdup(src_part_ptr->billing_weights_str);
	des_part_ptr->tres_fmt_str = xstrdup(src_part_ptr->tres_fmt_str);
    src_part_ptr->job_defaults_list = NULL;
	if(src_part_ptr->job_defaults_list){
		des_part_ptr->job_defaults_list = list_create(xfree_ptr);
		(void)copy_job_defaults_list(src_part_ptr->job_defaults_list, des_part_ptr->job_defaults_list);
	}
	des_part_ptr->allow_account_array = NULL;
	des_part_ptr->deny_account_array = NULL;
	accounts_list_build(des_part_ptr->allow_accounts, &des_part_ptr->allow_account_array);
	accounts_list_build(des_part_ptr->deny_accounts,  &des_part_ptr->deny_account_array);
	des_part_ptr->allow_uids = get_groups_members(des_part_ptr->allow_groups);

#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
    des_part_ptr->standby_nodes = NULL;
	if(src_part_ptr->standby_nodes){
		des_part_ptr->standby_nodes = xcalloc(1, sizeof(standby_nodes_t));
		des_part_ptr->standby_nodes->borrowed_nodes = xstrdup(src_part_ptr->standby_nodes->borrowed_nodes);
		des_part_ptr->standby_nodes->nodes = xstrdup(src_part_ptr->standby_nodes->nodes);
		des_part_ptr->standby_nodes->parameters = xstrdup(src_part_ptr->standby_nodes->parameters);
		des_part_ptr->standby_nodes->standby_node_bitmap = NULL;
		des_part_ptr->standby_nodes->borrowed_node_bitmap = NULL;
	}
#endif
	des_part_ptr->billing_weights = NULL;
	des_part_ptr->qos_ptr = NULL;
	des_part_ptr->tres_cnt = NULL;
	des_part_ptr->bf_data = NULL;
	return 0;
}

/*del_cache_part_state_record :Clear the memory of partition status 
 *update messages in the message queue.*/
extern void del_cache_part_state_record(part_state_record_t *src_part_ptr)
{
	xfree(src_part_ptr->name);
	xfree(src_part_ptr->nodes);
	xfree(src_part_ptr->nodesets);
	xfree(src_part_ptr->orig_nodes);
	xfree(src_part_ptr->tres_fmt_str);
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
	xfree(src_part_ptr->st_borrowed_nodes);
	xfree(src_part_ptr->st_nodes);
	xfree(src_part_ptr->st_parameters);
#endif
    xfree(src_part_ptr->deny_accounts);
    xfree(src_part_ptr->allow_groups);
    xfree(src_part_ptr->qos_char);
	FREE_NULL_BITMAP(src_part_ptr->node_bitmap);
	xfree(src_part_ptr->allow_accounts);
	xfree(src_part_ptr);
}
/*_add_part_state_to_queue: Copy the partition status update 
 *information and place it in the queue.*/
extern void _add_part_state_to_queue(part_record_t *part_ptr)
{
	if(!part_ptr)
		return;
	if(cachedup_realtime && cache_queue){
		debug4("%s CACHE_QUERY  add part state to queue %s ", __func__, part_ptr->name);
		slurm_cache_date_t *cache_msg = NULL;
		cache_msg = xmalloc(sizeof(slurm_cache_date_t));
		cache_msg->msg_type = UPDATE_CACHE_PART_RECORD;
        cache_msg->default_part_name = xstrdup(default_part_name);
		cache_msg->part_state_ptr = xmalloc(sizeof(part_state_record_t));
		cache_msg->part_state_ptr->name = xstrdup(part_ptr->name);
		cache_msg->part_state_ptr->state_up = part_ptr->state_up;
		cache_msg->part_state_ptr->flags = part_ptr->flags;
		cache_msg->part_state_ptr->total_nodes = part_ptr->total_nodes;
		cache_msg->part_state_ptr->total_cpus = part_ptr->total_cpus;
		cache_msg->part_state_ptr->max_cpu_cnt = part_ptr->max_cpu_cnt;
		cache_msg->part_state_ptr->max_core_cnt = part_ptr->max_core_cnt;
#if (defined __METASTACK_NEW_HETPART_SUPPORT) || (defined __METASTACK_NEW_PART_RBN)
		cache_msg->part_state_ptr->meta_flags = part_ptr->meta_flags;
#endif
		cache_msg->part_state_ptr->priority_tier = part_ptr->priority_tier;
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
		cache_msg->part_state_ptr->standby_nodes_ptr = false;
		if(part_ptr->standby_nodes){
			cache_msg->part_state_ptr->standby_nodes_ptr = true;
			cache_msg->part_state_ptr->st_borrowed_nodes = xstrdup(part_ptr->standby_nodes->borrowed_nodes);
			cache_msg->part_state_ptr->st_nodes = xstrdup(part_ptr->standby_nodes->nodes); 
			cache_msg->part_state_ptr->st_parameters = xstrdup(part_ptr->standby_nodes->parameters);
		}
#endif
		cache_msg->part_state_ptr->tres_fmt_str = xstrdup(part_ptr->tres_fmt_str);
		cache_msg->part_state_ptr->allow_accounts = xstrdup(part_ptr->allow_accounts);
		cache_msg->part_state_ptr->nodes = xstrdup(part_ptr->nodes);
		cache_msg->part_state_ptr->nodesets = xstrdup(part_ptr->nodesets);
		cache_msg->part_state_ptr->orig_nodes = xstrdup(part_ptr->orig_nodes);
        cache_msg->part_state_ptr->deny_accounts = xstrdup(part_ptr->deny_accounts);
        cache_msg->part_state_ptr->allow_groups = xstrdup(part_ptr->allow_groups);
        cache_msg->part_state_ptr->qos_char = xstrdup(part_ptr->qos_char);
        cache_msg->part_state_ptr->default_time= part_ptr->default_time;
        cache_msg->part_state_ptr->max_time = part_ptr->max_time;
        cache_msg->part_state_ptr->node_bitmap = NULL;
		if(part_ptr->node_bitmap)
			cache_msg->part_state_ptr->node_bitmap = bit_copy(part_ptr->node_bitmap);
		cache_enqueue(cache_msg);
	}	
}

/*update_cache_part_record: Update the cached data with the partition
 *status update information from the queue.*/
extern int update_cache_part_record(part_state_record_t *src_part_ptr)
{
	part_record_t *des_part_ptr = NULL;
//	int i,j;
//	hostlist_t alias_list;
//	bitstr_t *src_node_bitmap = NULL;
//	bitstr_t *des_node_bitmap = NULL;
//	bitstr_t *and_node_bitmap = NULL;
//	node_record_t *part_node_ptr = NULL;
//	char *alias = NULL;
	des_part_ptr = find_copy_part_record(src_part_ptr->name, cache_part_list);
	if(des_part_ptr){
		debug4("%s CACHE_QUERY  update part to cache %s ", __func__, src_part_ptr->name);
		des_part_ptr->state_up = src_part_ptr->state_up;
		des_part_ptr->flags = src_part_ptr->flags;
		des_part_ptr->total_nodes = src_part_ptr->total_nodes;
		des_part_ptr->total_cpus = src_part_ptr->total_cpus;
		des_part_ptr->max_cpu_cnt = src_part_ptr->max_cpu_cnt;
		des_part_ptr->max_core_cnt = src_part_ptr->max_core_cnt;
        des_part_ptr->default_time = src_part_ptr->default_time;
        des_part_ptr->max_time = src_part_ptr->max_time;
#if (defined __METASTACK_NEW_HETPART_SUPPORT) || (defined __METASTACK_NEW_PART_RBN)
		des_part_ptr->meta_flags = src_part_ptr->meta_flags;
#endif
		des_part_ptr->priority_tier = src_part_ptr->priority_tier;
		xfree(des_part_ptr->allow_accounts);
		des_part_ptr->allow_accounts = src_part_ptr->allow_accounts;
		src_part_ptr->allow_accounts = NULL;
//		if(des_part_ptr->nodes)
//			node_name2bitmap(des_part_ptr->nodes, false, &des_node_bitmap);
//		if(src_part_ptr->nodes)
//			node_name2bitmap(src_part_ptr->nodes, false, &src_node_bitmap);
		
		if(des_part_ptr->node_bitmap){
			FREE_NULL_BITMAP(des_part_ptr->node_bitmap);
		}
        xfree(des_part_ptr->nodes);
		des_part_ptr->nodes = src_part_ptr->nodes;
		src_part_ptr->nodes = NULL;
        xfree(des_part_ptr->nodesets);
		des_part_ptr->nodesets = src_part_ptr->nodesets;
		src_part_ptr->nodesets = NULL;
        xfree(des_part_ptr->orig_nodes);
		des_part_ptr->orig_nodes = src_part_ptr->orig_nodes;
		src_part_ptr->orig_nodes = NULL;
        xfree(des_part_ptr->deny_accounts);
        des_part_ptr->deny_accounts = src_part_ptr->deny_accounts;
        src_part_ptr->deny_accounts = NULL;
        xfree(des_part_ptr->allow_groups);
        des_part_ptr->allow_groups = src_part_ptr->allow_groups;
        src_part_ptr->allow_groups = NULL;
        xfree(des_part_ptr->qos_char);
        des_part_ptr->qos_char = src_part_ptr->qos_char;
        src_part_ptr->qos_char = NULL;
		xfree(des_part_ptr->tres_fmt_str);
		des_part_ptr->tres_fmt_str = src_part_ptr->tres_fmt_str;
		src_part_ptr->tres_fmt_str = NULL;
#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
		if(des_part_ptr->standby_nodes && src_part_ptr->standby_nodes_ptr){
			xfree(des_part_ptr->standby_nodes->borrowed_nodes);
			xfree(des_part_ptr->standby_nodes->nodes);
			xfree(des_part_ptr->standby_nodes->parameters);
			des_part_ptr->standby_nodes->borrowed_nodes = src_part_ptr->st_borrowed_nodes;	
			des_part_ptr->standby_nodes->nodes = src_part_ptr->st_nodes; 
			des_part_ptr->standby_nodes->parameters = src_part_ptr->st_parameters;
			src_part_ptr->st_borrowed_nodes = NULL;
			src_part_ptr->st_nodes = NULL;
			src_part_ptr->st_parameters = NULL;
		}else if(des_part_ptr->standby_nodes){	
			standby_nodes_free(des_part_ptr->standby_nodes);
			des_part_ptr->standby_nodes = NULL;
		}else if(src_part_ptr->standby_nodes_ptr){
			des_part_ptr->standby_nodes = xcalloc(1, sizeof(standby_nodes_t));
			des_part_ptr->standby_nodes->borrowed_nodes = src_part_ptr->st_borrowed_nodes;	
			des_part_ptr->standby_nodes->nodes = src_part_ptr->st_nodes; 
			des_part_ptr->standby_nodes->parameters = src_part_ptr->st_parameters;
			src_part_ptr->st_borrowed_nodes = NULL;
			src_part_ptr->st_nodes = NULL;
			src_part_ptr->st_parameters = NULL;
		}
#endif
		if(src_part_ptr->node_bitmap){
			des_part_ptr->node_bitmap = src_part_ptr->node_bitmap;
			src_part_ptr->node_bitmap = NULL;
		}
/*
		if(des_node_bitmap && !src_node_bitmap){
			alias_list = bitmap2hostlist(des_node_bitmap);
			while ((alias = hostlist_shift(alias_list))){
				part_node_ptr = find_cache_node_record(alias);
				if(part_node_ptr){
					for (i=0; i<part_node_ptr->part_cnt; i++) {
						if (part_node_ptr->part_pptr[i] != des_part_ptr)
							continue;
						part_node_ptr->part_cnt--;
						for (j=i; j<part_node_ptr->part_cnt; j++) {
							part_node_ptr->part_pptr[j] =
								part_node_ptr->part_pptr[j+1];
						}
						break;
					}
				}
				free(alias);
			}
			if (alias_list)
				hostlist_destroy(alias_list);
			FREE_NULL_BITMAP(des_node_bitmap);
		}else if(!des_node_bitmap && src_node_bitmap){
			alias_list = bitmap2hostlist(src_node_bitmap);
			while ((alias = hostlist_shift(alias_list))){
				part_node_ptr = find_cache_node_record(alias);
				if(part_node_ptr){
					for (i = 0; i < part_node_ptr->part_cnt; i++) {
						if (part_node_ptr->part_pptr[i] == des_part_ptr)
							break;
					}
					if (i == part_node_ptr->part_cnt) {
						part_node_ptr->part_cnt++;
						xrecalloc(part_node_ptr->part_pptr, part_node_ptr->part_cnt,
							  sizeof(part_record_t *));
						part_node_ptr->part_pptr[part_node_ptr->part_cnt-1] = des_part_ptr;
					}
				}
				free(alias);
			}
			if (alias_list)
				hostlist_destroy(alias_list);
			FREE_NULL_BITMAP(src_node_bitmap);
		}else if(des_node_bitmap && src_node_bitmap){
			if(!bit_equal(des_node_bitmap, src_node_bitmap)){
				and_node_bitmap = bit_copy(des_node_bitmap);
				bit_and(and_node_bitmap, src_node_bitmap);
				bit_or(des_node_bitmap,src_node_bitmap);
				bit_and_not(des_node_bitmap, and_node_bitmap);
				alias_list = bitmap2hostlist(des_node_bitmap);
				while ((alias = hostlist_shift(alias_list))){
					part_node_ptr = find_cache_node_record(alias);
					if(part_node_ptr){
						for (i = 0; i < part_node_ptr->part_cnt; i++) {
							if (part_node_ptr->part_pptr[i] == des_part_ptr){
								part_node_ptr->part_cnt--;
								for (j=i; j<part_node_ptr->part_cnt; j++) {
									part_node_ptr->part_pptr[j] =
										part_node_ptr->part_pptr[j+1];
								}
								break;
							}
						}
						if (i == part_node_ptr->part_cnt) {
							part_node_ptr->part_cnt++;
							xrecalloc(part_node_ptr->part_pptr, part_node_ptr->part_cnt,
								  sizeof(part_record_t *));
							part_node_ptr->part_pptr[part_node_ptr->part_cnt-1] = des_part_ptr;
						}
					}
					free(alias);
				}
				if (alias_list)
					hostlist_destroy(alias_list);
				FREE_NULL_BITMAP(and_node_bitmap);
			}
			FREE_NULL_BITMAP(des_node_bitmap);
			FREE_NULL_BITMAP(src_node_bitmap);
		}
*/
	}
	del_cache_part_state_record(src_part_ptr);
	return 0;
}

/*_add_cache_part: Copy the partition source data directly into the cached data.*/
extern part_record_t *_add_cache_part(part_record_t *src_part_ptr)
{
//	int i;
//	node_record_t *part_node_ptr = NULL;
//	hostlist_t alias_list;
//	char *alias = NULL;
	part_record_t *des_part_ptr = NULL;
	debug2("%s CACHE_QUERY realtime add part %s ", __func__, src_part_ptr->name);
	if((des_part_ptr = find_copy_part_record(src_part_ptr->name, cache_part_list))){
		return des_part_ptr;
	}
	des_part_ptr = xmalloc(sizeof(part_record_t));
	copy_part(src_part_ptr,des_part_ptr);
/*
	alias_list = hostlist_create(des_part_ptr->nodes);
	while ((alias = hostlist_shift(alias_list))){
		part_node_ptr = find_cache_node_record(alias);
		for (i = 0; i < part_node_ptr->part_cnt; i++) {
			if (part_node_ptr->part_pptr[i] == des_part_ptr)
				break;
		}
		if (i == part_node_ptr->part_cnt) {
			part_node_ptr->part_cnt++;
			xrecalloc(part_node_ptr->part_pptr, part_node_ptr->part_cnt,
				  sizeof(part_record_t *));
			part_node_ptr->part_pptr[part_node_ptr->part_cnt-1] = des_part_ptr;
		}
		free(alias);
	}
	if (alias_list)
		hostlist_destroy(alias_list);
*/
	list_append(cache_part_list, des_part_ptr);
	return des_part_ptr;
}

/*_add_part_to_queue: Make a complete copy of the source data in the partition structure.*/
extern part_record_t *_add_part_to_queue(part_record_t *src_part_ptr)
{
	debug2("%s CACHE_QUERY  add part to queue %s ", __func__, src_part_ptr->name);
	part_record_t *des_part_ptr = xmalloc(sizeof(part_record_t));
	copy_part(src_part_ptr,des_part_ptr);
	return des_part_ptr;
}

/*_add_queue_part_to_cache: Add the copied partition structure to the cache list.*/
extern part_record_t *_add_queue_part_to_cache(part_record_t *des_part_ptr)
{

//	int i;
//	node_record_t *part_node_ptr = NULL;
//	hostlist_t alias_list;
//	char *alias = NULL;
	debug2("%s CACHE_QUERY  add part to cache %s ", __func__, des_part_ptr->name);
	if(find_copy_part_record(des_part_ptr->name, cache_part_list)){
		_list_delete_cache_part(des_part_ptr);
		return NULL;
	}
/*
	alias_list = hostlist_create(des_part_ptr->nodes);
	while ((alias = hostlist_shift(alias_list))){
		part_node_ptr = find_cache_node_record(alias);
		if(part_node_ptr){
			for (i = 0; i < part_node_ptr->part_cnt; i++) {
				if (part_node_ptr->part_pptr[i] == des_part_ptr)
					break;
			}
			if (i == part_node_ptr->part_cnt) { 
				part_node_ptr->part_cnt++;
				xrecalloc(part_node_ptr->part_pptr, part_node_ptr->part_cnt,
					  sizeof(part_record_t *));
				part_node_ptr->part_pptr[part_node_ptr->part_cnt-1] = des_part_ptr;
			}
		}
		free(alias);
	}
	if (alias_list)
		hostlist_destroy(alias_list);
*/
	list_append(cache_part_list, des_part_ptr);
	return des_part_ptr;
}

/*_del_cache_part: Copy the partition source data directly into the cached data.*/
extern int _del_cache_part(char *part_name)
{
	debug2("%s CACHE_QUERY  delete part %s ", __func__, part_name);
	kill_cache_job_by_cache_part_name(part_name);
	list_delete_all(cache_part_list, list_find_part, part_name);
	return SLURM_SUCCESS;
}


static int _copy_part(void *object, void *arg)
{
	part_record_t *src_part_ptr = object;
	part_record_t *des_part_ptr = xmalloc(sizeof(part_record_t));
	copy_part(src_part_ptr,des_part_ptr);
	list_append(copy_part_list, des_part_ptr);
	return SLURM_SUCCESS;
}

void copy_all_part_state()
{
	slurmctld_lock_t config_read_lock = {
			NO_LOCK, NO_LOCK, NO_LOCK, READ_LOCK, NO_LOCK };
	copy_part_list = list_create(_copy_list_delete_part);
	lock_slurmctld(config_read_lock);
	if(part_list){
		list_for_each_ro(part_list, _copy_part, NULL);
	}
	if(default_part_name){
		default_copy_part_name = xstrdup(default_part_name);
	}
	unlock_slurmctld(config_read_lock);
}

void purge_cache_part_data()
{
	FREE_NULL_LIST(cache_part_list);
    xfree(default_cache_part_name);
	cache_part_list = NULL;
}



void replace_cache_part_data()
{
    default_cache_part_name = default_copy_part_name;
	cache_part_list = copy_part_list;
	copy_part_list = NULL;
    default_copy_part_name = NULL;
}


#ifdef __METASTACK_OPT_PART_VISIBLE
extern part_record_t **build_visible_cache_parts_user(slurmdb_user_rec_t *user_ret, 
				bool skip, bool locked)
{
	part_record_t **visible_parts_save = NULL;
	part_record_t **visible_parts = NULL;
	build_visible_parts_arg_t args = {0};

	/*
	 * if return, user_ret->assoc_list == NULL;
	 */
	if (skip)
		return NULL;

	visible_parts = xcalloc(list_count(cache_part_list) + 1,
				sizeof(part_record_t *));
	args.uid = user_ret->uid;
	args.visible_parts = visible_parts;
	args.user_rec.uid = user_ret->uid;

	assoc_mgr_lock_t locks = { .assoc = READ_LOCK, .user = READ_LOCK };
	if (!locked)
		assoc_mgr_lock(&locks);
	
	args.user_rec.assoc_list = fill_assoc_list(user_ret->uid, true);

	/*
	 * Save start pointer to start of the list so can point to start
	 * after appending to the list.
	 */
	visible_parts_save = visible_parts;
	list_for_each(cache_part_list, _build_visible_parts_foreach, &args);

	/* user_ret->assoc_list */
	// list_destroy(args.user_rec.assoc_list);
	user_ret->assoc_list = args.user_rec.assoc_list;
	args.user_rec.assoc_list = NULL;

	if (!locked)
		assoc_mgr_unlock(&locks);

	return visible_parts_save;
}
#endif

extern part_record_t **build_visible_cache_parts(uid_t uid, bool skip)
{
	part_record_t **visible_parts_save = NULL;
	part_record_t **visible_parts = NULL;
	build_visible_parts_arg_t args = {0};

	/*
	 * The array of visible parts isn't used for privileged (i.e. operators)
	 * users or when SHOW_ALL is requested, so no need to create list.
	 */
	if (skip)
		return NULL;

	visible_parts = xcalloc(list_count(cache_part_list) + 1,
				sizeof(part_record_t *));
	args.uid = uid;
	args.visible_parts = visible_parts;

	/*
	 * Save start pointer to start of the list so can point to start
	 * after appending to the list.
	 */
	visible_parts_save = visible_parts;
	list_for_each(cache_part_list, _build_visible_parts_foreach, &args);

	return visible_parts_save;
}

/*
 * pack_all_cache_part - dump all partition information for all partitions in
 *      machine independent form (for network transmission)
 * OUT buffer_ptr - the pointer is set to the allocated buffer.
 * OUT buffer_size - set to size of the buffer in bytes
 * IN show_flags - partition filtering options
 * IN uid - uid of user making request (for partition filtering)
 * global: part_list - global list of partition records
 * NOTE: the buffer at *buffer_ptr must be xfreed by the caller
 * NOTE: change slurm_load_part() in api/part_info.c if data format changes
 */
extern void pack_all_cache_part(char **buffer_ptr, int *buffer_size,
                          uint16_t show_flags, uid_t uid,
                          uint16_t protocol_version)
{
        int tmp_offset;
        time_t now = time(NULL);
        bool privileged = validate_operator(uid);
#ifdef __METASTACK_OPT_PART_VISIBLE
        _foreach_pack_part_info_t pack_info = {
                .buffer = init_buf(BUF_SIZE),
                .parts_packed = 0,
                .privileged = privileged,
                .protocol_version = protocol_version,
                .show_flags = show_flags,
                .uid = uid,
                .user_rec.uid = (uint32_t)uid,		
                .pack_cache = true,
        };

    pack_info.user_rec.assoc_list = NULL;
        /* also return user_rec.assoc_list */
        pack_info.visible_parts = build_visible_cache_parts_user(&pack_info.user_rec, privileged, false);
#else
        _foreach_pack_part_info_t pack_info = {
                .buffer = init_buf(BUF_SIZE),
                .parts_packed = 0,
                .privileged = privileged,
                .protocol_version = protocol_version,
                .show_flags = show_flags,
                .uid = uid,
                .visible_parts = build_visible_cache_parts(uid, privileged),	
                .pack_cache = true,
        };
#endif

        buffer_ptr[0] = NULL;
        *buffer_size = 0;

        /* write header: version and time */
        pack32(0, pack_info.buffer);
        pack_time(now, pack_info.buffer);
        list_for_each_ro(cache_part_list, _pack_part, &pack_info);
#ifdef __METASTACK_OPT_PART_VISIBLE
        /* if (privileged) 
         * assoc_list == NULL */
        if (pack_info.user_rec.assoc_list)
                list_destroy(pack_info.user_rec.assoc_list);
#endif

        /* put the real record count in the message body header */
        tmp_offset = get_buf_offset(pack_info.buffer);
        set_buf_offset(pack_info.buffer, 0);
        pack32(pack_info.parts_packed, pack_info.buffer);
        set_buf_offset(pack_info.buffer, tmp_offset);

        *buffer_size = get_buf_offset(pack_info.buffer);
        buffer_ptr[0] = xfer_buf_data(pack_info.buffer);
        xfree(pack_info.visible_parts);
}
#endif
