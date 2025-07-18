/*****************************************************************************\
 *  read_config.h - functions for reading slurmctld configuration
 *****************************************************************************
 *  Copyright (C) 2003-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2010 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Morris Jette <jette1@llnl.gov> et. al.
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

#ifndef _HAVE_READ_CONFIG_H
#define _HAVE_READ_CONFIG_H

/* Convert a comma delimited list of account names into a NULL terminated
 * array of pointers to strings. Call accounts_list_free() to release memory */
extern void accounts_list_build(char *accounts, char ***accounts_array);

/* Free memory allocated for an account array by accounts_list_build() */
extern void accounts_list_free(char ***accounts_array);

#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
#define MAX_NODES_CAN_BORROW    65533
#define DEFAULT_NODE_BORROW_INTERVAL 5
extern char *offline_reason;
extern int node_borrow_interval;		/* Detect the time interval for node borrowing */
extern void _add_node_to_parts(node_record_t *node_ptr, part_record_t *part_ptr);
extern void _build_node_partitions(node_record_t *node_ptr, part_record_t *part_ptr, bool add_part);
extern int  load_all_part_borrow_nodes(void);
extern void _remove_node_from_parts(node_record_t *node_ptr, bool clear_flag);
extern void _return_borrowed_node(node_record_t *node_ptr);
extern void _update_borrowed_node_up(node_record_t *node_ptr, time_t now);
extern void update_all_parts_resource(bool update_resv);
extern int  _select_node_to_borrow(part_record_t *part_ptr, int nodes_need_borrow);
extern bool _standby_node_avail(node_record_t *node_ptr);
extern int build_part_standby_nodes_bitmap(part_record_t *part_ptr);
extern int valid_standby_node_parameters(part_record_t *part_ptr);
extern void _update_node_borrow_state(node_record_t *node_ptr, part_record_t *part_ptr, bool borrowed);
extern bool validate_all_partitions_borrow_nodes(List part_list, bool update_resv);
extern bool validate_partition_borrow_nodes(part_record_t *part_ptr);
#endif

#ifdef __METASTACK_OPT_MSG_OUTPUT
extern bool enable_reason_detail;
#endif

/*
 * Free the global response_cluster_rec
 */
extern void cluster_rec_free(void);

/*
 * read_slurm_conf - load the slurm configuration from the configured file.
 * read_slurm_conf can be called more than once if so desired.
 * IN recover - replace job, node and/or partition data with latest
 *              available information depending upon value
 *              0 = use no saved state information, rebuild everything from
 *		    slurm.conf contents
 *              1 = recover saved job and trigger state,
 *                  node DOWN/DRAIN/FAIL state and reason information
 *              2 = recover all saved state
 * IN reconfig - true if SIGHUP or "scontrol reconfig" and there is state in
 *		 memory to preserve, otherwise recover state from disk
 * RET SLURM_SUCCESS if no error, otherwise an error code
 * Note: Operates on common variables only
 */
extern int read_slurm_conf(int recover, bool reconfig);

extern int dump_config_state_lite(void);
extern int load_config_state_lite(void);

/* For a configuration where available_features == active_features,
 * build new active and available feature lists */
extern void build_feature_list_eq(void);

/* For a configuration where available_features != active_features,
 * build new active and available feature lists */
extern void build_feature_list_ne(void);

/* Update active_feature_list or avail_feature_list
 * feature_list IN - List to update: active_feature_list or avail_feature_list
 * new_features IN - New active_features
 * node_bitmap IN - Nodes with the new active_features value */
extern void update_feature_list(List feature_list, char *new_features,
				bitstr_t *node_bitmap);

#ifdef __METASTACK_OPT_CACHE_QUERY	
extern void _validate_copy_het_jobs(void);
#endif

#endif /* !_HAVE_READ_CONFIG_H */
