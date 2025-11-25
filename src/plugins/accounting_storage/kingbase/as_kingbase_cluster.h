/*****************************************************************************\
 *  as_kingbase_cluster.h - functions dealing with clusters.
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
#ifndef _HAVE_KINGBASE_CLUSTER_H
#define _HAVE_KINGBASE_CLUSTER_H

#include "accounting_storage_kingbase.h"

extern int as_kingbase_add_clusters(kingbase_conn_t *kingbase_conn, uint32_t uid,
				 List cluster_list);

extern List as_kingbase_modify_clusters(kingbase_conn_t *kingbase_conn, uint32_t uid,
				     slurmdb_cluster_cond_t *cluster_cond,
				     slurmdb_cluster_rec_t *cluster);

extern List as_kingbase_remove_clusters(kingbase_conn_t *kingbase_conn, uint32_t uid,
				     slurmdb_cluster_cond_t *cluster_cond);

extern List as_kingbase_get_clusters(kingbase_conn_t *kingbase_conn, uid_t uid,
				  slurmdb_cluster_cond_t *cluster_cond);

extern List as_kingbase_get_cluster_events(kingbase_conn_t *kingbase_conn, uint32_t uid,
					slurmdb_event_cond_t *event_cond);

#ifdef __METASTACK_NEW_AUTO_SUPPLEMENT_AVAIL_NODES
extern int as_kingbase_node_borrow(kingbase_conn_t *kingbase_conn,
			      node_record_t *node_ptr,
			      time_t event_time, char *reason,
			      uint32_t reason_uid);

extern int as_kingbase_node_return(kingbase_conn_t *kingbase_conn, node_record_t *node_ptr,
			    time_t event_time);

extern List as_kingbase_get_cluster_borrow(kingbase_conn_t *kingbase_conn, uint32_t uid,
					slurmdb_borrow_cond_t *borrow_cond);
#endif

extern int as_kingbase_node_down(kingbase_conn_t *kingbase_conn,
			      node_record_t *node_ptr,
			      time_t event_time, char *reason,
			      uint32_t reason_uid);

extern int as_kingbase_node_up(kingbase_conn_t *kingbase_conn, node_record_t *node_ptr,
			    time_t event_time);

extern int as_kingbase_register_ctld(kingbase_conn_t *kingbase_conn,
				  char *cluster, uint16_t port);

extern int as_kingbase_fini_ctld(kingbase_conn_t *kingbase_conn,
			      slurmdb_cluster_rec_t *cluster_rec);

extern int as_kingbase_cluster_tres(kingbase_conn_t *kingbase_conn,
				 char *cluster_nodes, char **tres_str_in,
				 time_t event_time, uint16_t rpc_version);

extern int as_kingbase_get_fed_cluster_id(kingbase_conn_t *kingbase_conn,
				       const char *cluster,
				       const char *federation,
				       int last_id, int *ret_id);
#endif
