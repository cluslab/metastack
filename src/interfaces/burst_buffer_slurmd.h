/*****************************************************************************\
 *  burst_buffer.h - driver for burst buffer infrastructure and plugin
 *****************************************************************************
 *  Copyright (C) SchedMD LLC.
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

#ifndef _INTERFACES_BURST_BUFFER_SLURMD_H
#define _INTERFACES_BURST_BUFFER_SLURMD_H

#include "slurm/slurm.h"

/*
 * Initialize the burst buffer infrastructure.
 *
 * Returns a Slurm errno.
 */
extern int bb_g_init(void);

/*
 * Terminate the burst buffer infrastructure. Free memory.
 *
 * Returns a Slurm errno.
 */
extern int bb_g_fini(void);

/*
 **************************************************************************
 *                          P L U G I N   C A L L S                       *
 **************************************************************************
 */
/*
 * Load the current burst buffer state (e.g. how much space is available now).
 * Run at the beginning of each scheduling cycle in order to recognize external
 * changes to the burst buffer state (e.g. capacity is added, removed, fails,
 * etc.)
 *
 * init_config IN - true if called as part of slurmctld initialization
 * Returns a Slurm errno.
 */
extern int bb_g_load_state(bool init_config);

/*
 * Return string containing current burst buffer status
 * argc IN - count of status command arguments
 * argv IN - status command arguments
 * uid - authenticated UID
 * gid - authenticated GID
 * RET status string, release memory using xfree()
 */
extern char *bb_g_get_status(uint32_t argc, char **argv, uint32_t uid,
			     uint32_t gid);

/*
 * Pack current burst buffer state information for network transmission to
 * user (e.g. "scontrol show burst")
 *
 * Returns a Slurm errno.
 */

extern int bb_g_reconfig(void);

#endif
