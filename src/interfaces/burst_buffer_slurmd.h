#ifndef _INTERFACES_BURST_BUFFER_SLURMD_H
#define _INTERFACES_BURST_BUFFER_SLURMD_H

#include "slurm/slurm.h"

/* Forward declaration */
typedef struct xlist *List;

/*
 * Initialize the bb_api library infrastructure.
 *
 * Returns a Slurm errno.
 */
extern int bb_g_init(void);

/*
 * Terminate the bb_api library infrastructure. Free memory.
 *
 * Returns a Slurm errno.
 */
extern int bb_g_fini(void);

/*
 **************************************************************************
 *                    B B   A P I   W R A P P E R   F U N C T I O N S    *
 **************************************************************************
 */

/*
 * Test function to verify bb_api library loading
 * 
 * RET: 0 on success, SLURM_ERROR on failure
 */
extern int bb_g_bb_api_test_function(void);

/*
 * Get groups list from bb_api library
 * 
 * query_params IN - query parameters
 * bb_min_config IN - minimal burst buffer configuration
 * resp_out OUT - response output
 * RET list of groups, or NULL on error
 */
extern List bb_g_get_groups_burst_buffer(void *query_params, void *bb_min_config, void *resp_out);

#endif