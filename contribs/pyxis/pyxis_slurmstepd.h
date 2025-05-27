/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */

#ifndef PYXIS_SLURMSTEPD_H_
#define PYXIS_SLURMSTEPD_H_

/*
 * To disable this macro definition, the following files need to be modified: 
 *     args.h, config.c, pyxis_alloc.h, pyxis_slurmstepd.h, and pyxis_srun.h.
 */ 
#ifndef METASTACK_NEW_PYXIS_PARASTORAGE_SUPPORT
#define METASTACK_NEW_PYXIS_PARASTORAGE_SUPPORT
#endif

#include <slurm/spank.h>

int pyxis_slurmstepd_init(spank_t sp, int ac, char **av);

int pyxis_slurmstepd_post_opt(spank_t sp, int ac, char **av);

int pyxis_slurmstepd_exit(spank_t sp, int ac, char **av);

#endif /* PYXIS_SLURMSTEPD_H_ */
