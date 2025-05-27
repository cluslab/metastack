/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */

#ifndef CONFIG_H_
#define CONFIG_H_

/*
 * To disable this macro definition, the following files need to be modified: 
 *     args.h, config.c, pyxis_alloc.h, pyxis_slurmstepd.h, and pyxis_srun.h.
 */ 
#ifndef METASTACK_NEW_PYXIS_PARASTORAGE_SUPPORT
#define METASTACK_NEW_PYXIS_PARASTORAGE_SUPPORT
#endif

#include <limits.h>
#include <stdbool.h>

enum container_scope {
	SCOPE_JOB,
	SCOPE_GLOBAL,
};

struct plugin_config {
	char runtime_path[PATH_MAX];
	bool execute_entrypoint;
	enum container_scope container_scope;
	bool sbatch_support;
#ifdef METASTACK_NEW_PYXIS_PARASTORAGE_SUPPORT
	bool sharefs_support;
#endif
};

int pyxis_config_parse(struct plugin_config *config, int ac, char **av);

#endif /* CONFIG_H_ */
