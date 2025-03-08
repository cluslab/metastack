/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION. All rights reserved.
 */

#ifndef ARGS_H_
#define ARGS_H_

/*
 * To disable this macro definition, the following files need to be modified: 
 *     args.h, config.c, pyxis_alloc.h, pyxis_slurmstepd.h, and pyxis_srun.h.
 */ 
#ifndef METASTACK_NEW_PYXIS_PARASTORAGE_SUPPORT
#define METASTACK_NEW_PYXIS_PARASTORAGE_SUPPORT
#endif

#include <stdbool.h>
#include <stddef.h>

#include <slurm/spank.h>

#ifdef METASTACK_NEW_PYXIS_PARASTORAGE_SUPPORT
#include "config.h"
#endif

struct plugin_args {
	char *image;
	char **mounts;
	size_t mounts_len;
	char *workdir;
	char *container_name;
	char *container_name_flags;
	char *container_save;
	int mount_home;
	int remap_root;
	int entrypoint;
	int entrypoint_log;
	int writable;
	char **env_vars;
	size_t env_vars_len;
};

#ifdef METASTACK_NEW_PYXIS_PARASTORAGE_SUPPORT
struct plugin_args *pyxis_args_register(spank_t sp, struct plugin_config config);
#endif

bool pyxis_args_enabled(void);

int add_mount(const char *source, const char *target, const char *flags);

void remove_all_mounts(void);

void pyxis_args_free(void);

#endif /* ARGS_H_ */
