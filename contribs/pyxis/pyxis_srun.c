/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "pyxis_srun.h"
#include "args.h"

#ifdef METASTACK_NEW_PYXIS_PARASTORAGE_SUPPORT
#include "config.h"
#endif

struct plugin_context {
	struct plugin_args *args;
};

static struct plugin_context context = {
	.args = NULL,
};

int pyxis_srun_init(spank_t sp, int ac, char **av)
{
#ifdef METASTACK_NEW_PYXIS_PARASTORAGE_SUPPORT
	int ret = 0;
	struct plugin_config config = {0};

	ret = pyxis_config_parse(&config, ac, av);
	if (ret < 0) {
		slurm_error("pyxis: failed to parse configuration");
		return (-1);
	}	
	context.args = pyxis_args_register(sp, config);
#endif
	if (context.args == NULL) {
		slurm_error("pyxis: failed to register arguments");
		return (-1);
	}

	return (0);
}

int pyxis_srun_post_opt(spank_t sp, int ac, char **av)
{
	/* Calling pyxis_args_enabled() for arguments validation */
	pyxis_args_enabled();

	return (0);
}

int pyxis_srun_exit(spank_t sp, int ac, char **av)
{
	pyxis_args_free();

	memset(&context, 0, sizeof(context));

	return (0);
}


