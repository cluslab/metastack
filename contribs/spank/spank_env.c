#include "spank_env.h"

 /*
  * All spank plugins must define this macro for the Slurm plugin loader.
  */
SPANK_PLUGIN(spank_env, 1)


int slurm_spank_init (spank_t sp, int ac, char **av)
{
    return (0);
}


/*
 *  Add environment variables with specific prefix to spank_job_env
 */
int set_spank_env (spank_t sp, char *prefix)
{
    extern char **environ;
    char *name, *eq, *value;
    if (environ == NULL)
	return (0);

    int i;
    for (i = 0; environ[i]!=NULL; i++) {
        if (strncmp(environ[i], prefix, strlen(prefix)))
            continue;
        // environ[i] have a specific prefix
        // Request memory space and copy environ[i]
        name = strdup(environ[i]);
        eq = strchr(name, (int)'=');
        if (eq == NULL) {
            free(name);
            break;
        }
        // Replace "=" with '\0'
        eq[0] = '\0';
        value = eq + 1;
        spank_job_control_setenv(sp, name, value, 1);
        free(name);
    }
    return (0); 
}

/*
 * Obtain the qualified environment variables 
 * through the configuration file (plugstack.conf)
 */
int slurm_spank_init_post_opt (spank_t sp, int ac, char **av)
{
    int i;
    for (i = 0; i < ac; i++) {
        set_spank_env(sp, av[i]);
    }

    return (0);
}

// do nothing in those function, return 0 directly
int slurm_spank_exit (spank_t sp, int ac, char **av)
{    
    return (0);
}

int slurm_spank_job_prolog (spank_t sp, int ac, char **av)
{
    return (0);
}

int slurm_spank_local_user_init (spank_t sp, int ac, char **av)
{
    return (0);
}

int slurm_spank_user_init (spank_t sp, int ac, char **av)
{
    return (0);
}

int slurm_spank_task_init_privileged (spank_t sp, int ac, char **av)
{
    return (0);
}

int slurm_spank_task_init (spank_t sp, int ac, char **av)
{
    return (0);
}

int slurm_spank_task_post_fork (spank_t sp, int ac, char **av)
{
    return (0);
}

int slurm_spank_task_exit (spank_t sp, int ac, char **av)
{
    return (0);
}

int slurm_spank_job_epilog (spank_t sp, int ac, char **av)
{
    return (0);
}


int slurm_spank_slurmd_exit (spank_t sp, int ac, char **av)
{
    return (0);
}
