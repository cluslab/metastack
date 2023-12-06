#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <slurm/spank.h>

int slurm_spank_init (spank_t sp, int ac, char **av);
int slurm_spank_init_post_opt (spank_t sp, int ac, char **av);
int slurm_spank_job_prolog (spank_t sp, int ac, char **av);
int slurm_spank_local_user_init (spank_t sp, int ac, char **av);
int slurm_spank_user_init (spank_t sp, int ac, char **av);
int slurm_spank_task_init_privileged (spank_t sp, int ac, char **av);
int slurm_spank_task_init (spank_t sp, int ac, char **av);
int slurm_spank_task_post_fork (spank_t sp, int ac, char **av);
int slurm_spank_task_exit (spank_t sp, int ac, char **av);
int slurm_spank_job_epilog (spank_t sp, int ac, char **av);
int slurm_spank_slurmd_exit (spank_t sp, int ac, char **av);
int slurm_spank_exit (spank_t sp, int ac, char **av);
int set_spank_env(spank_t sp, char *prefix);
