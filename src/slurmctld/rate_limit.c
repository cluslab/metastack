/*****************************************************************************\
 *  rate_limit.c
 *****************************************************************************
 *  Copyright (C) 2023 SchedMD LLC.
 *  Written by Tim Wickberg <tim@schedmd.com>
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

#include <stdbool.h>

#include "src/common/macros.h"
#include "src/common/slurm_protocol_defs.h"
#include "src/common/xstring.h"

#include "src/slurmctld/slurmctld.h"
#include "src/slurmctld/locks.h"

#ifdef __METASTACK_NEW_RPC_RATE_LIMIT
/*
 * last_update is scaled by refill_period, and is not the direct unix time
 */
typedef struct {
	time_t last_update;
	uint32_t tokens;
	uid_t uid;
} user_bucket_t;

static user_bucket_t *user_buckets = NULL;
static bool rate_limit_enabled = false;
static pthread_mutex_t rate_limit_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * 30 tokens max, bucket refills 2 tokens per 1 second
 * retry_max_period - The maximum period for the client to send a RPC request to the server when rate limit.
 */
#define TABLE_SIZE       8192
#define BUCKET_SIZE      30
#define LIMIT_TYPE       3   /* limit all request */
#define REFILL_RATE      2
#define REFILL_PERIOD    1
#define RETRY_MAX_PERIOD 3600

static int table_size;
static int bucket_size;
static int limit_type;
static int refill_rate;
static int refill_period;
static int retry_max_period;

extern void rate_limit_shutdown(void)
{
	xfree(user_buckets);
	xhash_free(rl_config_hash_table);
	destroy_rl_user_hash(&rl_user_hash);
}

extern void rate_limit_init(void)
{
	char *tmp_ptr = NULL;
	char *limit_type_str = NULL;

	if (!xstrcasestr(slurm_conf.slurmctld_params, "rl_enable")) {
		rate_limit_enabled = false;
		info("RPC rate limiting disable");
		return;
	}
	info("RPC rate limiting enabled");

	//limit_type = LIMIT_TYPE; /* do this in func parse_limit_type */	
	table_size = TABLE_SIZE;
	bucket_size = BUCKET_SIZE;
	refill_rate = REFILL_RATE;
	refill_period = REFILL_PERIOD;
	retry_max_period = RETRY_MAX_PERIOD;


	if ((tmp_ptr = xstrcasestr(slurm_conf.slurmctld_params,
				   "rl_table_size="))) {
		int tmp = atoi(tmp_ptr + 14);
		if (tmp <= 0 ) {
			error("rl_table_size configured invalid value, use default value");
		} else {
			table_size = tmp;
		}
	}
	if ((tmp_ptr = xstrcasestr(slurm_conf.slurmctld_params,
				   "rl_bucket_size="))) {
		int tmp = atoi(tmp_ptr + 15);
		if(tmp <= 0 ) {
			error("rl_bucket_size configured invalid value, use default value");
		} else {
			bucket_size = tmp;
		}
	}
	if ((tmp_ptr = xstrcasestr(slurm_conf.slurmctld_params,
				   "rl_refill_rate="))) {
		int tmp = atoi(tmp_ptr + 15);
		if (tmp <= 0 ) {
			error("rl_refill_rate configured invalid value, use default value");
		} else {
			refill_rate = tmp;
		}
	}
	if ((tmp_ptr = xstrcasestr(slurm_conf.slurmctld_params,
				   "rl_refill_period="))) {
		int tmp = atoi(tmp_ptr + 17);
		if (tmp <= 0 ) {
			error("rl_refill_period configured invalid value, use default value");
		} else {
			refill_period = tmp;
		}
	}
	if ((tmp_ptr = xstrcasestr(slurm_conf.slurmctld_params,
				   "rl_retry_max_period="))) {
		int tmp = atoi(tmp_ptr + 20);
		if (tmp <= 0 || tmp > retry_max_period) {
			error("rl_retry_max_period configured invalid value, use default value");
		} else {
			retry_max_period = tmp;
		}
	}

	parse_limit_type(slurm_conf.slurmctld_params, LIMIT_TYPE, &limit_type);
	limit_type_str = get_limit_type_str(limit_type);

	user_buckets = xcalloc(table_size, sizeof(user_bucket_t));
	
	rate_limit_enabled = true;
	if (limit_type != 1) {
		debug("%s: rl_table_size=%d,rl_bucket_size=%d,rl_refill_rate=%d,rl_refill_period=%d,rl_retry_max_period=%d,limit_type=%s",
			__func__, table_size, bucket_size, refill_rate, refill_period, retry_max_period, limit_type_str);
	} else {
		debug("%s: all requests no rate limit", __func__);
	}

	/* parse rlconfig */
	add_rl_config_to_hash(limit_type, bucket_size, refill_rate);
	add_rl_user_to_hash();
}


/*
 * Return true if the limit's been exceeded.
 * False otherwise.
 */
extern bool rate_limit_exceeded(slurm_msg_t *msg)
{
	bool exceeded = false, user_rl_config = false;
	rl_user_record_t *rl_user_record = NULL;
	int start_position = 0, position = 0, user_limit_type = 0, user_bucket_size = 0, user_refill_rate = 0;

	if (!rate_limit_enabled) {
		return false;
	}	

	/*
	 * Exempt SlurmUser / root. Subjecting internal cluster traffic to
	 * the rate limit would break things really quickly. :)
	 * (We're assuming SlurmdUser is root here.)
	 */
	if (validate_slurm_user(msg->auth_uid)) {
		return false;
	}	

	if ((rl_user_record = find_rl_user_hash(&rl_user_hash, msg->auth_uid))) {
		user_limit_type = rl_user_record->limit_type;
		user_bucket_size = rl_user_record->bucket_size;
		user_refill_rate = rl_user_record->refill_rate;
		user_rl_config = true;
	}

	if (user_rl_config) {
		/* all requests no limit configured */
		if (user_limit_type == 1) {
			return false;		
		}		

		/* If only limit query requests configured, check if the msg type is query type */
		if (user_limit_type == 2) {
			bool rate_limit = false;
			switch (msg->msg_type)
			{
				case REQUEST_JOB_SBCAST_CRED:			// sbcast
				case REQUEST_STEP_LAYOUT:				// sattach
				case REQUEST_CRONTAB:					// scrontab
				case REQUEST_STATS_INFO:				// sdiag
				case REQUEST_PARTITION_INFO:			// sinfo
				case REQUEST_NODE_INFO:					// sinfo/scontrol show node/squeue -w
				case REQUEST_NODE_INFO_SINGLE:			// sinfo -n
				case REQUEST_PRIORITY_FACTORS:			// sprio
				case REQUEST_FED_INFO:					// squeue/scontrol show job
				case REQUEST_JOB_USER_INFO:				// squeue -u
				case REQUEST_JOB_INFO:					// squeue
				case REQUEST_JOB_INFO_SINGLE:			// scontrol show job
				case REQUEST_BUILD_INFO:				// scontrol show config
				case REQUEST_PING:						// scontrol ping/scontrol show config
				case REQUEST_RESERVATION_INFO:			// scontrol show resv
				case REQUEST_SHARE_INFO:				// sshare
				case REQUEST_JOB_STEP_INFO:				// sstat/scontrol show step
				case REQUEST_JOB_ALLOCATION_INFO:		//sstat
					rate_limit = true;
					break;
			}
			/* this msg type is not rate limit */
			if (!rate_limit) {
				return rate_limit;
			}
		}

		slurm_mutex_lock(&rate_limit_mutex);

		/*
		* Scan for position. Note that uid 0 indicates an unused slot,
		* since root is never subjected to the rate limit.
		* Naively hash the uid into the table. If that's not a match, keep
		* scanning for the next vacant spot. Wrap around to the front if
		* necessary once we hit the end.
		*/
		start_position = position = msg->auth_uid % table_size;
		while ((user_buckets[position].uid) &&
			(user_buckets[position].uid != msg->auth_uid)) {
			position++;
			if (position == table_size) {
				position = 0;
			}
			if (position == start_position) {
				position = table_size;
				break;
			}
		}

		if (position == table_size) {
			/*
			* Avoid the temptation to resize the table... you'd need to
			* rehash all the contents which would be annoying and slow.
			*/
			error("RPC Rate Limiting: ran out of user table space. User will not be limited.");
		} else if (!user_buckets[position].uid) {
			user_buckets[position].uid = msg->auth_uid;
			user_buckets[position].last_update = time(NULL) / refill_period;
			user_buckets[position].last_update /= refill_period;
			user_buckets[position].tokens = user_bucket_size - 1;
			debug3("%s: new entry for uid %u", __func__, msg->auth_uid);
		} else {
			time_t now = time(NULL) / refill_period;
			time_t delta = now - user_buckets[position].last_update;
			user_buckets[position].last_update = now;

			/* add tokens */
			if (delta) {
				user_buckets[position].tokens += (delta * user_refill_rate);
				user_buckets[position].tokens =
					MIN(user_buckets[position].tokens, user_bucket_size);
			}

			if (user_buckets[position].tokens)
				user_buckets[position].tokens--;
			else
				exceeded = true;

			debug3("%s: found uid %u at position %d remaining tokens %d%s",
				__func__, msg->auth_uid, position,
				user_buckets[position].tokens,
				(exceeded ? " rate limit exceeded" : ""));
		}
		slurm_mutex_unlock(&rate_limit_mutex);

		return exceeded;
	} else {
		/* all requests no limit configured */
		if (limit_type == 1) {
			return false;		
		}		

		/* If only limit query requests configured, check if the msg type is query type */
		if (limit_type == 2) {
			bool rate_limit = false;
			switch (msg->msg_type)
			{
				case REQUEST_JOB_SBCAST_CRED:			// sbcast
				case REQUEST_STEP_LAYOUT:				// sattach
				case REQUEST_CRONTAB:					// scrontab
				case REQUEST_STATS_INFO:				// sdiag
				case REQUEST_PARTITION_INFO:			// sinfo
				case REQUEST_NODE_INFO:					// sinfo/scontrol show node/squeue -w
				case REQUEST_NODE_INFO_SINGLE:			// sinfo -n
				case REQUEST_PRIORITY_FACTORS:			// sprio
				case REQUEST_FED_INFO:					// squeue/scontrol show job
				case REQUEST_JOB_USER_INFO:				// squeue -u
				case REQUEST_JOB_INFO:					// squeue
				case REQUEST_JOB_INFO_SINGLE:			// scontrol show job
				case REQUEST_BUILD_INFO:				// scontrol show config
				case REQUEST_PING:						// scontrol ping/scontrol show config
				case REQUEST_RESERVATION_INFO:			// scontrol show resv
				case REQUEST_SHARE_INFO:				// sshare
				case REQUEST_JOB_STEP_INFO:				// sstat/scontrol show step
				case REQUEST_JOB_ALLOCATION_INFO:		//sstat
					rate_limit = true;
					break;
			}
			/* this msg type is not rate limit */
			if (!rate_limit) {
				return rate_limit;
			}
		}

		slurm_mutex_lock(&rate_limit_mutex);

		/*
		* Scan for position. Note that uid 0 indicates an unused slot,
		* since root is never subjected to the rate limit.
		* Naively hash the uid into the table. If that's not a match, keep
		* scanning for the next vacant spot. Wrap around to the front if
		* necessary once we hit the end.
		*/
		start_position = position = msg->auth_uid % table_size;
		while ((user_buckets[position].uid) &&
			(user_buckets[position].uid != msg->auth_uid)) {
			position++;
			if (position == table_size) {
				position = 0;
			}
			if (position == start_position) {
				position = table_size;
				break;
			}
		}

		if (position == table_size) {
			/*
			* Avoid the temptation to resize the table... you'd need to
			* rehash all the contents which would be annoying and slow.
			*/
			error("RPC Rate Limiting: ran out of user table space. User will not be limited.");
		} else if (!user_buckets[position].uid) {
			user_buckets[position].uid = msg->auth_uid;
			user_buckets[position].last_update = time(NULL) / refill_period;
			user_buckets[position].last_update /= refill_period;
			user_buckets[position].tokens = bucket_size - 1;
			debug3("%s: new entry for uid %u", __func__, msg->auth_uid);
		} else {
			time_t now = time(NULL) / refill_period;
			time_t delta = now - user_buckets[position].last_update;
			user_buckets[position].last_update = now;

			/* add tokens */
			if (delta) {
				user_buckets[position].tokens += (delta * refill_rate);
				user_buckets[position].tokens =
					MIN(user_buckets[position].tokens, bucket_size);
			}

			if (user_buckets[position].tokens)
				user_buckets[position].tokens--;
			else
				exceeded = true;

			debug3("%s: found uid %u at position %d remaining tokens %d%s",
				__func__, msg->auth_uid, position,
				user_buckets[position].tokens,
				(exceeded ? " rate limit exceeded" : ""));
		}
		slurm_mutex_unlock(&rate_limit_mutex);

		return exceeded;
	}
}
#endif
