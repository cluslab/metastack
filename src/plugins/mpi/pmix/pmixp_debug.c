#include "pmixp_debug.h"

#ifndef NDEBUG

static pthread_mutex_t _init_lock = PTHREAD_MUTEX_INITIALIZER;

static int _thread_cnt_max = 0;
static int _thread_cnt = 0;
static __thread int _thread_idx = -1;
static __thread int _thread_err_reported = 0;
static int _buf_size = 0;
typedef struct {
	char *buf;
	size_t offset;
} pmixp_prof_buffer_t;

static pmixp_prof_buffer_t *_prof_buf = NULL;

static int _get_thread_idx()
{
	if( _thread_idx < 0 ) {
		slurm_mutex_lock(&_init_lock);
		if(_thread_cnt >= _thread_cnt_max) {
			PMIXP_DEBUG("no slots available: max = %d", _thread_cnt_max);
		}
		_thread_idx = _thread_cnt++;
		slurm_mutex_unlock(&_init_lock);
	}
	return _thread_idx;
}

void pmixp_profile_in(pmixp_profile_t *prof, char *buf, size_t size)
{
	int tid = _get_thread_idx();
	double ts;
	size_t out_size = sizeof(prof) + sizeof(ts) + sizeof(size) + size;

	PMIXP_DEBUG_GET_TS(ts);

	if( tid >= _thread_cnt_max) {
		/* Start output immediately */
		/* TODO: flush the portion of the buffer instead */
		if( !_thread_err_reported ) {
			_thread_err_reported = 1;
			PMIXP_ERROR("ERROR: Max allowed number (%d) of threads exceeded (%d), output immediately",
				    _thread_cnt_max, tid);
		}
		prof->output(prof->priv, ts, size, buf);
		return;
	}
	if( (out_size + _prof_buf[tid].offset) > _buf_size ) {
		/* Start output immediately */
		/* TODO: flush the portion of the buffer instead */
		if( !_thread_err_reported ) {
			_thread_err_reported = 1;
			PMIXP_ERROR("ERROR: Buffer for thread #%d is full, output immediately",
				    tid);
		}
		prof->output(prof->priv, ts, size, buf);
		return;
	}

	PMIXP_PROF_SERIALIZE(_prof_buf[tid].buf, _buf_size,
			     _prof_buf[tid].offset, prof);
	PMIXP_PROF_SERIALIZE(_prof_buf[tid].buf, _buf_size,
			     _prof_buf[tid].offset, ts);
	PMIXP_PROF_SERIALIZE(_prof_buf[tid].buf, _buf_size,
			     _prof_buf[tid].offset, size);
	memcpy(_prof_buf[tid].buf + _prof_buf[tid].offset, buf, size);
	_prof_buf[tid].offset += size;
}

static size_t pmixp_profile_out(char *buf, size_t remain)
{
	pmixp_profile_t *prof;
	size_t size;
	double ts;
	size_t offset = 0;

	PMIXP_PROF_DESERIALIZE(buf, remain, offset, prof);
	PMIXP_PROF_DESERIALIZE(buf, remain, offset, ts);
	PMIXP_PROF_DESERIALIZE(buf, remain, offset, size);
	prof->output(prof->priv, ts, size, buf + offset);
	offset += size;
	return offset;
}

static void _pmixp_profile_drain(pmixp_prof_buffer_t *pbuf)
{
	size_t offset = 0;
	while(offset < pbuf->offset) {
		size_t ret = pmixp_profile_out(pbuf->buf + offset,
					       pbuf->offset - offset);
		if( ret == 0 ) {
			PMIXP_ERROR("ERROR: zero offset returned on half dumped buffer: offset=%zu, len=%zu",
				    offset, pbuf->offset);
			break;
		}
		offset += ret;
	}
}

void pmixp_profile_init(int max_threads, size_t thread_buf_size)
{
	int i;
	_thread_cnt_max = max_threads;
	_buf_size = thread_buf_size;
	_prof_buf = xcalloc(_thread_cnt_max, sizeof(*_prof_buf));
	for(i=0; i<_thread_cnt_max;i++){
		_prof_buf[i].buf = xmalloc(thread_buf_size);
	}
}

void pmixp_profile_fini()
{
	int i;
	for(i=0; i<_thread_cnt_max;i++){
		_pmixp_profile_drain(&_prof_buf[i]);
		xfree(_prof_buf[i].buf);
	}
}

#endif
