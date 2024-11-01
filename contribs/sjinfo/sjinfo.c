#include <stdio.h>
#include <time.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdbool.h>
#include <curl/curl.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <jansson.h>
#include <getopt.h>
#include <unistd.h>
#include <pwd.h>
#include <assert.h>
#include <ctype.h>
#include <inttypes.h>
#include <errno.h>
#include "sjinfo.h"

/* Names for the values of the `has_arg' field of `struct option'.  */
#define no_argument		0
#define required_argument	1
#define optional_argument	2
#define SJINFO_VERSION_STRING "SLURM220508-0.0.2"
#define PACKAGE_NAME "sjinfo "
#define NO_VAL     (0xfffffffe)
#define NO_VAL64   (0xfffffffffffffffe)
#define C_STRING_INIT_SIZE 2048

#define LIST_MAGIC 0xDEADBEEF
#define LIST_ITR_MAGIC 0xDEADBEFF
#define list_iterator_free(_i) free(_i)
#define list_node_free(_p) free(_p)
#define list_free(_l) free(l)
#define list_node_alloc() malloc(sizeof(struct listNode))
#define list_iterator_alloc() malloc(sizeof(struct listIterator))


//struct tm *localtime_r(const time_t *timep, struct tm *result);

#ifndef MAX
#  define MAX(a,b) ((a) > (b) ? (a) : (b))
#endif

#ifndef MIN
#  define MIN(a,b) ((a) < (b) ? (a) : (b))
#endif

#define CONVERT_NUM_UNIT_EXACT 0x00000001
#define CONVERT_NUM_UNIT_NO    0x00000002
#define CONVERT_NUM_UNIT_RAW   0x00000004
//# define PRIu64		__PRI64_PREFIX "u"
/* unit types */
enum {
	UNIT_NONE,
	UNIT_KILO,
	UNIT_MEGA,
	UNIT_GIGA,
	UNIT_TERA,
	UNIT_PETA,
	UNIT_UNKNOWN
};

enum {
	UNIT_STEP,
	UNIT_EVENT,
    UNIT_OVERALL,
    UNIT_RUNJOB
};

enum {
    JOBID,
    STEP,
    USERNAME,
};

typedef struct unit_names {
	char *name;
	int name_len;
	int multiplier;
} unit_names_t;

static unit_names_t un[] = {
	{"seconds",	7,	1},
	{"second",	6,	1},
	{"minutes",	7,	60},
	{"minute",	6,	60},
	{"hours",	5,	(60*60)},
	{"hour",	4,	(60*60)},
	{"days",	4,	(24*60*60)},
	{"day",		3,	(24*60*60)},
	{"weeks",	5,	(7*24*60*60)},
	{"week",	4,	(7*24*60*60)},
	{NULL,		0,	0}
};

typedef struct {
    char *length;
	char *password;
	char *username;
    char *database;
    char *host;
    char *policy;
} slurm_influxdb;

/* Type for handling HTTP responses */
struct http_response {
	char *message;
	size_t size;
};

typedef struct {
	int opt_gid;		/* running persons gid */
	int opt_uid;		/* running persons uid */
	int units;		/* --units*/
	uint32_t convert_flags;	/* --noconvert */    
	char *opt_field_list;	/* --fields= */ 
    uint64_t level;
    bool desc_set;      /* output data in reverse order*/
    bool only_run_job;  /* querying for running jobs*/
} sjinfo_parameters_t;

typedef struct {
   char *  username;
   /*stepd*/ 
   char *time;
   unsigned long jobid;
   int stepid;
   double stepcpu; 
   double stepcpumin; 
   double stepcpumax;  
   double stepcpuave;
   unsigned long stepmem;
   unsigned long stepmemmin;
   unsigned long stepmemmax;
   unsigned long stepvmem;
   unsigned long stepvmemmin;
   unsigned long stepvmemmax; 
   unsigned long steppages;
   /*event*/
   unsigned long cputhreshold;
   unsigned long start;
   unsigned long end;
   int type1;   // Marking cpu frequency anomalies
   int type2;   // identify process anomalies
   int type3;   // identifies node communication anomalies
   /* overall */
   unsigned long end_last;
   unsigned long start_last;
   unsigned long sum_cpu;
   unsigned long sum_pid;
   unsigned long sum_node;
} interface_sjinfo_t;



typedef void (*ListDelF) (void *x);
struct listNode {
	void                 *data;         /* node's data                       */
	struct listNode      *next;         /* next node in list                 */
};

struct listIterator {
	unsigned int          magic;        /* sentinel for asserting validity   */
	struct xlist         *list;         /* the list being iterated           */
	struct listNode      *pos;          /* the next node to be iterated      */
	struct listNode     **prev;         /* addr of 'next' ptr to prv It node */
	struct listIterator  *iNext;        /* iterator chain for list_destroy() */
};
struct xlist {
	unsigned int          magic;        /* sentinel for asserting validity   */
	struct listNode      *head;         /* head of the list                  */
	struct listNode     **tail;         /* addr of last node's 'next' ptr    */
	struct listIterator  *iNext;        /* iterator chain for list_destroy() */
	ListDelF              fDel;         /* function to delete node data      */
	int                   count;        /* number of nodes in list           */
};
typedef struct xlist *List;
/***************
 *  Constants  *
 ***************/

/*
 *  List Iterator opaque data type.
 */
typedef struct listIterator *ListIterator;
typedef struct listNode * ListNode;
sjinfo_parameters_t params;
#define FORMAT_STRING_SIZE 34
char outbuf[FORMAT_STRING_SIZE];

#define list_alloc() malloc(sizeof(struct xlist))
/*
*External application output data memory, used to store encrypted data
*External application output data memory, used to store decrypted data
*/

uint8_t ct1[32] = {0};    
uint8_t plain1[32] = {0}; 

uint8_t ct2[32] = {0};    
uint8_t plain2[32] = {0}; 

uint8_t ct3[32] = {0};    
uint8_t plain3[32] = {0}; 
char data3[32] = {0};

int print_fields_parsable_print = 0;
int print_fields_have_header = 1;
char *fields_delimiter = NULL;

/*stepd table*/
List print_fields_list = NULL;
List print_value_list = NULL;
ListIterator print_fields_itr = NULL;

/*event table*/
List print_events_list = NULL;
List print_events_value_list = NULL;
ListIterator print_events_itr = NULL;

/*overall table*/
List print_overall_list = NULL;
List print_overall_value_list = NULL;
ListIterator print_overall_itr = NULL;

c_string_t *job_list = NULL;

enum {
    PRINT_FIELDS_PARSABLE_NOT = 0,
    PRINT_FIELDS_PARSABLE_ENDING,
    PRINT_FIELDS_PARSABLE_NO_ENDING
};


typedef enum {
    PRINT_JOBID,
    PRINT_STEPID,
    PRINT_STEPAVECPU,
    PRINT_STEPCPU,
    PRINT_STEPMEM,
    PRINT_STEPVMEM,
    PRINT_STEPPAGES,
    PRINT_MAXSTEPCPU,
    PRINT_MINSTEPCPU,
    PRINT_MAXSTEPMEM,
    PRINT_MINSTEPMEM,
    PRINT_MAXSTEPVMEM,
    PRINT_MINSTEPVMEM,
    PRINT_CPUTHRESHOLD,
    PRINT_START,
    PRINT_END,
    PRINT_LASTSTART,
    PRINT_LASTEND,
    PRINT_SUMCPU,
    PRINT_SUMPID,
    PRINT_SUMNODE,
    PRINT_TYPE      
} sjinfo_print_types_t;

#define MAX_POLICY_NAME_LENGTH 256	/* Set the maximum length of the reservation policy name */
typedef enum {
	NATIVERP,
	STEPDRP,
	EVENTRP
} RPType;
/**
 *
 * This function extracts and returns the appropriate retention policy based on the input string `rt_policy`
 * and the specified retention policy type (`NATIVERP`, `STEPDRP`, or `EVENTRP`). 
 * If specific policies for `NATIVERP`, `STEPDRP`, or `EVENTRP` are found in the input string,
 * they will be used accordingly. If not, the function returns a fallback value from other policy types.
 * If no valid policy is specified, the function defaults to "autogen".
 *
 * rt_policy[in]	A string containing comma-separated key-value pairs that specify 
 *                 	the retention policies. For example: "NATIVERP=30d,STEPDRP=7d,EVENTRP=14d".
 *					If `NULL` or empty, the function defaults to "autogen".
 * type[in]			The retention policy type, which determines which policy to extract 
 *					(can be `NATIVERP`, `STEPDRP`, or `EVENTRP`).
 *
 * return 			A dynamically allocated string representing the retention policy for the given type.
 *         			The caller is responsible for freeing the returned string.
 *         			If no valid retention policy is found, the function returns "autogen".
 */
static char* _parse_rt_policy(const char *rt_policy, RPType type) {
    if(rt_policy == NULL || rt_policy[0] == '\0'){
        return strdup("autogen");
    }

    // Initialize the values
    char native_retention_policy[MAX_POLICY_NAME_LENGTH], stepd_retention_policy[MAX_POLICY_NAME_LENGTH], event_retention_policy[MAX_POLICY_NAME_LENGTH];
    char *token = NULL, *rest = NULL, *copy = NULL, *rval = NULL;
	bool native_set = false, stepd_set = false, event_set = false;
    copy = strdup(rt_policy);
	/*
		Compatible with the original way of setting up retention policies
	*/
    strncpy(native_retention_policy, copy, sizeof(native_retention_policy) - 1);
    native_retention_policy[sizeof(native_retention_policy) - 1] = '\0'; // Ensure null termination

    strncpy(stepd_retention_policy, copy, sizeof(stepd_retention_policy) - 1);
    stepd_retention_policy[sizeof(stepd_retention_policy) - 1] = '\0';

    strncpy(event_retention_policy, copy, sizeof(event_retention_policy) - 1);
    event_retention_policy[sizeof(event_retention_policy) - 1] = '\0';

    token = strtok_r(copy, ",", &rest);

    while (token != NULL) {
        // Extracting key and value
        char *key = strtok(token, "=");
        char *value = strtok(NULL, "=");

        if (key != NULL && value != NULL) {
            if (strcmp(key, "NATIVERP") == 0) {
                if(strlen(value) >= MAX_POLICY_NAME_LENGTH)
					printf("warning: Retention policy names(%s) too long, Maximum length allowed : %d\n", value, MAX_POLICY_NAME_LENGTH);
				native_set = true;
                strncpy(native_retention_policy, value, sizeof(native_retention_policy) - 1);
                native_retention_policy[sizeof(native_retention_policy) - 1] = '\0';
            } else if (strcmp(key, "STEPDRP") == 0) {
                if(strlen(value) >= MAX_POLICY_NAME_LENGTH)
					printf("warning: Retention policy names(%s) too long, Maximum length allowed : %d\n", value, MAX_POLICY_NAME_LENGTH);
				stepd_set = true;
				strncpy(stepd_retention_policy, value, sizeof(stepd_retention_policy) - 1);
                stepd_retention_policy[sizeof(stepd_retention_policy) - 1] = '\0';
			} else if (strcmp(key, "EVENTRP") == 0) {
                if(strlen(value) >= MAX_POLICY_NAME_LENGTH)
					printf("warning: Retention policy names(%s) too long, Maximum length allowed : %d\n", value, MAX_POLICY_NAME_LENGTH);
				event_set = true;
				strncpy(event_retention_policy, value, sizeof(event_retention_policy) - 1);
                event_retention_policy[sizeof(event_retention_policy) - 1] = '\0';
			}
        }
        token = strtok_r(NULL, ",", &rest);
    }
    free(copy);
    switch (type)
    {
    case NATIVERP:
		if(native_set)
			rval = strdup(native_retention_policy);
		else
			rval = event_set ? strdup(event_retention_policy) : strdup(stepd_retention_policy);
        break;
	case STEPDRP:
		if(stepd_set)
			rval = strdup(stepd_retention_policy);
		else
			rval = event_set ? strdup(event_retention_policy) : strdup(native_retention_policy);
        break;
	case EVENTRP:
		if(event_set)
			rval = strdup(event_retention_policy);
		else
			rval = stepd_set ? strdup(stepd_retention_policy) : strdup(native_retention_policy);
        break;
    default:
        rval = strdup("autogen");
        break;
    }
    return rval;
}


typedef struct {
	int len;  /* what is the width of the print */
	char *name;  /* name to be printed in header */
	void (*print_routine) (); /* what is the function to print with  */
	uint16_t type; /* defined in the local function */
} print_field_t;
/*
    ##########  Custom string types for dynamic scaling  ##########
*/
typedef struct c_string
{
    char *str;
    size_t alloced;
    size_t len;
} c_string_t;

c_string_t *c_string_create(void) {
    c_string_t *cs;
    cs = calloc(1, sizeof(c_string_t));
    cs->str = malloc(C_STRING_INIT_SIZE);
    cs->str[0] = '\0';

    cs->alloced = C_STRING_INIT_SIZE;
    cs->len = 0;

    return cs;
}

void c_string_destory(c_string_t *cs){
    if(cs == NULL) return;
    free(cs->str);
    free(cs);
}

static void c_string_ensure_space(c_string_t *cs, size_t add_len) {
    if (cs == NULL || add_len == 0) return;

    if(cs->alloced >= cs->len + add_len + 1) return;

    while(cs->alloced < cs->len + add_len + 1) {
        cs->alloced <<= 1;
        if(cs->alloced == 0) {
            cs->alloced--;
        }
    }
    cs->str = realloc(cs->str, cs->alloced);
}

void c_string_append_str(c_string_t *cs, const char *str) {
    if(cs == NULL || str == NULL || *str == '\0') return;
    // if(len == 0) len = strlen(str);
    size_t len = strlen(str);

    c_string_ensure_space(cs, len);
    memmove(cs->str + cs->len, str, len);
    cs->len += len;
    cs->str[cs->len] = '\0';
}

void c_string_front_str(c_string_t *cs, const char *str) {
    if (cs == NULL || str == NULL || *str == '\0') return;

    size_t len = strlen(str);

    c_string_ensure_space(cs, len);
    memmove(cs->str + len, cs->str, cs->len);
    memmove(cs->str, str, len);
    cs->len += len;
    cs->str[cs->len] = '\0';
}

size_t c_string_len(const c_string_t *cs) {
    if (cs == NULL) return 0;
    return cs->len;
}

const char *c_string_peek(const c_string_t *cs) {
    if (cs == NULL) return NULL;
    return cs->str;
}

/*
    ##########  Custom string types for dynamic scaling  ##########
*/

/*Get the length of a string, taking into account the case where the string is empty*/
size_t safe_strlen(const char *str) {
    return str ? strlen(str) : 0;
}

char *my_strdup(const char *s) {
    size_t len = strlen(s) + 1;
    char *new_s = malloc(len);
    if (new_s == NULL) {
        return NULL; 
    }
    strcpy(new_s, s);
    return new_s;
}

List
list_create (ListDelF f)
{
	List l = list_alloc();

	l->magic = LIST_MAGIC;
	l->head = NULL;
	l->tail = &l->head;
	l->iNext = NULL;
	l->fDel = f;
	l->count = 0;

	return l;
}

/* list_destroy()
 */
void
list_destroy (List l)
{
	ListIterator i, iTmp;
	ListNode p, pTmp;

	assert(l != NULL);
	assert(l->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&l->mutex);

	i = l->iNext;
	while (i) {
		assert(i->magic == LIST_ITR_MAGIC);
		i->magic = ~LIST_ITR_MAGIC;
		iTmp = i->iNext;
		list_iterator_free(i);
		i = iTmp;
	}
	p = l->head;
	while (p) {
		pTmp = p->next;
		if (p->data && l->fDel)
			l->fDel(p->data);
		list_node_free(p);
		p = pTmp;
	}
	l->magic = ~LIST_MAGIC;
	list_free(l);
}

/* list_is_empty()
 */
int
list_is_empty (List l)
{
	int n;

	assert(l != NULL);
	assert(l->magic == LIST_MAGIC);
	n = l->count;
	return (n == 0);
}

/*
 * Return the number of items in list [l].
 * If [l] is NULL, return 0.
 */
int list_count(List l)
{
	int n;

	if (!l)
		return 0;

	assert(l->magic == LIST_MAGIC);
	n = l->count;
	return n;
}

/*
 * Inserts data pointed to by [x] into list [l] after [pp],
 * the address of the previous node's "next" ptr.
 * Returns a ptr to data [x], or NULL if insertion fails.
 * This routine assumes the list is already locked upon entry.
 */
static void *_list_node_create(List l, ListNode *pp, void *x)
{
	ListNode p;
	ListIterator i;

	assert(l != NULL);
	assert(l->magic == LIST_MAGIC);
	assert(pp != NULL);
	assert(x != NULL);

	p = list_node_alloc();

	p->data = x;
	if (!(p->next = *pp))
		l->tail = &p->next;
	*pp = p;
	l->count++;

	for (i = l->iNext; i; i = i->iNext) {
		assert(i->magic == LIST_ITR_MAGIC);
		if (i->prev == pp)
			i->prev = &p->next;
		else if (i->pos == p->next)
			i->pos = p;
		assert((i->pos == *i->prev) ||
		       ((*i->prev) && (i->pos == (*i->prev)->next)));
	}

	return x;
}

/* _list_append_locked()
 *
 * Append an item to the list. The function assumes
 * the list is already locked.
 */
static void *
_list_append_locked(List l, void *x)
{
	void *v;

	v = _list_node_create(l, l->tail, x);

	return v;
}

/* list_append()
 */
void *
list_append (List l, void *x)
{
	void *v;

	assert(l != NULL);
	assert(x != NULL);
	assert(l->magic == LIST_MAGIC);
	// slurm_rwlock_wrlock(&l->mutex);
	v = _list_append_locked(l, x);
	// slurm_rwlock_unlock(&l->mutex);

	return v;
}

/* list_append_list()
 */
int
list_append_list (List l, List sub)
{
	int n = 0;
	ListNode p;

	assert(l != NULL);
	assert(l->magic == LIST_MAGIC);
	assert(l->fDel == NULL);
	assert(sub != NULL);
	assert(sub->magic == LIST_MAGIC);

	// slurm_rwlock_wrlock(&l->mutex);
	// slurm_rwlock_wrlock(&sub->mutex);
	p = sub->head;
	while (p) {
		if (!_list_append_locked(l, p->data))
			break;
		n++;
		p = p->next;
	}

	// slurm_rwlock_unlock(&sub->mutex);
	// slurm_rwlock_unlock(&l->mutex);

	return n;
}

List list_shallow_copy(List l)
{
	List m = list_create(NULL);

	(void) list_append_list(m, l);

	return m;
}

/*
 * Removes the node pointed to by [*pp] from from list [l],
 * where [pp] is the address of the previous node's "next" ptr.
 * Returns the data ptr associated with list item being removed,
 * or NULL if [*pp] points to the NULL element.
 * This routine assumes the list is already locked upon entry.
 */
static void *_list_node_destroy(List l, ListNode *pp)
{
	void *v;
	ListNode p;
	ListIterator i;

	assert(l != NULL);
	assert(l->magic == LIST_MAGIC);
	//assert(_list_mutex_is_locked(&l->mutex));
	assert(pp != NULL);

	if (!(p = *pp))
		return NULL;

	v = p->data;
	if (!(*pp = p->next))
		l->tail = pp;
	l->count--;

	for (i = l->iNext; i; i = i->iNext) {
		assert(i->magic == LIST_ITR_MAGIC);
		if (i->pos == p)
			i->pos = p->next, i->prev = pp;
		else if (i->prev == &p->next)
			i->prev = pp;
		assert((i->pos == *i->prev) ||
		       ((*i->prev) && (i->pos == (*i->prev)->next)));
	}
	list_node_free(p);

	return v;
}

/* _list_pop_locked
 *
 * Pop an item from the list assuming the
 * the list is already locked.
 */
static void *
_list_pop_locked(List l)
{
	void *v;

	v = _list_node_destroy(l, &l->head);

	return v;
}

/* list_pop()
 */
void *
list_pop (List l)
{
	void *v;

	assert(l != NULL);
	assert(l->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&l->mutex);

	v = _list_pop_locked(l);
	//slurm_rwlock_unlock(&l->mutex);

	return v;
}

/* list_push()
 */
void *
list_push (List l, void *x)
{
	void *v;

	assert(l != NULL);
	assert(x != NULL);
	assert(l->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&l->mutex);

	v = _list_node_create(l, &l->head, x);
	//slurm_rwlock_unlock(&l->mutex);

	return v;
}


/* list_remove()
 */
void *
list_remove (ListIterator i)
{
	void *v = NULL;

	assert(i != NULL);
	assert(i->magic == LIST_ITR_MAGIC);
	assert(i->list->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&i->list->mutex);

	if (*i->prev != i->pos)
		v = _list_node_destroy(i->list, i->prev);
	//slurm_rwlock_unlock(&i->list->mutex);

	return v;
}

/* list_delete_item()
 */
int
list_delete_item (ListIterator i)
{
	void *v;

	assert(i != NULL);
	assert(i->magic == LIST_ITR_MAGIC);

	if ((v = list_remove(i))) {
		if (i->list->fDel)
			i->list->fDel(v);
		return 1;
	}

	return 0;
}

#define FREE_NULL_LIST(_X)			\
	do {					\
		if (_X) list_destroy (_X);	\
		_X	= NULL; 		\
	} while (0)

/* list_iterator_create()
 */
ListIterator
list_iterator_create (List l)
{
	ListIterator i;

	assert(l != NULL);
	i = list_iterator_alloc();

	i->magic = LIST_ITR_MAGIC;
	i->list = l;
	assert(l->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&l->mutex);

	i->pos = l->head;
	i->prev = &l->head;
	i->iNext = l->iNext;
	l->iNext = i;

	//slurm_rwlock_unlock(&l->mutex);

	return i;
}

static void * _list_next_locked(ListIterator i)
{
	ListNode p;

	if ((p = i->pos))
		i->pos = p->next;
	if (*i->prev != p)
		i->prev = &(*i->prev)->next;

	return (p ? p->data : NULL);
}


/* list_next()
 */
void *list_next (ListIterator i)
{
	void *rc;

	assert(i != NULL);
	assert(i->magic == LIST_ITR_MAGIC);
	assert(i->list->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&i->list->mutex);

	rc = _list_next_locked(i);

	//slurm_rwlock_unlock(&i->list->mutex);

	return rc;
}


/* list_iterator_destroy()
 */
void
list_iterator_destroy (ListIterator i)
{
	ListIterator *pi;

	assert(i != NULL);
	assert(i->magic == LIST_ITR_MAGIC);
	assert(i->list->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&i->list->mutex);

	for (pi = &i->list->iNext; *pi; pi = &(*pi)->iNext) {
		assert((*pi)->magic == LIST_ITR_MAGIC);
		if (*pi == i) {
			*pi = (*pi)->iNext;
			break;
		}
	}
	//slurm_rwlock_unlock(&i->list->mutex);

	i->magic = ~LIST_ITR_MAGIC;
	list_iterator_free(i);
}

/* list_iterator_reset()
 */
void
list_iterator_reset (ListIterator i)
{
	assert(i != NULL);
	assert(i->magic == LIST_ITR_MAGIC);
	assert(i->list->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&i->list->mutex);

    i->pos = i->list->head;
	i->prev = &i->list->head;

	//slurm_rwlock_unlock(&i->list->mutex);
}


typedef struct{
    uint32_t eK[44], dK[44];    // encKey, decKey
    int Nr; // 10 rounds
}AesKey;

 /*AES-128 packet length is 16 bytes*/
#define BLOCKSIZE 16


#define LOAD32H(x, y) \
  do { (x) = ((uint32_t)((y)[0] & 0xff)<<24) | ((uint32_t)((y)[1] & 0xff)<<16) | \
             ((uint32_t)((y)[2] & 0xff)<<8)  | ((uint32_t)((y)[3] & 0xff));} while(0)


#define STORE32H(x, y) \
  do { (y)[0] = (uint8_t)(((x)>>24) & 0xff); (y)[1] = (uint8_t)(((x)>>16) & 0xff);   \
       (y)[2] = (uint8_t)(((x)>>8) & 0xff); (y)[3] = (uint8_t)((x) & 0xff); } while(0)

/*Extract the nth byte starting from the low bit from uint32_t x*/ 
#define BYTE(x, n) (((x) >> (8 * (n))) & 0xff)

/*
 *used for keyExpansion
 *Byte replacement then rotate left by 1 bit
 */

#define MIX(x) (((S[BYTE(x, 2)] << 24) & 0xff000000) ^ ((S[BYTE(x, 1)] << 16) & 0xff0000) ^ \
                ((S[BYTE(x, 0)] << 8) & 0xff00) ^ (S[BYTE(x, 3)] & 0xff))


#define ROF32(x, n)  (((x) << (n)) | ((x) >> (32-(n))))

#define ROR32(x, n)  (((x) >> (n)) | ((x) << (32-(n))))

/* for 128-bit blocks, Rijndael never uses more than 10 rcon values */

static const uint32_t rcon[10] = {
        0x01000000UL, 0x02000000UL, 0x04000000UL, 0x08000000UL, 0x10000000UL,
        0x20000000UL, 0x40000000UL, 0x80000000UL, 0x1B000000UL, 0x36000000UL
};



unsigned char S[256] = {
        0x63, 0x7C, 0x77, 0x7B, 0xF2, 0x6B, 0x6F, 0xC5, 0x30, 0x01, 0x67, 0x2B, 0xFE, 0xD7, 0xAB, 0x76,
        0xCA, 0x82, 0xC9, 0x7D, 0xFA, 0x59, 0x47, 0xF0, 0xAD, 0xD4, 0xA2, 0xAF, 0x9C, 0xA4, 0x72, 0xC0,
        0xB7, 0xFD, 0x93, 0x26, 0x36, 0x3F, 0xF7, 0xCC, 0x34, 0xA5, 0xE5, 0xF1, 0x71, 0xD8, 0x31, 0x15,
        0x04, 0xC7, 0x23, 0xC3, 0x18, 0x96, 0x05, 0x9A, 0x07, 0x12, 0x80, 0xE2, 0xEB, 0x27, 0xB2, 0x75,
        0x09, 0x83, 0x2C, 0x1A, 0x1B, 0x6E, 0x5A, 0xA0, 0x52, 0x3B, 0xD6, 0xB3, 0x29, 0xE3, 0x2F, 0x84,
        0x53, 0xD1, 0x00, 0xED, 0x20, 0xFC, 0xB1, 0x5B, 0x6A, 0xCB, 0xBE, 0x39, 0x4A, 0x4C, 0x58, 0xCF,
        0xD0, 0xEF, 0xAA, 0xFB, 0x43, 0x4D, 0x33, 0x85, 0x45, 0xF9, 0x02, 0x7F, 0x50, 0x3C, 0x9F, 0xA8,
        0x51, 0xA3, 0x40, 0x8F, 0x92, 0x9D, 0x38, 0xF5, 0xBC, 0xB6, 0xDA, 0x21, 0x10, 0xFF, 0xF3, 0xD2,
        0xCD, 0x0C, 0x13, 0xEC, 0x5F, 0x97, 0x44, 0x17, 0xC4, 0xA7, 0x7E, 0x3D, 0x64, 0x5D, 0x19, 0x73,
        0x60, 0x81, 0x4F, 0xDC, 0x22, 0x2A, 0x90, 0x88, 0x46, 0xEE, 0xB8, 0x14, 0xDE, 0x5E, 0x0B, 0xDB,
        0xE0, 0x32, 0x3A, 0x0A, 0x49, 0x06, 0x24, 0x5C, 0xC2, 0xD3, 0xAC, 0x62, 0x91, 0x95, 0xE4, 0x79,
        0xE7, 0xC8, 0x37, 0x6D, 0x8D, 0xD5, 0x4E, 0xA9, 0x6C, 0x56, 0xF4, 0xEA, 0x65, 0x7A, 0xAE, 0x08,
        0xBA, 0x78, 0x25, 0x2E, 0x1C, 0xA6, 0xB4, 0xC6, 0xE8, 0xDD, 0x74, 0x1F, 0x4B, 0xBD, 0x8B, 0x8A,
        0x70, 0x3E, 0xB5, 0x66, 0x48, 0x03, 0xF6, 0x0E, 0x61, 0x35, 0x57, 0xB9, 0x86, 0xC1, 0x1D, 0x9E,
        0xE1, 0xF8, 0x98, 0x11, 0x69, 0xD9, 0x8E, 0x94, 0x9B, 0x1E, 0x87, 0xE9, 0xCE, 0x55, 0x28, 0xDF,
        0x8C, 0xA1, 0x89, 0x0D, 0xBF, 0xE6, 0x42, 0x68, 0x41, 0x99, 0x2D, 0x0F, 0xB0, 0x54, 0xBB, 0x16
};


unsigned char inv_S[256] = {
        0x52, 0x09, 0x6A, 0xD5, 0x30, 0x36, 0xA5, 0x38, 0xBF, 0x40, 0xA3, 0x9E, 0x81, 0xF3, 0xD7, 0xFB,
        0x7C, 0xE3, 0x39, 0x82, 0x9B, 0x2F, 0xFF, 0x87, 0x34, 0x8E, 0x43, 0x44, 0xC4, 0xDE, 0xE9, 0xCB,
        0x54, 0x7B, 0x94, 0x32, 0xA6, 0xC2, 0x23, 0x3D, 0xEE, 0x4C, 0x95, 0x0B, 0x42, 0xFA, 0xC3, 0x4E,
        0x08, 0x2E, 0xA1, 0x66, 0x28, 0xD9, 0x24, 0xB2, 0x76, 0x5B, 0xA2, 0x49, 0x6D, 0x8B, 0xD1, 0x25,
        0x72, 0xF8, 0xF6, 0x64, 0x86, 0x68, 0x98, 0x16, 0xD4, 0xA4, 0x5C, 0xCC, 0x5D, 0x65, 0xB6, 0x92,
        0x6C, 0x70, 0x48, 0x50, 0xFD, 0xED, 0xB9, 0xDA, 0x5E, 0x15, 0x46, 0x57, 0xA7, 0x8D, 0x9D, 0x84,
        0x90, 0xD8, 0xAB, 0x00, 0x8C, 0xBC, 0xD3, 0x0A, 0xF7, 0xE4, 0x58, 0x05, 0xB8, 0xB3, 0x45, 0x06,
        0xD0, 0x2C, 0x1E, 0x8F, 0xCA, 0x3F, 0x0F, 0x02, 0xC1, 0xAF, 0xBD, 0x03, 0x01, 0x13, 0x8A, 0x6B,
        0x3A, 0x91, 0x11, 0x41, 0x4F, 0x67, 0xDC, 0xEA, 0x97, 0xF2, 0xCF, 0xCE, 0xF0, 0xB4, 0xE6, 0x73,
        0x96, 0xAC, 0x74, 0x22, 0xE7, 0xAD, 0x35, 0x85, 0xE2, 0xF9, 0x37, 0xE8, 0x1C, 0x75, 0xDF, 0x6E,
        0x47, 0xF1, 0x1A, 0x71, 0x1D, 0x29, 0xC5, 0x89, 0x6F, 0xB7, 0x62, 0x0E, 0xAA, 0x18, 0xBE, 0x1B,
        0xFC, 0x56, 0x3E, 0x4B, 0xC6, 0xD2, 0x79, 0x20, 0x9A, 0xDB, 0xC0, 0xFE, 0x78, 0xCD, 0x5A, 0xF4,
        0x1F, 0xDD, 0xA8, 0x33, 0x88, 0x07, 0xC7, 0x31, 0xB1, 0x12, 0x10, 0x59, 0x27, 0x80, 0xEC, 0x5F,
        0x60, 0x51, 0x7F, 0xA9, 0x19, 0xB5, 0x4A, 0x0D, 0x2D, 0xE5, 0x7A, 0x9F, 0x93, 0xC9, 0x9C, 0xEF,
        0xA0, 0xE0, 0x3B, 0x4D, 0xAE, 0x2A, 0xF5, 0xB0, 0xC8, 0xEB, 0xBB, 0x3C, 0x83, 0x53, 0x99, 0x61,
        0x17, 0x2B, 0x04, 0x7E, 0xBA, 0x77, 0xD6, 0x26, 0xE1, 0x69, 0x14, 0x63, 0x55, 0x21, 0x0C, 0x7D
};

/* copy in[16] to state[4][4] */
int loadStateArray(uint8_t (*state)[4], const uint8_t *in) {
    int i = 0,j = 0;
    for (i = 0; i < 4; ++i) {
        for (j = 0; j < 4; ++j) {
            state[j][i] = *in++;
        }
    }
    return 0;
}

/* copy state[4][4] to out[16] */
int storeStateArray(uint8_t (*state)[4], uint8_t *out) {
    int i = 0, j = 0;
    for ( i = 0; i < 4; ++i) {
        for ( j = 0; j < 4; ++j) {
            *out++ = state[j][i];
        }
    }
    return 0;
}

/*Key expansion*/
int keyExpansion(const uint8_t *key, uint32_t keyLen, AesKey *aesKey) {

    if (NULL == key || NULL == aesKey){
        printf("keyExpansion param is NULL\n");
        return -1;
    }

    if (keyLen != 16){
        printf("keyExpansion keyLen = %d, Not support.\n", keyLen);
        return -1;
    }

    uint32_t *w = aesKey->eK;  //encryption key
    uint32_t *v = aesKey->dK;  //Decryption key

    /* keyLen is 16 Bytes, generate uint32_t W[44]. */

    /* W[0-3] */
    int i = 0, j = 0;
    for ( i = 0; i < 4; ++i) {
        LOAD32H(w[i], key + 4*i);
    }

    /* W[4-43] */
    for ( i = 0; i < 10; ++i) {
        w[4] = w[0] ^ MIX(w[3]) ^ rcon[i];
        w[5] = w[1] ^ w[4];
        w[6] = w[2] ^ w[5];
        w[7] = w[3] ^ w[6];
        w += 4;
    }

    w = aesKey->eK+44 - 4;
    /*The decryption key matrix is ​​the reverse order of the encryption key matrix,
     *which is convenient to use. Arrange the 11 matrices of ek in reverse order and 
     *assign them to dk as the decryption key.
     *That is, dk[0-3]=ek[41-44], dk[4-7]=ek[37-40]... dk[41-44]=ek[0-3]
     */
    for ( j = 0; j < 11; ++j) {

        for ( i = 0; i < 4; ++i) {
            v[i] = w[i];
        }
        w -= 4;
        v += 4;
    }

    return 0;
}

/*wheel key plus*/ 
int addRoundKey(uint8_t (*state)[4], const uint32_t *key) {
    uint8_t k[4][4];
    int i = 0, j = 0;
    /* i: row, j: col */
    for ( i = 0; i < 4; ++i) {
        for ( j = 0; j < 4; ++j) {
            /*Convert uint32 key[4] to matrix uint8 k[4][4] first*/
            k[i][j] = (uint8_t) BYTE(key[j], 3 - i); 
            state[i][j] ^= k[i][j];
        }
    }

    return 0;
}

/*Byte replacement*/
int subBytes(uint8_t (*state)[4]) {
    /* i: row, j: col */
     int i = 0, j = 0;
    for (i = 0; i < 4; ++i) {
        for ( j = 0; j < 4; ++j) {
            /*Directly use the original bytes as the S box data subscript*/
            state[i][j] = S[state[i][j]]; 
        }
    }

    return 0;
}

/*reverse byte replacement*/
int invSubBytes(uint8_t (*state)[4]) {
    /* i: row, j: col */
    int i = 0, j = 0;
    for ( i = 0; i < 4; ++i) {
        for ( j = 0; j < 4; ++j) {
            state[i][j] = inv_S[state[i][j]];
        }
    }
    return 0;
}

/*row shift*/
int shiftRows(uint8_t (*state)[4]) {
    uint32_t block[4] = {0};

    /* i: row */
        int i = 0;
    for ( i = 0; i < 4; ++i) {
    /*To facilitate row circular shifting, 
     *first put a row of 4 bytes into a uint_32 structure,
     *and then convert it into an independent 4-byte uint8_t after shifting.
     */
        LOAD32H(block[i], state[i]);
        block[i] = ROF32(block[i], 8*i);
        STORE32H(block[i], state[i]);
    }

    return 0;
}

/*retrograde shift*/
int invShiftRows(uint8_t (*state)[4]) {
    uint32_t block[4] = {0};

    /* i: row */
    int i = 0;
    for (i = 0; i < 4; ++i) {
        LOAD32H(block[i], state[i]);
        block[i] = ROR32(block[i], 8*i);
        STORE32H(block[i], state[i]);
    }

    return 0;
}

/* Galois Field (256) Multiplication of two Bytes
 * Two-byte Galois field multiplication
 */
uint8_t GMul(uint8_t u, uint8_t v) {
    uint8_t p = 0;
    int i = 0;
    for ( i = 0; i < 8; ++i) {
        if (u & 0x01) {    //
            p ^= v;
        }

        int flag = (v & 0x80);
        v <<= 1;
        if (flag) {
            v ^= 0x1B; /* x^8 + x^4 + x^3 + x + 1 */
        }

        u >>= 1;
    }

    return p;
}

/*column mix*/
int mixColumns(uint8_t (*state)[4]) {
    uint8_t tmp[4][4];
    uint8_t M[4][4] = {{0x02, 0x03, 0x01, 0x01},
                       {0x01, 0x02, 0x03, 0x01},
                       {0x01, 0x01, 0x02, 0x03},
                       {0x03, 0x01, 0x01, 0x02}};

    /* copy state[4][4] to tmp[4][4] */
    int i = 0, j = 0;
    for ( i = 0; i < 4; ++i) {
        for ( j = 0; j < 4; ++j){
            tmp[i][j] = state[i][j];
        }
    }

    for ( i = 0; i < 4; ++i) {
        for ( j = 0; j < 4; ++j) { 
            /*Galois field addition and multiplication*/
            state[i][j] = GMul(M[i][0], tmp[0][j]) ^ GMul(M[i][1], tmp[1][j])
                        ^ GMul(M[i][2], tmp[2][j]) ^ GMul(M[i][3], tmp[3][j]);
        }
    }

    return 0;
}

/*reverse column mix*/
int invMixColumns(uint8_t (*state)[4]) {
    uint8_t tmp[4][4];
    uint8_t M[4][4] = {{0x0E, 0x0B, 0x0D, 0x09},
                       {0x09, 0x0E, 0x0B, 0x0D},
                       {0x0D, 0x09, 0x0E, 0x0B},
                       {0x0B, 0x0D, 0x09, 0x0E}};

    /* copy state[4][4] to tmp[4][4] */
    int i = 0, j = 0;
    for ( i = 0; i < 4; ++i) {
        for ( j = 0; j < 4; ++j){
            tmp[i][j] = state[i][j];
        }
    }

    for ( i = 0; i < 4; ++i) {
        for ( j = 0; j < 4; ++j) {
            state[i][j] = GMul(M[i][0], tmp[0][j]) ^ GMul(M[i][1], tmp[1][j])
                          ^ GMul(M[i][2], tmp[2][j]) ^ GMul(M[i][3], tmp[3][j]);
        }
    }

    return 0;
}

/*
*AES-128 encryption interface, the input key should be 16 bytes in length, and the output length should be an integral multiple of 16 bytes.
*In this way, the output length is the same as the input length, and the function calls external memory to allocate memory for the output data.
*/
int aesEncrypt(const uint8_t *key, uint32_t keyLen, const uint8_t *pt, uint8_t *ct, uint32_t len) {
    
    AesKey aesKey;
    uint8_t *pos = ct;
    /*Decryption key pointer*/
    const uint32_t *rk = aesKey.eK;  
    //uint8_t out[BLOCKSIZE] = {0};
    uint8_t actualKey[16] = {0};
    uint8_t state[4][4] = {{}};

    if (NULL == key || NULL == pt || NULL == ct) {
        printf("param err.\n");
        return -1;
    }

    if (keyLen > 16){
        printf("keyLen must be 16.\n");
        return -1;
    }

    if (len % BLOCKSIZE){
        printf("inLen is invalid.\n");
        return -1;
    }

    memcpy(actualKey, key, keyLen);
    /*Key expansion*/
    keyExpansion(actualKey, 16, &aesKey);  

	/*Loop-encrypting data of multiple block lengths using ECB mode*/ 
    uint32_t i = 0, j = 0;
    for ( i = 0; i < len; i += BLOCKSIZE) {
		/*Convert 16-byte plaintext into a 4x4 state matrix for processing*/
        loadStateArray(state, pt);

        addRoundKey(state, rk);

        for ( j = 1; j < 10; ++j) {
            rk += 4;
            subBytes(state);   
            shiftRows(state);  
            mixColumns(state); 
            addRoundKey(state, rk); 
        }

        subBytes(state);    
        shiftRows(state);  
    
        addRoundKey(state, rk+4); 
		
		/*Convert 4x4 state matrix to uint8_t one-dimensional array output and save*/ 
        storeStateArray(state, pos);

        pos += BLOCKSIZE;  
        pt += BLOCKSIZE;   
        rk = aesKey.eK;   
    }
    return 0;
}

/*AES128 decryption, parameter requirements are the same as encryption*/
int aesDecrypt(const uint8_t *key, uint32_t keyLen, const uint8_t *ct, uint8_t *pt, uint32_t len) {
    AesKey aesKey;
    uint8_t *pos = pt;
    const uint32_t *rk = aesKey.dK; 
    //uint8_t out[BLOCKSIZE] = {0};
    uint8_t actualKey[16] = {0};
    uint8_t state[4][4] = {{}};

    if (NULL == key || NULL == ct || NULL == pt){
        printf("param err.\n");
        return -1;
    }

    if (keyLen > 16){
        printf("keyLen must be 16.\n");
        return -1;
    }

    if (len % BLOCKSIZE){
        printf("inLen is invalid.\n");
        return -1;
    }

    memcpy(actualKey, key, keyLen);
    keyExpansion(actualKey, 16, &aesKey);  
    uint32_t i = 0, j = 0;
    for ( i = 0; i < len; i += BLOCKSIZE) {
        /*Convert the 16-byte ciphertext into a 4x4 state matrix for processing*/
        loadStateArray(state, ct);
        /*Round secret key addition, same as encryption*/ 
        addRoundKey(state, rk);

        for ( j = 1; j < 10; ++j) {
            rk += 4;
            invShiftRows(state);   
            invSubBytes(state);     
            addRoundKey(state, rk); 
            invMixColumns(state);   
        }

        invSubBytes(state);   
        invShiftRows(state);
   
        addRoundKey(state, rk+4);  

        storeStateArray(state, pos); 
        pos += BLOCKSIZE;  
        ct += BLOCKSIZE;   
        rk = aesKey.dK;    
    }
    return 0;
}
/*Conveniently output hexadecimal data*/ 
void printHex(uint8_t *ptr, int len, char *tag) {
    printf("%s\ndata[%d]: ", tag, len);
    int i = 0;
    for ( i = 0; i < len; ++i) {
        printf("%.2X ", *ptr++);
    }
    printf("\n");
}

/*Read a hexadecimal string and convert it to a uint8_t type array*/
int read_hex_bytes_from_file(char *path_tmp, const uint8_t *key, slurm_influxdb *data)
{
	FILE *fp_out = NULL;
    char  tmp_str[1000] = {'0'};
    size_t  i = 1;
    int count = 1;
    uint8_t *aes_data1 = NULL;
    uint8_t *aes_data2 = NULL;
    uint8_t *aes_data3 = NULL;
    char *influxdb[3];
    if(path_tmp == NULL || key == NULL || data== NULL) {
        return -1;
    }

	fp_out = fopen(path_tmp, "r");
	if (fp_out != NULL) {
		while (fgets(tmp_str, 1000, fp_out) != NULL) {
             
			  switch (count) {
                case 1:
                    aes_data1 = (uint8_t *)malloc(strlen(tmp_str));
                    for (i = 0; i < strlen(tmp_str)-1; i+=2) {
                        char byte[2] = {tmp_str[i], tmp_str[i + 1]};
                        sscanf(byte, "%02X", (unsigned int *)&aes_data1[i / 2]);
                    }
                    aesDecrypt(key, 16, aes_data1, plain1, 32);

                    free(aes_data1);
                    break;
                case 2:
                    aes_data2 = (uint8_t *)malloc(strlen(tmp_str));   

                    for (i = 0; i <  strlen(tmp_str)-1; i+=2) {
                        char byte[2] = {tmp_str[i], tmp_str[i + 1]};
                        sscanf(byte, "%02X", (unsigned int *)&aes_data2[i / 2]);
                    }

                    aesDecrypt(key, 16, aes_data2, plain2, 32);
                    free(aes_data2);
                    break;

                case 3:
                    aes_data3 = (uint8_t *)malloc(strlen(tmp_str));
                    
                    for (i = 0; i < strlen(tmp_str)-1; i+=2) {
                        char byte[2] = {tmp_str[i], tmp_str[i + 1]};
                        sscanf(byte, "%02X", (unsigned int *)&aes_data3[i / 2]);
                    }
                    aesDecrypt(key, 16, aes_data3, plain3, 32);
                    free(aes_data3);
                    break;
                default:
                    if(count >= 4 && count <= 6) {
                        influxdb[count-4] = (char *)malloc(strlen(tmp_str));
                        for (i = 0; i < strlen(tmp_str)-1; i += 2) {
                            char byte[2] = {tmp_str[i], tmp_str[i + 1]};
                            sscanf(byte, "%02X", (unsigned int *)&influxdb[count-4][i / 2]);
                        }

                        if((count - 4)==0) {
                            strcpy(data->database, influxdb[count-4]);
                        }

                        if((count - 4)==1) {
                            strcpy(data->host, influxdb[count-4]);
                        }   

                        if((count - 4)==2) {
                            strcpy(data->policy, influxdb[count-4]);
                        }  
                        free(influxdb[count-4]);

                    }              
                    break;
                }
                char *ptr = NULL;
                ptr = strtok((char *)plain3, "#");

                if (ptr != NULL) {
                   
                    ptr = strtok((char *)plain3, ":");

                    if (ptr != NULL) {
                        int length1 =  atoi(ptr);
                        strncpy(data->username, (const char *)plain1, length1 + 1);
                        data->username[length1] = '\0';
                        ptr=strtok(NULL,",");
                        if (ptr != NULL) {
                        int length2 =  atoi(ptr);
                        strncpy(data->password, (const char *)plain2, length2 + 1);
                        data->password[length2] = '\0';
                        
                        }   
                    }
                }
              count++;
		}
		fclose(fp_out);
		
	} 
    return 0;
}

/* Callback to handle the HTTP response */
static size_t write_callback(void *contents, size_t size, size_t nmemb,
			      void *userp)
{
	size_t realsize = size * nmemb;
	struct http_response *mem = (struct http_response *) userp;

	mem->message = realloc(mem->message, mem->size + realsize + 1);
	memcpy(&(mem->message[mem->size]), contents, realsize);
	mem->size += realsize;
	mem->message[mem->size] = 0;
	return realsize;
}


char* influxdb_connect(slurm_influxdb *data, const char* sql, int type)
{
    struct http_response chunk;
    CURL *curl;
    CURLcode res;
    /*Assuming the maximum length of the URL does not exceed 2048 characters*/
    // char url[2048]; 
    //const char *needle = "select";

    if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
        printf("influxdb_connect init curl global all failed \n");
        return NULL;
    } else if ((curl = curl_easy_init()) == NULL) {
        printf("influxdb_connect init curl failed \n");
        return NULL;      
    }

    chunk.message = malloc(1);
	chunk.size = 0;
    
    // Initialize libcurl

    if(curl) {
       
        char *url1 = (char*)malloc(200);
        char *url2 = (char*)malloc(100 + strlen(sql));
        char *policy = _parse_rt_policy(data->policy, type);
        sprintf(url1, "%s/query?db=%s&rp=%s&precision=s", data->host, data->database, policy);
        if(policy) free(policy);
        sprintf(url2,"q=%s;",sql);
        curl_easy_setopt(curl, CURLOPT_URL, url1);

		curl_easy_setopt(curl, CURLOPT_USERNAME,
				 data->username);

        curl_easy_setopt(curl, CURLOPT_PASSWORD,
				  data->password);

        /*Set timeout to 300 seconds*/
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);

        curl_easy_setopt(curl, CURLOPT_POST, 1);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, url2);


        /* Set callback function to receive response data*/
  
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);

        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &chunk);
 
        /*Perform the HTTP GET request*/
        	static int error_cnt = 0;

        res = curl_easy_perform(curl);

        /* Check for errors*/
        if(res != CURLE_OK) {
            fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
            if ((error_cnt++ % 100) == 0)
            printf("curl_easy_perform failed to send data (discarded). Reason %s\n", curl_easy_strerror(res));
        }
        curl_easy_cleanup(curl);
        free(url1);
        free(url2);
    }
    if(res != CURLE_OK)
        return NULL;
    else
        return chunk.message;
    
}

/*Parse JSON data*/
void parse_json(const char *response,  char *username, int flag, time_t current_time) {
    json_t *root = NULL;
    json_error_t error;
    size_t r;
    root = json_loads(response, 0, &error);
    if (!root) {
        fprintf(stderr, "error: on line %d: %s\n", error.line, error.text);
        return;
    }
    /*Get the "results" array*/ 
    json_t *results = json_object_get(root, "results");
    size_t num_results = json_array_size(results);
    /*Iterate through each result*/ 
    for ( r = 0; r < num_results; ++r) {
        size_t s;
        json_t *result = json_array_get(results, r);
        json_t *series = json_object_get(result, "series");
        size_t num_series = json_array_size(series);
        s = params.desc_set ? num_series - 1 : 0;
        while(s < num_series) {
            interface_sjinfo_t *iinfo_overall = NULL;
            json_t *series_element = json_array_get(series, s);
            json_t *tags = json_object_get(series_element, "tags");
            const char *step = json_string_value(json_object_get(tags, "step"));
            const char* jobid = json_string_value(json_object_get(tags, "jobid"));
            json_t *columns = json_object_get(series_element, "columns");
            json_t *values = json_object_get(series_element, "values");
            iinfo_overall = malloc(sizeof(*iinfo_overall));
            iinfo_overall->time = malloc(60);
            iinfo_overall->username = malloc(60);
            if(jobid)
                iinfo_overall->jobid = atoi(jobid);
            if(step)
                iinfo_overall->stepid = atoi(step);
            iinfo_overall->sum_cpu = 0;
            iinfo_overall->sum_pid = 0;
            iinfo_overall->sum_node = 0;
            
            strcpy(iinfo_overall->username, username);

            if(flag == UNIT_STEP) {
                size_t i,j;
                size_t num_rows = json_array_size(values);
                i = params.desc_set ? num_rows - 1 : 0;
                while(i < num_rows){
                    json_t *row = json_array_get(values, i);
                    interface_sjinfo_t *iinfo = malloc(sizeof(*iinfo));  

                    iinfo->time = malloc(60);
                    iinfo->username = malloc(60);
                    if(jobid)
                        iinfo->jobid = atoi(jobid);
                    if(step)
                        iinfo->stepid = atoi(step);
                    strcpy(iinfo->username, username);

                    for (j = 0; j < json_array_size(columns); ++j) {
                        json_t *value = json_array_get(row, j);
                        const char * tmp_name = json_string_value(json_array_get(columns, j));
                        if ((strcmp(tmp_name, "last_stepavecpu") == 0)) {
                            iinfo->stepcpuave = json_real_value(value);
                        } else if (strcmp(tmp_name, "last_stepcpu") == 0) {
                            iinfo->stepcpu = json_real_value(value);
                        } else if (strcmp(tmp_name, "last_stepmem") == 0) {
                            iinfo->stepmem = (long long)json_integer_value(value);
                        } else if (strcmp(tmp_name, "last_stepvmem") == 0) {
                            iinfo->stepvmem = (long long)json_integer_value(value);
                        } else if (strcmp(tmp_name, "last_steppages") == 0) {
                            iinfo->steppages = (long long)json_integer_value(value);
                        }  else if (strcmp(tmp_name, "max_stepcpu") == 0) {
                            iinfo->stepcpumax = json_real_value(value);
                        } else if (strcmp(tmp_name, "min_stepcpu") == 0) {
                            iinfo->stepcpumin = json_real_value(value);
                        } else if (strcmp(tmp_name, "max_stepmem") == 0) {
                            iinfo->stepmemmax = json_integer_value(value);
                        } else if (strcmp(tmp_name, "min_stepmem") == 0) {
                            iinfo->stepmemmin = json_integer_value(value);
                        } else if (strcmp(tmp_name, "max_stepvmem") == 0) {
                            iinfo->stepvmemmax = json_integer_value(value);
                        } else if (strcmp(tmp_name, "min_stepvmem") == 0) {
                            iinfo->stepvmemmin = json_integer_value(value);
                        } 
                    }
                    list_append(print_value_list, iinfo);
                    (params.desc_set ? --i : ++i);
                }
            }else if(flag == UNIT_EVENT) {
                size_t i,j;
                size_t num_rows = json_array_size(values);
                i = params.desc_set ? num_rows - 1 : 0;
                while(i < num_rows){
                    json_t *row = json_array_get(values, i);
                    interface_sjinfo_t *iinfo = malloc(sizeof(*iinfo));
                    iinfo->time = malloc(60);
                    iinfo->username = malloc(60);
                    if(jobid)
                        iinfo->jobid = atoi(jobid);
                    if(step)
                        iinfo->stepid = atoi(step);
                    strcpy(iinfo->username, username);
                    for (j = 0; j < json_array_size(columns); ++j) {
                        json_t *value = json_array_get(row, j);
                        if(value == NULL)
                            continue;
                        const char * tmp_name = json_string_value(json_array_get(columns, j));
                        if (strcmp(tmp_name, "stepcpu") == 0) {
                            iinfo->stepcpu = json_real_value(value);
                        } else if (strcmp(tmp_name, "stepmem") == 0) {
                            iinfo->stepmem = (long long)json_integer_value(value);
                        } else if (strcmp(tmp_name, "stepvmem") == 0) {
                            iinfo->stepvmem = (long long)json_integer_value(value);
                        } else if (strcmp(tmp_name, "steppages") == 0) {
                            iinfo->steppages = (long long)json_integer_value(value);
                        } else if (strcmp(tmp_name, "cputhreshold") == 0) {
                            iinfo->cputhreshold = (long long)json_integer_value(value);
                        } else if (strcmp(tmp_name, "start") == 0) {
                            iinfo->start = (unsigned long)json_integer_value(value);
                            iinfo_overall->start_last = ((unsigned long)json_integer_value(value) > iinfo_overall->start_last) ? (unsigned long)json_integer_value(value) : iinfo_overall->start_last;
                        } else if (strcmp(tmp_name, "end") == 0) {
                            iinfo->end = (unsigned long)json_integer_value(value);
                            iinfo_overall->end_last = ((unsigned long)json_integer_value(value) > iinfo_overall->end_last) ? (unsigned long)json_integer_value(value) : iinfo_overall->end_last;
                        } else if (strcmp(tmp_name, "type1") == 0) {
                            iinfo->type1 = atoi(json_string_value(value));
                            if(iinfo->type1)
                                iinfo_overall->sum_cpu++;
                        } else if (strcmp(tmp_name, "type2") == 0) {
                            iinfo->type2 = atoi(json_string_value(value));
                            if(iinfo->type2) 
                                iinfo_overall->sum_pid++;
                        } else if (strcmp(tmp_name, "type3") == 0) {
                            iinfo->type3 = atoi(json_string_value(value));
                            if(iinfo->type3) 
                                iinfo_overall->sum_node++;
                        }
                    }
                    if(params.level & 0x0010){
                        list_append(print_events_value_list, iinfo);
                    }else{
                        free(iinfo->time);
                        free(iinfo->username);
                        free(iinfo);
                    }
                    (params.desc_set ? --i : ++i);
                }
            }else if(flag == UNIT_RUNJOB) {
                size_t i, j;
                size_t num_rows = json_array_size(values);
                i = params.desc_set ? num_rows - 1 : 0;
                while(i < num_rows){
                    json_t *row = json_array_get(values, i);
                    unsigned long interval_time = 0;
                    const char *ctime = NULL;
                    struct tm timeinfo;
                    int year, month, day, hour, minute, second;
                    for (j = 0; j < json_array_size(columns); ++j) {
                        json_t *value = json_array_get(row, j);
                        const char * tmp_name = json_string_value(json_array_get(columns, j));
                        if (strcmp(tmp_name, "ctime") == 0) {
                            ctime = json_string_value(value);
                            if (!ctime) {
                                fprintf(stderr, "Error: ctime is NULL at row %zu\n", i);
                                continue;
                            }
                        } else if (strcmp(tmp_name, "interval_time") == 0) {
                            if (json_is_integer(value)) {
                                interval_time = (unsigned long)json_integer_value(value);
                            } else {
                                fprintf(stderr, "Error: interval_time is not an integer at row %zu\n", i);
                                continue;
                            }
                        }
                    }
                    if (ctime) {
                        int matched = sscanf(ctime, "%d-%d-%dT%d:%d:%dZ", &year, &month, &day, &hour, &minute, &second);
                        if (matched != 6) {
                            fprintf(stderr, "Error: Failed to parse time string '%s'. Matched fields: %d\n", ctime, matched);
                            continue;
                        }
                    } else {
                        fprintf(stderr, "Error: ctime is not set for row %zu\n", i);
                        continue;
                    }
                    
                    memset(&timeinfo, 0, sizeof(struct tm));
                    timeinfo.tm_year = year - 1900;
                    timeinfo.tm_mon = month - 1;
                    timeinfo.tm_mday = day;
                    timeinfo.tm_hour = hour;
                    timeinfo.tm_min = minute;
                    timeinfo.tm_sec = second;
                    time_t job_time = timegm(&timeinfo); // Convert to UTC time
                    double diff = difftime(current_time, job_time);

                    if (diff <= interval_time) {
                        if(c_string_len(job_list) != 0){
                            c_string_append_str(job_list, ",");
                        }
                        c_string_append_str(job_list, jobid);
                    }
                    (params.desc_set ? --i : ++i);
                }
            }
            (params.desc_set ? --s : ++s);
            if((params.level & 0x0100) && (flag == UNIT_EVENT)) {
                list_append(print_overall_value_list, iinfo_overall);
            }else{
                free(iinfo_overall->time);
                free(iinfo_overall->username);
                free(iinfo_overall);
            }
        }
    }
    json_decref(root);
}


/* print this version of sjinfo */
void print_sjinfo_version(void)
{
	printf("%s %s\n", PACKAGE_NAME, SJINFO_VERSION_STRING);
}

/* print help */
void print_sjinfo_help(void)
{
    printf(
"sjinfo [<OPTION>]                                                         \n"
"    Valid <OPTION> values are:                                            \n"
"     -a, --all:                                                      \n"
"        When this value is specified, it is equivalent to specifying -l, -A, \n"
"        and -O simultaneously.                                             \n"
"     -A, --abnormal:                                                      \n"
"        Displays information about abnormal events during job execution   \n"
"     -d, --desc:                                                      \n"
"        Output data in reverse order  \n"
"     -e, --event:                                                        \n"
"        Print the abnormal events of the jobs.                            \n"
"        Supported fields:                                                 \n"
"        CPUUSA - CPU Utilization State Anomaly                            \n"
"        PidSA - Process State Anomaly                                    \n"
"        NodeSA - Node State Anomaly                                      \n"
"     -E, --end:                                                           \n"
"        The end time of the abnormal event.                              \n"
"     -h, --help:                                                          \n"
"        Help manual.                                                      \n"
"     -j, --jobs:                                                          \n"
"        Specify the job ID.                                               \n"
"     -l, --load:                                                           \n"
"        Displays load information during job run time          \n"
"     -o, --format:                                                        \n"
"        Print a list of fields that can be specified with the            \n"
"        '--format' option                                                 \n"
"        '--format='    JobID,StepID,StepCPU,StepAVECPU,StepMEM,StepVMEM,         \n"
"                       StepPages,MaxStepCPU,MinStepCPU,MaxStepMEM,            \n"
"                       MinStepMEM,MaxStepVMEM,MinStepVMEM,CPUthreshold,        \n"
"                       Start,End,Type,Last_start,Last_end,Sum_CPU,     \n"
"                       Sum_PID,Sum_NODE     \n"
"                                                                           \n"
"        Fields related to resource consumption:                           \n"
"        JobID:         Job ID                                                 \n"
"        StepID:        Job step ID                                            \n"
"        StepCPU:       CPU utilization within job step anomaly detection interval. \n"
"        StepAVECPU:    Average CPU utilization of job step.                 \n"
"        StepMEM:       Real-time usage of job step memory.                    \n"
"        StepVMEM:      Real-time virtual memory usage of job steps.          \n"
"        StepPages:     Job step pagefault real-time size during                \n"
"                       the current job step running cycle.                    \n"
"        MaxStepCPU:    Maximum CPU utilization during the current job        \n"
"                       step running cycle.                                    \n"
"        MinStepCPU:    Minimum CPU utilization during the current job        \n"
"                       step running cycle.                                    \n"
"        MaxStepMEM:    Maximum memory value within the current                \n"
"                       job step running cycle.                                \n"
"        MinStepMEM:    Minimum memory value within the current job            \n"
"                       step running cycle.                                    \n"
"        MaxStepVMEM:   The maximum value of virtual memory during            \n"
"                       the current job step running cycle.                   \n"
"        MinStepVMEM:   The minimum value of virtual memory during the        \n"
"                       current job step running cycle.                       \n"
"                                                                             \n"
"        Fields related to abnormal events:                                \n"
"        CPUthreshold:  Set the CPU utilization threshold for a job.         \n"
"        Start:         The start time of the abnormal event.                  \n"
"        End:           The end time of the exception event.                   \n"
"        Type:          Type of abnormal event.                                \n"
"                                                                             \n"
"        Fields related to abnormal events overall:                             \n"
"        Last_start:    The start time of the most recent anomaly.        \n"
"        Last_end:      The end time of the most recent anomaly.                \n"
"        Sum_CPU:       Total number of CPU abnormal events.                   \n"
"        Sum_PID:       Total number of PROCESS abnormal events.                    \n"
"        Sum_NODE:      Total number of NODE abnormal events.                    \n"
"     -O, --overall:                                                           \n"
"        Displays general information about the abnormal event          \n"
"     -r, --running:                                                           \n"
"        Display running job data (this option depends on the acquisition\n"
"        interval set by the job-monitor and may not be real-time)          \n"
"        When retrieving a running job, the time interval is limited to one hour, \n"
"        meaning that any data that has not been updated in more than one hour is \n"
"        considered finished. This will cover the vast majority of cases. If the \n"
"        data collection interval is longer than one hour, you can use -S to \n"
"        specify a larger interval.\n"
"     -s, --steps:                                                          \n"
"        Specify the steps.                                               \n"
"     -S, --start:                                                         \n"
"        The start time of the job's abnormal event (e.g., 2024-05-07T08:00:00). \n"
"     -V, --version:                                                       \n"
"        Print sjinfo version.                                              \n"
"     -m                                                                    \n"
"        Convert KB to MB (default is in KB).                               \n"
"     -g                                                                    \n"
"        Convert KB to GB (default is in KB).                               \n"
"\n");
}

void time_format(char *time_go, time_t tran_time, bool now) 
{
   
    time_t rawtime = time(NULL);
    
    if(now) {
        struct tm *timeinfo = gmtime(&rawtime);
        char time_go1[21];
        strftime(time_go1, sizeof(time_go1), "%Y-%m-%dT%H:%M:%SZ", timeinfo);
        sprintf(time_go,"'%s'",time_go1);
    } else {
        struct tm *timeinfo = gmtime(&tran_time);
        /*ISO 8601 format*/
        if(tran_time !=0 ) {
            char time_go1[21]; 
            strftime(time_go1, sizeof(time_go1), "%Y-%m-%dT%H:%M:%SZ", timeinfo);
            sprintf(time_go,"'%s'",time_go1);
        } else {
            /* Getting the local time */
            struct tm timeinfo = *localtime(&rawtime);
            /* Gets the start time of the date on the day of the local time */
            timeinfo.tm_hour = 0;
            timeinfo.tm_min = 0;
            timeinfo.tm_sec = 0;

            time_t local_start_of_today = mktime(&timeinfo);
            /* Convert to utc time */
            struct tm *utc_timeinfo = gmtime(&local_start_of_today);

            char time_go1[21];
            strftime(time_go1, sizeof(time_go1), "%Y-%m-%dT%H:%M:%SZ", utc_timeinfo);
            sprintf(time_go, "'%s'", time_go1);
        }
    }
    
}

extern void destroy_config_vale(void *object)
{
	interface_sjinfo_t *key_pair_ptr = (interface_sjinfo_t *)object;

	if (key_pair_ptr) {
		free(key_pair_ptr->time);
		free(key_pair_ptr->username);
		free(key_pair_ptr);
	}
    
}

extern void destroy_config_key_pair(void *object)
{
	interface_sjinfo_t *key_pair_ptr = (interface_sjinfo_t *)object;

	if (key_pair_ptr) {
		free(key_pair_ptr->time);
		free(key_pair_ptr->username);
		free(key_pair_ptr);
	}
}

void sjinfo_init(slurm_influxdb *influxdb_data)
{
    /*given sufficient length*/
    influxdb_data->username = malloc(32) ;
    influxdb_data->password = malloc(32);
    influxdb_data->database = malloc(640) ;
    influxdb_data->host = malloc(640);
    influxdb_data->policy = malloc(640);
    /*step table*/
	print_fields_list = list_create(NULL);
	print_fields_itr = list_iterator_create(print_fields_list);

    print_value_list = list_create(destroy_config_vale);
    /*event table*/
    print_events_list = list_create(NULL);
	print_events_itr = list_iterator_create(print_events_list);
    print_events_value_list = list_create(destroy_config_key_pair);

    /*event overall table*/
    print_overall_list = list_create(NULL);
	print_overall_itr = list_iterator_create(print_overall_list);
    print_overall_value_list = list_create(destroy_config_key_pair);

    job_list = c_string_create();
}


void sjinfo_fini(slurm_influxdb * influxdb_data)
{
    free(influxdb_data->username);
    free(influxdb_data->password);
    free(influxdb_data->database);
    free(influxdb_data->host);
    free(influxdb_data->policy);
    /*step table*/
	if (print_fields_itr)
		list_iterator_destroy(print_fields_itr);
    if(print_fields_list)
	    FREE_NULL_LIST(print_fields_list);
    if(print_value_list)
	    FREE_NULL_LIST(print_value_list);

    /*event table*/
    if(print_events_itr)
        list_iterator_destroy(print_events_itr);
    if(print_events_list)
        FREE_NULL_LIST(print_events_list);
    if(print_events_value_list)
	    FREE_NULL_LIST(print_events_value_list);
    
    /*event overall table*/
    if(print_overall_itr)
        list_iterator_destroy(print_overall_itr);
    if(print_overall_list)
        FREE_NULL_LIST(print_overall_list);
    if(print_overall_value_list)
	    FREE_NULL_LIST(print_overall_value_list);
    
}

time_t slurm_mktime(struct tm *tp)
{
	/* Force tm_isdt to -1. */
	tp->tm_isdst = -1;
	return mktime(tp);
}

/* convert "HH:MM[:SS] [AM|PM]" string to numeric values
 * time_str (in): string to parse
 * pos (in/out): position of parse start/end
 * hour, minute, second (out): numberic values
 * RET: -1 on error, 0 otherwise
 */
static int _get_time(const char *time_str, int *pos, int *hour, int *minute,
		     int *second)
{
	int hr, min, sec;
	int offset = *pos;

	/* get hour */
	if ((time_str[offset] < '0') || (time_str[offset] > '9'))
		goto prob;
	hr = time_str[offset++] - '0';
	if (time_str[offset] != ':') {
		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		hr = (hr * 10) + time_str[offset++] - '0';
	}
	if (hr > 23) {
		offset -= 2;
		goto prob;
	}
	if (time_str[offset] != ':')
		goto prob;
	offset++;

	/* get minute */
	if ((time_str[offset] < '0') || (time_str[offset] > '9'))
                goto prob;
	min = time_str[offset++] - '0';
	if ((time_str[offset] < '0') || (time_str[offset] > '9'))
		goto prob;
	min = (min * 10)  + time_str[offset++] - '0';
	if (min > 59) {
		offset -= 2;
		goto prob;
	}

	/* get optional second */
	if (time_str[offset] == ':') {
		offset++;
		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		sec = time_str[offset++] - '0';
		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		sec = (sec * 10)  + time_str[offset++] - '0';
		if (sec > 59) {
			offset -= 2;
			goto prob;
		}
	} else
		sec = 0;

	while (isspace((int)time_str[offset])) {
		offset++;
	}
	if (strncasecmp(time_str + offset, "pm", 2)== 0) {
		hr += 12;
		if (hr > 23) {
			if (hr == 24)
				hr = 12;
			else
				goto prob;
		}
		offset += 2;
	} else if (strncasecmp(time_str + offset, "am", 2) == 0) {
		if (hr > 11) {
			if (hr == 12)
				hr = 0;
			else
				goto prob;
		}
		offset += 2;
	}

	*pos = offset - 1;
	*hour   = hr;
	*minute = min;
	*second = sec;
	return 0;

 prob:	*pos = offset;
	return -1;
}

/* convert "MMDDYY" "MM.DD.YY" or "MM/DD/YY" string to numeric values
 * or "YYYY-MM-DD string to numeric values
* time_str (in): string to parse
 * pos (in/out): position of parse start/end
 * month, mday, year (out): numberic values
 * RET: -1 on error, 0 otherwise
 */
static int _get_date(const char *time_str, int *pos, int *month, int *mday,
		     int *year)
{
	int mon, day, yr;
	int offset = *pos;
	int len;

	if (!time_str)
		goto prob;

	len = strlen(time_str);

	if ((len >= (offset+7)) && (time_str[offset+4] == '-')
	    && (time_str[offset+7] == '-')) {
		/* get year */
		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		yr = time_str[offset++] - '0';

		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		yr = (yr * 10) + time_str[offset++] - '0';

		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		yr = (yr * 10) + time_str[offset++] - '0';

		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		yr = (yr * 10) + time_str[offset++] - '0';

		offset++; // for the -

		/* get month */
		mon = time_str[offset++] - '0';
		if ((time_str[offset] >= '0') && (time_str[offset] <= '9'))
			mon = (mon * 10) + time_str[offset++] - '0';
		if ((mon < 1) || (mon > 12)) {
			offset -= 2;
			goto prob;
		}

		offset++; // for the -

		/* get day */
		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		day = time_str[offset++] - '0';
		if ((time_str[offset] >= '0') && (time_str[offset] <= '9'))
			day = (day * 10) + time_str[offset++] - '0';
		if ((day < 1) || (day > 31)) {
			offset -= 2;
			goto prob;
		}

		*pos = offset - 1;
		*month = mon - 1;	/* zero origin */
		*mday  = day;
		*year  = yr - 1900;     /* need to make it slurm_mktime
					   happy 1900 == "00" */
		return 0;
	}

	/* get month */
	mon = time_str[offset++] - '0';
	if ((time_str[offset] >= '0') && (time_str[offset] <= '9'))
		mon = (mon * 10) + time_str[offset++] - '0';
       	if ((mon < 1) || (mon > 12)) {
		offset -= 2;
		goto prob;
	}
	if ((time_str[offset] == '.') || (time_str[offset] == '/'))
		offset++;

	/* get day */
	if ((time_str[offset] < '0') || (time_str[offset] > '9'))
		goto prob;
	day = time_str[offset++] - '0';
	if ((time_str[offset] >= '0') && (time_str[offset] <= '9'))
		day = (day * 10) + time_str[offset++] - '0';
	if ((day < 1) || (day > 31)) {
		offset -= 2;
		goto prob;
	}
	if ((time_str[offset] == '.') || (time_str[offset] == '/'))
		offset++;

	/* get optional year */
	if ((time_str[offset] >= '0') && (time_str[offset] <= '9')) {
		yr = time_str[offset++] - '0';
		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		yr = (yr * 10) + time_str[offset++] - '0';
	} else
		yr = 0;

	*pos = offset - 1;
	*month = mon - 1;	/* zero origin */
	*mday  = day;
	if (yr)
		*year  = yr + 100;	/* 1900 == "00" */
	return 0;

 prob:	*pos = offset;
	return -1;
}

/* convert time differential string into a number of seconds
 * time_str (in): string to parse
 * pos (in/out): position of parse start/end
 * delta (out): delta in seconds
 * RET: -1 on error, 0 otherwise
 */
static int _get_delta(const char *time_str, int *pos, long *delta)
{
	int i, offset;
	long cnt = 0;
	int digit = 0;

	for (offset = (*pos) + 1;
	     ((time_str[offset] != '\0') && (time_str[offset] != '\n'));
	     offset++) {
		if (isspace((int)time_str[offset]))
			continue;
		for (i=0; un[i].name; i++) {
			if (!strncasecmp((time_str + offset),
					 un[i].name, un[i].name_len)) {
				offset += un[i].name_len;
				cnt    *= un[i].multiplier;
				break;
			}
		}
		if (un[i].name)
			break;	/* processed unit name */
		if ((time_str[offset] >= '0') && (time_str[offset] <= '9')) {
			cnt = (cnt * 10) + (time_str[offset] - '0');
			digit++;
			continue;
		}
		goto prob;
	}

	if (!digit)	/* No numbers after the '=' */
		return -1;

	*pos = offset - 1;
	*delta = cnt;
	return 0;

 prob:	*pos = offset - 1;
	return -1;
}

/* Convert string to equivalent time value
 * input formats:
 *   today or tomorrow
 *   midnight, noon, fika (3 PM), teatime (4 PM)
 *   HH:MM[:SS] [AM|PM]
 *   MMDD[YY] or MM/DD[/YY] or MM.DD[.YY]
 *   MM/DD[/YY]-HH:MM[:SS]
 *   YYYY-MM-DD[THH:MM[:SS]]
 *   now[{+|-}count[seconds(default)|minutes|hours|days|weeks]]
 *
 * Invalid input results in message to stderr and return value of zero
 * NOTE: not thread safe
 * NOTE: by default this will look into the future for the next time.
 * if you want to look in the past set the past flag.
 */
extern time_t parse_time(const char *time_str, int past)
{
	time_t time_now;
	struct tm time_now_tm;
	int    hour = -1, minute = -1, second = 0;
	int    month = -1, mday = -1, year = -1;
	int    pos = 0;
	struct tm res_tm;
	time_t ret_time;

	if (strncasecmp(time_str, "uts", 3) == 0) {
		char *last = NULL;
		long uts = strtol(time_str+3, &last, 10);
		if ((uts < 1000000) || (uts == LONG_MAX) ||
		    (last == NULL) || (last[0] != '\0'))
			goto prob;
		return (time_t) uts;
	}

	time_now = time(NULL);
	localtime_r(&time_now, &time_now_tm);

	for (pos=0; ((time_str[pos] != '\0') && (time_str[pos] != '\n'));
	     pos++) {
		if (isblank((int)time_str[pos]) ||
		    (time_str[pos] == '-') || (time_str[pos] == 'T'))
			continue;
		if (strncasecmp(time_str+pos, "today", 5) == 0) {
			month = time_now_tm.tm_mon;
			mday = time_now_tm.tm_mday;
			year = time_now_tm.tm_year;
			pos += 4;
			continue;
		}
		if (strncasecmp(time_str+pos, "tomorrow", 8) == 0) {
			time_t later = time_now + (24 * 60 * 60);
			struct tm later_tm;
			localtime_r(&later, &later_tm);
			month = later_tm.tm_mon;
			mday = later_tm.tm_mday;
			year = later_tm.tm_year;
			pos += 7;
			continue;
		}
		if (strncasecmp(time_str+pos, "midnight", 8) == 0) {
			hour   = 0;
			minute = 0;
			second = 0;
			pos += 7;
			continue;
		}
		if (strncasecmp(time_str+pos, "noon", 4) == 0) {
			hour   = 12;
			minute = 0;
			second = 0;
			pos += 3;
			continue;
		}
		if (strncasecmp(time_str+pos, "fika", 4) == 0) {
			hour   = 15;
			minute = 0;
			second = 0;
			pos += 3;
			continue;
		}
		if (strncasecmp(time_str+pos, "teatime", 7) == 0) {
			hour   = 16;
			minute = 0;
			second = 0;
			pos += 6;
			continue;
		}
		if (strncasecmp(time_str+pos, "now", 3) == 0) {
			int i;
			long delta = 0;
			time_t later;
			struct tm later_tm;
			for (i=(pos+3); ; i++) {
				if (time_str[i] == '+') {
					pos += i;
					if (_get_delta(time_str, &pos, &delta))
						goto prob;
					break;
				}
				if (time_str[i] == '-') {
					pos += i;
					if (_get_delta(time_str, &pos, &delta))
						goto prob;
					delta = -delta;
					break;
				}
				if (isblank((int)time_str[i]))
					continue;
				if ((time_str[i] == '\0')
				    || (time_str[i] == '\n')) {
					pos += (i-1);
					break;
				}
				pos += i;
				goto prob;
			}
			later    = time_now + delta;
			localtime_r(&later, &later_tm);
			month = later_tm.tm_mon;
			mday = later_tm.tm_mday;
			year = later_tm.tm_year;
			hour = later_tm.tm_hour;
			minute = later_tm.tm_min;
			second = later_tm.tm_sec;
			continue;
		}

		if ((time_str[pos] < '0') || (time_str[pos] > '9'))
			/* invalid */
			goto prob;
		/* We have some numeric value to process */
		if ((time_str[pos+1] == ':') || (time_str[pos+2] == ':')) {
			/* Parse the time stamp */
			if (_get_time(time_str, &pos, &hour, &minute, &second))
				goto prob;
			continue;
		}

		if (_get_date(time_str, &pos, &month, &mday, &year))
			goto prob;
	}
/* 	printf("%d/%d/%d %d:%d\n",month+1,mday,year,hour+1,minute);  */


	if ((hour == -1) && (month == -1))		/* nothing specified, time=0 */
		return (time_t) 0;
	else if ((hour == -1) && (month != -1)) {	/* date, no time implies 00:00 */
		hour = 0;
		minute = 0;
	}
	else if ((hour != -1) && (month == -1)) {
		/* time, no date implies soonest day */
		if (past || (hour >  time_now_tm.tm_hour)
		    ||  ((hour == time_now_tm.tm_hour)
			 && (minute > time_now_tm.tm_min))) {
			/* today */
			month = time_now_tm.tm_mon;
			mday = time_now_tm.tm_mday;
			year = time_now_tm.tm_year;
		} else {/* tomorrow */
			time_t later = time_now + (24 * 60 * 60);
			struct tm later_tm;
			localtime_r(&later, &later_tm);
			month = later_tm.tm_mon;
			mday = later_tm.tm_mday;
			year = later_tm.tm_year;
		}
	}
	if (year == -1) {
		if (past) {
			if (month > time_now_tm.tm_mon) {
				/* last year */
				year = time_now_tm.tm_year - 1;
			} else  {
				/* this year */
				year = time_now_tm.tm_year;
			}
		} else if ((month  >  time_now_tm.tm_mon)
			   ||  ((month == time_now_tm.tm_mon)
				&& (mday > time_now_tm.tm_mday))
			   ||  ((month == time_now_tm.tm_mon)
				&& (mday == time_now_tm.tm_mday)
				&& (hour >  time_now_tm.tm_hour))
			   ||  ((month == time_now_tm.tm_mon)
				&& (mday == time_now_tm.tm_mday)
				&& (hour == time_now_tm.tm_hour)
				&& (minute > time_now_tm.tm_min))) {
			/* this year */
			year = time_now_tm.tm_year;
		} else {
			/* next year */
			year = time_now_tm.tm_year + 1;
		}
	}

	/* convert the time into time_t format */
	memset(&res_tm, 0, sizeof(res_tm));
	res_tm.tm_sec   = second;
	res_tm.tm_min   = minute;
	res_tm.tm_hour  = hour;
	res_tm.tm_mday  = mday;
	res_tm.tm_mon   = month;
	res_tm.tm_year  = year;

/* 	printf("%d/%d/%d %d:%d\n",month+1,mday,year,hour,minute); */
	if ((ret_time = slurm_mktime(&res_tm)) != -1)
		return ret_time;

 prob:	fprintf(stderr, "Invalid time specification (pos=%d): %s\n", pos, time_str);
	//errno = ESLURM_INVALID_TIME_VALUE;
	return (time_t) 0;
}


/*
 * Convert number from one unit to another.
 * By default, Will convert num to largest divisible unit.
 * Appends unit type suffix -- if applicable.
 *
 * IN num: number to convert.
 * OUT buf: buffer to copy converted number into.
 * IN buf_size: size of buffer.
 * IN orig_type: The original type of num.
 * IN spec_type: Type to convert num to. If specified, num will be converted up
 * or down to this unit type.
 * IN divisor: size of type
 * IN flags: flags to control whether to convert exactly or not at all.
 */
extern void convert_num_unit2(double num, char *buf, int buf_size,
			      int orig_type, int spec_type, int divisor,
			      uint32_t flags)
{
	char *unit = "\0KMGTP?";
	uint64_t i;

	if ((int64_t)num == 0) {
		snprintf(buf, buf_size, "0");
		return;
	}

	if ((unsigned int)spec_type != NO_VAL) {
		/* spec_type overrides all flags */
		if (spec_type < orig_type) {
			while (spec_type < orig_type) {
				num *= divisor;
				orig_type--;
			}
		} else if (spec_type > orig_type) {
			while (spec_type > orig_type) {
				num /= divisor;
				orig_type++;
			}
		}
	} else if (flags & CONVERT_NUM_UNIT_RAW) {
		orig_type = UNIT_NONE;
	} else if (flags & CONVERT_NUM_UNIT_NO) {
		/* no op */
	} else if (flags & CONVERT_NUM_UNIT_EXACT) {
		/* convert until we would loose precision */
		/* half values  (e.g., 2.5G) are still considered precise */

		while (num >= divisor
		       && ((uint64_t)num % (divisor / 2) == 0)) {
			num /= divisor;
			orig_type++;
		}
	} else {
		/* aggressively convert values */
		while (num >= divisor) {
			num /= divisor;
			orig_type++;
		}
	}

	if (orig_type < UNIT_NONE || orig_type > UNIT_PETA)
		orig_type = UNIT_UNKNOWN;
	i = (uint64_t)num;
	/* Here we are checking to see if these numbers are the same,
	 * meaning the float has not floating point.  If we do have
	 * floating point print as a float.
	*/
	if ((double)i == num)
		snprintf(buf, buf_size, "%"PRIu64"%c", i, unit[orig_type]);
	else
		snprintf(buf, buf_size, "%.2f%c", num, unit[orig_type]);
}

void convert_num_unit(double num, char *buf, int buf_size,
			     int orig_type, int spec_type, uint32_t flags)
{
	convert_num_unit2(num, buf, buf_size, orig_type, spec_type, 1024,
			  flags);
}

char* reassemble_job_ids(const char *input) {
    const char *delim = ",";
    const char *prefix = "jobid = '";
    const char *suffix = "'";
    size_t i = 0;
    char *output = NULL;
    /*The initial size is 1, leaving room for the string terminator '\0'.*/
    size_t output_size = 1; 
    size_t input_length = strlen(input);
    
    /*Iterate over the input string and calculate the required output string length*/ 
    for (i = 0; i < input_length; ++i) {
        if (input[i] == ',') {
            output_size += strlen(" or ") + strlen(prefix) + strlen(suffix);
        }
    }
    
    /*Allocate enough memory to store the output string*/
    output = (char*)malloc(output_size);
    if (output == NULL) {
        printf("Memory allocation error.\n");
        return NULL;
    }
    
    /*Start building the output string*/
    strcpy(output, "");
    char *token = strtok((char*)input, delim);
    while (token != NULL) {
        /*Append one " or jobid = 'token'" to the output string each time.
         *Add 7 to make room for " or " and the string terminator '\0'
         */
        output = realloc(output, output_size + strlen(token) + strlen(prefix) + strlen(suffix) + 7); 
        if (output == NULL) {
            printf("Memory allocation error.\n");
            return NULL;
        }
        strcat(output, " or ");
        strcat(output, prefix);
        strcat(output, token);
        strcat(output, suffix);
        token = strtok(NULL, delim);
    }
    
    /*If the output string is not empty, remove the first " or "*/
    if (strlen(output) > 4) {
        memmove(output, output + 4, strlen(output) - 3);
    }
    
    return output;
}

int strcat_field(c_string_t* sql, const char *str, int field)
{
    int rc = 0;
    if(sql == NULL || str == NULL || c_string_peek(sql) == NULL) {
        rc = -1;
        return rc;
    }

    const char *prefix;
    switch (field){
        case JOBID:
            prefix = "jobid = '";
            break;
        case STEP:
            prefix = "step = '";
            break;
        case USERNAME:
            prefix = "username = '";
            break;
        default:
            rc = -1;
            return rc;
    }

    const char suffix[] = "' ";
    const char and[] = " and ";
    const char or[] = " or ";
    bool first = false;

    char *fields = strdup(str);
    if(!fields) {
        printf("strdup failed !\n");
        return -1; // strdup failed
    }
    c_string_t *tmp_fields = c_string_create();
    const char delimiters[] = ",";

    char *field_value = strtok(fields, delimiters);
    while (field_value != NULL) {
        if(!first) {
            c_string_append_str(tmp_fields, and);
            c_string_append_str(tmp_fields, "(");
            first = true;
        }else {
            c_string_append_str(tmp_fields, or);
        }
        c_string_append_str(tmp_fields, prefix);
        c_string_append_str(tmp_fields, ((field == STEP && strcasecmp(field_value, "batch") == 0) ? "-5" : field_value));
        c_string_append_str(tmp_fields, suffix);
        field_value = strtok(NULL, delimiters);
    }
    if(first) c_string_append_str(tmp_fields, ")");

    if(rc != -1) {
        c_string_append_str(sql, c_string_peek(tmp_fields));
    }
    c_string_destory(tmp_fields);
    if(fields) {
        free(fields);
    }
    return rc;
}

int stract_time(bool start_label, bool jobid_out, time_t usage_start,
                         time_t usage_end, char *start, c_string_t *sql_str) 
{
    int rc = 0;
    if(start == NULL || sql_str == NULL || c_string_peek(sql_str) == NULL) {
        rc =-1;
        return rc;
    }
    if(start_label && jobid_out) {
        /*Specify job and start time*/
        if(usage_start > usage_end) {
            printf("Start time requested is after end time.\n");
            rc = -1;
            return rc;
        }
        const char and[] = " and ";
        const char time[] = " time >= ";
        c_string_append_str(sql_str, and);
        c_string_append_str(sql_str, time);
        c_string_append_str(sql_str, start);
        
    } else if(start_label && !jobid_out){  
        /*Specify the start time without specifying the job*/
        const char and[] = " and ";
        const char time[] = " time >= ";
        c_string_append_str(sql_str, and);
        c_string_append_str(sql_str, time);
        c_string_append_str(sql_str, start);
    } else if(!start_label && jobid_out) {
        //do nothing

    } else if(!start_label && !jobid_out) {
        start = malloc(100);
        //char tmp_start[80];
        const char and[] = " and ";
        const char time[] = " time >= ";
        time_format(start, 0, false);
        c_string_append_str(sql_str, and);
        c_string_append_str(sql_str, time);
        c_string_append_str(sql_str, start);
        free(start);
    }
    return rc;
       
}

int parse_command_and_query(int argc, char **argv, slurm_influxdb *data) 
{
    int rc = 0;
	int c, optionIndex = 0;
    char* jobids = NULL;
    char* steps = NULL;
    char* start = NULL;
    char* end = NULL;
    char* events = NULL;
    char* user = NULL;

    bool start_label = false;
    bool end_label = false;
    bool user_label = false;

    bool jobid_out = false;
    bool step_out = false;
    bool no_step= false;
    bool no_jobid = false;
    char deauft_events[] = "CPUUSA,PidSA,NodeSA"; 
    if(data == NULL) {
        rc = -1;
        return rc;
    }
    c_string_t *sql_step = c_string_create();
    c_string_t *sql_event = c_string_create();
    c_string_t *sql_runjob = c_string_create();
    char *buffer_str = malloc(2048);
    
    end = malloc(100);
    start = malloc(100);
 
    char sheet_step[] = "Stepd";
    char sheet_event[] = "Event";
    /*Parameter collection sql statement assembly*/
    char sql_step_head[] = "SELECT LAST(\"stepcpuave\") AS last_stepavecpu,LAST(\"stepcpu\") AS last_stepcpu, "
                "LAST(\"stepmem\") AS last_stepmem, LAST(\"stepvmem\") AS last_stepvmem, LAST(\"steppages\") AS last_steppages, "
                "MAX(\"stepcpu\") AS max_stepcpu, MIN(\"stepcpu\") AS min_stepcpu, MAX(\"stepmem\") AS max_stepmem, "
                "MIN(\"stepmem\") AS min_stepmem, MAX(\"stepvmem\") AS max_stepvmem, "
                "MIN(\"stepvmem\") AS min_stepvmem";

    /*Event event sql statement assembly*/
    char sql_event_head[] = "SELECT * ";
    
    char *sql_runjob_head = "SELECT time as ctime,LAST(\"interval_time\") as interval_time ";
    char *sql_runjob_tail = " group by jobid";

    char sql_tail[] = " group by step,jobid";
    struct passwd *pw;
    /* record start time */
    time_t usage_start = 0;
    /* record end time */
    time_t usage_end = time(NULL);
    
    params.convert_flags = CONVERT_NUM_UNIT_EXACT;
	params.units = NO_VAL;
	params.opt_uid = getuid();
	params.opt_gid = getgid();
    /*
        |-e|overall|event|load|
    */
    params.level = 0x0000;
    assert(params.opt_uid != -1);
    pw = getpwuid(params.opt_uid);
	static struct option long_options[] = {
                {"abnormal",    no_argument,        0,      'A'},
                {"all",         no_argument,        0,      'a'},
                {"desc",        no_argument,        0,      'd'},
                {"event",       required_argument,  0,      'e'},
                {"end",         required_argument,  0,      'E'},
                {"help",        no_argument,        0,      'h'},
                {"jobs",        required_argument,  0,      'j'},
                {"load",        no_argument,        0,      'l'},
                {"format",      required_argument,  0,      'o'},
                {"running",     no_argument,        0,      'r'},
                {"steps",       required_argument,  0,      's'},
                {"start",       required_argument,  0,      'S'},   
                {"user",        required_argument,  0,      'u'},      
                {"version",     no_argument,        0,      'V'},
                {"overall",     no_argument,        0,      'O'},
                {0,             0,                  0,      0}};
    while (1) {		/* now cycle through the command line */
		c = getopt_long(argc, argv,
				"de:E:j:s:lo:rS:u:VOmgaAh",
				long_options, &optionIndex);      
        if (c == -1) {
            no_jobid = true;
            no_step = true;
        }
        switch (c) {
            case 'a':
                params.level |= 0x0111;
                break;
            case 'A':
                params.level |= 0x0010;
                break;
            case 'd':
                params.desc_set = true;
                break;
        	case 'e':
                params.level |= 0x1000;
                events = malloc(strlen(optarg)+30);
                sprintf(events,"%s",optarg);
                break;
        	case 'E':
                end_label = true;
                usage_end = parse_time(optarg, 1);
                break;
        	case 'h':
                print_sjinfo_help();
                exit(0);
                break;
        	case 'j':
                jobids = malloc(strlen(optarg)+20);
                sprintf(jobids,"%s",optarg);
                jobid_out = true;
                break;
            case 'l':
                params.level |= 0x0001;
                break;
        	case 'o':
                params.opt_field_list = malloc(strlen(optarg)+20);
                sprintf(params.opt_field_list,"%s",optarg);
                break;
            case 'r':
                params.only_run_job = true;
                break;
            case 's':
                steps = malloc(strlen(optarg)+20);
                sprintf(steps,"%s",optarg);
                step_out = true;
                break;
        	case 'S':
                start_label = true;
                usage_start = parse_time(optarg, 1);
                break;
        	case 'u':
                user_label = true;
                user = malloc(strlen(optarg)+20);
                sprintf(user,"%s",optarg);
                break;
            case 'V':
                print_sjinfo_version();
                exit(0);
            case 'm':
                params.units = UNIT_MEGA;
                break;
            case 'g':
                params.units = UNIT_GIGA;
                break;
            case 'O':
                params.level |= 0x0100;
                break;
    		case '?':	/* getopt() has explained it */
			    exit(1);
            default:
            break;
        }
        if(no_jobid || no_step)
            break;
    }

    if(params.level == 0x0000 || params.level == 0x1000) 
        params.level |= 0x0001;
    if((params.level & 0x0001) && (params.level & 0x1000)){
        printf("The -e option must be used with either -A or -O !\n");
        exit(1);
    }
    if(!end_label) {
        time_format(end, 0, true);
        end_label = true;
    } else {
        time_format(end, usage_end, false);
    }

    if(start_label) {
        time_format(start, usage_start, false);
    }

    if(params.level & 0x0001) {
        sprintf(buffer_str,"%s from %s where time <= %s ",sql_step_head, sheet_step, end);
        c_string_append_str(sql_step, buffer_str);
        memset(buffer_str, 0, 2048);
    } 

    /*
        The reason we bitwise 0x0110 instead of 0x0010 is that the exception event overview 
        data is counted at the same time as the exception event overview data, so when -O is 
        specified, the -A flow is still executed, but the -A message is not output
    */
    if(params.level & 0x0110) {
        sprintf(buffer_str,"%s from %s where time <= %s ",sql_event_head, sheet_event, end);
        c_string_append_str(sql_event, buffer_str);
        memset(buffer_str, 0, 2048);
    }

    if(params.only_run_job) {
        char tmp[64];
        char start_tmp[64];
        sprintf(buffer_str,"%s from %s where time <= %s ",sql_runjob_head, sheet_step, end);
        c_string_append_str(sql_runjob, buffer_str);
        memset(buffer_str, 0, 2048);
        
        /*
            The time range for finding a running job is set to one hour
        */
        if(start_label == false) {
            time_t rawtime = time(NULL) - 3600;
            struct tm *timeinfo = gmtime(&rawtime);
            strftime(tmp, sizeof(tmp), "%Y-%m-%dT%H:%M:%SZ", timeinfo);
            sprintf(start_tmp,"'%s'",tmp);
        }else{
            strncpy(start_tmp, start, sizeof(start_tmp) - 1);
            start_tmp[sizeof(start_tmp) - 1] = '\0';
        }

        if(jobid_out) {
            rc = strcat_field(sql_runjob, jobids, JOBID);
            if(rc == -1) goto fail;
        }
        
        rc = stract_time(true, jobid_out, usage_start, usage_end, start_tmp, sql_runjob);
        if(rc == -1) goto fail;

        if(params.opt_uid != 0) {
            char user[60] ={'0'};
            sprintf(user," and username = '%s' ", pw->pw_name);
            c_string_append_str(sql_runjob, user);
        } else if(user_label) {
            rc = strcat_field(sql_runjob, user, USERNAME);
            if(rc == -1) goto fail;  
        }
        c_string_append_str(sql_runjob, sql_runjob_tail);
        /* debug */
        // printf("sql_runjob = %s\n", c_string_peek(sql_runjob));
        char * response = influxdb_connect(data, c_string_peek(sql_runjob), STEPDRP);
        /* debug */
        // printf("response_runjob = %s\n", response);
        if(response){
            parse_json(response, pw->pw_name, UNIT_RUNJOB, time(NULL));
        }else {
            rc = -1;
            goto fail;
        }
        if(response)
            free(response);
        if(c_string_len(job_list) != 0) {
            if(jobids) free(jobids);
            jobids = strdup(c_string_peek(job_list));
            jobid_out = true;
            c_string_destory(job_list);
            printf("jobids = %s\n", jobids);
        }else{
            c_string_destory(job_list);
            goto fail;
        }
    }

    if(jobid_out) {
        if(params.level & 0x0001) {
            rc = strcat_field(sql_step, jobids, JOBID);
            if(rc == -1) goto fail;
        }
        if(params.level & 0x0110) {
            rc = strcat_field(sql_event, jobids, JOBID);
            if(rc == -1) goto fail;
        }
    }

    if(step_out) {
        if(params.level & 0x0001) {
            rc = strcat_field(sql_step, steps, STEP);
            if(rc == -1) goto fail;
        }
        if(params.level & 0x0110) {
            rc = strcat_field(sql_event, steps, STEP);
            if(rc == -1) goto fail;
        }
    }


    if(params.level & 0x1000) {
        bool splicing = false;
        c_string_t *tmp_events = c_string_create();

        const char and[] = " and ";
        const char or[] = " or ";
        const char prefix1[] = "type1 = ";
        const char prefix2[] = "type2 = ";
        const char prefix3[] = "type3 = ";
        const char delimiters[] = ",";
        if(events == NULL) {
            events = malloc(strlen(deauft_events)+1);
            strcpy(events, deauft_events);
        }
        char *event = strtok(events, delimiters);
        bool first =  false;    
        while (event != NULL) { 
            if(!first) {
                c_string_append_str(tmp_events, and);
                c_string_append_str(tmp_events, "(");
                first = true;
            } else
                c_string_append_str(tmp_events, or);
            
            if (strcasecmp(event, "CPUUSA") == 0) { 
                c_string_append_str(tmp_events, prefix1);
                c_string_append_str(tmp_events, "'1'");
            } else if (strcasecmp(event, "PidSA") == 0) {
                c_string_append_str(tmp_events, prefix2);
                c_string_append_str(tmp_events, "'1'");
               
            } else if (strcasecmp(event, "NodeSA") == 0) {
                c_string_append_str(tmp_events, prefix3);
                c_string_append_str(tmp_events, "'1'");
            } else 
                splicing = true;
            event = strtok(NULL, delimiters);
        }   
        if(first) c_string_append_str(tmp_events, ")");
        if(!splicing) {
            c_string_append_str(sql_event, c_string_peek(tmp_events));
            c_string_destory(tmp_events);
        } else {
            printf("Please enter a valid event field -e \n");
            c_string_destory(tmp_events);
            rc = -1;
            goto fail;
        }
  
    }

    if(params.level & 0x0001) {
        rc =  stract_time(start_label, jobid_out, usage_start,
                            usage_end, start, sql_step);
        if(rc == -1)
            goto fail;
    }
    if(params.level & 0x0110) {
        rc = stract_time(start_label, jobid_out, usage_start,
                            usage_end, start, sql_event);
        if(rc == -1)
          goto fail;
    }

    if(params.opt_uid != 0) {
        if( params.level & 0x0001) {
            char user[60] ={'0'};
            sprintf(user," and username = '%s' ", pw->pw_name);
            c_string_append_str(sql_step, user);
        }
        if( params.level & 0x0110) {
            char user[60] ={'0'};
            sprintf(user," and username = '%s' ", pw->pw_name);
            c_string_append_str(sql_event, user);
        }
    } else if(user_label) {
        if(params.level & 0x0001){
            rc = strcat_field(sql_step, user, USERNAME);
            if(rc == -1)
            goto fail; 
        }
        if(params.level & 0x0110){
            rc = strcat_field(sql_event, user, USERNAME);
            if(rc == -1)
            goto fail;  
        }
    }
    if(params.level & 0x0001) {
        c_string_append_str(sql_step, sql_tail);
        /* debug */
        // printf("sql_step = %s\n", c_string_peek(sql_step));
        char * response = influxdb_connect(data, c_string_peek(sql_step), STEPDRP);
        /* debug */
        // printf("response_step = %s\n", response);
        if(response)
            parse_json(response, pw->pw_name, UNIT_STEP, 0);
        else {
            rc = -1;
            goto fail;
        }
        if(response)
            free(response);    
    } 
    if(params.level & 0x0110) {
        c_string_append_str(sql_event, sql_tail);
        /* debug */
        // printf("sql_event = %s\n", c_string_peek(sql_event));
        char * response = influxdb_connect(data, c_string_peek(sql_event), EVENTRP);
        /* debug */
        // printf("response_event = %s\n", response);
        if(response)
            parse_json(response, pw->pw_name, UNIT_EVENT, 0);
        else {
            rc = -1;
            goto fail;
        }
        if(response)
            free(response);    
    } 
fail:
    if(events)
        free(events);  
    if(sql_step)
        c_string_destory(sql_step);
    if(sql_event)
        c_string_destory(sql_event);
    if(sql_runjob)
        c_string_destory(sql_runjob);
    if(buffer_str)
        free(buffer_str);
    if(jobids)
        free(jobids);    
    if(start)
        free(start);
    if(steps)
        free(steps);
    if(end)
        free(end);    
    if(user)
        free(user);
    return rc;
}

extern void print_fields_str(print_field_t *field, char *value, int last)
{

	int abs_len = abs(field->len);
	char temp_char[abs_len+1];
	char *print_this = NULL;



	if (!value) {
		if (print_fields_parsable_print)
			print_this = "";
		else
			print_this = " ";
	} else
		print_this = value;

	if (print_fields_parsable_print == PRINT_FIELDS_PARSABLE_NO_ENDING
	   && last)
		printf("%s", print_this);
	else if (print_fields_parsable_print && !fields_delimiter)
		printf("%s|", print_this);
	else if (print_fields_parsable_print && fields_delimiter)
		printf("%s%s", print_this, fields_delimiter);
	else {
		if (value) {
			int len = strlen(value);
			memcpy(&temp_char, value, MIN(len, abs_len) + 1);
			if (len > abs_len)
				temp_char[abs_len-1] = '+';
			print_this = temp_char;
		}

		if (field->len == abs_len)
			printf("%*.*s ", abs_len, abs_len, print_this);
		else
			printf("%-*.*s ", abs_len, abs_len, print_this);
	}
}

extern void print_fields_header(List print_fields_list)
{
	ListIterator itr = NULL;
	print_field_t *field = NULL;
	int curr_inx = 1;
	int field_count = 0;

	if (!print_fields_list || !print_fields_have_header)
		return;

	field_count = list_count(print_fields_list);

	itr = list_iterator_create(print_fields_list);
	while ((field = list_next(itr))) {
		if (print_fields_parsable_print
		   == PRINT_FIELDS_PARSABLE_NO_ENDING
		   && (curr_inx == field_count))
			printf("%s", field->name);
		else if (print_fields_parsable_print
			 && fields_delimiter) {
			printf("%s%s", field->name, fields_delimiter);
		} else if (print_fields_parsable_print
			 && !fields_delimiter) {
			printf("%s|", field->name);

		} else {
			int abs_len = abs(field->len);
			printf("%*.*s ", field->len, abs_len, field->name);
		}
		curr_inx++;
	}
	list_iterator_reset(itr);
	printf("\n");

	if (print_fields_parsable_print) {
		list_iterator_destroy(itr);
		return;
	}

	while ((field = list_next(itr))) {
        int i;
		int abs_len = abs(field->len);
		for (i = 0; i < abs_len; i++)
			putchar('-');
		putchar(' ');
	}
	list_iterator_destroy(itr);
	printf("\n");
}


void print_options(List print_list, List value_list, ListIterator print_itr)
{
        int field_count = list_count(print_list);

        /*Print title*/
        print_fields_header(print_list);
        print_field_t *field = NULL;

        interface_sjinfo_t * sjinfo_print;
        int curr_inx = 1;
        uint64_t tmp_uint64 = NO_VAL64;
        char tmp_char[200] = {'0'};
        char tmp_extern[] = "batch";
        //int tmp_int = NO_VAL;
        struct tm *timeinfo;

        ListIterator print_value_itr = NULL;
        print_value_itr = list_iterator_create(value_list);

		while ((sjinfo_print = list_next(print_value_itr))) {
            list_iterator_reset(print_itr);
            while ((field = list_next(print_itr))) {
                    memset(tmp_char,'\0',sizeof(tmp_char));
                    switch (field->type) {
                    case PRINT_JOBID:
                        sprintf(tmp_char, "%lu", sjinfo_print->jobid);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                    break;
                    case PRINT_STEPID:
                        if((sjinfo_print->stepid == -5))
                            sprintf(tmp_char, "%s", tmp_extern);
                        else
                            sprintf(tmp_char, "%d", sjinfo_print->stepid);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                    break;
                    case PRINT_STEPAVECPU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->stepcpuave);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));                            
                    break;
                    case PRINT_STEPCPU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->stepcpu);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                    break;
                    case PRINT_STEPMEM:
                        tmp_uint64 = sjinfo_print->stepmem;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf,
                                    sizeof(outbuf), UNIT_NONE,
                                    params.units,
                                    params.convert_flags);
                        field->print_routine(field,
                                    outbuf,
                                    (curr_inx == field_count));    
                    break;   
                    case PRINT_STEPVMEM:
                        tmp_uint64 = sjinfo_print->stepvmem;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf,
                                    sizeof(outbuf), UNIT_NONE,
                                    params.units,
                                    params.convert_flags);
                        field->print_routine(field,
                                    outbuf,
                                    (curr_inx == field_count));    
                    break; 
                    case PRINT_STEPPAGES:
                        sprintf(tmp_char, "%lu", sjinfo_print->steppages);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                    break; 
                    case PRINT_MAXSTEPCPU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->stepcpumax);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                    break;
                    case PRINT_MINSTEPCPU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->stepcpumin);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                    break;      
                    case PRINT_MAXSTEPMEM:
                        tmp_uint64 = sjinfo_print->stepmemmax;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf,
                                    sizeof(outbuf), UNIT_NONE,
                                    params.units,
                                    params.convert_flags);
                        field->print_routine(field,
                                    outbuf,
                                    (curr_inx == field_count));  
                    break; 
                    case PRINT_MINSTEPMEM:
                        tmp_uint64 = sjinfo_print->stepmemmin;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf,
                                    sizeof(outbuf), UNIT_NONE,
                                    params.units,
                                    params.convert_flags);
                        field->print_routine(field,
                                    outbuf,
                                    (curr_inx == field_count));  
                    break; 
                    case PRINT_MAXSTEPVMEM:
                        tmp_uint64 = sjinfo_print->stepvmemmax;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf,
                                    sizeof(outbuf), UNIT_NONE,
                                    params.units,
                                    params.convert_flags);
                        field->print_routine(field,
                                    outbuf,
                                    (curr_inx == field_count));    
                    break;
                    case PRINT_MINSTEPVMEM:
                        //sprintf(tmp_char, "%lld", sjinfo_print->stepvmemmin);
                        tmp_uint64 = sjinfo_print->stepvmemmin;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf,
                                    sizeof(outbuf), UNIT_NONE,
                                    params.units,
                                    params.convert_flags);
                        field->print_routine(field,
                                    outbuf,
                                    (curr_inx == field_count));      
                    break;   

                    case PRINT_CPUTHRESHOLD:
                        sprintf(tmp_char, "%lu%%", sjinfo_print->cputhreshold);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                    break; 

                    case PRINT_START:
                        timeinfo = localtime((const time_t *)&sjinfo_print->start);
                        if(timeinfo){
                            strftime(tmp_char, sizeof(tmp_char), "%Y-%m-%dT%H:%M:%S", timeinfo);
                            field->print_routine(field,
                            tmp_char,
                            (curr_inx == field_count));
                        }else{
                            fprintf(stderr, "Error: Failed to convert time from start. Value: %lu\n", sjinfo_print->start);
                        }
                    break;  

                    case PRINT_END:
                        timeinfo = localtime((const time_t *)&sjinfo_print->end);
                        if(timeinfo){
                            strftime(tmp_char, sizeof(tmp_char), "%Y-%m-%dT%H:%M:%S", timeinfo);
                            field->print_routine(field,
                            tmp_char,
                            (curr_inx == field_count));
                        }else{
                            fprintf(stderr, "Error: Failed to convert time from end. Value: %lu\n", sjinfo_print->end);
                        }
                        
                    break; 
                    
                    case PRINT_TYPE:
                        if(sjinfo_print->type1) {
                            strcat(tmp_char, "|Abnormal CPU utilization|");
                        } 
                        if(sjinfo_print->type2) {
                            strcat(tmp_char, "|Operational process anomalies|");
                        }
                        if(sjinfo_print->type3) {
                            strcat(tmp_char, "|Node communication exception|");
                        }
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                    break;   

                    case PRINT_SUMCPU:
                        sprintf(tmp_char, "%lu", sjinfo_print->sum_cpu);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                    break;

                    case PRINT_SUMPID:
                        sprintf(tmp_char, "%lu", sjinfo_print->sum_pid);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                    break;

                    case PRINT_SUMNODE:
                        sprintf(tmp_char, "%lu", sjinfo_print->sum_node);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                    break;

                    case PRINT_LASTSTART:
                        timeinfo = localtime((const time_t *)&sjinfo_print->start_last);
                        if(timeinfo){
                            strftime(tmp_char, sizeof(tmp_char), "%Y-%m-%dT%H:%M:%S", timeinfo);
                            field->print_routine(field,
                            tmp_char,
                            (curr_inx == field_count));
                        }else{
                            fprintf(stderr, "Error: Failed to convert time from start_last. Value: %lu\n", sjinfo_print->start_last);
                        }
                    break;

                    case PRINT_LASTEND:
                        timeinfo = localtime((const time_t *)&sjinfo_print->end_last);
                        if(timeinfo){
                            strftime(tmp_char, sizeof(tmp_char), "%Y-%m-%dT%H:%M:%S", timeinfo);
                            field->print_routine(field,
                            tmp_char,
                            (curr_inx == field_count));  
                        }else{
                            fprintf(stderr, "Error: Failed to convert time from end_last. Value: %lu\n", sjinfo_print->end_last);
                        }
                    break; 
                    }
             
            }
            printf("\n");
        }

        if (print_value_itr)
		    list_iterator_destroy(print_value_itr);    
}

void field_split(char *field_str, print_field_t* fields_tmp, List sj_list)
{
    int i = 0;
    char *end = NULL, *start = NULL;
    char *field_copy = malloc(strlen(field_str) + 10);
    strcpy(field_copy, field_str);
    start = field_copy;
    strcat(start,",");
    while ((end = strstr(start, ","))) {
        char *tmp_char = NULL;
        int command_len = 0;
        int newlen = 0;
        bool newlen_set = false;

        *end = 0;
        while (isspace(*start))
            start++;	/* discard whitespace */
        if (!(int)*start)
            continue;

        if ((tmp_char = strstr(start, "\%"))) {
            newlen_set = true;
            newlen = atoi(tmp_char+1);
            tmp_char[0] = '\0';
        }

        command_len = strlen(start);
            
        if (!strncasecmp("ALL", start, command_len)) {
            for (i = 0; fields_tmp[i].name; i++) {
                if (newlen_set)
                    fields_tmp[i].len = newlen;
                list_append(sj_list, &fields_tmp[i]);
                start = end + 1;
            }
            start = end + 1;
            continue;
        }
            
        for (i = 0; fields_tmp[i].name; i++) {
            if (!strncasecmp(fields_tmp[i].name, start, command_len))
                goto foundfield;
        }
        continue;
    foundfield:
        if (newlen_set)
            fields_tmp[i].len = newlen;

        list_append(sj_list, &fields_tmp[i]);
        start = end + 1;

    }
    if(field_copy)
        free(field_copy);

}

void print_field(uint64_t level)
{

    char *opt_step_list = malloc(160);
    char *opt_event_list = malloc(160);
    char *opt_overall_list = malloc(160);
    char base_step_field[] = "JobID,StepID,StepCPU,"
                "StepAVECPU,StepMEM,StepVMEM,StepPages,MaxStepCPU,"
                "MinStepCPU,MaxStepMEM,MinStepMEM,MaxStepVMEM,MinStepVMEM,";
    char base_event_field[] = "JobID,StepID,StepCPU,"
                "StepMEM,StepVMEM,StepPages,CPUthreshold,Start,End,Type,";
    char base_overall_field[] = "JobID,StepID,Last_start,"
                "Last_end,SUM_CPU,SUM_PID,SUM_NODE";

    print_field_t fields[] = {
    	{12, "JobID",           print_fields_str,   PRINT_JOBID},
        {6,  "StepID",          print_fields_str,   PRINT_STEPID},
        {12, "StepAVECPU",      print_fields_str,   PRINT_STEPAVECPU},        
        {12, "StepCPU",         print_fields_str,   PRINT_STEPCPU},
        {12, "StepMEM",         print_fields_str,   PRINT_STEPMEM},      
    	{12, "StepVMEM",        print_fields_str,   PRINT_STEPVMEM},
        {12, "StepPages",       print_fields_str,   PRINT_STEPPAGES},
    	{12, "MaxStepCPU",      print_fields_str,   PRINT_MAXSTEPCPU},
        {12, "MinStepCPU",      print_fields_str,   PRINT_MINSTEPCPU},
    	{12, "MaxStepMEM",      print_fields_str,   PRINT_MAXSTEPMEM},
        {12, "MinStepMEM",      print_fields_str,   PRINT_MINSTEPMEM}, 
    	{12, "MaxStepVMEM",     print_fields_str,   PRINT_MAXSTEPVMEM},
        {12, "MinStepVMEM",     print_fields_str,   PRINT_MINSTEPVMEM}, 
        {0,  NULL,              NULL,               0}
    };

    print_field_t field_event[] = {
    	{12, "JobID",           print_fields_str,   PRINT_JOBID},
        {6,  "StepID",          print_fields_str,   PRINT_STEPID},       
        {12, "StepCPU",         print_fields_str,   PRINT_STEPCPU},
        {12, "StepMEM",         print_fields_str,   PRINT_STEPMEM},      
    	{12, "StepVMEM",        print_fields_str,   PRINT_STEPVMEM},
        {12, "StepPages",       print_fields_str,   PRINT_STEPPAGES},
    	{12, "CPUthreshold",    print_fields_str,   PRINT_CPUTHRESHOLD},
        {22, "Start",           print_fields_str,   PRINT_START}, 
        {22, "End",             print_fields_str,   PRINT_END},
        {24, "Type",            print_fields_str,   PRINT_TYPE}, 
        {0,  NULL,              NULL,               0}
    };

    print_field_t field_overall[] = {
    	{12, "JobID",       print_fields_str,   PRINT_JOBID},
        {6,  "StepID",      print_fields_str,   PRINT_STEPID},       
        {22, "Last_start",  print_fields_str,   PRINT_LASTSTART},
        {22, "Last_end",    print_fields_str,   PRINT_LASTEND},      
    	{12, "Sum_CPU",     print_fields_str,   PRINT_SUMCPU},
        {12, "Sum_PID",     print_fields_str,   PRINT_SUMPID},
    	{12, "Sum_NODE",    print_fields_str,   PRINT_SUMNODE},
        {0,  NULL,          NULL,               0}
    };

    if(!params.opt_field_list) {
        /*Consider the scenario where only one side of the field has it*/
        if(level & 0x0001) {
            strcpy(opt_step_list,base_step_field);
            field_split(opt_step_list, fields, print_fields_list);
        } 

        if(level & 0x0010)  {
            strcpy(opt_event_list,base_event_field);
            field_split(opt_event_list, field_event, print_events_list);
        }

        if(level & 0x0100) {
            strcpy(opt_overall_list,base_overall_field);
            field_split(opt_overall_list, field_overall, print_overall_list);
        }

    } else {
        if(level & 0x0001) 
            field_split(params.opt_field_list, fields, print_fields_list);
        if(level & 0x0010)
            field_split(params.opt_field_list, field_event, print_events_list);
        if(level & 0x0100) {
            field_split(params.opt_field_list, field_overall, print_overall_list);
        }
    }


    if(list_count(print_fields_list) > 0){
        printf("***************************************************************************** \n");
        printf("******       Display resource consumption information of job steps    *******\n");
        printf("***************************************************************************** \n");
        printf("\n");
        print_options(print_fields_list, print_value_list ,print_fields_itr);
        printf("\n");
    }


    if(list_count(print_events_list) > 0) {
        printf("***************************************************************************** \n");
        printf("******       Display job step exception event information            ******** \n");
        printf("***************************************************************************** \n");
        printf("\n");
        print_options(print_events_list, print_events_value_list, print_events_itr);
    }
       
    if(list_count(print_overall_list) > 0) {
        printf("***************************************************************************** \n");
        printf("*******     Display job step exception event overall information      ******* \n");
        printf("***************************************************************************** \n");
        printf("\n");
        print_options(print_overall_list, print_overall_value_list, print_overall_itr);
    }

    free(opt_step_list);
    free(opt_event_list);
    free(opt_overall_list);
}


int main(int argc ,char** argv) {

    slurm_influxdb* influxdb_data = malloc(sizeof(slurm_influxdb));
    char* configpath = NULL;
    int rc = 0 ;
    char tmp_conf[] = "/etc/acct_gather.conf.key";

    if(strcmp(KEYDIR, "NONE") == 0) {
         configpath = my_strdup("/etc/slurm/acct_gather.conf.key");
    } else {
        char* def_conf = NULL;
        def_conf = malloc(strlen(KEYDIR)+strlen(tmp_conf)+2);
        sprintf(def_conf, "%s%s",KEYDIR,tmp_conf);
        configpath = my_strdup(def_conf);
        free(def_conf);
    }

    struct stat statbuf;
    if(stat(configpath, &statbuf) != 0){
        printf("File location does not exist of %s \n", configpath);
        goto file_fail;
    }


    /*16-bit encryption key*/
    const uint8_t key[]="xxx";
    
    sjinfo_init(influxdb_data);

    rc = read_hex_bytes_from_file(configpath, key, influxdb_data);
    if(rc == -1)
         goto file_fail;
    rc =  parse_command_and_query(argc, argv, influxdb_data);
    if(rc == -1)
         goto file_fail;

    print_field(params.level);
    sjinfo_fini(influxdb_data);
file_fail:
    if (configpath)
        free(configpath);
    if(rc == -1 )
         sjinfo_fini(influxdb_data);
    free(influxdb_data);
    if(params.opt_field_list)
        free(params.opt_field_list);

    return 0;
}

