/*****************************************************************************\
 *  spost.c - implementation-independent job of influxdb info
 *  functions
 *****************************************************************************
 *  Copyright (C) 2005 Hewlett-Packard Development Company, L.P.

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

/*****************************************************************************\
 *  Modification history
 *
\*****************************************************************************/
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <stdlib.h>
#include <stddef.h>
#include <fcntl.h>
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
/* general return codes */
#define SLURM_SUCCESS   0
#define ESPANK_SUCCESS 0
#define SLURM_ERROR    -1

#define MAX_SQL_STR   1024*4

#define DEF_TIMERS	struct timeval tv1, tv2; char tv_str[20] = ""; long delta_t;
#define START_TIMER	gettimeofday(&tv1, NULL)
#define END_TIMER do {							\
	gettimeofday(&tv2, NULL);					\
	slurm_diff_tv_str(&tv1, &tv2, tv_str, 20, NULL, 0, &delta_t);	\
} while (0)
#define END_TIMER2(from) do {						\
	gettimeofday(&tv2, NULL);					\
	slurm_diff_tv_str(&tv1, &tv2, tv_str, 20, from, 0, &delta_t);	\
} while (0)
#define END_TIMER3(from, limit) do {					\
	gettimeofday(&tv2, NULL);					\
	slurm_diff_tv_str(&tv1, &tv2, tv_str, 20, from, limit, &delta_t); \
} while (0)
#define DELTA_TIMER	delta_t
#define TIME_STR 	tv_str
/* Type for handling HTTP responses */
struct http_response {
	char *message;
	size_t size;
};
#define xstrfmtcat(__p, __fmt, args...)	_xstrfmtcat(&(__p), __fmt, ## args)

typedef struct {
	int opt_gid;		/* running persons gid */
	int opt_uid;		/* running persons uid */
	int units;		/* --units*/
	uint32_t convert_flags;	/* --noconvert */    
	char *opt_field_list;	/* --fields= */ 
    // char *opt_field_list1;	/* --fields= */ 
	// char *opt_field_list2;	/* --fields2= */ 
    uint64_t level;

} spost_parameters_t;

typedef struct{
    uint32_t eK[44], dK[44];    // encKey, decKey
    int Nr; // 10 rounds
}AesKey;

 /*AES-128 packet length is 16 bytes*/
#define BLOCKSIZE 16 

uint8_t ct1[32] = {0};    
uint8_t plain1[32] = {0}; 

uint8_t ct2[32] = {0};    
uint8_t plain2[32] = {0}; 

uint8_t ct3[32] = {0};    
uint8_t plain3[32] = {0}; 
char data3[32] = {0};
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


#define CONVERT_NUM_UNIT_EXACT 0x00000001
#define CONVERT_NUM_UNIT_NO    0x00000002
#define CONVERT_NUM_UNIT_RAW   0x00000004

#define NO_VAL     (0xfffffffe)
#define NO_VAL64   (0xfffffffffffffffe)

/* for 128-bit blocks, Rijndael never uses more than 10 rcon values */
static const uint32_t rcon[10] = {
        0x01000000UL, 0x02000000UL, 0x04000000UL, 0x08000000UL, 0x10000000UL,
        0x20000000UL, 0x40000000UL, 0x80000000UL, 0x1B000000UL, 0x36000000UL
};

spost_parameters_t params;
/*
 * slurm_diff_tv_str - build a string showing the time difference between two
 *		       times
 * IN tv1 - start of event
 * IN tv2 - end of event
 * OUT tv_str - place to put delta time in format "usec=%ld"
 * IN len_tv_str - size of tv_str in bytes
 * IN from - where the function was called form
 */
extern void slurm_diff_tv_str(struct timeval *tv1, struct timeval *tv2,
			      char *tv_str, int len_tv_str, const char *from,
			      long limit, long *delta_t)
{
	char p[64] = "";
	struct tm tm;
	int debug_limit = limit;

	(*delta_t)  = (tv2->tv_sec - tv1->tv_sec) * 1000000;
	(*delta_t) += tv2->tv_usec;
	(*delta_t) -= tv1->tv_usec;
	snprintf(tv_str, len_tv_str, "usec=%ld", *delta_t);
	if (from) {
		if (!limit) {
			/* NOTE: The slurmctld scheduler's default run time
			 * limit is 4 seconds, but that would not typically
			 * be reached. See "max_sched_time=" logic in
			 * src/slurmctld/job_scheduler.c */
			limit = 3000000;
			debug_limit = 1000000;
		}
		if ((*delta_t > debug_limit) || (*delta_t > limit)) {
			if (!localtime_r(&tv1->tv_sec, &tm))
				printf("localtime_r(): %m");
			if (strftime(p, sizeof(p), "%T", &tm) == 0)
				printf("strftime(): %m");
			if (*delta_t > limit) {
				printf("Warning: Note very large processing "
					"time from %s: %s began=%s.%3.3d",
					from, tv_str, p,
					(int)(tv1->tv_usec / 1000));
			} else {	/* Log anything over 1 second here */
				printf("Note large processing time from %s: "
				      "%s began=%s.%3.3d",
				      from, tv_str, p,
				      (int)(tv1->tv_usec / 1000));
			}
		}
	}
}


typedef enum {
	NATIVERP,
	STEPDRP,
	EVENTRP,
	APPTYPERP,
	RPCNT
} RPType;
static const char *RPTypeNames[] = {
	"NATIVERP",
    "STEPDRP",
    "EVENTRP",
    "APPTYPERP",
};
/**
 * @brief Parses the runtime policy string and extracts the policy value matching the specified type.
 *
 * This function takes a runtime policy string (rt_policy) and a target policy type (type), and parses the comma-separated
 * components of the policy string. If a matching type is found, it returns the associated value. If no matching policy is found,
 * it returns the default value "autogen". If the input rt_policy is NULL or an empty string, it directly returns "autogen".
 *
 * If the rt_policy string contains a standalone value (without any key-value pairs), it is treated as the default return value.
 * If no matching key is found, but a standalone value exists, the function returns that value.
 * If the string does not contain any valid key-value pairs, it is returned as is.
 *
 * @param rt_policy The runtime policy string, consisting of comma-separated key-value pairs (e.g., "type1=value1,type2=value2")
 *                  or a single default value.
 * @param type The target policy type, used to match a specific policy value. This is an enum representing different policy types.
 *
 * @return A string containing the policy value corresponding to the specified type.
 *         - If a matching key is found, returns its associated value.
 *         - If no matching key is found but a standalone default value exists, returns that value.
 *         - If no valid key-value pairs are found, returns the original rt_policy string.
 *         - If the input is NULL or empty, returns "autogen".
 *         - If at least one key is found but the requested type is missing, returns "autogen".
 */
static char* _parse_rt_policy(const char *rt_policy, RPType type) {
    int i = 0;
    if (rt_policy == NULL || rt_policy[0] == '\0') {
        return strdup("autogen");
    }

    // If rt_policy does not contain ',' or '=', return it directly
    if (strchr(rt_policy, ',') == NULL && strchr(rt_policy, '=') == NULL) {
        return strdup(rt_policy);
    }

    int found_any_keyword = 0;
    char *default_value = NULL; 
    char *policy_copy = strdup(rt_policy);
    if (!policy_copy) {
        return strdup("autogen");
    }

    char *saveptr = NULL;
    char *token = strtok_r(policy_copy, ",", &saveptr);

    while (token) {
        char *value = strchr(token, '=');
        if (value) {
            *value = '\0';  
            value++;        

            for (i = 0; i < RPCNT; i++) {
                if (strcmp(token, RPTypeNames[i]) == 0) {
                    found_any_keyword = 1;
                    if (i == (int)type) {
                        char *result = strdup(value);
                        if (default_value) free(default_value);
                        free(policy_copy);
                        return result;
                    }
                }
            }
        } else {
            // If no '=' is found, treat it as the default value
            if (default_value) free(default_value);
            default_value = strdup(token);
        }
        token = strtok_r(NULL, ",", &saveptr);
    }

    free(policy_copy);

    // Return the default value if no match is found
    if (default_value) {
        return default_value;
    }

    // If a key is found but no match for type, return "autogen"
    if (found_any_keyword) {
        return strdup("autogen");
    }

    // If no key-value structure is found, return the original string
    return strdup(rt_policy);
}

/*
 * Give me a copy of the string as if it were printf.
 * This is stdarg-compatible routine, so vararg-compatible
 * functions can do va_start() and invoke this function.
 *
 *   fmt (IN)		format of string and args if any
 *   RETURN		copy of formated string
 */
static size_t _xstrdup_vprintf(char **str, const char *fmt, va_list ap)
{
	/* Start out with a size of 100 bytes. */
	int n, size = 100;
	va_list our_ap;
	char *p = malloc(size);

	while (1) {
		/* Try to print in the allocated space. */
		va_copy(our_ap, ap);
		n = vsnprintf(p, size, fmt, our_ap);
		va_end(our_ap);
		/* If that worked, return the string. */
		if (n > -1 && n < size) {
			*str = p;
			return n;
		}
		/* Else try again with more space. */
		if (n > -1)               /* glibc 2.1 */
			size = n + 1;           /* precisely what is needed */
		else                      /* glibc 2.0 */
			size *= 2;              /* twice the old size */
		p = realloc(p, size);
	}
	/* NOTREACHED */
}

/*
 * append formatted string with printf-style args to buf, expanding
 * buf as needed
 */
void _xstrfmtcat(char **str, const char *fmt, ...)
{
	char *p = NULL;
	va_list ap;

	va_start(ap, fmt);
	_xstrdup_vprintf(&p, fmt, ap);
	va_end(ap);

	if (!p)
		return;

	/* If str does not exist yet, just give it back p directly */
	if (!*str) {
		*str = p;
		return;
	}

	strcat(*str, p);
	free(p);
}
/* Callback to handle the HTTP response */
static size_t _write_callback(void *contents, size_t size, size_t nmemb,
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
typedef struct {
    char *length;
	char *password;
	char *username;
    char *database;
    char *host;
    char *rt_policy;
} slurm_influxdb;

#define MAX_STRING_LENGTH 2000

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

/*reverse column mix*/
static int invMixColumns(uint8_t (*state)[4]) {
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

/*AES128 decryption, parameter requirements are the same as encryption*/
static int aesDecrypt(const uint8_t *key, uint32_t keyLen, const uint8_t *ct, uint8_t *pt, uint32_t len) {
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
/*Read a hexadecimal string and convert it to a uint8_t type array*/
static int read_hex_bytes_from_file(char *path_tmp, const uint8_t *key, slurm_influxdb *data)
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
        printf("path_tmp err.\n");
        return SLURM_ERROR;
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
                            strcpy(data->rt_policy, influxdb[count-4]);
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
    return SLURM_SUCCESS;
}



static int _send_data2(slurm_influxdb *influxdb_conf, char *datastr2)
{
	CURL *curl_handle = NULL;
	CURLcode res;
	struct http_response chunk;
	int rc = SLURM_SUCCESS;
	long response_code = 0;
	static int error_cnt = 0;
	char *url = NULL, *rt_policy = NULL;
	//size_t length;
	/*
	 * Every compute node which is sampling data will try to establish a
	 * different connection to the influxdb server. The data will not be 
	 * cached and will be sent in real time at the head node of the job.
	 */
	DEF_TIMERS;
	START_TIMER;

	if (curl_global_init(CURL_GLOBAL_ALL) != 0) {
		printf("curl_easy_init in CURL_GLOBAL_ALL\n");
		rc = SLURM_ERROR;
		goto cleanup_global_init;
	} else if ((curl_handle = curl_easy_init()) == NULL) {
		printf("curl_easy_init in curl_handle\n");
		rc = SLURM_ERROR;
		goto cleanup_easy_init;
	}

    rt_policy = _parse_rt_policy(influxdb_conf->rt_policy, RPCNT);
	xstrfmtcat(url, "%s/write?db=%s&rp=%s&precision=ns", influxdb_conf->host,
		   influxdb_conf->database, rt_policy);
	if(rt_policy) free(rt_policy);

	chunk.message = malloc(1);
	chunk.size = 0;

	curl_easy_setopt(curl_handle, CURLOPT_URL, url);
	if (influxdb_conf->password)
		curl_easy_setopt(curl_handle, CURLOPT_PASSWORD,
				 influxdb_conf->password);

	curl_easy_setopt(curl_handle, CURLOPT_POST, 1);
	curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, datastr2);
	curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDSIZE, strlen(datastr2));
	if (influxdb_conf->username)
		curl_easy_setopt(curl_handle, CURLOPT_USERNAME,
				 influxdb_conf->username);
	curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, _write_callback);
	curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *) &chunk);

	if ((res = curl_easy_perform(curl_handle)) != CURLE_OK) {
		if ((error_cnt++ % 100) == 0)
			printf("curl_easy_perform failed to send data (discarded). Reason: %s\n",
			                 curl_easy_strerror(res));
		rc = SLURM_ERROR;
		goto cleanup;
	}

	if ((res = curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE,
				     &response_code)) != CURLE_OK) {
		printf("curl_easy_getinfo response code failed: %s\n",
		                                    curl_easy_strerror(res));
		rc = SLURM_ERROR;
		goto cleanup;
	}

	/* In general, status codes of the form 2xx indicate success,
	 * 4xx indicate that InfluxDB could not understand the request, and
	 * 5xx indicate that the system is overloaded or significantly impaired.
	 * Errors are returned in JSON.
	 * https://docs.influxdata.com/influxdb/v0.13/concepts/api/
	 */
	if (response_code >= 200 && response_code <= 205) {
		printf("data write success \n");
		if (error_cnt > 0)
			error_cnt = 0;
	} else {
		rc = SLURM_ERROR;
		printf("data write failed, response code: %ld \n", response_code);
			/* Strip any trailing newlines. */
			while (chunk.message[strlen(chunk.message) - 1] == '\n')
				chunk.message[strlen(chunk.message) - 1] = '\0';
			printf("JSON response body: %s \n", chunk.message);
	}


cleanup:
	free(chunk.message);
	free(url);
cleanup_easy_init:
	curl_easy_cleanup(curl_handle);
cleanup_global_init:
	curl_global_cleanup();
	END_TIMER;
	printf("took %s to send data \n", TIME_STR);
	return rc;	
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

void spost_init(slurm_influxdb *influxdb_data)
{
    /*given sufficient length*/
    influxdb_data->username = malloc(320) ;
    memset(influxdb_data->username, 0, 320);
    influxdb_data->password = malloc(320);
    memset(influxdb_data->password, 0, 320);
    influxdb_data->database = malloc(640) ;
    memset(influxdb_data->database, 0, 640);
    influxdb_data->host = malloc(640);
    memset(influxdb_data->host, 0, 320);
    influxdb_data->rt_policy = malloc(640);
    memset(influxdb_data->rt_policy, 0, 640);
}


void spost_fini(slurm_influxdb * influxdb_data)
{
    free(influxdb_data->username);
    free(influxdb_data->password);
    free(influxdb_data->database);
    free(influxdb_data->host);
    free(influxdb_data->rt_policy);
    free(influxdb_data);
}

typedef enum {
	PROFILE_FIELD_NOT_SET,
	PROFILE_FIELD_UINT64,
	PROFILE_FIELD_DOUBLE
} acct_gather_profile_field_type_t;

typedef struct {
	char *name;
	acct_gather_profile_field_type_t type;
} acct_gather_profile_dataset_t;


typedef struct {
	char *name;
	acct_gather_profile_field_type_t type;
} send_string_t;



int contains_non_digit(const char *str) {
       /* Check if the string is empty */
    if (str == NULL) {
        return SLURM_ERROR;
    }
    /* Check if the first character is a negative sign */
    if (*str == '-') {
        str++; // Skip the negative sign and check the rest of the string
    }

    /* Traverse the string until encountering the null terminator '\0' */
    while (*str) {
        /* if the current character is not a digit */
        if (!isdigit((unsigned char)*str)) { 
            /* non-digit character exists*/
            return SLURM_ERROR; 
        }
        str++;
    }
    /*all characters are digits*/ 
    return SLURM_SUCCESS; 
}

/* "Generate a nanosecond-level timestamp" */ 
uint64_t get_nanotime() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (ts.tv_sec * 1000000000LL) + ts.tv_nsec;  
}

static char* _record_table(char * data, uint32_t jobid, int stepd, uid_t uid, char *username)
{
    if (data == NULL || username == NULL) {
        perror("data or user is NULL");
        return NULL;
    }
	time_t ct;
    int tmp = 0x7FFFFFFF;
    char dataset[10] = "Spost"; /* dataset associated to this task when profiling */
    ct = time(NULL);
    char *str = NULL;
    uint64_t timestamp = get_nanotime(); 
    char hostbuffer[2048] = {'0'};
    int hostname = gethostname(hostbuffer, sizeof(hostbuffer));
    if(hostname == -1) {
        memset(hostbuffer, 0, sizeof(hostbuffer));
        strcpy(hostbuffer,"no_host");
    }

    if(stepd != 0x7FFFFFFF) {
        xstrfmtcat(str, "%s,uid=%"PRIu64",step=%d,sendnode=%s jobid=%d,data=\"%s\",username=\"%s\",record_time=%"PRIu64" %"PRIu64"\n", 
                                dataset, uid, stepd, hostbuffer, jobid, data, username, (uint64_t)ct, timestamp);        
    }
    else
        xstrfmtcat(str, "%s,uid=%"PRIu64",step=%d,sendnode=%s jobid=%d,data=\"%s\",username=\"%s\",record_time=%"PRIu64" %"PRIu64"\n", 
                                dataset, uid, tmp, hostbuffer, jobid, data, username, (uint64_t)ct, timestamp);
    return str;
}
void print_spost_help(void)
{
    printf(
"spost [<OPTION>]                                                          \n"
"    Ordinary users can only query their own jobs.                          \n"
"    The flow limit for each user on the same node is 30 times in 10 seconds.\n"
"    Valid <OPTION> values are:                                            \n"
"     -j, --jobs:                                                          \n"
"        Specify the job ID,                                               \n"
"        and you can also specify the job step ID simultaneously,          \n"
"          e.g., spost -j 1001.batch.                                      \n"
"     -p, --post:                                                          \n"
"         Send customized messages,                                        \n"
"         Maximum support for sending 4k data in a single transmission.   \n" 
"\n");
}


struct flock lock;
void lock_file_write(int fd)
{
    /* Lock file */
    lock.l_type = F_WRLCK;    // Write lock
    lock.l_whence = SEEK_SET; // The lock starts from the beginning of the file.
    lock.l_start = 0;         // The lock starts from the file.
    lock.l_len = 0;           // Lock the entire file.
    if (fcntl(fd, F_SETLKW, &lock) == -1) {
        perror("Error locking file");
        close(fd);
        exit(1);
    }
}
void unlock_file_write(int fd)
{
    /* Unlock the file. */ 
    lock.l_type = F_UNLCK;   //Unlock
    if (fcntl(fd, F_SETLK, &lock) == -1) {
        perror("Error unlocking file");
        close(fd);
        exit(1);
    }
}

int create_directory(const char *path, char *script, uid_t uid, bool *flag) 
{
    int rc = SLURM_SUCCESS;
    if((path == NULL) ||(script == NULL)) {
        perror("path or script is NULL");
        return SLURM_ERROR;
    }
    struct stat st;
    char* full_path = NULL;
    xstrfmtcat(full_path, "%s/%s",path, script);
    int retry_cnt = 0;
    time_t timestamp = time(NULL);
    int count = 1;
    if (stat(path, &st) == 0) {
        if (S_ISDIR(st.st_mode)) {
            if (access(full_path, F_OK) == 0) {

                if (access(full_path, R_OK|W_OK) != 0) {
                    if(chmod(full_path, 0755) < 0) {
                         free(full_path);
                         return SLURM_ERROR;
                    }
                }
                uid_t uid_read;
                time_t timestamp_read;
                int count_read;
                char buffer[800];  
                int fd = -1;
                ssize_t bytes_read = -1;
                do {
                    retry_cnt++;
                    rc = SLURM_SUCCESS;
                    fd = open(full_path, O_RDWR  | O_CREAT , 0644);  
                    if (fd == -1) {
                        //perror("Error opening file, retrying");
                        rc =  SLURM_ERROR;
                        usleep(1000); 
                        continue;
                    }
                    lock_file_write(fd);

                    bytes_read = read(fd, buffer, sizeof(buffer) - 1);  
                    if (bytes_read == -1) {
                        //perror("Error reading file, retrying");
                        //printf("Error code: %d\n", errno);  
                        unlock_file_write(fd);
                        *flag = true;
                        close(fd);
                        rc =  SLURM_ERROR;
                        usleep(1000); 
                        continue;
                    }
        
                    /* Use sscanf to parse what you read */ 
                    int parsed_count = sscanf(buffer, "%x,%lx,%x\n", &uid_read, &timestamp_read, &count_read);

                    if (parsed_count != 3) {
                        //printf("Error reading data, retrying.\n");
                        unlock_file_write(fd);
                        close(fd);
                        *flag = true;
                        rc = SLURM_ERROR;
                        usleep(1000); 
                        continue;
                    }
                    unlock_file_write(fd);
                    close(fd); 
                } while(rc == SLURM_ERROR && retry_cnt < 10);

               
                if(rc == SLURM_ERROR) {
                    perror("Error reading data error");
                    free(full_path);
                    return rc;
                }
                    
                //printf("UID: %d, Timestamp: %ld Count:%d\n", uid_read, timestamp_read, count_read);
                retry_cnt = 0;
                do {
                    retry_cnt++;
                    /* Reopen the file to clear its contents. `O_TRUNC` will clear the file contents. */ 
                    fd = open(full_path, O_WRONLY | O_TRUNC);  
                    lock_file_write(fd);
                    if (fd == -1) {
                        //perror("Error opening file for writing");
                        rc =  SLURM_ERROR;
                        usleep(1000); 
                        continue;                       
                    }
                    double tmp = difftime(timestamp, timestamp_read);
                    if(tmp <= 10 ) {
                        if(count_read < 30 && count_read >= 0) {
                            count = count_read + 1; 
                            char buffer[2560] = {'0'};
                            int len = snprintf(buffer, sizeof(buffer), "%x,%lx,%x\n",uid, timestamp_read, count);
                            ssize_t bytes_written = write(fd, buffer, len);
                            if (bytes_written == -1) {
                                //perror("Error writing to file, retrying");
                                unlock_file_write(fd);
                                close(fd);
                                rc = SLURM_ERROR;
                                usleep(1000); 
                                continue;
                            }              
                            *flag = false;    
                        } else {
                            if(count_read < 0 ) {
                                count = 30;
                            }
                            int len = snprintf(buffer, sizeof(buffer), "%x,%lx,%x\n", uid, timestamp_read, count_read);
                            ssize_t bytes_written = write(fd, buffer, len);
                            if (bytes_written == -1) {
                                //perror("Error writing to file, retrying");
                                unlock_file_write(fd);
                                close(fd);
                                rc = SLURM_ERROR;
                                usleep(1000); 
                                continue;
                            }  
                            *flag = true;
                            rc = 2;
                            printf("Your user has triggered too many requests within 10 seconds." 
                                    "Please try again later. Currently, each user is allowed to execute" 
                                    " a maximum of 30 requests per node within 10 seconds.\n");

                        }
                
                    } else if( tmp > 10 ) {
                        *flag = false;  
                        count = 1;
                        int len = snprintf(buffer, sizeof(buffer), "%x,%lx,%x\n", uid, timestamp, count);
                        ssize_t bytes_written = write(fd, buffer, len);
                        if (bytes_written == -1) {
                            //perror("Error writing to file, retrying");
                            unlock_file_write(fd);
                            close(fd);
                            rc = SLURM_ERROR;
                            usleep(1000); 
                            continue;
                        }  
                    } 
                    unlock_file_write(fd);
                    close(fd);
                }while(rc == SLURM_ERROR && retry_cnt < 5);

                if(rc == SLURM_ERROR) {
                    perror("Error write data error");
                    free(full_path);
                    return rc;
                }
 
            } else {
                int fd = open(full_path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);  // Permissions: rw-r--r--
                lock_file_write(fd);
                char buffer[2560] = {'0'};
                /* Format the string, convert the numeric part to hexadecimal, and separate it with commas */ 
                int len = snprintf(buffer, sizeof(buffer), "%x,%lx,%x\n", uid, timestamp, count);
                ssize_t bytes_written = write(fd, buffer, len);
                if (bytes_written == -1) {
                    perror("Error writing to file");
                    unlock_file_write(fd);
                    close(fd);
                    free(full_path);
                    return SLURM_ERROR;
                }
                *flag = true;
                unlock_file_write(fd);
                close(fd);
            }
        } else {
            fprintf(stderr, "'%s' exists but is not a directory.\n", path);
            free(full_path);
            return SLURM_ERROR;
        }

    } else {
        mode_t old_umask;
        old_umask = umask(0);
        if (mkdir(path, 0777) == -1) {
            perror("mkdir failed");
            free(full_path);
            return SLURM_ERROR;
        } 
        umask(old_umask); 
        printf("Directory '%s' created successfully.\n", path);
        int fd = open(full_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        lock_file_write(fd);
        *flag = true; 
        char buffer[2560] = {'0'};
        int len = snprintf(buffer, sizeof(buffer), "%x,%lx,%x\n", uid, timestamp, count);
        ssize_t bytes_written = write(fd, buffer, len);
        if (bytes_written == -1) {
            perror("Error writing to file");
            unlock_file_write(fd);
            close(fd);
            free(full_path);
            return SLURM_ERROR;
        }
        unlock_file_write(fd);
        close(fd);
    }
    free(full_path);
    return rc;
}

int rpc_limit()
{   
    int rc = SLURM_SUCCESS;
    struct passwd *pw;
    pw =   getpwuid(getuid());
    bool sleep_ms = false;
    char *path = NULL;
    char *script = NULL;
    char *rm_script  = NULL;
    xstrfmtcat(path,"/tmp/uid_post_flush");
    xstrfmtcat(script,"limit_uid_%d",pw->pw_uid);   
    xstrfmtcat(rm_script,"%s/%s",path,script);
    rc = create_directory(path, script, pw->pw_uid, &sleep_ms);
    if(rc == SLURM_ERROR)
        remove(rm_script);
    if(sleep_ms)
        usleep(300000); 
    if(path)
        free(path);
    if(script)
        free(script);
    if(rm_script)
        free(rm_script);
    return rc;
}

static int parse_command_and_send(int argc, char **argv, slurm_influxdb *data, char *post_str) 
{
    struct passwd *pw;
    int c = -1, optionIndex = 0, rc = SLURM_SUCCESS ;
    //bool no_jobid          = false; 
    params.convert_flags   = CONVERT_NUM_UNIT_EXACT;
	params.units           = NO_VAL;
	params.opt_uid         = getuid();
	params.opt_gid         = getgid();
    params.level           = 0x001;
    bool jobid_label       = false;
    bool post_lable        = false; 
    char* jobids           = NULL;
    char* send             = NULL;
    int   stepd            = 0x7FFFFFFF;
    uint32_t job_id        = 0;
    char *job              = NULL; 
    char *stepids          = NULL;    
    assert(params.opt_uid != -1);
    
    pw = getpwuid(params.opt_uid);
	static struct option long_options[] = {
                {"jobid",      required_argument,        0,    'j'},
                {"post",       required_argument,        0,    'p'},
                {"help",       no_argument,        0,    'h'},
                {0,            0,		                 0,     0}};
    while (1) {		/* now cycle through the command line */
		c = getopt_long(argc, argv,
				"p:j:h",
				long_options, &optionIndex);      
        if (c == -1) {
            //no_jobid = true;
            break;
        }
        switch (c) {
        	case 'j':
                jobid_label = true;
                jobids = malloc(strlen(optarg)+1);
                sprintf(jobids,"%s",optarg);
                break;
        	case 'p':
                post_lable = true;
                sprintf(post_str,"%s",optarg);
                break;
        	case 'h':
                print_spost_help();
                exit(1);
    		case '?':	/* getopt() has explained it */
			    exit(1);
            default:
                break;
        }
        // if(no_jobid)
        //     break;     
    }

    if(jobid_label) {
        if(strlen(jobids)> 400) {
            printf("The job ID being sent is too long. \n");
            rc = SLURM_ERROR;
            goto fail;          
        }

        char *p  = strchr(jobids, '.');
        if(p != NULL && *p !='\0') {
            stepids = malloc(sizeof(char) * 400);
            job = malloc(sizeof(char) * 400);

            strncpy(job, jobids, p - jobids); 
            job[p - jobids] = '\0';
            strcpy(stepids, p + 1); 
        }

        if(job != NULL) {
            if(contains_non_digit(job) != 0) {
                printf("There is an error in the job ID format specification "
                                    "(other: only one job can be specified).\n");
                rc = SLURM_ERROR;
                goto fail;
            } 
            job_id = (uint32_t) atol(job);
        } else {
            if(contains_non_digit(jobids) != 0) {
                printf("There is an error in the job ID format specification "
                                    "(other: only one job can be specified).\n");
                rc = SLURM_ERROR;
                goto fail;
            } 
            job_id = (uint32_t) atol(jobids);
        }

        if(stepids) {
            
            if(strncmp(stepids, "batch", 5) == 0) {
                stepd = -5;
            } else if(strncmp(stepids, "extern", 6) == 0){
                stepd = -4;
            } else if(contains_non_digit(stepids) == 0) {
                stepd = atol(stepids);
            } else {
                printf("Invalid characters have been specified. (other: only one job can be specified). \n");
                rc = SLURM_ERROR;
                goto fail;
            }
        }

    } else {
        printf("Please specify a single job ID using -j.\n");
        rc = SLURM_ERROR;
        goto fail;
    }

    if(post_lable) {
        /*max send 4k*/
       if(strlen(post_str) >= MAX_SQL_STR) {
         rc = SLURM_ERROR;
         printf("The specified string length is too long. "
                         "Please reduce the length of the custom string.\n");
         goto fail;
       } 
    } else{
         printf("Please specify the push string using -p.\n");
         rc = SLURM_ERROR;
         goto fail;
    }

    send = _record_table(post_str, job_id, stepd, params.opt_uid, pw->pw_name);
    if(send)
        _send_data2(data, send);
    else
        rc = SLURM_ERROR;
fail:
    if(jobids)
        free(jobids);
    if(job)
        free(job);
    if(stepids)
        free(stepids);
    if(send)
        free(send);
   return rc;
}

int main(int argc ,char** argv) {
     
    slurm_influxdb* influxdb_data = malloc(sizeof(slurm_influxdb));
    char* configpath = NULL;
    char* post_str = NULL;

    int rc = SLURM_SUCCESS ;
    char tmp_conf[] = "/etc/acct_gather.conf.key";
    
    rc = rpc_limit();
    if(rc == SLURM_ERROR || rc ==2) {
        free(influxdb_data);
        return SLURM_SUCCESS;   
    }
    post_str = calloc(MAX_SQL_STR, sizeof(char));
    memset(post_str, '\0', MAX_SQL_STR *sizeof(char) );
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
        goto file_fail1;
    }

    spost_init(influxdb_data);
    /*16-bit encryption key*/
    const uint8_t key[]="fcad715bd73b5cb0";
    rc = read_hex_bytes_from_file(configpath, key, influxdb_data);
    if(rc == SLURM_ERROR)
         goto file_fail;
    rc =  parse_command_and_send(argc, argv, influxdb_data, post_str);
    if(rc == SLURM_ERROR)
         goto file_fail;    

file_fail:
    spost_fini(influxdb_data);
file_fail1:
    free(post_str);
    if (configpath)
        free(configpath);
    return SLURM_SUCCESS;
}