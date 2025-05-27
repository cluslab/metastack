/*****************************************************************************
 *
 *  Copyright (C) 2007-2008 Lawrence Livermore National Security, LLC.
 *  Produced at Lawrence Livermore National Laboratory.
 *  Written by Mark Grondona <mgrondona@llnl.gov>.
 *
 *  UCRL-CODE-235358
 * 
 *  This file is part of chaos-spankings, a set of spank plugins for SLURM.
 * 
 *  This is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 ****************************************************************************/
#ifndef _SJINFO_H
#define _SJINFO_H



#define CPU_ABNORMAL_FLAG "cpu"
#define PROCESS_ABNORMAL_FLAG "process"
#define NODE_ABNORMAL_FLAG "node"
#define CPU_ABNORMAL_FLAG_DESC "CPU utilization below threshold"
#define PROCESS_ABNORMAL_FLAG_DESC "Operational process anomalies"
#define NODE_ABNORMAL_FLAG_DESC "Node communication exception"
#define SACCT_MAX_ENTRIES 200
#include <stdio.h>
typedef struct c_string {
    char *str;
    size_t alloced;
    size_t len;
} c_string_t;

typedef struct print_field {
	int len;  /* what is the width of the print */
	char *name;  /* name to be printed in header */
	void (*print_routine) (); /* what is the function to print with  */
	uint16_t type; /* defined in the local function */
} print_field_t;

typedef struct sacct_entry{
    long long int jobid;
    long long int stepdid;
    long long int reqmem;
    long long int alloc_cpu;

} sacct_entry_t;

typedef int (*ListFindF) (void *x, void *key);
/* */
/**
 * Creates and initializes a new c_string_t object.
 *
 * @return A pointer to the newly created c_string_t object.
 */
c_string_t *c_string_create(void);

/**
 * Destroys and frees the memory associated with the specified c_string_t object.
 *
 * @param cs A pointer to the c_string_t object to be destroyed.
 */
void c_string_destory(c_string_t *cs);

/**
 * Appends a given C-style string to the end of the c_string_t object.
 *
 * @param cs  A pointer to the c_string_t object.
 * @param str The C-style string (null-terminated) to append.
 */
void c_string_append_str(c_string_t *cs, const char *str);

/**
 * Prepends a given C-style string to the beginning of the c_string_t object.
 *
 * @param cs  A pointer to the c_string_t object.
 * @param str The C-style string (null-terminated) to prepend.
 */
void c_string_front_str(c_string_t *cs, const char *str);

/**
 * Returns the length of the string stored in the c_string_t object (excluding the null terminator).
 *
 * @param cs A pointer to the c_string_t object.
 * @return   The length of the string as a size_t value.
 */
size_t c_string_len(const c_string_t *cs);

/**
 * Retrieves a pointer to the internal C-style string stored in the c_string_t object.
 *
 * @param cs A pointer to the c_string_t object.
 * @return   A const char* pointer to the internal string.
 */
const char *c_string_peek(const c_string_t *cs);

// /* Names for the values of the `has_arg' field of `struct option'.  */

extern void print_fields_str(print_field_t *field, char *value, int last);

#define KEYDIR "NONE"
#endif /* !_SJINFO_H */

