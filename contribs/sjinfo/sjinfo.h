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

#include <stdio.h>

struct c_string;
typedef struct c_string c_string_t;
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

#define KEYDIR "/opt/gridview/slurm"
#endif /* !_SJINFO_H */