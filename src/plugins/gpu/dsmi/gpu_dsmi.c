/*****************************************************************************\
 *  npu_generic.c - Support generic interface to a NPU.
 *****************************************************************************
 *
 *  Copyright (C) 2020 Computer Center, Peking University
 *  *  Produced at Computer Center, Peking University.
 *  *  Written by Yinping Ma <mayinping@pku.edu.cn>, Chun Fan <fanchun@pku.edu.cn>.
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
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
 *
\*****************************************************************************/

#define _GNU_SOURCE

#include <dlfcn.h>
#include "src/common/slurm_xlator.h"
#include "src/common/gres.h"
#include "src/common/log.h"
#include "src/common/list.h"
#include "ctype.h"
#include "../common/gpu_common.h"

#include "dsmi_common_interface.h"

/*
 * #defines needed to test DSMI.
 */
#define NPU_MODE_FREQ	1
#define NPU_MODE_MEM	2
#define MAX_CPUS	0x8000
#define ULONG_BYTES	(sizeof(unsigned long))
#define ULONG_BITS	(ULONG_BYTES * 8)
/*
 * The # of unsigned longs needed to accommodate a bitmask array capable
 * of representing MAX_CPUS cpus (will vary if 32-bit or 64-bit)
 * E.g. for a 130 CPU 64-bit machine: (130 + 63) / 64 = 3.02
 * -> Integer division floor -> 3 ulongs to represent 130 CPUs
 */
#define CPU_SET_SIZE	((MAX_CPUS + (ULONG_BITS-1)) / ULONG_BITS)
#define NVLINK_SELF	-1
#define NVLINK_NONE	0
#define FREQS_SIZE	512
#define FREQS_CONCISE	5 // This must never be smaller than 5, or error

#define NPU_LOW		((unsigned int) -1)
#define NPU_MEDIUM	((unsigned int) -2)
#define NPU_HIGH_M1	((unsigned int) -3)
#define NPU_HIGH	((unsigned int) -4)

static bitstr_t	*saved_npus = NULL;

// dsmiReturn_t
/*
define a enum struct like nvmlReturn_t 
*/
typedef enum dsmiReturn_enum {
	DSMI_SUCCESS = 0,        
	DSMI_ERROR_UNINITIALIZED = 1,
	DSMI_ERROR_INVALID_ARGUMENT = 2,
	DSMI_ERROR_NOT_SUPPORTED = 3,
	DSMI_ERROR_NO_PERMISSION = 4,
	DSMI_ERROR_ALREADY_INITIALIZED = 5,
	DSMI_ERROR_NOT_FOUND = 6,
	DSMI_ERROR_INSUFFICIENT_SIZE = 7,
	DSMI_ERROR_INSUFFICIENT_POWER = 8,
	DSMI_ERROR_DRIVER_NOT_LOADED = 9,
	DSMI_ERROR_TIMEOUT = 10,
	DSMI_ERROR_IRQ_ISSUE = 11,
	DSMI_ERROR_LIBRARY_NOT_FOUND = 12,
	DSMI_ERROR_FUNCTION_NOT_FOUND = 13,
	DSMI_ERROR_CORRUPTED_INFOROM = 14,
	DSMI_ERROR_NPU_IS_LOST = 15,
	DSMI_ERROR_RESET_REQUIRED = 16,
	DSMI_ERROR_OPERATING_SYSTEM = 17,
	DSMI_ERROR_LIB_RM_VERSION_MISMATCH = 18,
	DSMI_ERROR_IN_USE = 19,
	DSMI_ERROR_UNKNOWN = 999
} dsmiReturn_t;
typedef struct dsmiPciInfo_st {
	union {
		struct {
#ifdef SLURM_BIGENDIAN
			uint64_t domain : 32;
			uint64_t reserved : 16;
			uint64_t bus : 8;
			uint64_t device : 5;
			uint64_t function : 3;
#else
			uint64_t function : 3;
			uint64_t device : 5;
			uint64_t bus : 8;
			uint64_t reserved : 16;
			uint64_t domain : 32;
#endif
		};
		uint64_t bdfid;
	};
} dsmiPciInfo_t;

#define DSMI_STRING_BUFFER_SIZE			80
#define MAX_NPU_NUM                     10

/*
 * These variables are required by the generic plugin interface.  If they
 * are not found in the plugin, the plugin loader will ignore it.
 *
 * plugin_name - A string giving a human-readable description of the
 * plugin.  There is no maximum length, but the symbol must refer to
 * a valid string.
 *
 * plugin_type - A string suggesting the type of the plugin or its
 * applicability to a particular form of data or method of data handling.
 * If the low-level plugin API is used, the contents of this string are
 * unimportant and may be anything.  Slurm uses the higher-level plugin
 * interface which requires this string to be of the form
 *
 *	<application>/<method>
 *
 * where <application> is a description of the intended application of
 * the plugin (e.g., "auth" for Slurm authentication) and <method> is a
 * description of how this plugin satisfies that application.  Slurm will
 * only load authentication plugins if the plugin_type string has a prefix
 * of "auth/".
 *
 * plugin_version - an unsigned 32-bit integer containing the Slurm version
 * (major.minor.micro combined into a single number).
 */
const char	plugin_name[]		= "NPU Generic plugin";
const char	plugin_type[]		= "gpu/dsmi";
const uint32_t	plugin_version		= SLURM_VERSION_NUMBER;
static log_level_t log_lvl              = LOG_LEVEL_DEBUG5;

/*
* Helper method for converting DSMI error codes into readable strings.
*/
const char * dsmiErrorString(dsmiReturn_t result)
{
	switch(result)
	{
		case 1: return "DSMI was not initialized with dsmiInit().";break;
		case 2: return "a supplied argument is invalid.";break;
		case 3: return "the requested operation is not available on target device.";break;
		case 4: return "you don't have permission to perform this operation.";break;
		case 5: return "DSMI has already initialized.";break;
		case 6: return "a query to find an object was unccessful.";break;
		case 7: return "an input argument is not large enough.";break;
		case 8: return "a device's external power cables are not properly attached.";break;
		case 9: return "NPU driver is not loaded.";break;
		case 10: return "user provided timeout passed.";break;
		case 11: return "NPU Kernel detected an interrupt issue with a NPU.";break;
		case 12: return "DSMI Shared Library couldn't be found or loaded.";break;
		case 13: return "local version of DSMI doesn't implement this function.";break;
		case 14: return "infoROM is corrupted";break;
		case 15: return "the NPU has fallen off the bus or has otherwise become inaccessible.";break;
		case 16: return "the NPU requires a reset before it can be used again.";break;
		case 17: return "the NPU control device has been blocked by the operating system/cgroups.";break;
		case 18: return "RM detects a driver/library version mismatch.";break;
		case 19: return "an operation cannot be performed because the NPU is currently in use.";break;
		case 999: return "an internal driver error occurred.";break;
		default: return "Unknown Error.";
	}
}

/*
* Retrieves the version of the DSMI library.
* TODO: not realized yet
*/
dsmiReturn_t dsmiSystemGetDSMIVersion(char *version, unsigned int length)
{
	// dsmiReturn_t dsmi_rc;
	strcpy(version, "DSMI 1.0");
	return DSMI_SUCCESS;
}

/*
* Retrieves the name of this device.
* return
*/
dsmiReturn_t dsmiDeviceGetName(uint32_t device, char *name, unsigned int length)
{
	int ret = 0;
	// get the name of device, like atlas 800        in nvidia: esla C2070
	struct dsmi_chip_info_stru info = {{0},{0},{0}};
	ret = dsmi_get_chip_info(0, &info);
	strcpy(name, (char *)info.chip_type);
    strcat(name, " ");
    strcat(name, (char *)info.chip_name);
    strcat(name, " ");
    strcat(name, (char *)info.chip_ver);

	if(ret != 0 )
        error("DSMI: Failed to get name of the NPU.");
	return DSMI_SUCCESS;
}

/*
* Retrieves minor number for the device. The minor number for the device is such 
* that the Atlas device node file for each NPU will have the form /dev/davinci[minor number].
* TODO: not realized yet
*/
dsmiReturn_t dsmiDeviceGetMinorNumber ( uint32_t device, unsigned int* minorNumber )
{
    int ret = 0;
    unsigned int  phyid = 0;
    ret = dsmi_get_phyid_from_logicid (device,&phyid);
    if(ret == 0)
    	*minorNumber =  phyid;
    else 
    	error("DSMI: Failed to get minor number.");
    return DSMI_SUCCESS;
}

/*
* Set clocks that applications will lock to. 
* TODO: not realized yet
*/
dsmiReturn_t dsmiDeviceSetApplicationsClocks ( uint32_t device, unsigned int  memClockMHz, unsigned int  graphicsClockMHz )
{
	// dsmiReturn_t dsmi_rc;
	return DSMI_SUCCESS;
}

/*
* Resets the application clock to the default value 
* TODO: not realized yet
*/
dsmiReturn_t dsmiDeviceResetApplicationsClocks ( uint32_t device )
{
	// dsmiReturn_t dsmi_rc;
	return DSMI_SUCCESS;
}

static void _set_cpu_set_bitstr(bitstr_t *cpu_set_bitstr,
				unsigned long *cpu_set,
				unsigned int cpu_set_size)
{
	int j, k, b;
	int bit_cur;
	int bitstr_bits = (int) bit_size(cpu_set_bitstr);
	int cpu_set_bits = (cpu_set_size * ULONG_BITS);

	// If this fails, then something went horribly wrong
	if (bitstr_bits != cpu_set_bits)
		fatal("%s: bitstr_bits != cpu_set_bits", __func__);

	bit_cur = bitstr_bits - 1;

	// Iterate through each cpu_set long int
	for (j = cpu_set_size - 1; j >= 0; --j) {
		// Iterate through the bytes of the jth ulong bitmask
		char *bitmask = (char *) &cpu_set[j];
#ifdef SLURM_BIGENDIAN
		for (k = 0; k < ULONG_BYTES; ++k) {
#else
		for (k = ULONG_BYTES - 1; k >= 0; --k) {
#endif // SLURM_BIGENDIAN
			unsigned char byte = bitmask[k];
			unsigned char mask;
			// If byte is zero, nothing to set
			if (byte == 0) {
				bit_cur -= 8;
				continue;
			}

			// Test each bit of byte, from MSB to LSB. Set if needed
			mask = 0x80;
			for (b = 0; b < 8; ++b) {
				if (byte & mask)
					bit_set(cpu_set_bitstr, bit_cur);
				mask >>= 1;
				bit_cur--;
			}
			xassert(mask == 0x00);
		}
	}

	xassert(bit_cur == -1);
	// If DSMI gave us an empty CPU affinity, then something is very wrong
	if (bit_set_count(cpu_set_bitstr) == 0)
		fatal("%s: cpu_set_bitstr is empty! No CPU affinity for device",
		      __func__);
}


/*
 * Initialize the NPU library. This takes a few seconds
 */
// static void _dsmi_init(void)
// {
// 	debug("initialize dsmi library.");
// 	// return DSMI_SUCCESS;
// }

/*
 * Undo _dsmi_init
 * TODO: not realized yet
 */
// static void _dsmi_shutdown(void)
// {
// 	debug("%s: unloading %s", __func__, plugin_name);
// 	// return DSMI_SUCCESS;
// }

static unsigned int _xlate_freq_value(char *npu_freq)
{
	unsigned int value;

	if ((npu_freq[0] < '0') && (npu_freq[0] > '9'))
		return 0;	/* Not a numeric value */
	value = strtoul(npu_freq, NULL, 10);
	return value;
}

static unsigned int _xlate_freq_code(char *npu_freq)
{
	if (!npu_freq || !npu_freq[0])
		return 0;
	if ((npu_freq[0] >= '0') && (npu_freq[0] <= '9'))
		return 0;	/* Pure numeric value */
	if (!strcasecmp(npu_freq, "low"))
		return NPU_LOW;
	else if (!strcasecmp(npu_freq, "medium"))
		return NPU_MEDIUM;
	else if (!strcasecmp(npu_freq, "highm1"))
		return NPU_HIGH_M1;
	else if (!strcasecmp(npu_freq, "high"))
		return NPU_HIGH;

	debug("%s: %s: Invalid job NPU frequency (%s)",
	      plugin_type, __func__, npu_freq);
	return 0;	/* Bad user input */
}

static void _parse_npu_freq2(char *npu_freq, unsigned int *npu_freq_code,
			     unsigned int *npu_freq_value,
			     unsigned int *mem_freq_code,
			     unsigned int *mem_freq_value, bool *verbose_flag)
{
	char *tmp, *tok, *sep, *save_ptr = NULL;
	if (!npu_freq || !npu_freq[0])
		return;
	tmp = xstrdup(npu_freq);
	tok = strtok_r(tmp, ",", &save_ptr);
	while (tok) {
		sep = strchr(tok, '=');
		if (sep) {
			sep[0] = '\0';
			sep++;
			if (!strcasecmp(tok, "memory")) {
				if (!(*mem_freq_code = _xlate_freq_code(sep)) &&
				    !(*mem_freq_value =_xlate_freq_value(sep))){
					debug("Invalid job NPU memory frequency: %s",
					      tok);
				}
			} else {
				debug("%s: %s: Invalid job device frequency type: %s",
				      plugin_type, __func__, tok);
			}
		} else if (!strcasecmp(tok, "verbose")) {
			*verbose_flag = true;
		} else {
			if (!(*npu_freq_code = _xlate_freq_code(tok)) &&
			    !(*npu_freq_value = _xlate_freq_value(tok))) {
				debug("Invalid job NPU frequency: %s", tok);
			}
		}
		tok = strtok_r(NULL, ",", &save_ptr);
	}
	xfree(tmp);
}

static void _parse_npu_freq(char *npu_freq, unsigned int *npu_freq_num,
			    unsigned int *mem_freq_num, bool *verbose_flag)
{
	unsigned int def_npu_freq_code = 0, def_npu_freq_value = 0;
	unsigned int def_mem_freq_code = 0, def_mem_freq_value = 0;
	unsigned int job_npu_freq_code = 0, job_npu_freq_value = 0;
	unsigned int job_mem_freq_code = 0, job_mem_freq_value = 0;
	char *def_freq;

	_parse_npu_freq2(npu_freq, &job_npu_freq_code, &job_npu_freq_value,
			 &job_mem_freq_code, &job_mem_freq_value, verbose_flag);

	// Defaults to high for both mem and gfx
	def_freq = slurm_get_gpu_freq_def();
	_parse_npu_freq2(def_freq, &def_npu_freq_code, &def_npu_freq_value,
			 &def_mem_freq_code, &def_mem_freq_value, verbose_flag);
	xfree(def_freq);

	if (job_npu_freq_code)
		*npu_freq_num = job_npu_freq_code;
	else if (job_npu_freq_value)
		*npu_freq_num = job_npu_freq_value;
	else if (def_npu_freq_code)
		*npu_freq_num = def_npu_freq_code;
	else if (def_npu_freq_value)
		*npu_freq_num = def_npu_freq_value;

	if (job_mem_freq_code)
		*mem_freq_num = job_mem_freq_code;
	else if (job_mem_freq_value)
		*mem_freq_num = job_mem_freq_value;
	else if (def_mem_freq_code)
		*mem_freq_num = def_mem_freq_code;
	else if (def_mem_freq_value)
		*mem_freq_num = def_mem_freq_value;
}

static int _sort_freq_descending(const void *a, const void *b)
{
	return (*(unsigned long*)b - *(unsigned long*)a);
}

/*
 * Get all possible memory frequencies for the device
 *
 * device		(IN) The device handle
 * mem_freqs_size	(IN/OUT) The size of the mem_freqs array; this will be
 * 			overwritten with the number of memory freqs found.
 * mem_freqs		(OUT) The possible memory frequencies, sorted in
 * 			descending order
 *
 * Return true if successful, false if not.
 */
static bool _dsmi_get_mem_freqs(uint32_t device,
				unsigned int *mem_freqs_size,
				unsigned int *mem_freqs)
{
	int ret = 0;
	DEF_TIMERS;
	START_TIMER;
    int device_num = 0;
    ret = dsmi_get_device_count(&device_num);

	if(ret !=0 )
	{
		error("%s: Failed to get supported memory frequencies for the "
		      "NPU.", __func__);
		return false;
	}
	struct dsmi_memory_info_stru device_memory_info = {0};
	mem_freqs_size = (unsigned int *)&device_num;
	int i;
	for(i = 0; i < device_num; ++i )
	{
		ret = dsmi_get_memory_info(i, &device_memory_info);
		mem_freqs[i] = device_memory_info.freq;
	}
	END_TIMER;
	debug3("get memory frequency took %ld microseconds",
	       DELTA_TIMER);

	qsort(mem_freqs, *mem_freqs_size,
	      sizeof(unsigned int), _sort_freq_descending);

	if ((*mem_freqs_size > 1) &&
	    (mem_freqs[0] < mem_freqs[(*mem_freqs_size)-1])) {
		error("%s: mem frequencies are not stored in descending order!",
		      __func__);
		return false;
	}
	return true;
}

/*
 * Get all possible graphics frequencies for the device
 *
 * device		(IN) The device handle
 * mem_freq		(IN) The memory frequency to get graphics freqs for.
 * gfx_freqs_size	(IN/OUT) The size of the gfx_freqs array; this will
 * 			be overwritten with the number of graphics freqs found.
 * gfx_freqs		(OUT) The possible graphics frequencies, sorted in
 * 			descending order
 *
 * Return true if successful, false if not.
 */
static bool _dsmi_get_gfx_freqs(uint32_t device,
				unsigned int mem_freq,
				unsigned int *gfx_freqs_size,
				unsigned int *gfx_freqs)
{
	DEF_TIMERS;
	START_TIMER;
    int device_num = 0;
	int ret = -1;
    ret = dsmi_get_device_count(&device_num);
	info("%s:DSMI:device_num=%d",__func__,device_num);
    unsigned int frequency = 0;
	int i;
	for(i = 0; i < device_num; ++i )
	{
		ret = dsmi_get_device_frequency(i, 7, &frequency);
		gfx_freqs[i] = frequency;
		//ret = 0;
		//gfx_freqs[i] = 0;
		info("DSMI:%d,ret=%d",i,ret);
	}

	END_TIMER;
	debug3("get npu frequencies took %ld microseconds",
	       DELTA_TIMER);
	if (ret != 0) {
		error("%s: Failed to get supported graphics frequencies for the"
		      " NPU at mem frequency %u.", __func__,frequency);
		return false;
	}

	qsort(gfx_freqs, *gfx_freqs_size,
	      sizeof(unsigned int), _sort_freq_descending);

	if ((*gfx_freqs_size > 1) &&
	    (gfx_freqs[0] < gfx_freqs[(*gfx_freqs_size)-1])) {
		error("%s: gfx frequencies are not stored in descending order!",
		      __func__);
		return false;
	}
	return true;
}

/*
 * Print out all possible graphics frequencies for the given device and mem
 * freq. If there are many frequencies, only prints out a few.
 *
 * device		(IN) The device handle
 * mem_freq		(IN) The memory frequency to get graphics freqs for.
 * gfx_freqs_size	(IN) The size of the gfx_freqs array
 * gfx_freqs		(IN) A preallocated empty array of size gfx_freqs_size
 * 			to fill with possible graphics frequencies
 * l			(IN) The log level at which to print
 *
 * NOTE: The contents of gfx_freqs will be modified during use.
 */
static void _dsmi_print_gfx_freqs(uint32_t device, unsigned int mem_freq,
				  unsigned int gfx_freqs_size,
				  unsigned int *gfx_freqs, log_level_t l)
{
	unsigned int size = gfx_freqs_size;
	bool concise = false;
	unsigned int i;

	if (!_dsmi_get_gfx_freqs(device, mem_freq, &size, gfx_freqs))
		return;

	if (size > FREQS_CONCISE)
		concise = true;

	log_var(l, "        Possible NPU Graphics Frequencies (%u):", size);
	log_var(l, "        ---------------------------------");
	if (!concise) {
		for (i = 0; i < size; ++i) {
			log_var(l, "          *%u MHz [%u]", gfx_freqs[i], i);
		}
		return;
	}
	// first, next, ..., middle, ..., penultimate, last
	log_var(l, "          *%u MHz [0]", gfx_freqs[0]);
	log_var(l, "          *%u MHz [1]", gfx_freqs[1]);
	log_var(l, "          ...");
	log_var(l, "          *%u MHz [%u]", gfx_freqs[(size - 1) / 2],
	     (size - 1) / 2);
	log_var(l, "          ...");
	log_var(l, "          *%u MHz [%u]", gfx_freqs[size - 2], size - 2);
	log_var(l, "          *%u MHz [%u]", gfx_freqs[size - 1], size - 1);
}

/*
 * Print out all possible memory and graphics frequencies for the given device.
 * If there are more than FREQS_SIZE frequencies, prints a summary instead
 *
 * device	(IN) The device handle
 * l		(IN) The log level at which to print
 */
static void _dsmi_print_freqs(uint32_t device, log_level_t l)
{
	unsigned int mem_size = FREQS_SIZE;
	unsigned int mem_freqs[FREQS_SIZE] = {0};
	unsigned int gfx_freqs[FREQS_SIZE] = {0};
	unsigned int i;
	bool concise = false;

	if (!_dsmi_get_mem_freqs(device, &mem_size, mem_freqs))
		return;

	if (mem_size > FREQS_CONCISE)
		concise = true;

	log_var(l, "Possible NPU Memory Frequencies (%u):", mem_size);
	log_var(l, "-------------------------------");
	if (concise) {
		// first, next, ..., middle, ..., penultimate, last
		unsigned int tmp;
		log_var(l, "    *%u MHz [0]", mem_freqs[0]);
		_dsmi_print_gfx_freqs(device, mem_freqs[0], FREQS_SIZE,
				      gfx_freqs, l);
		log_var(l, "    *%u MHz [1]", mem_freqs[1]);
		_dsmi_print_gfx_freqs(device, mem_freqs[1], FREQS_SIZE,
				      gfx_freqs, l);
		log_var(l, "    ...");
		tmp = (mem_size - 1) / 2;
		log_var(l, "    *%u MHz [%u]", mem_freqs[tmp], tmp);
		_dsmi_print_gfx_freqs(device, mem_freqs[tmp], FREQS_SIZE,
				      gfx_freqs, l);
		log_var(l, "    ...");
		tmp = mem_size - 2;
		log_var(l, "    *%u MHz [%u]", mem_freqs[tmp], tmp);
		_dsmi_print_gfx_freqs(device, mem_freqs[tmp], FREQS_SIZE,
				      gfx_freqs, l);
		tmp = mem_size - 1;
		log_var(l, "    *%u MHz [%u]", mem_freqs[tmp], tmp);
		_dsmi_print_gfx_freqs(device, mem_freqs[tmp], FREQS_SIZE,
				      gfx_freqs, l);
		return;
	}

	for (i = 0; i < mem_size; ++i) {
		log_var(l,"    *%u MHz [%u]", mem_freqs[i], i);
		_dsmi_print_gfx_freqs(device, mem_freqs[i], FREQS_SIZE,
				      gfx_freqs, l);
	}
}

/*
 * Convert frequency to nearest valid frequency found in frequency array
 *
 * freq		(IN/OUT) The frequency to check, in MHz. Also the output, if
 * 		it needs to be changed.
 * freqs_size	(IN) The size of the freqs array
 * freqs	(IN) An array of frequency values in MHz, sorted highest to
 * 		lowest
 *
 * Inspired by src/common/cpu_frequency#_cpu_freq_freqspec_num()
 */
static void _get_nearest_freq(unsigned int *freq, unsigned int freqs_size,
			      unsigned int *freqs)
{
	unsigned int i;

	if (!freq || !(*freq)) {
		log_var(log_lvl, "%s: No frequency supplied", __func__);
		return;
	}
	if (!freqs || !(*freqs)) {
		log_var(log_lvl, "%s: No frequency list supplied", __func__);
		return;
	}
	if (freqs_size <= 0) {
		log_var(log_lvl, "%s: Frequency list is empty", __func__);
		return;
	}

	// Check for special case values; freqs is sorted in descending order
	switch ((*freq)) {
	case NPU_LOW:
		*freq = freqs[freqs_size - 1];
		debug2("Frequency NPU_LOW: %u MHz", *freq);
		return;

	case NPU_MEDIUM:
		*freq = freqs[(freqs_size - 1) / 2];
		debug2("Frequency NPU_MEDIUM: %u MHz", *freq);
		return;

	case NPU_HIGH_M1:
		if (freqs_size == 1)
			*freq = freqs[0];
		else
			*freq = freqs[1];
		debug2("Frequency NPU_HIGH_M1: %u MHz", *freq);
		return;

	case NPU_HIGH:
		*freq = freqs[0];
		debug2("Frequency NPU_HIGH: %u MHz", *freq);
		return;

	default:
		debug2("Freq is not a special case. Continue...");
		break;
	}

	/* check if freq is out of bounds of freqs */
	if (*freq > freqs[0]) {
		log_var(log_lvl, "Rounding requested frequency %u MHz down to "
			"%u MHz (highest available)", *freq, freqs[0]);
		*freq = freqs[0];
		return;
	} else if (*freq < freqs[freqs_size - 1]) {
		log_var(log_lvl, "Rounding requested frequency %u MHz up to %u "
			"MHz (lowest available)", *freq, freqs[freqs_size - 1]);
		*freq = freqs[freqs_size - 1];
		return;
	}

	/* check for frequency, and round up if no exact match */
	for (i = 0; i < freqs_size - 1;) {
		if (*freq == freqs[i]) {
			// No change necessary
			debug2("No change necessary. Freq: %u MHz", *freq);
			return;
		}
		i++;
		/*
		 * Step down to next element to round up.
		 * Safe to advance due to bounds checks above here
		 */
		if (*freq > freqs[i]) {
			log_var(log_lvl, "Rounding requested frequency %u MHz "
				"up to %u MHz (next available)", *freq,
				freqs[i - 1]);
			*freq = freqs[i - 1];
			return;
		}
	}
	error("%s: Got to the end of the function. This shouldn't happen. Freq:"
	      " %u MHz", __func__, *freq);
}

/*
 * Get the nearest valid memory and graphics clock frequencies
 *
 * device		(IN) The Atlas NPU device handle
 * mem_freq		(IN/OUT) The requested memory frequency, in MHz. This
 * 			will be overwritten with the output value, if different.
 * gfx_freq 		(IN/OUT) The requested graphics frequency, in MHz. This
 * 			will be overwritten with the output value, if different.
 */
static void _dsmi_get_nearest_freqs(uint32_t device,
				    unsigned int *mem_freq,
				    unsigned int *gfx_freq)
{
	unsigned int mem_freqs[FREQS_SIZE] = {0};
	unsigned int mem_freqs_size = FREQS_SIZE;
	unsigned int gfx_freqs[FREQS_SIZE] = {0};
	unsigned int gfx_freqs_size = FREQS_SIZE;

	// Get the memory frequencies
	if (!_dsmi_get_mem_freqs(device, &mem_freqs_size, mem_freqs))
		return;

	// Set the nearest valid memory frequency for the requested frequency
	_get_nearest_freq(mem_freq, mem_freqs_size, mem_freqs);

	// Get the graphics frequencies at this memory frequency
	if (!_dsmi_get_gfx_freqs(device, *mem_freq, &gfx_freqs_size, gfx_freqs))
		return;
	// Set the nearest valid graphics frequency for the requested frequency
	_get_nearest_freq(gfx_freq, gfx_freqs_size, gfx_freqs);
}

/*
 * Set the memory and graphics clock frequencies for the NPU
 *
 * device	(IN) The Atlas NPU device ID
 * mem_freq	(IN) The memory clock frequency, in MHz
 * gfx_freq	(IN) The graphics clock frequency, in MHz
 *
 * Returns true if successful, false if not
 */
static bool _dsmi_set_freqs(uint32_t device, unsigned int mem_freq,
			    unsigned int gfx_freq)
{
	dsmiReturn_t dsmi_rc;
	DEF_TIMERS;
	START_TIMER;
	dsmi_rc = dsmiDeviceSetApplicationsClocks(device, mem_freq, gfx_freq);
	END_TIMER;
	debug3("dsmiDeviceSetApplicationsClocks(%u, %u) took %ld microseconds",
	       mem_freq, gfx_freq, DELTA_TIMER);
	if (dsmi_rc != DSMI_SUCCESS) {
		error("%s: Failed to set memory and graphics clock frequency "
		      "pair (%u, %u) for the NPU: %s", __func__, mem_freq,
		      gfx_freq, dsmiErrorString(dsmi_rc));
		return false;
	}
	return true;
}

/*
 * Reset the memory and graphics clock frequencies for the NPU to the same
 * default frequencies that are used after system reboot or driver reload. This
 * default cannot be changed.
 *
 * device	(IN) The Atlas NPU device ID
 *
 * Returns true if successful, false if not
 */
static bool _dsmi_reset_freqs(uint32_t device)
{
	dsmiReturn_t dsmi_rc;
	DEF_TIMERS;

	START_TIMER;
	dsmi_rc = dsmiDeviceResetApplicationsClocks(device);
	END_TIMER;
	debug3("dsmiDeviceResetApplicationsClocks() took %ld microseconds",
	       DELTA_TIMER);
	if (dsmi_rc != DSMI_SUCCESS) {
		error("%s: Failed to reset NPU frequencies to the hardware default: %s",
		      __func__, dsmiErrorString(dsmi_rc));
		return false;
	}
	return true;
}

static unsigned int _dsmi_get_gfx_freq(uint32_t device)
{
	unsigned int freq = 0;
	DEF_TIMERS;
	START_TIMER;
	int ret = 0;
	ret = dsmi_get_device_frequency(0, 7, &freq);
	END_TIMER;
	debug3("dsmi_get_device_frequency(AI core) took %ld microseconds",
	        DELTA_TIMER);
    if(ret != 0 ){
    	error("%s: Failed to get the NPU frequency: %u", __func__,freq);
    	return 0;
    }
    return freq;
}

static unsigned int _dsmi_get_mem_freq(uint32_t device)
{
	unsigned int freq = 0;
	DEF_TIMERS;
	START_TIMER;
	int ret = 0;
	ret = dsmi_get_device_frequency(0, 1, &freq);
	END_TIMER;
	debug3("dsmi_get_device_frequency(memory) took %ld microseconds",
	        DELTA_TIMER);
    if(ret != 0 ){
    	error("%s: Failed to get the NPU frequency: %u", __func__,freq);
    	return 0;
    }
    return freq;
}

/*
 * Convert a frequency value to a string
 * Returned string must be xfree()'ed
 */
static char *_freq_value_to_string(unsigned int freq)
{
	switch (freq) {
	case NPU_LOW:
		return xstrdup("low");
		break;
	case NPU_MEDIUM:
		return xstrdup("medium");
		break;
	case NPU_HIGH:
		return xstrdup("high");
		break;
	case NPU_HIGH_M1:
		return xstrdup("highm1");
		break;
	default:
		return xstrdup_printf("%u", freq);
		break;
	}
}

/*
 * Reset the frequencies of each NPU in the step to the hardware default
 * NOTE: DSMI must be initialized beforehand
 *
 * npus		(IN) A bitmap specifying the NPUs on which to operate.
 */
static void _reset_freq(bitstr_t *npus)
{
	int npu_len = bit_size(npus);
	int i = -1, count = 0, count_set = 0;
	bool freq_reset = false;

	/*
	 * Reset the frequency of each device allocated to the step
	 */
	for (i = 0; i < npu_len; i++) {
		// uint32_t device;
		if (!bit_test(npus, i))
			continue;
		count++;

		debug2("Memory frequency before reset: %u",
		       _dsmi_get_mem_freq(i));
		debug2("Graphics frequency before reset: %u",
		       _dsmi_get_gfx_freq(i));
		freq_reset =_dsmi_reset_freqs(i);
		debug2("Memory frequency after reset: %u",
		       _dsmi_get_mem_freq(i));
		debug2("Graphics frequency after reset: %u",
		       _dsmi_get_gfx_freq(i));

		// TODO: Check to make sure that the frequency reset

		if (freq_reset) {
			log_var(log_lvl, "Successfully reset NPU[%d]", i);
			count_set++;
		} else {
			log_var(log_lvl, "Failed to reset NPU[%d]", i);
		}
	}

	if (count_set != count) {
		log_var(log_lvl,
			"%s: Could not reset frequencies for all NPUs. "
			"Set %d/%d total NPUs", __func__, count_set, count);
		fprintf(stderr, "Could not reset frequencies for all NPUs. "
			"Set %d/%d total NPUs\n", count_set, count);
	}
}

/*
 * Set the frequencies of each NPU specified for the step
 * NOTE: DSMI must be initialized beforehand
 *
 * npus		(IN) A bitmap specifying the NPUs on which to operate.
 * npu_freq	(IN) The frequencies to set each of the NPUs to. If a NULL or
 * 		empty memory or graphics frequency is specified, then NpuFreqDef
 * 		will be consulted, which defaults to "high,memory=high" if not
 * 		set.
 */
static void _set_freq(bitstr_t *npus, char *npu_freq)
{
	bool verbose_flag = false;
	int npu_len = 0;
	int i = -1, count = 0, count_set = 0;
	unsigned int npu_freq_num = 0, mem_freq_num = 0;
	bool freq_set = false, freq_logged = false;
	char *tmp = NULL;
	bool task_cgroup = false;
	bool constrained_devices = false;
	bool cgroups_active = false;

	/*
	 * Parse frequency information
	 */
	debug2("_parse_npu_freq(%s)", npu_freq);
	_parse_npu_freq(npu_freq, &npu_freq_num, &mem_freq_num, &verbose_flag);
	if (verbose_flag)
		debug2("verbose_flag ON");

	tmp = _freq_value_to_string(mem_freq_num);
	debug2("Requested NPU memory frequency: %s", tmp);
	xfree(tmp);
	tmp = _freq_value_to_string(npu_freq_num);
	debug2("Requested NPU graphics frequency: %s", tmp);
	xfree(tmp);

	if (!mem_freq_num || !npu_freq_num) {
		debug2("%s: No frequencies to set", __func__);
		return;
	}

	// Check if NPUs are constrained by cgroups
	cgroup_conf_init();
	if (slurm_cgroup_conf.constrain_devices)
		constrained_devices = true;
	
	// Check if task/cgroup plugin is loaded
	if (strstr(slurm_conf.task_plugin, "cgroup"))
		task_cgroup = true;

	// If both of these are true, then NPUs will be constrained
	if (constrained_devices && task_cgroup) {
		cgroups_active = true;
		npu_len = bit_set_count(npus);
		debug2("%s: cgroups are configured. Using LOCAL NPU IDs",
		       __func__);
	} else {
	 	npu_len = bit_size(npus);
		debug2("%s: cgroups are NOT configured. Assuming GLOBAL NPU IDs",
		       __func__);
	}

	/*
	 * Set the frequency of each device allocated to the step
	 */
	for (i = 0; i < npu_len; i++) {
		char *sep = "";
		// uint32_t device;

		// Only check the global NPU bitstring if not using cgroups
		if (!cgroups_active && !bit_test(npus, i)) {
			debug2("Passing over Atlas device %u", i);
			continue;
		}
		count++;

		debug2("Setting frequency of Atlas device %u", i);
		_dsmi_get_nearest_freqs(i, &mem_freq_num, &npu_freq_num);

		debug2("Memory frequency before set: %u",
		       _dsmi_get_mem_freq(i));
		debug2("Graphics frequency before set: %u",
		       _dsmi_get_gfx_freq(i));
		freq_set = _dsmi_set_freqs(i, mem_freq_num, npu_freq_num);
		debug2("Memory frequency after set: %u",
		       _dsmi_get_mem_freq(i));
		debug2("Graphics frequency after set: %u",
		       _dsmi_get_gfx_freq(i));

		if (mem_freq_num) {
			xstrfmtcat(tmp, "%smemory_freq:%u", sep, mem_freq_num);
			sep = ",";
		}
		if (npu_freq_num) {
			xstrfmtcat(tmp, "%sgraphics_freq:%u", sep,
				   npu_freq_num);
		}

		if (freq_set) {
			log_var(log_lvl, "Successfully set NPU[%d] %s", i, tmp);
			count_set++;
		} else {
			log_var(log_lvl, "Failed to set NPU[%d] %s", i, tmp);
		}

		if (verbose_flag && !freq_logged) {
			fprintf(stderr, "NpuFreq=%s\n", tmp);
			freq_logged = true;	/* Just log for first NPU */
		}
		xfree(tmp);
	}

	if (count_set != count) {
		log_var(log_lvl,
			"%s: Could not set frequencies for all NPUs. "
			"Set %d/%d total NPUs", __func__, count_set, count);
		fprintf(stderr, "Could not set frequencies for all NPUs. "
			"Set %d/%d total NPUs\n", count_set, count);
	}
}

static void _dsmi_get_driver(char *driver, unsigned int len)
{
	int ret = 0;
    	unsigned int len_driver = -1;
	dsmi_get_version(0, driver, len, &len_driver);

	if (ret != 0) {
		error("DSMI: Failed to get the version of the system's graphics"
		      "driver.");
		driver[0] = '\0';
	}
}

/*
 * Get the version of the DSMI library
 */
static void _dsmi_get_version(char *version, unsigned int len)
{
	dsmiReturn_t dsmi_rc = dsmiSystemGetDSMIVersion(version, len);
	if (dsmi_rc != DSMI_SUCCESS) {
		error("DSMI: Failed to get the version of the system's graphics"
		      "version: %s", dsmiErrorString(dsmi_rc));
		version[0] = '\0';
	}
}

/*
 * Replace all space characters in a string with underscores, and make all
 * characters lower case
 */
static void _underscorify_tolower(char *str)
{
	for (int i = 0; str[i]; i++) {
		str[i] = tolower(str[i]);
		if (str[i] == ' ')
			str[i] = '_';
	}
}

/*
 * Get the name of the NPU
 *
 * dv_ind	(IN) The device index
 * device_name	(OUT) Name of NPU devices
 * size		(OUT) Size of name
 */
static void _dsmi_get_device_name(uint32_t device, char *device_name,
				 unsigned int size)
{
	dsmiReturn_t dsmi_rc = dsmiDeviceGetName(device, device_name, size);
	if (dsmi_rc != DSMI_SUCCESS) {
		error("DSMI: Failed to get name of the NPU: %s",
		      dsmiErrorString(dsmi_rc));
	}
	_underscorify_tolower(device_name);
}

/*
 * Retrieves minor number for the device. The minor number for the device is
 * such that the Atlas device node file for each NPU will have the form
 * /dev/davinci[minor_number].
 */
static void _dsmi_get_device_minor_number(uint32_t device,
					 unsigned int *minor)
{
	dsmiReturn_t dsmi_rc = dsmiDeviceGetMinorNumber(device, minor);
	if (dsmi_rc != DSMI_SUCCESS) {
		error("DSMI: Failed to get minor number of NPU: %s",
		      dsmiErrorString(dsmi_rc));
	}
}

/*
 * Does a linear search for string str in array of strings str_arr, starting
 * from index 0.
 * Returns the index of the first match found, else returns -1 if not found.
 *
 * str - the string to search for
 * str_array - the array of strings to search in
 * size - the size of str_arr
 */
// static int _get_index_from_str_arr(char *str, char **str_arr, unsigned int size)
// {
// 	int i;
// 	if (str_arr == NULL || str == NULL)
// 		return -1;
// 	for (i = 0; i < size; ++i) {
// 		if (xstrcmp(str, str_arr[i]) == 0) {
// 			return i;
// 		}
// 	}
// 	return -1;
// }

/*
 * Get the total # of NPUs in the system
 */
extern void gpu_p_get_device_count(unsigned int *device_count)
{
        *device_count = 0;
        int tmp_device_count = 0;
        int ret;
        ret = dsmi_get_device_count(&tmp_device_count);
        if (ret != 0) {
                error("DSMI: Failed to get device count.");
                *device_count = 0;
        } else
                *device_count = (unsigned int)tmp_device_count;
}

/*
 * Creates and returns a gres conf list of detected huawei npus on the node.
 * If an error occurs, return NULL
 * Caller is responsible for freeing the list.
 *
 * If the Atlas SCMI API exists, then query NPU info,
 * so the user doesn't need to specify manually in gres.conf.
 * Specifically populate cpu affinity and nvlink information
 */
static List _get_system_npu_list_dsmi(node_config_load_t *node_config)
{
	unsigned int i;
	unsigned int device_count = 0;
	List gres_list_system = list_create(destroy_gres_slurmd_conf);
	char driver[DSMI_STRING_BUFFER_SIZE];
	char version[DSMI_STRING_BUFFER_SIZE];

	// _dsmi_init();
	_dsmi_get_driver(driver, DSMI_STRING_BUFFER_SIZE);
	_dsmi_get_version(version, DSMI_STRING_BUFFER_SIZE);
	debug("Systems Graphics Driver Version: %s", driver);
	debug("DSMI Library Version: %s", version);

	gpu_p_get_device_count(&device_count);
	debug2("Device count: %d", device_count);

	// Loop through all the NPUs on the system and add to gres_list_system
	for (i = 0; i < device_count; ++i) {
		unsigned int minor_number = 0;
		char *device_file = NULL;
		char device_name[DSMI_STRING_BUFFER_SIZE] = {0};
		// char device_brand[DSMI_STRING_BUFFER_SIZE] = {0};
		// dsmiPciInfo_t pci_info;
		// uint64_t uuid = 0;

		_dsmi_get_device_name(i, device_name, DSMI_STRING_BUFFER_SIZE);
		//_dsmi_get_device_brand(i, device_brand,
		//		       DSMI_STRING_BUFFER_SIZE);
		_dsmi_get_device_minor_number(i, &minor_number);
		// pci_info.bdfid = 0;
		//_rsmi_get_device_pci_info(i, &pci_info);
		//_rsmi_get_device_unique_id(i, &uuid);

		xstrfmtcat(device_file, "/dev/davinci%u", minor_number);

		debug2("NPU index %u:", i);
		debug2("    Name: %s", device_name);
		//debug2("    Brand/Type: %s", device_brand);
		//debug2("    UUID: %lx", uuid);
		//debug2("    PCI Domain/Bus/Device/Function: %u:%u:%u.%u",
		//       pci_info.domain,
		//       pci_info.bus, pci_info.device, pci_info.function);
		debug2("    Device File (minor number): %s", device_file);
		if (minor_number != i+128)
			debug("Note: NPU index %u is different from minor # %u",
			      i, minor_number);

		// Print out possible memory frequencies for this device
		_dsmi_print_freqs(i, LOG_LEVEL_DEBUG2);

		add_gres_to_list(gres_list_system, "npu", 1,
				 node_config->cpu_cnt, NULL, NULL,
				 device_file, NULL, NULL, NULL,
				 GRES_CONF_ENV_DSMI);

		xfree(device_file);
	}

	// _dsmi_shutdown();

	info("%u NPU system device(s) detected", device_count);
	return gres_list_system;
}

extern int init(void)
{
	if (!dlopen("libdrvdsmi_host.so", RTLD_NOW | RTLD_GLOBAL))
		fatal("DSMI configured, but wasn't found.");

	debug("%s: %s loaded", __func__, plugin_name);

	return SLURM_SUCCESS;
}

extern int fini(void)
{
	debug("%s: unloading %s", __func__, plugin_name);

	return SLURM_SUCCESS;
}

extern int gpu_p_reconfig(void)
{
	return SLURM_SUCCESS;
}


extern List gpu_p_get_system_gpu_list(node_config_load_t *node_config)
{
	List gres_list_system = NULL;

	if (!(gres_list_system = _get_system_npu_list_dsmi(node_config)))
		error("System NPU detection failed");

	return gres_list_system;
}

extern void gpu_p_step_hardware_init(bitstr_t *usable_npus, char *tres_freq)
{
	char *freq = NULL;
	char *tmp = NULL;

	xassert(tres_freq);
	xassert(usable_npus);

	if (!usable_npus)
		return;		/* Job allocated no NPUs */
	if (!tres_freq)
		return;		/* No TRES frequency spec */

	if (!(tmp = strstr(tres_freq, "npu:")))
		return;		/* No NPU frequency spec */

	freq = xstrdup(tmp + 4);
	if ((tmp = strchr(freq, ';')))
		tmp[0] = '\0';

	// Save a copy of the NPUs affected, so we can reset things afterwards
	FREE_NULL_BITMAP(saved_npus);
	saved_npus = bit_copy(usable_npus);

	// _dsmi_init();
	// Set the frequency of each NPU index specified in the bitstr
	_set_freq(usable_npus, freq);
	xfree(freq);
}

extern void gpu_p_step_hardware_fini(void)
{
	if (!saved_npus)
		return;

	// Reset the frequencies back to the hardware default
	_reset_freq(saved_npus);
	FREE_NULL_BITMAP(saved_npus);
	// _dsmi_shutdown();
}

extern char *gpu_p_test_cpu_conv(char *cpu_range)
{
	unsigned long cpu_set[CPU_SET_SIZE];
	bitstr_t *cpu_aff_mac_bitstr;
	int i;
	char *result;
	info("%s: cpu_range: %s", __func__, cpu_range);

	if (!cpu_range) {
		error("cpu_range is null");
		return xstrdup("");
	}

	if (cpu_range[0] != '~') {
		error("cpu_range doesn't start with `~`!");
		return xstrdup("");
	}

	// Initialize cpu_set to 0
	for (i = 0; i < CPU_SET_SIZE; ++i) {
		cpu_set[i] = 0;
	}

	if (xstrcmp(cpu_range, "~zero") == 0) {
		// nothing
	} else if (xstrcmp(cpu_range, "~max") == 0) {
		for (i = 0; i < CPU_SET_SIZE; ++i) {
			cpu_set[i] = (unsigned long)-1;
		}
	} else if (xstrcmp(cpu_range, "~one") == 0) {
		cpu_set[0] = 1;
	} else if (xstrcmp(cpu_range, "~three") == 0) {
		cpu_set[0] = 3;
	} else if (xstrcmp(cpu_range, "~half") == 0) {
		cpu_set[0] = 0xff00;
	} else if (cpu_range[1] == 'X') {
		/*
		 * Put in all -1's for each X
		 * Limit to CPU_SET_SIZE
		 */
		int count = MIN(strlen(&cpu_range[1]), CPU_SET_SIZE);
		for (i = 0; i < count; ++i) {
			cpu_set[i] = (unsigned long)-1;
		}
		for (i = count; i < CPU_SET_SIZE; ++i) {
			cpu_set[i] = 0;
		}
	} else {
		error("Unknown test keyword");
		return xstrdup("");
	}

	// Print out final cpu set
	for (i = 0; i < CPU_SET_SIZE; ++i) {
		if ((signed) cpu_set[i] == -1)
			printf("X");
		else {
			if (cpu_set[i] > 9)
				printf("(%lu)", cpu_set[i]);
			else
				printf("%lu", cpu_set[i]);
		}
	}
	printf("\n");

	cpu_aff_mac_bitstr = bit_alloc(MAX_CPUS);
	// Convert from DSMI cpu bitmask to slurm bitstr_t (machine fmt)
	_set_cpu_set_bitstr(cpu_aff_mac_bitstr, cpu_set, CPU_SET_SIZE);

	// Convert from bitstr_t to cpu range str
	result = bit_fmt_full(cpu_aff_mac_bitstr);

	bit_free(cpu_aff_mac_bitstr);
	return result;
}

extern int gpu_p_energy_read(uint32_t dv_ind, gpu_status_t *npu)
{
	return SLURM_SUCCESS;
}

