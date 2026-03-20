#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>
#include "slurm/slurm.h"
#include "src/common/log.h"
#include "src/common/read_config.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "bb_curl_wrapper.h"
#include <limits.h>

struct memory {
    char *response;
    size_t size;
};

/* Internal callback, concatenate response to memory */
static size_t write_callback(void *data, size_t size, size_t nmemb, void *userp)
{
    size_t realsize = size * nmemb;
    struct memory *mem = (struct memory *)userp;

    char *ptr = xrealloc(mem->response, mem->size + realsize + 1);
    if (!ptr)
        return 0;

    mem->response = ptr;
    memcpy(&(mem->response[mem->size]), data, realsize);
    mem->size += realsize;
    mem->response[mem->size] = '\0';

    return realsize;
}
/* discard body */
static size_t discard_callback(void *ptr, size_t size, size_t nmemb, void *userdata)
{
    return size * nmemb;
}

static size_t header_callback(char *buffer, size_t size, size_t nitems, void *userdata)
{
    size_t total_size = size * nitems;
    char *token_buf = (char *)userdata;

    /* Expect a response header line: Token: <value> */
    if (xstrncasecmp(buffer, "Token:", 6) == 0) {
        /* Strip the "Token:" prefix and leading whitespace */
        const char *value = buffer + 6;
        while (*value == ' ' || *value == '\t')
            value++;
        /* Remove trailing line breaks */
        size_t len = strcspn(value, "\r\n");
        strncpy(token_buf, value, len);
        token_buf[len] = '\0';
    }

    return total_size;
}

/* login for getting token */
extern int rest_login(const char *url, const char *user, const char *password, char *token_buf)
{
    CURL *curl = curl_easy_init();
    if (!curl)
        return -1;

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);

    /* set basic auth */
    curl_easy_setopt(curl, CURLOPT_USERNAME, user);
    curl_easy_setopt(curl, CURLOPT_PASSWORD, password);

    /* Set the header callback, passing token_buf directly to it. */
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, header_callback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, token_buf);

    /* ignore ssl */
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    /* discard body */
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, discard_callback);

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type:application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        log_flag(BURST_BUF, "%s: curl POST failed: %s", __func__, curl_easy_strerror(res));
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        return -1;
    }
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    if (token_buf[0] == '\0') {
        log_flag(BURST_BUF, "%s: token not found in response headers", __func__);
        return -1;
    }
    return 0;
}

extern int permanent_rest_login(const char *url, const char *user, const char *password, char *token_buf)
{
    CURL *curl = curl_easy_init();
    if (!curl)
        return -1;

    /* make json data */
    char post_data[512];
    int n = snprintf(post_data, sizeof(post_data),
        "{\"username\":\"%s\",\"password\":\"%s\",\"permanentTokenFlag\":true,\"clientType\":\"REST\"}",
        user, password);
    if (n < 0 || n >= sizeof(post_data)) {
        error("%s: login POST body exceeds buffer or snprintf failed", __func__);
        curl_easy_cleanup(curl);
        return -1;
    }

    /* set url and post_data */
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_data);

    /* set header callback and get token */
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, header_callback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, token_buf);


    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);


    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, discard_callback);

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type:application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        log_flag(BURST_BUF, "%s: curl POST failed: %s", __func__, curl_easy_strerror(res));
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        return -1;
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (token_buf[0] == '\0') {
        error("%s: token not found in response headers", __func__);
        return -1;
    }

    return 0;
}


/* Universal RESTful API calls with token */
int call_rest_api_with_token(const char *url, const char *method, const char *body,
    const char *token, char **response_out)
{
    if (!url || !method || !response_out || !token ) {
        return -1;
    }
    CURL *curl = curl_easy_init();
    if (!curl)
        return -1;
    struct memory chunk = { 0 };
    chunk.response = xmalloc(1);
    chunk.size = 0;
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &chunk);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    /* set method */
    if (method && strcasecmp(method, "POST") == 0) {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
    } else if (method && strcasecmp(method, "PUT") == 0) {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
    } else if (method && strcasecmp(method, "DELETE") == 0) {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    } /* Default GET */
    /* set body */
    if (body && (strcasecmp(method, "POST") == 0 || strcasecmp(method, "PUT") == 0)) {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
    }
    /* token header */
    struct curl_slist *headers = NULL;
    if (token) {
        char header_buf[512];
        snprintf(header_buf, sizeof(header_buf), "token: %s", token);
        headers = curl_slist_append(headers, header_buf);
    }
    /* Content-Type header */
    headers = curl_slist_append(headers, "Content-Type:application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        log_flag(BURST_BUF, "%s: curl perform failed: %s", __func__, curl_easy_strerror(res));
        xfree(chunk.response);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        return -1;
    }
    *response_out = chunk.response; /* caller must xfree */
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return 0;
}




/**
 * @brief Perform a REST call via libcurl with Bearer-style token header and timeout.
 * @param response_out Out: JSON body; caller must free
 * @return BB_SUCCESS, BB_CODE_ERROR, BB_API_ERROR, or BB_API_TIMEOUT
 */
extern int call_rest_api_with_token_timeout(const char *url, const char *method, const char *body,
    const char *token, uint32_t timeout, char **response_out)
{
    if (!url || !method || !response_out || !token ) {
        return BB_CODE_ERROR;
    }
    CURL *curl = curl_easy_init();
    if (!curl)
        return BB_CODE_ERROR;
    struct memory chunk = { 0 };
    chunk.response = xmalloc(1);
    chunk.size = 0;
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &chunk);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    
    if (timeout > 0 && timeout <= LONG_MAX) {
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)timeout);
    } else if (timeout > 0 && timeout > LONG_MAX) {
        log_flag(BURST_BUF, "%s: timeout exceeds long max; clamping to LONG_MAX", __func__);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)LONG_MAX);
    } else if (timeout < 0) {
        error("%s: invalid timeout (< 0)", __func__);
        xfree(chunk.response);
        curl_easy_cleanup(curl);
        return BB_CODE_ERROR;
    }
    
    /* set method, default GET */
    if (method && strcasecmp(method, "POST") == 0) {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
    } else if (method && strcasecmp(method, "PUT") == 0) {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
    } else if (method && strcasecmp(method, "DELETE") == 0) {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    } 
    /* set body */
    if (body && (strcasecmp(method, "POST") == 0 || strcasecmp(method, "PUT") == 0)) {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
    }
    /* token header */
    struct curl_slist *headers = NULL;
    if (token) {
        char header_buf[512];
        snprintf(header_buf, sizeof(header_buf), "token: %s", token);
        headers = curl_slist_append(headers, header_buf);
    }
    /* Content-Type header */
    headers = curl_slist_append(headers, "Content-Type:application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    CURLcode res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
        int ret_code = BB_API_ERROR;
        if (res == CURLE_OPERATION_TIMEDOUT) {
            log_flag(BURST_BUF, "%s: curl request timed out: %s", __func__, curl_easy_strerror(res));
            ret_code = BB_API_TIMEOUT;
        } else {
            log_flag(BURST_BUF, "%s: curl perform failed: %s", __func__, curl_easy_strerror(res));
        }
        xfree(chunk.response);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        return ret_code;
    }
    *response_out = chunk.response; /* caller must xfree */
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return BB_SUCCESS;
}
