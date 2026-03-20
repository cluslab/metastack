#ifndef BB_CURL_WRAPPER_H
#define BB_CURL_WRAPPER_H

#include <stddef.h>


typedef enum {
    BB_SUCCESS = 0,
    BB_CODE_ERROR = -1, 
    BB_API_ERROR = -2, 
    BB_API_TIMEOUT = -3,
    BB_SUCCESS_NO_DATA = 1
} bb_error;

/* Session login (Basic auth); returns 0 on success */
extern int rest_login(const char *url, const char *user, const char *password, char *token_buf);

/* Permanent-token login (JSON POST); returns 0 on success */
extern int permanent_rest_login(const char* url, const char* user, const char* password, char* token_buf);

/* REST request with token header; caller must xfree() *response_out */
extern int call_rest_api_with_token(const char* url, const char* method, const char* body,
    const char* token, char** response_out);
/**
 * @brief REST request via libcurl with token header and overall timeout (seconds).
 * @param response_out Output: response body; caller must xfree()
 * @return BB_SUCCESS, BB_CODE_ERROR, BB_API_ERROR, or BB_API_TIMEOUT
 */
extern int call_rest_api_with_token_timeout(const char *url, const char *method, const char *body,
    const char *token, uint32_t timeout, char **response_out);
#endif 
