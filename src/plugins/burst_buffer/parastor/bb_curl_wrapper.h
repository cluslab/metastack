#ifndef BB_CURL_WRAPPER_H
#define BB_CURL_WRAPPER_H

#include <stddef.h>

/* login for getting token, return 0 on success */
extern int rest_login(const char *url, const char *user, const char *password, char *token_buf);

/* login for getting permanent token, return 0 on success */
extern int permanent_rest_login(const char* url, const char* user, const char* password, char* token_buf);   

/* unviersal function of rest api call with token */
extern int call_rest_api_with_token(const char* url, const char* method, const char* body,
    const char* token, char** response_out);
#endif 
