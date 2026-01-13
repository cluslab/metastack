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

/* login for getting token, return 0 on success */
extern int rest_login(const char *url, const char *user, const char *password, char *token_buf);

/* login for getting permanent token, return 0 on success */
extern int permanent_rest_login(const char* url, const char* user, const char* password, char* token_buf);   

/* unviersal function of rest api call with token */
extern int call_rest_api_with_token(const char* url, const char* method, const char* body,
    const char* token, char** response_out);
/** 
 * @brief 通过CURL调用RESTful API
 * @param url
 * @param method
 * @param body
 * @param token
 * @param timeout
 * @param response_out 出参：返回JSON格式响应体
 * @return 0表示成功，-1表示代码错误，-2表示接口错误，-3表示接口超时
 */
extern int call_rest_api_with_token_timeout(const char *url, const char *method, const char *body,
    const char *token, long timeout, char **response_out);
#endif 
