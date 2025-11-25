/******************************************************************************
* 版权信息：北京人大金仓信息技术股份有限公司

* 作者：KingbaseES

* 文件名：libkci_fe.h

* 功能描述：此文件包含结构体以及前端KES应用程序使用的外部函数的定义。

* 其它说明：

* 函数列表：

* 修改记录：
  1.修改时间：

  2.修改人：

  3.修改内容：

******************************************************************************/


#ifndef LIBKCI_FE_H //前端宏
#define LIBKCI_FE_H

#ifdef __cplusplus
extern "C"
{
#endif //宏 __cplusplus结束

#include <stdio.h>  //标准输入输出
#include <stdbool.h>    //标准bool类型

/*
 * kingbase_ext.h定义了后端的外部可见类型，比如Oid。
 */
#include "kingbase_ext.h"

/*
 * KCIcopyResult的选项标志
 */
#define KB_COPYRES_ATTRS          0x01
#define KB_COPYRES_TUPLES         0x02  /* 隐含KB_COPYRES_ATTRS */
#define KB_COPYRES_EVENTS         0x04
#define KB_COPYRES_NOTICEHOOKS    0x08

#define RETURN_IF(condition, retValue)  \
    if (condition) {                        \
        return (retValue);                  \
    }

#define libkci_free(__p)      \
    do {                     \
        if ((__p) != NULL) { \
            free((__p));     \
            (__p) = NULL;    \
        }                    \
    } while (0)


/* 应用程序可见的enum类型 */

/*
 * 虽然可以向这些列表中添加内容，但是不应该删除不再使用的值，
 * 也不应该重新定义常量--这将破坏与现有代码的兼容性。
 */

typedef enum
{
    CONNECTION_OK,
    CONNECTION_BAD,
    /* 以下为非阻塞模式的状态 */

    /*
     * 这些状态不应该被依赖
     * 它们只应该被用于用户反馈或类似的目的。
     */
    CONNECT_STARTED,            /* 等待连接  */
    CONNECT_MADE,           /* 连接完毕;等待发送。     */
    CONNECT_AWAITING_RESPONSE,  /* 等待后端的响应。*/
    CONNECT_AUTH_OK,            /* 收到认证;等待后端启动 */
    CONNECT_SETENV,         /* 设置环境变量 */
    CONNECT_SSL_STARTUP,        /* NSSL协商 */
    CONNECT_NEEDED,         /* 内部状态:需要connect () */
    CONNECT_CHECK_WRITABLE, /* 是否可以建立一个可写的连接。*/
    CONNECT_CONSUME,            /* 等待所有挂起的消息并使用它们。*/
    CONNECT_GSS_STARTUP     /* GSSAPI协商 */
} KCIConnectionStatus;;

typedef enum
{
    POLLING_FAILED = 0,
    POLLING_READING,        /* 这两种状态表明 */
    POLLING_WRITING,        /* 有用户可能在再次轮询前使用了select */
    POLLING_OK,
    POLLING_ACTIVE      /* 未使用的状态;暂时保留以便向后兼容*/
} KCIPollingStatus;;

typedef enum
{
    EXECUTE_EMPTY_QUERY = 0,        /* 执行空查询字符串 */
    EXECUTE_COMMAND_OK,         /* 后端正确执行了一个不返回任何内容的查询命令 */
    EXECUTE_TUPLES_OK,          /* 一个返回元组的查询命令被后端正确执行，KCIResult包含结果元组*/
    EXECUTE_COPY_OUT,               /* 正在拷贝出数据 */
    EXECUTE_COPY_IN,                /* 正在拷贝入数据 */
    EXECUTE_BAD_RESPONSE,           /* 从后端收到一个意外响应 */
    EXECUTE_NONFATAL_ERROR,     /* 通知或警告信息 */
    EXECUTE_FATAL_ERROR,            /* 查询失败 */
    KCIRES_COPY_BOTH,           /* 正在拷贝出/入数据s */
    KCIRES_SINGLE_TUPLE         /* 来自较大结果集的单个元组 */
} KCIExecuteStatus;;

typedef enum
{
    TRANSACTION_IDLE,               /* 连接处于空闲状态 */
    TRANSACTION_ACTIVE,             /* 命令执行中 */
    TRANSACTION_INTRANS,            /* 在事务块内处于空闲状态 */
    TRANSACTION_INERROR,            /* 在失败的事务内处于空闲状态 */
    TRANSACTION_UNKNOWN             /* 无法确定状态 */
} KCITransactionStatus;;

typedef enum
{
    ERRORS_TERSE,               /* 返回的消息只包括严重性、主要文本以及位置，这些内容通常在单一行中 */
    ERRORS_DEFAULT,         /* 返回的消息包括上面的信息加上细节、提示或者上下文域（这些可能跨越多行）。 */
    ERRORS_VERBOSE,         /* 返回的消息包括所有可以可用的域 */
    KCIERRORS_SQLSTATE          /* 返回的消息仅包括错误严重性和SQLSTATE错误代码 */
} KCIErrorVerbosity;;

typedef enum
{
    KCISHOW_CTX_NEVER,      /* 查看上下文字段 */
    KCISHOW_CTX_ERRORS,     /* 仅为错误显示上下文(默认) */
    KCISHOW_CTX_ALWAYS      /* 始终显示上下文字段 */
} KCIContextVisibility;;

/*
 * KCIPing -这个枚举的顺序不应该被改变，因为值是通过sys_isready对外公开的。
 */

typedef enum
{
    KCIPING_OK,                 /* 服务器正在接受连接 */
    KCIPING_REJECT,             /* 服务器处于活动状态，但拒绝连接 */
    KCIPING_NO_RESPONSE,            /* 无法建立连接程序 */
    KCIPING_NO_ATTEMPT          /* 未尝试连接(错误的参数) */
} KCIPing;;

/* KCIConnection封装到后端的连接。应用程序不应该知道这个结构体的内容。
 */
typedef struct KCIConnection KCIConnection;;

/* KCIResult封装查询的结果
 * (或者更准确地说，是单个SQL命令的查询结果——给KCIsendQuery的查询字符串可以包含多个命令，因此返回多个KCIResult对象)。
 * 应用程序不应该知道这个结构体的内容。
 */
typedef struct kb_result KCIResult;;

/* KCICancel封装取消现有连接上正在运行的查询所需的信息。
 * 应用程序不应该知道这个结构体的内容。
 */
typedef struct KCICancel KCICancel;;

/* KCINotify表示一个NOTIFY消息的发生。
 * 理想情况下，这将是一个不透明的类型定义
 */
typedef struct KCINotify
{
    char       *relname;;       /* 通知条件名称 */
    int         be_pid;;            /* 通知服务器进程号 */
    char       *extra;;         /* 通知参数 */
    /* 下面的字段是libkci的私有字段;应用程序不应该使用它们 */
    struct KCINotify *next;;        /* 链接列表 */
} KCINotify;;

/* 通知处理回调的函数类型 */
typedef void (*KCInoticeReceiver) (void *arg, const KCIResult *res);;
typedef void (*KCInoticeProcessor) (void *arg, const char *message);;

/* KCIResultPrint()的打印选项 */
typedef char KCIbool;;

/*
 * KCIArgBlock -- 用于KCIfn()参数的结构
 */
typedef struct
{
    int         len;;//长度
    int         isint;;//是否为int类型
    union
    {/* 不能使用void (dec compiler barfs)    */
        int        *ptr;;//指针
        int         integer;;//整形数据
    }           u;;
} KCIArgBlock;;

typedef struct _KCIprintOption
{
    KCIbool     header;;            /* 打印输出字段标题和行数 */
    KCIbool     align;;         /* 填充对齐字段 */
    KCIbool     standard;;      /* 旧版格式 */
    KCIbool     html3;;         /* 输出HTML表 */
    KCIbool     expanded;;      /* 扩展表 */
    KCIbool     pager;;         /* 如果需要，使用pager进行输出 */
    char       *field_sep;;     /* 字段分隔符 */
    char       *tableOpt;;      /* 插入到HTML <table ...> */
    char       *caption;;       /* HTML <caption> */
    char      **fieldName;;     /* null终止的替换字段名数组 */
} KCIprintOption;;



/* ----------------
 * 结构用于KCIConnectionGetDefaultOptions返回的conninfo参数定义或KCIconninfoParse。
 *
 * 除了“val”之外的所有字段都指向静态字符串，这是不可更改的。
 * "val"要么是NULL，要么是malloc的当前值字符串。
 * KCIConnectionFreeOptions()将释放val字符串和KCIconninfooption数组本身。
 * ----------------
 */
typedef struct _KCIconninfoOption
{
    char       *keyword;;       /* 选项的关键字 */
    char       *envvar;;            /* 备选项的环境变量名 */
    char       *compiled;;      /* 备选项的默认值中已编译的部分   */
    char       *val;;           /* 选项的当前值，或NULL */
    char       *label;;         /* 连接对话框中字段的标签 */
    char       *dispchar;       /* 指示如何在连接对话框中显示此字段。
                                 * 取值为:
                                 * ""按原样显示输入值
                                 * "*"密码字段隐藏值
                                 * "D"调试选项-默认不显示*/
    int         dispsize;;      /* 对话框的字符字段大小   */
} KCIconninfoOption;;



/* ----------------
 * KCIresAttDesc -- 查询结果单个属性(列)的数据
 * ----------------
 */
typedef struct KCIresAttDesc
{
    char       *name;;          /* 列名 */
    Oid         tableid;;       /* 源表，如果已知 */
    int         columnid;;      /* 源字段, if known */
    int         format;;            /* 格式化值的代码(文本/二进制) */
    Oid         typid;;         /* 类型id */
    int         typlen;;            /* 类型大小 */
    int         atttypmod;;     /* 特定类型的修饰符信息 */
    //HAVE_CE
  int cl_atttypmod;;    /* 原始特定类型修饰符 */
  const void* rec;;     /* 缓存元信息 */
    //
} KCIresAttDesc;;

/* ----------------
 * 导出libkci的函数
 * ----------------
 */

/* === 文件front_connect.c中的函数 === */

/* 建立到后端的新客户端连接 */
/* 异步(非阻塞) */
extern KCIConnection *KCIConnectionStart(const char *conninfo);;
extern KCIConnection *KCIConnectionStartParams(const char *const *keywords,
                                    const char *const *values, int expand_dbname);;
extern KCIPollingStatus KCIConnectionPoll(KCIConnection *conn);;

/* 同步(阻塞) */
extern KCIConnection *KCIConnectionCreate(const char *conninfo);;
extern KCIConnection *KCIConnectionCreateParams(const char *const *keywords,
                                 const char *const *values, int expand_dbname);;
extern KCIConnection *KCIConnectionCreateDeprecated(const char *kbhost, const char *kbport,
                            const char *kboptions, const char *kbtty,
                            const char *dbName,
                            const char *login, const char *pwd);;

#define KCIsetdb(M_KBHOST,M_KBPORT,M_KBOPT,M_KBTTY,M_DBNAME)  \
    KCIConnectionCreateDeprecated(M_KBHOST, M_KBPORT, M_KBOPT, M_KBTTY, M_DBNAME, NULL, NULL)

/* 关闭当前连接并释放KCIConnection数据结构 */
extern void KCIConnectionDestory(KCIConnection *conn);;

/* 获取KCIconnectdb已知的连接选项信息 */
extern KCIconninfoOption *KCIConnectionGetDefaultOptions(void);;

/* 解析连接选项，和Poconnectdb一样 */
extern KCIconninfoOption *KCIconninfoParse(const char *conninfo, char **errmsg);;

/* 返回活动连接使用的连接选项 */
extern KCIconninfoOption *KCIconninfo(KCIConnection *conn);;

/* 释放KCIConnectionGetDefaultOptions()或KCIconninfoParse()返回的数据结构 */
extern void KCIConnectionFreeOptions(KCIconninfoOption *connOptions);;

/*
 * 关闭当前连接，并使用相同的参数重新建立一个新连接
 */
/* 异步(非阻塞) */
extern int  KCIConnectionReconnectStart(KCIConnection *conn);;
extern KCIPollingStatus KCIConnectionReconnectPoll(KCIConnection *conn);;

/* 同步(阻塞) */
extern void KCIConnectionReconnect(KCIConnection *conn);;

/* 请求cancel结构 */
extern KCICancel *KCICancelAlloc(KCIConnection *conn);;

/* 释放cancel结构 */
extern void KCICancelDealloc(KCICancel *cancel);;

/* 分配取消请求 */
extern int  KCICancelSend(KCICancel *cancel, char *errbuf, int errbufsize);;

/* 向后兼容KCIcancel的版本;非线程安全 */
extern int  KCICancelCurrent(KCIConnection *conn);;

/* SSL信息功能 */
extern const char *const *KCIsslAttributeNames(KCIConnection *conn);;
extern const char *KCIsslAttribute(KCIConnection *conn, const char *attribute_name);;
extern void *KCIsslStruct(KCIConnection *conn, const char *struct_name);;
extern int  KCIsslInUse(KCIConnection *conn);;


/* KCIConnection对象的访问器函数 */
extern int  KCIconnectionUsedPassword(const KCIConnection *conn);;
extern int  KCIConnectionGetClientEncoding(const KCIConnection *conn);;
extern int  KCIConnectionSetClientEncoding(KCIConnection *conn, const char *encoding);;
extern int  KCIConnectionGetProtocolVersion(const KCIConnection *conn);;
extern int  KCIConnectionGetServerVersion(const KCIConnection *conn);;
extern int  KCIConnectionGetSocket(const KCIConnection *conn);;
extern int  KCIConnectionGetBackendPid(const KCIConnection *conn);;
extern int  KCIconnectionNeedsPassword(const KCIConnection *conn);;
extern char *KCIConnectionGetLastError(const KCIConnection *conn);;

extern const char *KCIConnectionGetParameterValue(const KCIConnection *conn, const char *paramName);;
extern KCITransactionStatus KCIConnectionGetTransactionStatus(const KCIConnection *conn);;
extern KCIConnectionStatus KCIConnectionGetStatus(const KCIConnection *conn);;
extern char *KCIConnectionGetPort(const KCIConnection *conn);;
extern char *KCIConnectionGetDatabase(const KCIConnection *conn);;
extern char *KCIConnectionGetHost(const KCIConnection *conn);;
extern char *KCIConnectionGetUser(const KCIConnection *conn);;
extern char *KCIConnectionGetPassword(const KCIConnection *conn);;
extern char *KCIConnectionGetTty(const KCIConnection *conn);;
extern char *KCIConnectionGetCommandLineOptions(const KCIConnection *conn);;
extern char *KCIConnectionGetHostaddr(const KCIConnection *conn);;



/* 获取与连接关联的OpenSSL结构。如果未加密连接或使用了任何其他TLS库，则返回NULL。*/
extern void *KCIConnectionGetSSL(KCIConnection *conn);;

/* 告诉libkci它是否需要初始化OpenSSL */
extern void KCIInitializeSSLEnv(int do_init);;

/* 告诉libkci是否需要初始化OpenSSL的更详细方法 */
extern void KCIinitOpenSSL(int do_ssl, int do_crypto);;

/* 如果使用了GSSAPI加密，则返回true */
extern int  KCIgssEncInUse(KCIConnection *conn);;

/* 如果正在使用GSSAPI，则返回GSSAPI上下文 */
extern void *KCIgetgssctx(KCIConnection *conn);;

/* 设置KCIConnectionGetLastError和KCIResultGetErrorString的详细信息 */
extern KCIErrorVerbosity KCISetVerbosity(KCIConnection *conn, KCIErrorVerbosity verbosity);;

/* 为KCIConnectionGetLastError和PoresultErrorMessage设置CONTEXT可见性 */
extern KCIContextVisibility KCIsetErrorContextVisibility(KCIConnection *conn, KCIContextVisibility show_context);;

/* 启用/禁用tracing */
extern void KCISetLogFile(KCIConnection *conn, FILE *debug_port);;
extern void KCIResetLogFile(KCIConnection *conn);;

/*
 *     用于设置防止并发访问libkci所需的非线程安全函数的回调。
 *     默认实现使用libkci内部互斥锁。
 *     只需要多线程应用程序在其应用程序中和KES连接中都使用kerberos。
 */
typedef void (*kcithreadlock_t) (int acquire);;

extern kcithreadlock_t KCISetLockThreadFunc(kcithreadlock_t newhandler);;

/* 覆盖默认的通知处理例程 */
extern KCInoticeReceiver KCISetNoticeReceiver(KCIConnection *conn, KCInoticeReceiver proc, void *arg);;
extern KCInoticeProcessor KCISetNoticeProcessor(KCIConnection *conn, KCInoticeProcessor proc, void *arg);;

/* === 文件front_exec.c中的函数 === */

/*对于补丁数据到db, nParams org: sal中的占位符*/
extern KCIResult *KCIStatementExecutePrepared_ext(KCIConnection *conn,
                                const char *stmtName, int nParams,
                                const char *const *paramValues, const int *paramLengths,
                                const int *paramFormats, int resultFormat, int nParamsArray_count);;

/* 简单查询和扩展查询 */
extern KCIResult *KCIStatementExecute(KCIConnection *conn, const char *query);;
extern KCIResult *KCIStatementPrepare(KCIConnection *conn, const char *stmtName, //分析准备
                                      const char *query, int nParams, const Oid *paramTypes);;


extern KCIResult *KCIStatementExecutePrepared(KCIConnection *conn, //绑定 执行
                                const char *stmtName, int nParams,
                                const char *const *paramValues, const int *paramLengths,
                                const int *paramFormats, int resultFormat);;
extern KCIResult *KCIStatementExecuteParams(KCIConnection *conn,
                              const char *command, int nParams, const Oid *paramTypes,  const char *const *paramValues,
                              const int *paramLengths, const int *paramFormats, int resultFormat);;
/* 用于多结果或异步查询的接口 */

extern int  KCIStatementSendPrepare(KCIConnection *conn, const char *stmtName,
                          const char *query, int nParams, const Oid *paramTypes);;
extern int  KCIStatementSend(KCIConnection *conn, const char *query);;
extern int  KCIStatementSendPrepared(KCIConnection *conn,
                                const char *stmtName, int nParams, const char *const *paramValues,
                                const int *paramLengths, const int *paramFormats, int resultFormat);;
extern int  KCIStatementSendParams(KCIConnection *conn,
                              const char *command, int nParams, const Oid *paramTypes, const char *const *paramValues,
                              const int *paramLengths, const int *paramFormats, int resultFormat);;
#ifdef STABILITY
extern KCIResult *KCIStatementExecuteFinishMoreResult(KCIConnection *conn, KCIResult *lastResult);;
extern int  KCIStatementExecutePrepare(KCIConnection *conn);;
#endif

/*对于补丁数据到db, nParams_org: sql中的占位符*/
extern KCIResult *KCIConnectionFetchResult(KCIConnection *conn);;
extern int  KCIsetSingleRowMode(KCIConnection *conn);;
extern int  KCIStatementSendPrepared_ext(KCIConnection *conn,
                                const char *stmtName, int nParams, const char *const *paramValues,
                                const int *paramLengths, const int *paramFormats, int resultFormat, int nParamsArray_count);;

/* 已弃用的拷贝入/出函数 */
extern int  KCICopyReadLine(KCIConnection *conn, char *string, int length);;
extern int  KCICopyWriteLine(KCIConnection *conn, const char *string);;
extern int  KCICopyReadLineAsync(KCIConnection *conn, char *buffer, int bufsize);;
extern int  KCICopyWriteBytes(KCIConnection *conn, const char *buffer, int nbytes);;
extern int  KCICopySync(KCIConnection *conn);;

/* 拷贝入/出的函数 */
extern int  KCICopySendData(KCIConnection *conn, const char *buffer, int nbytes);;
extern int  KCICopySendEOF(KCIConnection *conn, const char *errormsg);;
extern int  KCICopyReceiveData(KCIConnection *conn, char **buffer, int async);;

/* 管理异步查询的函数 */
extern int  KCIConnectionForceRead(KCIConnection *conn);;
extern int  KCIConnectionIsBusy(KCIConnection *conn);;

/* 设置后端阻塞/非阻塞连接 */
extern KCIPing KCIpingParams(const char *const *keywords,
                           const char *const *values,
                           int expand_dbname);;
extern KCIPing KCIping(const char *conninfo);;
extern int  KCIConnectionIsNonBlocking(const KCIConnection *conn);;
extern int  KCIConnectionSetNonBlocking(KCIConnection *conn, int arg);;
extern int  KCISupportMultiThread(void);;

/* LISTEN/NOTIFY相关函数 */
extern KCINotify *KCIGetNextNotification(KCIConnection *conn);;


/*
 * "Fast path" 接口 --- 不推荐使用
 */
extern KCIResult *KCIExecuteFunction(KCIConnection *conn,
                      int fnid,
                      int *result_buf,
                      int *result_len,
                      int result_is_int,
                      const KCIArgBlock *args,
                      int nArgs);;

/* 强制写入写入缓冲区(或者至少尝试) */
extern int  KCIConnectionFlush(KCIConnection *conn);;

/* KCIResult对象的访问器函数 */
extern int  KCIResultGetColumnCount(const KCIResult *res);;
extern int  KCIResultGetRowCount(const KCIResult *res);;
extern int  KCIResultIsBinary(const KCIResult *res);;
extern char *KCIResultGetStatusString(KCIExecuteStatus status);;
extern char *KCIResultGetErrorString(const KCIResult *res);;
extern char *KCIresultVerboseErrorMessage(const KCIResult *res, KCIErrorVerbosity verbosity, KCIContextVisibility show_context);;
extern char *KCIResultGetErrorField(const KCIResult *res, int fieldcode);;

extern char *KCIResultGetColumnName(const KCIResult *res, int field_num);;
extern KCIExecuteStatus KCIResultGetStatusCode(const KCIResult *res);;
extern int  KCIResultGetColumnNo(const KCIResult *res, const char *field_name);;
extern int  KCIResultGetRelationOidOfColumncol(const KCIResult *res, int field_num);;
extern int  KCIResultGetColumnFormat(const KCIResult *res, int field_num);;
extern char *KCIResultInsertRowOidStr(const KCIResult *res);;   /* 旧版的 */
extern Oid  KCIResultInsertRowOid(const KCIResult *res);;   /* 新版的 */
extern Oid  KCIResultGetRelationOidOfColumn(const KCIResult *res, int field_num);;
extern Oid  KCIResultGetColumnType(const KCIResult *res, int field_num);;
extern int  KCIResultGetParamCount(const KCIResult *res);;
extern Oid  KCIResultGetParamType(const KCIResult *res, int param_num);;
extern int  KCIResultGetColumnLength(const KCIResult *res, int field_num);;
extern int  KCIResultGetColumnTypmod(const KCIResult *res, int field_num);;
extern char *KCIResultGetAffectedCount(KCIResult *res);;
extern char *KCIResultGetCommandStatus(KCIResult *res);;
extern int  KCIResultGetColumnValueLength(const KCIResult *res, int tup_num, int field_num);;
extern char *KCIResultGetColumnValue(const KCIResult *res, int tup_num, int field_num);;
extern int  KCIResultColumnIsNull(const KCIResult *res, int tup_num, int field_num);;

/* 没有给出密码时出错 */
/* 注意:依赖于此已弃用;使用KCIconnectionNeedsPassword() */
#define KCInoPasswordSupplied   "fe_sendauth: no password supplied\n"

/* 存在向后兼容 */
#define KCIFreeNotification(ptr) KCIFree(ptr)

/* 用于释放其他已分配的结果，如KCINotify结构 */
extern void KCIFree(void *ptr);;

/* 描述准备好的语句和入口 */
extern int  KCIStatementSendDescribePrepared(KCIConnection *conn, const char *stmt);;
extern int  KCICursorSendDescribe(KCIConnection *conn, const char *portal);;
extern KCIResult *KCIStatementDescribePrepared(KCIConnection *conn, const char *stmt);;
extern KCIResult *KCICursorDescribe(KCIConnection *conn, const char *portal);;

/* 删除KCIResult */
extern void KCIResultDealloc(KCIResult *res);;

/* 在查询中包含字符串之前引用字符串。 */
extern unsigned char *KCIUnescapeBytea(const unsigned char *strtext, size_t *retbuflen);;
unsigned char *KCIEscapeByteaConn1(KCIConnection *conn, const unsigned char *from, size_t from_length, size_t *to_length,
    bool add_quotes);;
extern char *KCIescapeIdentifier(KCIConnection *conn,  const char *str,  size_t len);;
extern size_t KCIEscapeStringEx(KCIConnection *conn,
                                 char *to, const char *from,
                                 size_t length, int *error);;
extern char *KCIescapeLiteral(KCIConnection *conn,
                             const char *str,  size_t len);;
extern unsigned char *KCIEscapeByteaEx(KCIConnection *conn, const unsigned char *from,
                                        size_t from_length, size_t *to_length);;


extern size_t KCIEscapeString(char *to, const char *from, size_t length);;
extern unsigned char *KCIEscapeBytea(const unsigned char *from, size_t from_length,
                                    size_t *to_length);;
extern KCIResult* checkRefreshCacheOnError(KCIConnection* conn);;
#ifdef STABILITY
extern void KCIsetEnableUpperColName(KCIConnection *conn);;
extern int KCIgetEnableUpperColName(void);;
extern int KCIcheckReplicationMode(KCIConnection *conn);;
#endif

/* 创建和操作KCIResults */
extern int  KCIsetvalue(KCIResult *res, int tup_num, int field_num, char *value, int len);;
extern int  KCIsetResultAttrs(KCIResult *res, int numAttributes, KCIresAttDesc *attDescs);;
extern void *KCIresultAlloc(KCIResult *res, size_t nBytes);;
extern size_t KCIResultMemorySize(const KCIResult *res);;
extern KCIResult *KCIResultCreate(KCIConnection *conn, KCIExecuteStatus status);;
extern KCIResult *KCIcopyResult(const KCIResult *src, int flags);;


/* === 文件front_print.c中的函数 === */

/*
 * 非常古老的打印程序
 */

extern void KCIResultPrintTuples(const KCIResult *res,
                          FILE *fout,   /* 输出流 */ int printAttName, /* 打印属性名 */
                          int terseOutput,  /* 分隔符集 */ int width);; /* 列的宽度，如果为0，则使用可变宽度*/

extern void KCIResultPrintEx(const KCIResult *res, FILE *fp,    /* 将输出发送到哪里 */
                            int fillAlign,  /* 用空格填充字段 */ const char *field_sep,  /* 字段分隔符 */
                            int printHeader,    /* 显示标题? */ int quiet);;

extern void KCIResultPrint(FILE *fout, /* 输出流 */
                    const KCIResult *res, const KCIprintOption *ps);;   /* option结构 */


/* === 文件front_lobj.c中的函数 === */

/* 大对象访问程序 */
extern Oid  lo_creat(KCIConnection *conn, int mode);;
extern Oid  lo_create(KCIConnection *conn, Oid lobjId);;
extern int  lo_close(KCIConnection *conn, int fd);;
extern int  lo_open(KCIConnection *conn, Oid lobjId, int mode);;
extern int  lo_write(KCIConnection *conn, int fd, const char *buf, size_t len);;
extern int  lo_read(KCIConnection *conn, int fd, char *buf, size_t len);;
extern int  lo_tell(KCIConnection *conn, int fd);;
extern kb_int64 lo_tell64(KCIConnection *conn, int fd);;
extern int  lo_lseek(KCIConnection *conn, int fd, int offset, int whence);;
extern kb_int64 lo_lseek64(KCIConnection *conn, int fd, kb_int64 offset, int whence);;
extern int  lo_truncate(KCIConnection *conn, int fd, size_t len);;
extern int  lo_truncate64(KCIConnection *conn, int fd, kb_int64 len);;
extern Oid  lo_import(KCIConnection *conn, const char *filename);;
extern Oid  lo_import_with_oid(KCIConnection *conn, const char *filename, Oid lobjId);;
extern int  lo_export(KCIConnection *conn, Oid lobjId, const char *filename);;
extern int  lo_unlink(KCIConnection *conn, Oid lobjId);;


/* === 文件front_auth.c中的函数 === */

extern char *KCIEncryptPwd(const char *passwd, const char *user);;
extern char *KCIEncryptPwdConn(KCIConnection *conn, const char *passwd, const char *user, const char *algorithm);;

/* === 文件front_communicate.c中的函数 === */

/* 从环境变量KCICLIENTENCODING获取编码id */
extern int  KCIGetEnvEncoding(void);;

/* 获取正在使用的libkci库的版本 */
extern int  KCIlibVersion(void);;

/* 确定*s处多字节编码字符的显示长度 */
extern int  KCIGetStringDisplayLength(const char *s, int encoding);;

/* 确定*s处多字节编码字符的长度 */
extern int  KCIGetStringByteLength(const char *s, int encoding);;


/* === 文件encnames.c中的函数 === */
#ifndef PG_CHAR_TO_ENCODING
#define PG_CHAR_TO_ENCODING
extern int  pg_char_to_encoding(const char *name);;
extern const char *pg_encoding_to_char(int encoding);;
extern int  kbValidServerEncodingId(int encoding);;
#endif

#ifdef __cplusplus
}
#endif

#endif                          /* libkci_FE_H */
