/******************************************************************************
* 版权信息：(c) 1999-2022 北京人大金仓信息技术股份有限公司
*
* 作者：
*
* 文件名：kingbase_ext.h
*
* 功能描述：
* 这个文件包含的声明在Kingbase中随处可见，并且对前端接口库
* 的客户端可见。例如，Oid类型是libkci和其他库的API的一部分。
*
*
* 其它说明：
* 特定于特定接口的声明应该放在该接口的头文件中(例如libkci-fe.h)。
* 这个文件只用于基本的Kingbase声明。
*
* 用户编写的C函数不会被视为“Kingbase外部的”。这些功能就像对
* 后端本身的本地修改，并使用原本是Kingbase内部的头文件来与
* 后端进行接口。
*
* 函数列表：
* 1.
* 2.
*
* 修改记录：
* 1.修改时间：
*  
* 2.修改人：
*  
* 3.修改内容：
*
******************************************************************************/
#ifndef SYS_KINGBASE_EXT_H
#define SYS_KINGBASE_EXT_H

#include "sys_config_ext.h"

/*
 * 对象ID是Kingbase的基本类型。
 */
typedef unsigned int Oid;;

/*
 * 定义无效的OID 
 */
#ifdef __cplusplus
#define InvalidOid (Oid(0)) 	/* C++ 风格 */
#else
#define InvalidOid ((Oid) 0)	/* C风格 */
#endif

#ifdef KingbaseES
/* 嵌套函数 */
#define ANONYMOUSBLOCKOID	((Oid) 9999)
#endif /* KingbaseES */

#define OID_MAX  UINT_MAX
/* 你需要包含<limits.h>使用上面的#define */

#define atooid(s) ((Oid) strtoul((s), NULL, 10))
/* 上面的需要<stdlib.h> */

typedef KB_INT64_TYPE kb_int64;; /* 定义一个带符号的64位整数类型，用于客户端API声明。 */

/*
 * 错误消息字段的标识符。保存在这里是为了保持前端和后端之间
 * 的公共关系，并将它们导出到libkci应用程序中。
 */
#define KB_DIAG_SEVERITY			'S'	/* 错误消息标识符服务 */
#define KB_DIAG_SEVERITY_NONLOCALIZED 		'V'	/* 错误消息标识符服务未本地化 */
#define KB_DIAG_SQLSTATE			'C'	/* 错误消息标识符错误码 */
#define KB_DIAG_MESSAGE_PRIMARY 		'M'	/* 错误消息标识符主键 */
#define KB_DIAG_MESSAGE_DETAIL			'D'	/* 错误消息标识符细节描述 */
#define KB_DIAG_MESSAGE_HINT			'H'	/* 错误消息标识符暗示 */
#define KB_DIAG_STATEMENT_POSITION 		'P'	/* 错误消息标识符语句位置 */
#define KB_DIAG_INTERNAL_POSITION 		'p'	/* 错误消息标识符内部位置 */
#define KB_DIAG_INTERNAL_QUERY			'q'	/* 错误消息标识符内部查询 */
#define KB_DIAG_CONTEXT				'W'	/* 错误消息标识符上下文 */
#define KB_DIAG_SCHEMA_NAME			's'	/* 错误消息标识符模式名称 */
#define KB_DIAG_TABLE_NAME			't'	/* 错误消息标识符表名称 */
#define KB_DIAG_COLUMN_NAME			'c'	/* 错误消息标识符列属性名称 */
#define KB_DIAG_DATATYPE_NAME			'd'	/* 错误消息标识符数据库名称 */
#define KB_DIAG_CONSTRAINT_NAME 		'n'	/* 错误消息标识符约束名称 */
#define KB_DIAG_SOURCE_FILE			'F'	/* 错误消息标识符源文件 */
#define KB_DIAG_SOURCE_LINE			'L'	/* 错误消息标识符源文件行数 */
#define KB_DIAG_SOURCE_FUNCTION 		'R'	/* 错误消息标识符源函数 */

typedef int	TriBool;

#define TRIUNKNOWN	((TriBool) 2)
#define TRITRUE		((TriBool) 1)
#define TRIFALSE	((TriBool) 0)

#endif /* SYS_KINGBASE_EXT_H */

