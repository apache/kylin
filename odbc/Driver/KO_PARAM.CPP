/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


// ----------------------------------------------------------------------------
//
// File:    KO_PARAM.CPP
//
// Purpose: This file contains functions for implementing
//          parameterized queries. Since I have not shown
//          implementation of params in the sample, all the
//          functions r just placeholders. Params work
//          thru APD and IPD just like actual results
//          work thru ARD and IRD
//
// Exported functions:
//                       SQLBindParameter
//                       SQLDescribeParam
//                       SQLNumParams
//                       SQLParamData
//
// ----------------------------------------------------------------------------
#include "stdafx.h"


// -----------------------------------------------------------------------
// to bind a buffer to a parameter marker in an SQL statement
// -----------------------------------------------------------------------

RETCODE SQL_API SQLBindParameter ( SQLHSTMT pStmt,
                                   SQLUSMALLINT pParamNum,
                                   SQLSMALLINT pIOType,
                                   SQLSMALLINT pValueType,
                                   SQLSMALLINT pParamType,
                                   SQLUINTEGER pColSize,
                                   SQLSMALLINT pDecimalDigits,
                                   SQLPOINTER pParamValuePtr,
                                   SQLINTEGER pParamValueSize,
                                   SQLINTEGER* pParamValueSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLBindParameter called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLBindParameter not implemented" ) );
    return SQL_ERROR;
}


// -----------------------------------------------------------------------
// to get param descriptions for a specified bound param
// -----------------------------------------------------------------------

RETCODE SQL_API SQLDescribeParam ( SQLHSTMT pStmt,
                                   SQLUSMALLINT pParamNum,
                                   SQLSMALLINT* pDataTypePtr,
                                   SQLUINTEGER* pParamSizePtr,
                                   SQLSMALLINT* pDecimalDigitsPtr,
                                   SQLSMALLINT* pNullablePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLDescribeParam called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLDescribeParam not implemented" ) );
    return SQL_ERROR;
}


// -----------------------------------------------------------------------
// to get the number of parameters in a statement
// -----------------------------------------------------------------------

RETCODE SQL_API SQLNumParams ( SQLHSTMT pStmt, SQLSMALLINT* pParamCountPtr )
{
    // since the concept of parameters has not been implemented
    // this function returns zero in the number of params to
    // make sure that the calle does not proceed forward on this issue
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLNumParams called" ) );

    if ( pParamCountPtr )
    {
        *pParamCountPtr = 0;
    }

    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to be used wth SQLPutData to provide param data at execution time
// -----------------------------------------------------------------------

RETCODE SQL_API SQLParamData ( SQLHSTMT pStmt, SQLPOINTER* pValuePtrPtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLParamData called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLParamData not implemented" ) );
    return SQL_ERROR;
}

