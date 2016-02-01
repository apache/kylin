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
// File:    KO_ATTR.CPP
//
// Notes:   Contains the attribute/option set/get functions
//          for environment, connection and statement
//          also contains some other functions like setting of
//          cursor name etc.
//
// Exported functions:
//                       SQLSetEnvAttr
//                       SQLGetEnvAttr
//                       SQLSetConnectAttr
//                       SQLGetConnectAttr
//                       SQLSetStmtAttr
//                       SQLGetStmtAttr
//                       SQLGetCursorName
//                       SQLSetCursorName
// ----------------------------------------------------------------------------

#include "stdafx.h"


// -----------------------------------------------------------------------
// to set environment specific attributes
// -----------------------------------------------------------------------


RETCODE SQL_API SQLSetEnvAttr ( SQLHENV pEnv, SQLINTEGER pAttr, SQLPOINTER pDataPtr, SQLINTEGER pDataSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLSetEnvAttr called" ) );
    __CHK_HANDLE ( pEnv, SQL_HANDLE_ENV, SQL_ERROR );
    _SQLFreeDiag ( _DIAGENV ( pEnv ) );

    switch ( pAttr )
    {
        case SQL_ATTR_ODBC_VERSION :
            ( ( pODBCEnv ) pEnv ) -> AttrODBCVersion = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_CONNECTION_POOLING :
            ( ( pODBCEnv ) pEnv ) -> AttrConnPooling = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_CP_MATCH :
            ( ( pODBCEnv ) pEnv ) -> AttrCPMatch = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_OUTPUT_NTS :
            ( ( pODBCEnv ) pEnv ) -> AttrOutputNTS = ( ULong ) pDataPtr;
            break;

        default :
            return SQL_ERROR; // unknown attribute
    }

    return SQL_SUCCESS;
}


// -----------------------------------------------------------------------
// to get current value of specified env attribute
// -----------------------------------------------------------------------

RETCODE SQL_API SQLGetEnvAttr ( SQLHENV pEnv,
                                SQLINTEGER pAttr,
                                SQLPOINTER pDataPtr,
                                SQLINTEGER pDataSize,
                                SQLINTEGER* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetEnvAttr called" ) );
    __CHK_HANDLE ( pEnv, SQL_HANDLE_ENV, SQL_ERROR );
    _SQLFreeDiag ( _DIAGENV ( pEnv ) );

    switch ( pAttr )
    {
        case SQL_ATTR_ODBC_VERSION :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCEnv ) pEnv ) -> AttrODBCVersion;
            break;

        case SQL_ATTR_CONNECTION_POOLING :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCEnv ) pEnv ) -> AttrConnPooling;
            break;

        case SQL_ATTR_CP_MATCH :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCEnv ) pEnv ) -> AttrCPMatch;
            break;

        case SQL_ATTR_OUTPUT_NTS :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCEnv ) pEnv ) -> AttrOutputNTS;
            break;

        default :
            return SQL_ERROR; // unknown attribute
    }

    return SQL_SUCCESS;
}

// --------------------------------------------------------------------
// to set attributes specific to a connection
// --------------------------------------------------------------------
RETCODE SQL_API SQLSetConnectAttrW ( SQLHDBC hdbc,
                                     SQLINTEGER fAttribute,
                                     SQLPOINTER rgbValue,
                                     SQLINTEGER cbValue )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLSetConnectAttrW, Attr: %d, DataPtr: %d, DataSize: %d", fAttribute,
        rgbValue, cbValue ) );
    RETCODE code = SQLSetConnectAttr ( hdbc, fAttribute, rgbValue, cbValue );
    return code;
}


RETCODE SQL_API SQLSetConnectAttr ( SQLHDBC pConn, SQLINTEGER pAttr, SQLPOINTER pDataPtr, SQLINTEGER pDataSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLSetConnectAttr, Attr: %d, DataPtr: %d, DataSize: %d", pAttr, pDataPtr,
        pDataSize ) );
    __CHK_HANDLE ( pConn, SQL_HANDLE_DBC, SQL_ERROR );
    _SQLFreeDiag ( _DIAGCONN ( pConn ) );

    switch ( pAttr )
    {
        case SQL_ATTR_ANSI_APP :
            return SQL_ERROR;

        case SQL_ATTR_ACCESS_MODE :
            ( ( pODBCConn ) pConn ) -> AccessMode = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_ASYNC_ENABLE :
            ( ( pODBCConn ) pConn ) -> AsyncEnable = ( ULong ) pDataPtr;
            break;

            //case SQL_ATTR_AUTO_IPD:  read-only

        case SQL_ATTR_AUTOCOMMIT :
            ( ( pODBCConn ) pConn ) -> AutoCommit = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_CONNECTION_DEAD :
            // (( pODBCConn )pConn)->AutoCommit = ( ULong )pDataPtr;
            break;

        case SQL_ATTR_CONNECTION_TIMEOUT :
            //(( pODBCConn )pConn)->TimeOut = *(( ULong* )pDataPtr );
            break;

        case SQL_ATTR_CURRENT_CATALOG : // current database
            // assumes that current database is what it should be
            // return _SQLCopyCharData ( _DIAGCONN(pConn), (( pODBCConn )pConn)->CurrDB, 32, NULL, 16, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_LOGIN_TIMEOUT :
            // (( pODBCConn )pConn)->LoginTimeOut = *(( ULong* )pDataPtr );
            break;

        case SQL_ATTR_METADATA_ID :
            ( ( pODBCConn ) pConn ) -> MetaDataID = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_ODBC_CURSORS :
            ( ( pODBCConn ) pConn ) -> ODBCCursors = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_PACKET_SIZE :
            // not a typical option
            return SQL_ERROR;

        case SQL_ATTR_QUIET_MODE :
            ( ( pODBCConn ) pConn ) -> Window = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_TRACE :
            // only for driver manager
            break;

        case SQL_ATTR_TRACEFILE :
            // only for driver manager
            break;

        case SQL_ATTR_TRANSLATE_LIB :
            // translation still not implemented
            break;

        case SQL_ATTR_TRANSLATE_OPTION :
            // translation still not implemented
            break;

        case SQL_ATTR_TXN_ISOLATION :
            ( ( pODBCConn ) pConn ) -> TxnIsolation = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_MAX_ROWS :
            ( ( pODBCConn ) pConn ) -> MaxRows = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_QUERY_TIMEOUT :
            ( ( pODBCConn ) pConn ) -> QueryTimeout = ( ULong ) pDataPtr;
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQLSetConnectAttr is not recognized, Attr: %d, DataPtr: %d, DataSize: %d", pAttr,
                pDataPtr, pDataSize ) );
            return SQL_SUCCESS;
    }

    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLSetConnectAttr, Attr: %d, DataPtr: %d, DataSize: %d is done!", pAttr,
        pDataPtr, pDataSize ) );
    return SQL_SUCCESS;
}


// -----------------------------------------------------------------------
// to get current value of specified conn attribute
// -----------------------------------------------------------------------
RETCODE SQL_API SQLGetConnectAttrW ( SQLHDBC pConn,
                                     SQLINTEGER pAttr,
                                     SQLPOINTER pDataPtr,
                                     SQLINTEGER pDataSize,
                                     SQLINTEGER* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetConnectAttrW, Attr: %d, DataPtr: %d, DataSize: %d", pAttr, pDataPtr,
        pDataSize ) );
    __CHK_HANDLE ( pConn, SQL_HANDLE_DBC, SQL_ERROR );
    _SQLFreeDiag ( _DIAGCONN ( pConn ) );

    switch ( pAttr )
    {
        case SQL_ATTR_ACCESS_MODE :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> AccessMode;
            break;

        case SQL_ATTR_ASYNC_ENABLE :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> AsyncEnable;
            break;

        case SQL_ATTR_AUTO_IPD :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> AutoIPD;
            break;

        case SQL_ATTR_AUTOCOMMIT :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> AutoCommit;
            break;

        case SQL_ATTR_CONNECTION_DEAD :
            * ( ( ULong* ) pDataPtr ) = SQL_CD_TRUE ;
            break;

        case SQL_ATTR_CONNECTION_TIMEOUT :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> TimeOut;
            break;

        case SQL_ATTR_CURRENT_CATALOG : // current database
            return _SQLCopyWCharData ( _DIAGCONN ( pConn ), pDataPtr, pDataSize, pDataSizePtr, 32, "default", -1 );

        case SQL_ATTR_LOGIN_TIMEOUT :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> LoginTimeOut;
            break;

        case SQL_ATTR_METADATA_ID :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> MetaDataID;
            break;

        case SQL_ATTR_ODBC_CURSORS :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> ODBCCursors;
            break;

        case SQL_ATTR_PACKET_SIZE :
            return SQL_ERROR; // not a typical option

        case SQL_ATTR_QUIET_MODE :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> Window;
            break;

        case SQL_ATTR_TRACE :
            // basically for driver manager
            break;

        case SQL_ATTR_TRACEFILE :
            // basically for driver manager
            break;

        case SQL_ATTR_TRANSLATE_LIB :
            // not implemented
            break;

        case SQL_ATTR_TRANSLATE_OPTION :
            // not implemented
            break;

        case SQL_ATTR_TXN_ISOLATION :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> TxnIsolation;
            break;

        case SQL_ATTR_MAX_ROWS :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> MaxRows;
            break;

        case SQL_ATTR_QUERY_TIMEOUT :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> QueryTimeout;
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQLGetConnectAttrW, Attr: %d, DataPtr: %d, DataSize: %d", pAttr, pDataPtr, pDataSize ) );
            return SQL_SUCCESS;
    }

    return SQL_SUCCESS;
}

RETCODE SQL_API SQLGetConnectAttr ( SQLHDBC pConn,
                                    SQLINTEGER pAttr,
                                    SQLPOINTER pDataPtr,
                                    SQLINTEGER pDataSize,
                                    SQLINTEGER* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetConnectAttr, Attr: %d, DataPtr: %d, DataSize: %d", pAttr, pDataPtr,
        pDataSize ) );
    __CHK_HANDLE ( pConn, SQL_HANDLE_DBC, SQL_ERROR );
    _SQLFreeDiag ( _DIAGCONN ( pConn ) );

    switch ( pAttr )
    {
        case SQL_ATTR_ACCESS_MODE :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> AccessMode;
            break;

        case SQL_ATTR_ASYNC_ENABLE :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> AsyncEnable;
            break;

        case SQL_ATTR_AUTO_IPD :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> AutoIPD;
            break;

        case SQL_ATTR_AUTOCOMMIT :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> AutoCommit;
            break;

        case SQL_ATTR_CONNECTION_DEAD :
            * ( ( ULong* ) pDataPtr ) = SQL_CD_TRUE ;
            break;

        case SQL_ATTR_CONNECTION_TIMEOUT :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> TimeOut;
            break;

        case SQL_ATTR_CURRENT_CATALOG : // current database
            return _SQLCopyCharData ( _DIAGCONN ( pConn ), pDataPtr, pDataSize, pDataSizePtr, 32, "default", -1 );

        case SQL_ATTR_LOGIN_TIMEOUT :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> LoginTimeOut;
            break;

        case SQL_ATTR_METADATA_ID :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> MetaDataID;
            break;

        case SQL_ATTR_ODBC_CURSORS :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> ODBCCursors;
            break;

        case SQL_ATTR_PACKET_SIZE :
            return SQL_ERROR; // not a typical option

        case SQL_ATTR_QUIET_MODE :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> Window;
            break;

        case SQL_ATTR_TRACE :
            // basically for driver manager
            break;

        case SQL_ATTR_TRACEFILE :
            // basically for driver manager
            break;

        case SQL_ATTR_TRANSLATE_LIB :
            // not implemented
            break;

        case SQL_ATTR_TRANSLATE_OPTION :
            // not implemented
            break;

        case SQL_ATTR_TXN_ISOLATION :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> TxnIsolation;
            break;

        case SQL_ATTR_MAX_ROWS :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> MaxRows;
            break;

        case SQL_ATTR_QUERY_TIMEOUT :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCConn ) pConn ) -> QueryTimeout;
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQLGetConnectAttr not recognized, Attr: %d, DataPtr: %d, DataSize: %d", pAttr, pDataPtr,
                pDataSize ) );
            return SQL_SUCCESS;
    }

    return SQL_SUCCESS;
}


// --------------------------------------------------------------------
// to set attributes specific to a statement
// --------------------------------------------------------------------


RETCODE SQL_API SQLSetStmtAttrW ( SQLHSTMT pStmt,
                                  SQLINTEGER pAttr,
                                  SQLPOINTER pDataPtr,
                                  SQLINTEGER pDataSize )

{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLSetStmtAttrW, Attr: %d, DataPtr: %d, DataSize: %d", pAttr, pDataPtr,
        pDataSize ) );
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );

    switch ( pAttr )
    {
        case 1226 : /* MS SQL Server Extension */
        case 1227 :
        case 1228 :
            break;

        case SQL_ATTR_APP_PARAM_DESC :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_APP_PARAM_DESC - Cannot set descriptor" ) );
            return SQL_ERROR;

        case SQL_ATTR_APP_ROW_DESC :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_APP_ROW_DESC - Cannot set descriptor" ) );
            return SQL_ERROR;

        case SQL_ATTR_ASYNC_ENABLE :
            ( ( pODBCStmt ) pStmt ) -> AsyncEnable = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_CONCURRENCY :
            ( ( pODBCStmt ) pStmt ) -> Concurrency = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_CURSOR_SCROLLABLE :
            ( ( pODBCStmt ) pStmt ) -> CursorScroll = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_CURSOR_SENSITIVITY :
            ( ( pODBCStmt ) pStmt ) -> CursorSensitivity = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_CURSOR_TYPE :
            ( ( pODBCStmt ) pStmt ) -> CursorType = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_ENABLE_AUTO_IPD :
            ( ( pODBCStmt ) pStmt ) -> AutoIPD = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_FETCH_BOOKMARK_PTR :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_FETCH_BOOKMARK_PTR - option not implemented" ) );
            return SQL_ERROR;

        case SQL_ATTR_IMP_PARAM_DESC :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_IMP_PARAM_DESC - Cannot set descriptor" ) );
            return SQL_ERROR;

        case SQL_ATTR_IMP_ROW_DESC :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_IMP_ROW_DESC - Cannot set descriptor" ) );
            return SQL_ERROR;

        case SQL_ATTR_KEYSET_SIZE :
            ( ( pODBCStmt ) pStmt ) -> KeysetSize = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_MAX_LENGTH :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_MAX_LENGTH - option not implemented" ) );
            return SQL_ERROR;

        case SQL_ATTR_MAX_ROWS :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_MAX_ROWS - option not implemented" ) );
            return SQL_ERROR;

        case SQL_ATTR_METADATA_ID :
            ( ( pODBCStmt ) pStmt ) -> MetaDataID = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_NOSCAN :
            ( ( pODBCStmt ) pStmt ) -> NoScan = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_PARAM_BIND_TYPE :
            _SQLSetAPDField ( & ( ( ( pODBCStmt ) pStmt ) -> APD ), SQL_DESC_BIND_TYPE, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_PARAM_BIND_OFFSET_PTR :
            _SQLSetAPDField ( & ( ( ( pODBCStmt ) pStmt ) -> APD ), SQL_DESC_BIND_OFFSET_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_PARAM_OPERATION_PTR :
            _SQLSetAPDField ( & ( ( ( pODBCStmt ) pStmt ) -> APD ), SQL_DESC_ARRAY_STATUS_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_PARAM_STATUS_PTR :
            _SQLSetIPDField ( & ( ( ( pODBCStmt ) pStmt ) -> IPD ), SQL_DESC_ARRAY_STATUS_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_PARAMS_PROCESSED_PTR :
            // ???? note there is an error in MSDN on SQLSetDescField which says ARD/APD instead of IRD/IPD for this option
            _SQLSetIPDField ( & ( ( ( pODBCStmt ) pStmt ) -> IPD ), SQL_DESC_ROWS_PROCESSED_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_PARAMSET_SIZE :
            _SQLSetAPDField ( & ( ( ( pODBCStmt ) pStmt ) -> APD ), SQL_DESC_ARRAY_SIZE, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_QUERY_TIMEOUT :
            if ( pDataPtr != NULL )
            {
                ( ( pODBCStmt ) pStmt ) -> QryTimeout = ( ( ULong ) pDataPtr );
            }

            break;

        case SQL_ATTR_RETRIEVE_DATA :
            ( ( pODBCStmt ) pStmt ) -> RetrieveData = ( ULong ) pDataPtr;
            break;

        case SQL_ROWSET_SIZE :
        case SQL_ATTR_ROW_ARRAY_SIZE :
            _SQLSetARDField ( & ( ( ( pODBCStmt ) pStmt ) -> ARD ), SQL_DESC_ARRAY_SIZE, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_ROW_BIND_OFFSET_PTR :
            _SQLSetARDField ( & ( ( ( pODBCStmt ) pStmt ) -> ARD ), SQL_DESC_BIND_OFFSET_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_ROW_BIND_TYPE :
            _SQLSetARDField ( & ( ( ( pODBCStmt ) pStmt ) -> ARD ), SQL_DESC_BIND_TYPE, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_ROW_OPERATION_PTR :
            _SQLSetARDField ( & ( ( ( pODBCStmt ) pStmt ) -> ARD ), SQL_DESC_ARRAY_STATUS_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_ROW_STATUS_PTR :
            _SQLSetIRDField ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), SQL_DESC_ARRAY_STATUS_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_ROWS_FETCHED_PTR :
            _SQLSetIRDField ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), SQL_DESC_ROWS_PROCESSED_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_SIMULATE_CURSOR :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_SIMULATE_CURSOR - option not implemented" ) );
            return SQL_ERROR;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQLSetStmtAttrW : Stmt attr - unknown %d", pAttr ) );
            return SQL_ERROR;
    }

    return SQL_SUCCESS;
}

RETCODE SQL_API SQLSetStmtAttr ( SQLHSTMT pStmt,
                                 SQLINTEGER pAttr,
                                 SQLPOINTER pDataPtr,
                                 SQLINTEGER pDataSize )

{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLSetStmtAttr, Attr: %d, DataPtr: %d, DataSize: %d", pAttr, pDataPtr,
        pDataSize ) );
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );

    switch ( pAttr )
    {
        case SQL_ATTR_APP_PARAM_DESC :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_APP_PARAM_DESC - Cannot set descriptor" ) );
            return SQL_ERROR;

        case SQL_ATTR_APP_ROW_DESC :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_APP_ROW_DESC - Cannot set descriptor" ) );
            return SQL_ERROR;

        case SQL_ATTR_ASYNC_ENABLE :
            ( ( pODBCStmt ) pStmt ) -> AsyncEnable = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_CONCURRENCY :
            ( ( pODBCStmt ) pStmt ) -> Concurrency = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_CURSOR_SCROLLABLE :
            ( ( pODBCStmt ) pStmt ) -> CursorScroll = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_CURSOR_SENSITIVITY :
            ( ( pODBCStmt ) pStmt ) -> CursorSensitivity = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_CURSOR_TYPE :
            ( ( pODBCStmt ) pStmt ) -> CursorType = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_ENABLE_AUTO_IPD :
            ( ( pODBCStmt ) pStmt ) -> AutoIPD = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_FETCH_BOOKMARK_PTR :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_FETCH_BOOKMARK_PTR - option not implemented" ) );
            return SQL_ERROR;

        case SQL_ATTR_IMP_PARAM_DESC :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_IMP_PARAM_DESC - Cannot set descriptor" ) );
            return SQL_ERROR;

        case SQL_ATTR_IMP_ROW_DESC :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_IMP_ROW_DESC - Cannot set descriptor" ) );
            return SQL_ERROR;

        case SQL_ATTR_KEYSET_SIZE :
            ( ( pODBCStmt ) pStmt ) -> KeysetSize = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_MAX_LENGTH :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_MAX_LENGTH - option not implemented" ) );
            return SQL_ERROR;

        case SQL_ATTR_MAX_ROWS :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_MAX_ROWS - option not implemented" ) );
            return SQL_ERROR;

        case SQL_ATTR_METADATA_ID :
            ( ( pODBCStmt ) pStmt ) -> MetaDataID = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_NOSCAN :
            ( ( pODBCStmt ) pStmt ) -> NoScan = ( ULong ) pDataPtr;
            break;

        case SQL_ATTR_PARAM_BIND_TYPE :
            _SQLSetAPDField ( & ( ( ( pODBCStmt ) pStmt ) -> APD ), SQL_DESC_BIND_TYPE, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_PARAM_BIND_OFFSET_PTR :
            _SQLSetAPDField ( & ( ( ( pODBCStmt ) pStmt ) -> APD ), SQL_DESC_BIND_OFFSET_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_PARAM_OPERATION_PTR :
            _SQLSetAPDField ( & ( ( ( pODBCStmt ) pStmt ) -> APD ), SQL_DESC_ARRAY_STATUS_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_PARAM_STATUS_PTR :
            _SQLSetIPDField ( & ( ( ( pODBCStmt ) pStmt ) -> IPD ), SQL_DESC_ARRAY_STATUS_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_PARAMS_PROCESSED_PTR :
            // ???? note there is an error in MSDN on SQLSetDescField which says ARD/APD instead of IRD/IPD for this option
            _SQLSetIPDField ( & ( ( ( pODBCStmt ) pStmt ) -> IPD ), SQL_DESC_ROWS_PROCESSED_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_PARAMSET_SIZE :
            _SQLSetAPDField ( & ( ( ( pODBCStmt ) pStmt ) -> APD ), SQL_DESC_ARRAY_SIZE, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_QUERY_TIMEOUT :
            if ( pDataPtr != NULL )
            {
                ( ( pODBCStmt ) pStmt ) -> QryTimeout = * ( ( ULong* ) pDataPtr );
            }

            break;

        case SQL_ATTR_RETRIEVE_DATA :
            ( ( pODBCStmt ) pStmt ) -> RetrieveData = ( ULong ) pDataPtr;
            break;

        case SQL_ROWSET_SIZE :
        case SQL_ATTR_ROW_ARRAY_SIZE :
            _SQLSetARDField ( & ( ( ( pODBCStmt ) pStmt ) -> ARD ), SQL_DESC_ARRAY_SIZE, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_ROW_BIND_OFFSET_PTR :
            _SQLSetARDField ( & ( ( ( pODBCStmt ) pStmt ) -> ARD ), SQL_DESC_BIND_OFFSET_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_ROW_BIND_TYPE :
            _SQLSetARDField ( & ( ( ( pODBCStmt ) pStmt ) -> ARD ), SQL_DESC_BIND_TYPE, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_ROW_OPERATION_PTR :
            _SQLSetARDField ( & ( ( ( pODBCStmt ) pStmt ) -> ARD ), SQL_DESC_ARRAY_STATUS_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_ROW_STATUS_PTR :
            _SQLSetIRDField ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), SQL_DESC_ARRAY_STATUS_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_ROWS_FETCHED_PTR :
            _SQLSetIRDField ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), SQL_DESC_ROWS_PROCESSED_PTR, pDataPtr, pDataSize );
            break;

        case SQL_ATTR_SIMULATE_CURSOR :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_SIMULATE_CURSOR - option not implemented" ) );
            return SQL_ERROR;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQLSetStmtAttr: Stmt attr - unknown %d", pAttr ) );
            return SQL_ERROR;
    }

    return SQL_SUCCESS;
}


RETCODE SQL_API SQLGetStmtAttrW ( SQLHSTMT pStmt,
                                  SQLINTEGER pAttr,
                                  SQLPOINTER pDataPtr,
                                  SQLINTEGER pDataSize,
                                  SQLINTEGER* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetStmtAttrW, Attr: %d, DataPtr: %d, DataSize: %d", pAttr, pDataPtr,
        pDataSize ) );
    return SQLGetStmtAttr ( pStmt, pAttr, pDataPtr, pDataSize, pDataSizePtr );
}


// --------------------------------------------------------------------
// to get attributes specific to a statement
// --------------------------------------------------------------------

RETCODE SQL_API SQLGetStmtAttr ( SQLHSTMT pStmt,
                                 SQLINTEGER pAttr,
                                 SQLPOINTER pDataPtr,
                                 SQLINTEGER pDataSize,
                                 SQLINTEGER* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetStmtAttr, Attr: %d, DataPtr: %d, DataSize: %d", pAttr, pDataPtr,
        pDataSize ) );
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );

    switch ( pAttr )
    {
        case SQL_ATTR_APP_PARAM_DESC :
            * ( ( Long* ) pDataPtr ) = ( Long ) & ( ( ( pODBCStmt ) pStmt ) -> APD );
            break;

        case SQL_ATTR_APP_ROW_DESC :
            * ( ( Long* ) pDataPtr ) = ( Long ) & ( ( ( pODBCStmt ) pStmt ) -> ARD );
            break;

        case SQL_ATTR_ASYNC_ENABLE :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCStmt ) pStmt ) -> AsyncEnable;
            break;

        case SQL_ATTR_CONCURRENCY :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCStmt ) pStmt ) -> Concurrency;
            break;

        case SQL_ATTR_CURSOR_SCROLLABLE :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCStmt ) pStmt ) -> CursorScroll;
            break;

        case SQL_ATTR_CURSOR_SENSITIVITY :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCStmt ) pStmt ) -> CursorSensitivity;
            break;

        case SQL_ATTR_CURSOR_TYPE :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCStmt ) pStmt ) -> CursorType;
            break;

        case SQL_ATTR_ENABLE_AUTO_IPD :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCStmt ) pStmt ) -> AutoIPD;
            break;

        case SQL_ATTR_FETCH_BOOKMARK_PTR :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_FETCH_BOOKMARK_PTR - option not implemented" ) );
            return SQL_ERROR;

        case SQL_ATTR_IMP_PARAM_DESC :
            * ( ( Long* ) pDataPtr ) = ( Long ) & ( ( ( pODBCStmt ) pStmt ) -> IPD );
            break;

        case SQL_ATTR_IMP_ROW_DESC :
            * ( ( Long* ) pDataPtr ) = ( Long ) & ( ( ( pODBCStmt ) pStmt ) -> IRD );
            break;

        case SQL_ATTR_KEYSET_SIZE :
            * ( ( Long* ) pDataPtr ) = ( ( pODBCStmt ) pStmt ) -> KeysetSize;
            break;

        case SQL_ATTR_MAX_LENGTH :
            * ( ( ULong* ) pDataPtr ) = 0;
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_MAX_LENGTH - option not implemented" ) );
            return SQL_ERROR;

        case SQL_ATTR_MAX_ROWS :
            * ( ( ULong* ) pDataPtr ) = 0;
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_MAX_ROWS - option not implemented" ) );
            return SQL_ERROR;

        case SQL_ATTR_METADATA_ID :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCStmt ) pStmt ) -> MetaDataID;
            break;

        case SQL_ATTR_NOSCAN :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCStmt ) pStmt ) -> NoScan;
            break;

        case SQL_ATTR_PARAM_BIND_TYPE :
            _SQLGetAPDField ( & ( ( ( pODBCStmt ) pStmt ) -> APD ), SQL_DESC_BIND_TYPE, pDataPtr, pDataSize, pDataSizePtr );
            break;

        case SQL_ATTR_PARAM_BIND_OFFSET_PTR :
            _SQLGetAPDField ( & ( ( ( pODBCStmt ) pStmt ) -> APD ), SQL_DESC_BIND_OFFSET_PTR, pDataPtr, pDataSize, pDataSizePtr );
            break;

        case SQL_ATTR_PARAM_OPERATION_PTR :
            _SQLGetAPDField ( & ( ( ( pODBCStmt ) pStmt ) -> APD ), SQL_DESC_ARRAY_STATUS_PTR, pDataPtr, pDataSize, pDataSizePtr );
            break;

        case SQL_ATTR_PARAM_STATUS_PTR :
            _SQLGetIPDField ( & ( ( ( pODBCStmt ) pStmt ) -> IPD ), SQL_DESC_ARRAY_STATUS_PTR, pDataPtr, pDataSize, pDataSizePtr );
            break;

        case SQL_ATTR_PARAMS_PROCESSED_PTR :
            // ???? note there is an error in MSDN on SQLSetDescField which says ARD/APD instead of IRD/IPD for this option
            _SQLGetAPDField ( & ( ( ( pODBCStmt ) pStmt ) -> APD ), SQL_DESC_ROWS_PROCESSED_PTR, pDataPtr, pDataSize,
                              pDataSizePtr );
            break;

        case SQL_ATTR_PARAMSET_SIZE :
            _SQLGetAPDField ( & ( ( ( pODBCStmt ) pStmt ) -> APD ), SQL_DESC_ARRAY_SIZE, pDataPtr, pDataSize, pDataSizePtr );
            break;

        case SQL_ATTR_QUERY_TIMEOUT :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCStmt ) pStmt ) -> QryTimeout;
            break;

        case SQL_ATTR_RETRIEVE_DATA :
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCStmt ) pStmt ) -> RetrieveData;
            break;

        case SQL_ROWSET_SIZE :
        case SQL_ATTR_ROW_ARRAY_SIZE :
            _SQLGetARDField ( & ( ( ( pODBCStmt ) pStmt ) -> ARD ), SQL_DESC_ARRAY_SIZE, pDataPtr, pDataSize, pDataSizePtr );
            break;

        case SQL_ATTR_ROW_BIND_OFFSET_PTR :
            _SQLGetARDField ( & ( ( ( pODBCStmt ) pStmt ) -> ARD ), SQL_DESC_BIND_OFFSET_PTR, pDataPtr, pDataSize, pDataSizePtr );
            break;

        case SQL_ATTR_ROW_BIND_TYPE :
            _SQLGetARDField ( & ( ( ( pODBCStmt ) pStmt ) -> ARD ), SQL_DESC_BIND_TYPE, pDataPtr, pDataSize, pDataSizePtr );
            break;

        case SQL_ATTR_ROW_NUMBER :
            // ??? in case of rowset being > 1, start row is being returned
            * ( ( ULong* ) pDataPtr ) = ( ( pODBCStmt ) pStmt ) -> CurRowsetStartRowPos;
            break;

        case SQL_ATTR_ROW_OPERATION_PTR :
            _SQLGetARDField ( & ( ( ( pODBCStmt ) pStmt ) -> ARD ), SQL_DESC_ARRAY_STATUS_PTR, pDataPtr, pDataSize, pDataSizePtr );
            break;

        case SQL_ATTR_ROW_STATUS_PTR :
            _SQLGetIRDField ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), SQL_DESC_ARRAY_STATUS_PTR, pDataPtr, pDataSize, pDataSizePtr );
            break;

        case SQL_ATTR_ROWS_FETCHED_PTR :
            _SQLGetIRDField ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), SQL_DESC_ROWS_PROCESSED_PTR, pDataPtr, pDataSize,
                              pDataSizePtr );
            break;

        case SQL_ATTR_SIMULATE_CURSOR :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQL_ATTR_SIMULATE_CURSOR - option not implemented" ) );
            return SQL_ERROR;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQLGetStmtAttr: Stmt attr - unknown %d", pAttr ) );
            return SQL_ERROR;
    }

    return SQL_SUCCESS;
}

// --------------------------------------------------------------------
// to get the name of the current cursor
// --------------------------------------------------------------------

RETCODE SQL_API SQLGetCursorName ( SQLHSTMT pStmt,
                                   SQLCHAR* pDataPtr,
                                   SQLSMALLINT pDataSize,
                                   SQLSMALLINT* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetCursorName called" ) );
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );
    _SQLCopyCharData ( _DIAGSTMT ( pStmt ), pDataPtr, pDataSize, pDataSizePtr, 32, "default", -1 );
    return SQL_SUCCESS;
}

// --------------------------------------------------------------------
// to set the name of the current cursor
// --------------------------------------------------------------------

RETCODE SQL_API SQLSetCursorName ( SQLHSTMT pStmt,
                                   SQLCHAR* pDataPtr,
                                   SQLSMALLINT pDataSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetCursorName called" ) );
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );
    return SQL_SUCCESS;
}

