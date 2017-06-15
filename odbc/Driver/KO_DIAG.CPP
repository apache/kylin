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
// File:    KO_DIAG.CPP
//
// Purpose: Contains DIAG related functions.
//          Contains debug/log related functions.
//          The diagnostic functions work as per ODBC
//          specification while the log functions
//          are useful for debugging.
//
//          Each function call to the driver manager
//          can be logged by it using Tracing feature. Over
//          and above that our driver can also log call
//          to each function. This is useful to trace the
//          error faster and with precision. The _ODBCPopMsg
//          function is used to show an immediate popup msg
//
//          Diagnostic message are to be maintained as a list
//          at environment, connection and statement level. The
//          details r encapsulated inside ODBCDiag structures
//          defined in KylinODBC.h
//
//          SQLGetDiagRec allows u to get the most common fields
//          of a diagnostic message which can be either an error
//          or info. SQLGetDiagField allows you to get all the
//          possible info but one field at a time
//
//          The local function _SQLPutDiagRow puts the row in the
//          diag structure. There r versions of this function depending
//          upon whether the diagnostic handle is directly available
//          to caller and whether row and col info is applicable to the
//          diagnostic situation
//
// Exported functions:
//                       SQLGetDiagField
//                       SQLGetDiagRec
//
// ----------------------------------------------------------------------------
#include "stdafx.h"

#include <fcntl.h>
#include <io.h>
#include <sys/stat.h>
#include <stdio.h>
#include <time.h>
#include <stdarg.h>


// --------------------- local functions ---------------------------------
static pODBCDiagRow _SQLGetDiagRowX ( pODBCDiag pHandle, Word pRowNum );
static pODBCDiag _SQLGetDiagHandle ( SQLSMALLINT pHandleType, SQLHANDLE pHandle );

// --------------------- local functions ---------------------------------
static LogLevel currentLogLevel =
LogLevel_INFO;// Global variable to be set at the begining of pragram starts

// -----------------------------------------------------------------------
// to get a specified field from the specified diag row
// -----------------------------------------------------------------------


RETCODE SQL_API SQLGetDiagFieldW ( SQLSMALLINT pHandleType,
                                   SQLHANDLE pHandle,
                                   SQLSMALLINT pRecNum,
                                   SQLSMALLINT pFldID,
                                   SQLPOINTER pDataPtr,
                                   SQLSMALLINT pDataSize,
                                   SQLSMALLINT* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "SQLGetDiagFieldW called, HandleType: %d, RecNum: %d, InfoType: %d, BufLen: %d", pHandleType, pRecNum, pFldID,
        pDataSize ) );
    bool c;
    pODBCDiag diag;
    pODBCDiagRow diagrow;
    SQLSMALLINT dummySize = 0;//used when pDataSizePtr is NULL

    if ( pDataSizePtr == NULL )
    {
        pDataSizePtr = &dummySize;
    }

    __CHK_HANDLE ( pHandle, pHandleType, SQL_ERROR );
    diag = _SQLGetDiagHandle ( pHandleType, pHandle );

    if ( !diag )
    {
        return SQL_ERROR;
    }

    // field may be in diag header
    c = FALSE;

    // check the field type - header fields
    switch ( pFldID )
    {
        case SQL_DIAG_CURSOR_ROW_COUNT :
        case SQL_DIAG_ROW_COUNT : // rows affected by update/insert
            if ( pHandleType == SQL_HANDLE_STMT )
            {
                if ( pDataPtr )
                {
                    * ( ( SQLLEN* ) pDataPtr ) = ( pHandle ) ? ( ( pODBCStmt ) pHandle ) -> RowCount : 0;
                }

                return SQL_SUCCESS;
            }

            else
            {
                return SQL_ERROR;
            }

        case SQL_DIAG_DYNAMIC_FUNCTION :
            if ( pHandleType == SQL_HANDLE_STMT )
            {
                _SQLCopyWCharDataW ( diag, pDataPtr, pDataSize, pDataSizePtr, 16, ( ( pODBCStmt ) pHandle ) -> Stmt,
                                     ( ( pODBCStmt ) pHandle ) -> StmtLen );
                return SQL_SUCCESS;
            }

            else
            {
                return SQL_ERROR;
            }

            return SQL_ERROR;

        case SQL_DIAG_DYNAMIC_FUNCTION_CODE :
            if ( pHandleType == SQL_HANDLE_STMT )
            {
                if ( pDataPtr )
                {
                    * ( ( StrPtr ) pDataPtr ) = 1;
                } // ??? debug test only

                _SQLCopyWCharDataW ( diag, pDataPtr, pDataSize, pDataSizePtr, 16, ( ( pODBCStmt ) pHandle ) -> Stmt,
                                     ( ( ( pODBCStmt ) pHandle ) -> StmtLen ) );
                return SQL_SUCCESS;
            }

            else
            {
                return SQL_ERROR;
            }

        case SQL_DIAG_NUMBER :
        { // number of rows in diag
            Word i;

            // check if there r any diag rows
            if ( diag -> DiagRows )
            {
                // loop to count the rows
                for ( i = 1 , diagrow = diag -> DiagRows; diagrow != NULL; diagrow = diagrow -> Next , i ++ );

                if ( pDataPtr )
                {
                    * ( ( Word* ) pDataPtr ) = i;
                }
            }

            else if ( pDataPtr )
            {
                * ( ( Word* ) pDataPtr ) = 0;
            }

            return SQL_SUCCESS;
        }
            break;

        default :
            c = TRUE;
    }

    // check if only a header field was required
    if ( c == FALSE )
    {
        return SQL_SUCCESS;
    }

    // check row and buffer
    if ( pRecNum <= 0 || pDataSize < 0 )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "GetDiagField  xxx1" ) );
        return SQL_ERROR;
    }

    // now get the desired row first
    if ( ( diagrow = _SQLGetDiagRowX ( diag, pRecNum ) ) == NULL )
    {
        return SQL_NO_DATA;
    }

    // now set info as per the field required
    switch ( pFldID )
    {
        case SQL_DIAG_CLASS_ORIGIN :
            _SQLCopyWCharData ( diag, pDataPtr, pDataSize, pDataSizePtr, 16, "ODBC 3.0", -1 );
            break;

        case SQL_DIAG_COLUMN_NUMBER :

            // needs to be implemented
            if ( pDataPtr )
            {
                * ( ( Long* ) pDataPtr ) = diagrow -> Col;
            }

            break;

        case SQL_DIAG_CONNECTION_NAME :
            if ( pDataPtr )
            {
                * ( ( char* ) pDataPtr ) = 0;
            }

            if ( pDataSizePtr )
            {
                *pDataSizePtr = 0;
            }

            break;

        case SQL_DIAG_MESSAGE_TEXT :
            return _SQLCopyWCharData ( diag, pDataPtr, pDataSize, pDataSizePtr, 16, diagrow -> Msg, -1 );

        case SQL_DIAG_NATIVE :
            if ( pDataPtr )
            {
                * ( ( Long* ) pDataPtr ) = diagrow -> NativeErrorCode;
            }

            if ( pDataSizePtr )
            {
                *pDataSizePtr = 0;
            }

            break;

        case SQL_DIAG_ROW_NUMBER :

            // needs to be implemented
            if ( pDataPtr )
            {
                * ( ( Long* ) pDataPtr ) = diagrow -> Row;
            }

            break;

        case SQL_DIAG_SERVER_NAME :
        {
            CStrPtr svr;

            // if handle type is connection
            if ( pHandleType == SQL_HANDLE_DBC )
            {
                svr = ( ( pODBCConn ) pHandle ) -> Server;
            }

            else if ( pHandleType == SQL_HANDLE_STMT && ( ( pODBCStmt ) pHandle ) -> Conn )
            {
                svr = ( ( pODBCStmt ) pHandle ) -> Conn -> Server;
            }

            else
            {
                svr = "";
            }

            return _SQLCopyWCharData ( diag, pDataPtr, pDataSize, pDataSizePtr, 16, svr, -1 );
        }

        case SQL_DIAG_SQLSTATE :
            return _SQLCopyWCharData ( diag, pDataPtr, pDataSize, pDataSizePtr, 16, diagrow -> State, -1 );

        case SQL_DIAG_SUBCLASS_ORIGIN :
            // ??? dummy
            return _SQLCopyWCharData ( diag, pDataPtr, pDataSize, pDataSizePtr, 16, diagrow -> State, -1 );

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQLGetDiagField called, HandleType: %d, RecNum: %d, InfoType: %d, BufLen: %d",
                pHandleType, pRecNum, pFldID, pDataSize ) );
            return SQL_ERROR;
    }

    return SQL_SUCCESS;
}


RETCODE SQL_API SQLGetDiagField ( SQLSMALLINT pHandleType,
                                  SQLHANDLE pHandle,
                                  SQLSMALLINT pRecNum,
                                  SQLSMALLINT pFldID,
                                  SQLPOINTER pDataPtr,
                                  SQLSMALLINT pDataSize,
                                  SQLSMALLINT* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "SQLGetDiagField called, HandleType: %d, RecNum: %d, InfoType: %d, BufLen: %d", pHandleType, pRecNum, pFldID,
        pDataSize ) );
    bool c;
    pODBCDiag diag;
    pODBCDiagRow diagrow;
    SQLSMALLINT dummySize = 0;//used when pDataSizePtr is NULL

    if ( pDataSizePtr == NULL )
    {
        pDataSizePtr = &dummySize;
    }

    __CHK_HANDLE ( pHandle, pHandleType, SQL_ERROR );
    diag = _SQLGetDiagHandle ( pHandleType, pHandle );

    if ( !diag )
    {
        return SQL_ERROR;
    }

    // field may be in diag header
    c = FALSE;

    // check the field type - header fields
    switch ( pFldID )
    {
        case SQL_DIAG_CURSOR_ROW_COUNT :
        case SQL_DIAG_ROW_COUNT : // rows affected by update/insert
            if ( pHandleType == SQL_HANDLE_STMT )
            {
                if ( pDataPtr )
                {
                    * ( ( SQLLEN* ) pDataPtr ) = ( pHandle ) ? ( ( pODBCStmt ) pHandle ) -> RowCount : 0;
                }

                return SQL_SUCCESS;
            }

            else
            {
                return SQL_ERROR;
            }

        case SQL_DIAG_DYNAMIC_FUNCTION :
            if ( pHandleType == SQL_HANDLE_STMT )
            {
                unique_ptr <char[]> p ( wchar2char ( ( ( pODBCStmt ) pHandle ) -> Stmt ) );
                _SQLCopyCharData ( diag, pDataPtr, pDataSize, pDataSizePtr, 16, p . get (), ( ( pODBCStmt ) pHandle ) -> StmtLen );
                return SQL_SUCCESS;
            }

            else
            {
                return SQL_ERROR;
            }

            return SQL_ERROR;

        case SQL_DIAG_DYNAMIC_FUNCTION_CODE :
            if ( pHandleType == SQL_HANDLE_STMT )
            {
                if ( pDataPtr )
                {
                    * ( ( StrPtr ) pDataPtr ) = 1;
                } // ??? debug test only

                unique_ptr <char[]> p ( wchar2char ( ( ( pODBCStmt ) pHandle ) -> Stmt ) );
                _SQLCopyCharData ( diag, pDataPtr, pDataSize, pDataSizePtr, 16, p . get (), ( ( ( pODBCStmt ) pHandle ) -> StmtLen ) );
                return SQL_SUCCESS;
            }

            else
            {
                return SQL_ERROR;
            }

        case SQL_DIAG_NUMBER :
        { // number of rows in diag
            Word i;

            // check if there r any diag rows
            if ( diag -> DiagRows )
            {
                // loop to count the rows
                for ( i = 1 , diagrow = diag -> DiagRows; diagrow != NULL; diagrow = diagrow -> Next , i ++ );

                if ( pDataPtr )
                {
                    * ( ( Word* ) pDataPtr ) = i;
                }
            }

            else if ( pDataPtr )
            {
                * ( ( Word* ) pDataPtr ) = 0;
            }

            return SQL_SUCCESS;
        }
            break;

        default :
            c = TRUE;
    }

    // check if only a header field was required
    if ( c == FALSE )
    {
        return SQL_SUCCESS;
    }

    // check row and buffer
    if ( pRecNum <= 0 || pDataSize < 0 )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "GetDiagField  xxx1" ) );
        return SQL_ERROR;
    }

    // now get the desired row first
    if ( ( diagrow = _SQLGetDiagRowX ( diag, pRecNum ) ) == NULL )
    {
        return SQL_NO_DATA;
    }

    // now set info as per the field required
    switch ( pFldID )
    {
        case SQL_DIAG_CLASS_ORIGIN :
            _SQLCopyCharData ( diag, pDataPtr, pDataSize, pDataSizePtr, 16, "ODBC 3.0", -1 );
            break;

        case SQL_DIAG_COLUMN_NUMBER :

            // needs to be implemented
            if ( pDataPtr )
            {
                * ( ( Long* ) pDataPtr ) = diagrow -> Col;
            }

            break;

        case SQL_DIAG_CONNECTION_NAME :
            if ( pDataPtr )
            {
                * ( ( char* ) pDataPtr ) = 0;
            }

            if ( pDataSizePtr )
            {
                *pDataSizePtr = 0;
            }

            break;

        case SQL_DIAG_MESSAGE_TEXT :
            return _SQLCopyCharData ( diag, pDataPtr, pDataSize, pDataSizePtr, 16, diagrow -> Msg, -1 );

        case SQL_DIAG_NATIVE :
            if ( pDataPtr )
            {
                * ( ( Long* ) pDataPtr ) = diagrow -> NativeErrorCode;
            }

            if ( pDataSizePtr )
            {
                *pDataSizePtr = 0;
            }

            break;

        case SQL_DIAG_ROW_NUMBER :

            // needs to be implemented
            if ( pDataPtr )
            {
                * ( ( Long* ) pDataPtr ) = diagrow -> Row;
            }

            break;

        case SQL_DIAG_SERVER_NAME :
        {
            CStrPtr svr;

            // if handle type is connection
            if ( pHandleType == SQL_HANDLE_DBC )
            {
                svr = ( ( pODBCConn ) pHandle ) -> Server;
            }

            else if ( pHandleType == SQL_HANDLE_STMT && ( ( pODBCStmt ) pHandle ) -> Conn )
            {
                svr = ( ( pODBCStmt ) pHandle ) -> Conn -> Server;
            }

            else
            {
                svr = "";
            }

            return _SQLCopyCharData ( diag, pDataPtr, pDataSize, pDataSizePtr, 16, svr, -1 );
        }

        case SQL_DIAG_SQLSTATE :
            return _SQLCopyCharData ( diag, pDataPtr, pDataSize, pDataSizePtr, 16, diagrow -> State, -1 );

        case SQL_DIAG_SUBCLASS_ORIGIN :
            // ??? dummy
            return _SQLCopyCharData ( diag, pDataPtr, pDataSize, pDataSizePtr, 16, diagrow -> State, -1 );

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQLGetDiagField called, HandleType: %d, RecNum: %d, InfoType: %d, BufLen: %d",
                pHandleType, pRecNum, pFldID, pDataSize ) );
            return SQL_ERROR;
    }

    return SQL_SUCCESS;
}

RETCODE SQL_API SQLGetDiagRecW ( SQLSMALLINT pHandleType,
                                 SQLHANDLE pHandle,
                                 SQLSMALLINT pRecNum,
                                 SQLWCHAR* pSqlstate,
                                 SQLINTEGER* pNativeErrorPtr,
                                 SQLWCHAR* pMsgTxtPtr,
                                 SQLSMALLINT pMsgTxtSize,
                                 SQLSMALLINT* pMsgTxtSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetDiagRecW called, HandleType: %d, RecNum: %d, BufLen: %d", pHandleType,
        pRecNum, pMsgTxtSize ) );
    pODBCDiag diag;
    pODBCDiagRow diagrow;
    __CHK_HANDLE ( pHandle, pHandleType, SQL_ERROR );
    diag = _SQLGetDiagHandle ( pHandleType, pHandle );

    if ( !diag )
    {
        return SQL_ERROR;
    }

    // now get the desired diag row
    if ( ( diagrow = _SQLGetDiagRowX ( diag, pRecNum ) ) == NULL )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "sql no data" ) );
        return SQL_NO_DATA;
    }

    // sql-state
    if ( pSqlstate )
    {
        char2wchar ( diagrow -> State, pSqlstate, -1 );
    }

    // native error code
    if ( pNativeErrorPtr )
    {
        ( *pNativeErrorPtr ) = diagrow -> NativeErrorCode;
    }

    // msg
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The message is %s", diagrow->Msg ) );
    _SQLCopyWCharData ( diag, pMsgTxtPtr, pMsgTxtSize, pMsgTxtSizePtr, 16, diagrow -> Msg, -1, false );

    // debug
    //__ODBCLOG(_ODBCLogMsg(LogLevel_DEBUG,"SQLGetDiagRec msg: %s", pMsgTxtPtr ? ( StrPtr )pMsgTxtPtr : "(unknown)" ));
    RETCODE ret = ( pMsgTxtSizePtr && ( *pMsgTxtSizePtr ) > pMsgTxtSize ) ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The SQLGetDiagRecW return code is %d", ret ) );
    return ret;
}


// -----------------------------------------------------------------------
// to get all details of a diag row on specified handle
// -----------------------------------------------------------------------

RETCODE SQL_API SQLGetDiagRec ( SQLSMALLINT pHandleType,
                                SQLHANDLE pHandle,
                                SQLSMALLINT pRecNum,
                                SQLCHAR* pSqlstate,
                                SQLINTEGER* pNativeErrorPtr,
                                SQLCHAR* pMsgTxtPtr,
                                SQLSMALLINT pMsgTxtSize,
                                SQLSMALLINT* pMsgTxtSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetDiagRec called, HandleType: %d, RecNum: %d, BufLen: %d", pHandleType,
        pRecNum, pMsgTxtSize ) );
    pODBCDiag diag;
    pODBCDiagRow diagrow;
    __CHK_HANDLE ( pHandle, pHandleType, SQL_ERROR );
    diag = _SQLGetDiagHandle ( pHandleType, pHandle );

    if ( !diag )
    {
        return SQL_ERROR;
    }

    // now get the desired diag row
    if ( ( diagrow = _SQLGetDiagRowX ( diag, pRecNum ) ) == NULL )
    {
        return SQL_NO_DATA;
    }

    // sql-state
    if ( pSqlstate )
    {
        strcpy ( ( char* ) pSqlstate, diagrow -> State );
    }

    // native error code
    if ( pNativeErrorPtr )
    {
        ( *pNativeErrorPtr ) = diagrow -> NativeErrorCode;
    }

    // msg
    _SQLCopyCharData ( diag, pMsgTxtPtr, pMsgTxtSize, pMsgTxtSizePtr, 16, diagrow -> Msg, -1 );
    // debug
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetDiagRec msg: %s", pMsgTxtPtr ? ( StrPtr ) pMsgTxtPtr : "(unknown)" ) );
    return ( pMsgTxtSizePtr && ( *pMsgTxtSizePtr ) > pMsgTxtSize ) ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to post/record a diagnostic message in the specified DIAG struct
// -----------------------------------------------------------------------

pODBCDiagRow _SQLPutDiagRow ( pODBCDiag pDiag, StrPtr pFunc, StrPtr pState, Long pNativeErrorCode,
                              StrPtr pMsgArgs, va_list pArgs )
{
	// to avoid: TST1003: The message for sqlstate '01000' is longer than SQL_MAX_MESSAGE_LENGTH - 1.
    Char s[SQL_MAX_MESSAGE_LENGTH - 1]; // arbitary and maximum length of message
    pODBCDiagRow l;
    pODBCDiagRow r;

    // first set the value of source function, assuming less than 64
    if ( pFunc )
    {
        strcpy ( pDiag -> Func, pFunc );
    }

    // create message template
    strcpy ( s, "[Kylin][ODBC 1.0(w) Driver]" );

    // check if there is some message
    if ( pMsgArgs )
    {
		vsnprintf ( s  + strlen ( s ), SQL_MAX_MESSAGE_LENGTH - 2 - strlen(s), pMsgArgs, pArgs );
    }

    else
    {
        strcat ( s, "(unknown)" );
    }

    // allocate a new row
    r = new ODBCDiagRow;
    // reset
    memset ( r, 0, sizeof ( ODBCDiagRow) );

    // set the values for state and native error code
    if ( pState )
    {
        strncpy ( r -> State, pState, SQL_SQLSTATE_SIZE );
    }

    r -> NativeErrorCode = pNativeErrorCode;
    // allocate space for message
    r -> Msg = new Char[strlen ( s ) + 1];
    strcpy ( r -> Msg, s );
    // set the default row and col values
    r -> Row = SQL_ROW_NUMBER_UNKNOWN;
    r -> Col = SQL_COLUMN_NUMBER_UNKNOWN;
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "DiagMsg: %s", s ) );

    // ATTACH TO LIST

    // check for any existing rows
    if ( pDiag -> DiagRows )
    {
        // iterate to last row
        for ( l = pDiag -> DiagRows; l -> Next != NULL; l = l -> Next );

        // attach
        l -> Next = r; // next of last row
        r -> Prev = l; // last row becomes the prev row of this row
    }

    else
    {
        // make it the first row
        pDiag -> DiagRows = r;
    }

    return r;
}

// -----------------------------------------------------------------------
// to post/record a diagnostic message in the specified DIAG struct
// -----------------------------------------------------------------------

eGoodBad _SQLPutDiagRow ( SQLSMALLINT pHandleType, SQLHANDLE pHandle, StrPtr pFunc, StrPtr pState,
                          Long pNativeErrorCode, StrPtr pMsgArgs, ... )
{
    // note
    // this implementation of function does not take row/col as param
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLPutDiagRow called, handle type is %d", pHandleType ) );
    va_list args;
    pODBCDiag diag;
    pODBCDiagRow r;
    __CHK_HANDLE ( pHandle, pHandleType, BAD );
    diag = _SQLGetDiagHandle ( pHandleType, pHandle );

    if ( !diag )
    {
        return BAD;
    }

    // get va_args
    va_start ( args, pMsgArgs );
    r = _SQLPutDiagRow ( diag, pFunc, pState, pNativeErrorCode, pMsgArgs, args );
    va_end ( args );
    return ( r ) ? GOOD : BAD;
}

// -----------------------------------------------------------------------
// to post/record a diagnostic message in the specified DIAG struct
// -----------------------------------------------------------------------

eGoodBad _SQLPutDiagRow ( SQLSMALLINT pHandleType, SQLHANDLE pHandle, StrPtr pFunc, StrPtr pState,
                          Long pNativeErrorCode, Long pRow, Long pCol, StrPtr pMsgArgs, ... )
{
    // note
    // this implementation of function takes take row/col as param
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLPutDiagRow called" ) );
    va_list args;
    pODBCDiag diag;
    pODBCDiagRow r;
    __CHK_HANDLE ( pHandle, pHandleType, BAD );
    diag = _SQLGetDiagHandle ( pHandleType, pHandle );

    if ( !diag )
    {
        return BAD;
    }

    // get va_args
    va_start ( args, pMsgArgs );
    r = _SQLPutDiagRow ( diag, pFunc, pState, pNativeErrorCode, pMsgArgs, args );
    va_end ( args );

    if ( r )
    {
        r -> Row = pRow;
        r -> Col = pCol;
        return GOOD;
    }

    return BAD;
}

// -----------------------------------------------------------------------
// to free a specifed diag, plucks from parent/list AND FREEs it
// -----------------------------------------------------------------------

eGoodBad _SQLFreeDiag ( pODBCDiag pHandle )
{
    pODBCDiagRow t;

    // check if rows r there to be freed
    while ( pHandle -> DiagRows )
    {
        ////// PLUCK
        t = pHandle -> DiagRows;
        pHandle -> DiagRows = t -> Next;

        if ( pHandle -> DiagRows )
        {
            ( pHandle -> DiagRows ) -> Prev = NULL;
        }

        ////// PLUCKED

        // free contents of row
        if ( t -> Msg )
        {
            delete[] t -> Msg;
            t -> Msg = NULL;
        }

        // free the row itself
        delete t;
        // no rows
        pHandle -> DiagRows = NULL;
    }

    // free any other contents of the diag structure
    return GOOD;
}


// -----------------------------------------------------------------------
// to get the diag handle out of specified handle
// -----------------------------------------------------------------------

static pODBCDiag _SQLGetDiagHandle ( SQLSMALLINT pHandleType, SQLHANDLE pHandle )
{
    // first extract the diag details
    switch ( pHandleType )
    {
        case SQL_HANDLE_ENV :
            return & ( ( ( pODBCEnv ) pHandle ) -> Diag );

        case SQL_HANDLE_DBC :
            return & ( ( ( pODBCConn ) pHandle ) -> Diag );

        case SQL_HANDLE_STMT :
            return & ( ( ( pODBCStmt ) pHandle ) -> Diag );

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQLGetDiagRec was called with desciptor handle" ) );
            return NULL; // ??? support for desc diag not available now
    }
}


// -----------------------------------------------------------------------
// to get a specified diag row from link list
// -----------------------------------------------------------------------

static pODBCDiagRow _SQLGetDiagRowX ( pODBCDiag pHandle, Word pRowNum )
{
    Word i;
    pODBCDiagRow r;

    // loop to locate the specified row
    for ( i = 1 , r = pHandle -> DiagRows; r != NULL && i <= pRowNum && i != pRowNum; r = r -> Next , i ++ );

    // return the row or NULL as the case may be
    return ( r != NULL && i == pRowNum ) ? r : NULL;
}

// -----------------------------------------------------------------------
// to show a message immmediately to a user
// -----------------------------------------------------------------------

void _ODBCPopMsg ( const char* pMsgArgs, ... )
{
    Char s[4096]; // arbitary and maximum length of message
    va_list args;

    // check if there is some message
    if ( pMsgArgs )
    {
        // convert to full msg including parsed params
        va_start ( args, pMsgArgs );
        vsprintf ( s, pMsgArgs, args );
        va_end ( args );
    }

    else
    {
        s[0] = 0;
    }

    _ODBCLogMsg ( LogLevel_FATAL, s );
#ifdef SHIPPING
    // no thing
    _ODBCLogMsg ( LogLevel_FATAL, "In Shipping mode, Skip popping up window..." );
#else
    // show as window
    MessageBox ( GetDesktopWindow(), s, "Kylin ODBC Says :", MB_OK );
#endif
}

//pop directly
void __ODBCPopMsg ( const char* pMsgArgs, ... )
{
    Char s[4096]; // arbitary and maximum length of message
    va_list args;

    // check if there is some message
    if ( pMsgArgs )
    {
        // convert to full msg including parsed params
        va_start ( args, pMsgArgs );
        vsprintf ( s, pMsgArgs, args );
        va_end ( args );
    }

    else
    {
        s[0] = 0;
    }

#ifdef _CONSOLE
    // show on console
    fprintf ( stderr, s )
#else
    // show as window
    MessageBox ( GetDesktopWindow (), s, "Kylin ODBC Says:", MB_OK );
#endif
}

// -----------------------------------------------------------------------
// to start log for debugging
// -----------------------------------------------------------------------

bool fileExist ( char* path )
{
    DWORD dwAttrib = GetFileAttributes ( path );
    return ( dwAttrib != INVALID_FILE_ATTRIBUTES &&
        ! ( dwAttrib & FILE_ATTRIBUTE_DIRECTORY ) );
}

void _ODBCLogStart ( void )
{
    OutputDebugString ( "ODBCLogStart is called\n" ); // for DBMON
    StrPtr t;
    time_t ltime;

    // check if log file is open
    if ( gLogFile == -1 )
    {
        OutputDebugString ( "Try to init the log file\n" ); // for DBMON

        if ( GetFileAttributes ( "c:\\kylin" ) == INVALID_FILE_ATTRIBUTES )
        {
            if ( !CreateDirectory ( "c:\\kylin", NULL ) )
            {
                if ( GetLastError () == ERROR_ALREADY_EXISTS )
                {
                    // directory already exists
                    //good
                    OutputDebugString ( "Log folder already exist, ? \n" ); // for DBMON
                }

                else
                {
                    // creation failed due to some other reason
                    OutputDebugString ( "Failed to creat log folder\n" ); // for DBMON
                    throw;
                }
            }
        }

        else
        {
            OutputDebugString ( "Log folder already exist\n" ); // for DBMON
        }

        bool debugMode = fileExist ( "c:\\kylin\\debug" ) || fileExist ( "c:\\kylin\\debug.txt" );

        if ( debugMode )
        {
            currentLogLevel = LogLevel_DEBUG;
            OutputDebugString ( "Debug mode is on" ); // for DBMON
        }

        OutputDebugString ( "Try to open log file\n" ); // for DBMON
        time_t now = time ( 0 );
        struct tm tstruct;
        char buffer[100];
        tstruct = *localtime ( &now );
        strftime ( buffer, 100, "%Y-%m-%d.%X", &tstruct );

        for ( unsigned int i = 0; i < strlen ( buffer ); i++ )
        {
            if ( buffer[i] == '.' || buffer[i] == ':' )
            {
                buffer[i] = '-';
            }
        }

        char file[256];
        file[0] = '\0';
        strcat ( file, "c:\\kylin\\odbc-" );
        strcat ( file, buffer );
        strcat ( file, ".log" );
        printf ( file );
        gLogFile = _open ( file, _O_CREAT | _O_APPEND | _O_RDWR, _S_IWRITE );
        OutputDebugString ( "Finish to open log file\n" ); // for DBMON

        if ( gLogFile == -1 )
        {
            OutputDebugString ( "Open Log file failed.\n" ); // for DBMON
        }

        else
        {
            OutputDebugString ( "Open Log file succeed, logs are written to log file now.\n" ); // for DBMON
        }

        // set the log start time
        time ( &ltime );
        t = ctime ( &ltime );
        _write ( gLogFile, "Log start: ", strlen ( "Log start: " ) );
        _write ( gLogFile, t, strlen ( t ) );
        _write ( gLogFile, "\n", 1 );
        //if(t!=NULL)
        //delete[] t;
    }

    // check if file opened succesfully
    if ( gLogFile != -1 )
    {
        ++ gLogUsage;
    } // increment log usage

    else
    {
        __ODBCPOPMSG ( ( __ODBCPopMsg ( "Log failed" ) ) );
    }
}

// -----------------------------------------------------------------------
// to stop log
// -----------------------------------------------------------------------

void _ODBCLogStop ( int pForce )
{
    StrPtr t;
    time_t ltime;

    // check if log file is being used
    if ( gLogUsage > 0 )
    {
        // decrement log usage
        -- gLogUsage;
        // commit
        _commit ( gLogFile );

        // check if log needs to be closed
        if ( gLogUsage == 0 || pForce == 1 )
        {
            // set the log start time
            time ( &ltime );
            t = ctime ( &ltime );
            _write ( gLogFile, "Log end: ", strlen ( "Log end: " ) );
            _write ( gLogFile, t, strlen ( t ) );
            _write ( gLogFile, "\n", 1 );
            //if(t!=NULL)
            //delete t;
            _close ( gLogFile );
            gLogFile = -1;
        }
    }
}

// --------------------------------------------------------------------
// log a specified message
// --------------------------------------------------------------------
void _ODBCLogMsg ( LogLevel level, const wchar_t* textW )
{
    unique_ptr <char[]> p ( wchar2char ( textW ) );
    _ODBCLogMsg ( level, p . get () );
}

void _ODBCLogMsg ( LogLevel level, const char* pMsgArgs, ... )
{
    if ( level < currentLogLevel )
    {
        return;//skip
    }

    Char s[4096]; // arbitary and maximum length of message
    va_list args;

    // check if there is a log file
    if ( gLogFile != -1 )
    {
        switch ( level )
        {
            case LogLevel_DEBUG :
                _write ( gLogFile, "[DEBUG]", 7 );
                break;

            case LogLevel_INFO :
                _write ( gLogFile, "[INFO ]", 7 );
                break;

            case LogLevel_WARN :
                _write ( gLogFile, "[WARN ]", 7 );
                break;

            case LogLevel_ERROR :
                _write ( gLogFile, "[ERROR]", 7 );
                break;

            case LogLevel_FATAL :
                _write ( gLogFile, "[FATAL]", 7 );
                break;

            default :
                break;
        }

        char ts[24];
		SYSTEMTIME sys; 
		GetLocalTime( &sys ); 
		sprintf( ts, "%4d-%02d-%02d %02d:%02d:%02d.%03d", sys.wYear, sys.wMonth, sys.wDay, sys.wHour, sys.wMinute, sys.wSecond, sys.wMilliseconds); 

        _write ( gLogFile, "[", 1 );
        _write ( gLogFile, ts, strlen ( ts ) );
        _write ( gLogFile, "]", 1 );

        // MSG PARSING

        // check if there is some message
        if ( pMsgArgs )
        {
            // convert to full msg including parsed params
            va_start ( args, pMsgArgs );
            vsnprintf ( s, 4095, pMsgArgs, args );
            va_end ( args );
            // Omit out of buffer message
            if ( strlen ( s ) >= 4095 )
            {
                s[4092] = '.';
                s[4093] = '.';
                s[4094] = '.';
                s[4095] = 0;
            }

        }

        else
        {
            s[0] = 0;
        }

        // LOG
        _write ( gLogFile, s, strlen ( s ) );
        _write ( gLogFile, "\n", 1 );
    }
}

