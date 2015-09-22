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
// File:    KO_ALLOC.CPP
//
// Notes:   Contains function for managing allocation and release
//          of the three structures/handles - Environment,
//          Connection and Statement. Statements r
//          encapsulated inside connection while all connections
//          inside environment
//
// Exported functions:
//                       SQLAllocEnv
//                       SQLAllocConnect
//                       SQLAllocConnect
//                       SQLAllocStmt
//                       SQLAllocHandle
//                       SQLFreeStmt
//                       SQLDisconnect
//                       SQLFreeConnect
//                       SQLFreeHandle
//                       SQLFreeEnv
// ----------------------------------------------------------------------------

#include "stdafx.h"

// --------------------------- local functions --------------------------------
static eGoodBad         _SQLAttachStmt ( pODBCConn pConn, pODBCStmt pStmt );
static eGoodBad         _SQLDetachStmt ( pODBCConn pConn, pODBCStmt pStmt );

static RETCODE SQL_API _SQLFreeStmtResult ( pODBCStmt pHandle );
static RETCODE SQL_API _SQLFreeStmtCols ( pODBCStmt pHandle );
static RETCODE SQL_API _SQLFreeStmtParams ( pODBCStmt pHandle );
static RETCODE SQL_API _SQLFreeStmtAll ( pODBCStmt pHandle );
static RETCODE SQL_API _SQLFreeStmts ( pODBCStmt pHandle );

static eGoodBad         _SQLAttachConn ( pODBCEnv pEnv, pODBCConn pConn );
static eGoodBad         _SQLDetachConn ( pODBCEnv pEnv, pODBCConn pConn );


static RETCODE SQL_API _SQLFreeConnect ( pODBCConn pHandle );
static RETCODE SQL_API _SQLFreeConnects ( pODBCConn pHandle );

static RETCODE SQL_API _SQLFreeEnv ( pODBCEnv pHandle );


// -----------------------------------------------------------------------
// to allocate a environment
// -----------------------------------------------------------------------

RETCODE SQL_API SQLAllocEnv ( HENV* pOutputHandlePtr ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLAllocEnv called" ) );
    pODBCEnv     env = NULL;
    
    // precaution
    if ( pOutputHandlePtr == 0 ) {
        __ODBCPOPMSG ( _ODBCPopMsg ( "SQLAllocEnv - bad params" ) );
        return SQL_ERROR;
    }
    
    // initializations
    *pOutputHandlePtr = 0;
    // allocate an env
    env = new ODBCEnv;
    // clear the env attributes
    memset ( env, 0, sizeof ( ODBCEnv ) );
    // set the handle signature
    ( ( pODBCEnv ) env )->Sign = SQL_HANDLE_ENV;
    // default values
    ( ( pODBCEnv ) env )->AttrODBCVersion =
        SQL_OV_ODBC3;                        // SQL_ATTR_ODBC_VERSION                200
    ( ( pODBCEnv ) env )->AttrConnPooling =
        SQL_CP_OFF;                          // SQL_ATTR_CONNECTION_POOLING          201
    ( ( pODBCEnv ) env )->AttrCPMatch     =
        SQL_CP_STRICT_MATCH;                 // SQL_ATTR_CP_MATCH                    202
    ( ( pODBCEnv ) env )->AttrOutputNTS   = SQL_TRUE;                            // SQL_ATTR_OUTPUT_NTS
    // pass back to caller
    *pOutputHandlePtr = ( HENV ) env;
    return SQL_SUCCESS;
}


// -----------------------------------------------------------------------
// to allocate a connection
// -----------------------------------------------------------------------

RETCODE SQL_API SQLAllocConnect ( HENV pEnvHandle, HDBC* pOutputHandlePtr ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLAllocConnect called" ) );
    pODBCConn    conn;
    __CHK_HANDLE ( pEnvHandle, SQL_HANDLE_ENV, SQL_ERROR );
    _SQLFreeDiag ( _DIAGENV ( pEnvHandle ) );
    
    // precaution
    if ( pOutputHandlePtr == 0 ) {
        __ODBCPOPMSG ( _ODBCPopMsg ( "SQLAllocConnect - bad params" ) );
        return SQL_ERROR;
    }
    
    // initializations
    *pOutputHandlePtr = 0;
    // allocate a conn
    conn = new ODBCConn;
    // clear the conn attributes
    memset ( conn, 0, sizeof ( ODBCConn ) );
    // set the handle signature
    ( ( pODBCConn ) conn )->Sign = SQL_HANDLE_DBC;
    // default values
    ( ( pODBCConn ) conn )->AccessMode      = SQL_MODE_READ_ONLY;
    ( ( pODBCConn ) conn )->AutoIPD         = SQL_FALSE;
    ( ( pODBCConn ) conn )->AsyncEnable     = SQL_ASYNC_ENABLE_OFF;
    ( ( pODBCConn ) conn )->AutoCommit      = SQL_AUTOCOMMIT_ON;
    ( ( pODBCConn ) conn )->TimeOut         = 0;
    ( ( pODBCConn ) conn )->LoginTimeOut    = 0;
    ( ( pODBCConn ) conn )->MetaDataID      = SQL_FALSE;
    ( ( pODBCConn ) conn )->ODBCCursors     = SQL_CUR_USE_DRIVER;
    ( ( pODBCConn ) conn )->Window          = NULL;
    ( ( pODBCConn ) conn )->TxnIsolation    = 0;
    ( ( pODBCConn ) conn )->MaxRows         = 0;
    ( ( pODBCConn ) conn )->QueryTimeout    = 0;
    // attach it to link list
    _SQLAttachConn ( ( pODBCEnv ) pEnvHandle, conn );
    // pass back conn to caller
    *pOutputHandlePtr = ( HDBC ) conn;
    return SQL_SUCCESS;
}


// -----------------------------------------------------------------------
// to allocate a statement
// -----------------------------------------------------------------------

RETCODE SQL_API SQLAllocStmt ( HDBC pConnHandle, HSTMT* pOutputHandlePtr ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLAllocStmt called" ) );
    pODBCStmt    stmt = NULL;
    __CHK_HANDLE ( pConnHandle, SQL_HANDLE_DBC, SQL_ERROR );
    _SQLFreeDiag ( _DIAGCONN ( pConnHandle ) );
    
    // precaution
    if ( pOutputHandlePtr == 0 ) {
        __ODBCPOPMSG ( _ODBCPopMsg ( "SQLAllocStmt - bad params" ) );
        return SQL_ERROR;
    }
    
    // initializations
    *pOutputHandlePtr = 0;
    // allocate a stmt
    stmt = new ODBCStmt;
    // clear the stmt attributes
    memset ( stmt, 0, sizeof ( ODBCStmt ) );
    // set the handle signature
    ( ( pODBCStmt ) stmt )->Sign = SQL_HANDLE_STMT;
    // default values
    ( ( pODBCStmt ) stmt )->AsyncEnable         = SQL_ASYNC_ENABLE_OFF;
    ( ( pODBCStmt ) stmt )->Concurrency         = SQL_CONCUR_READ_ONLY;
    ( ( pODBCStmt ) stmt )->CursorScroll        = SQL_NONSCROLLABLE;
    ( ( pODBCStmt ) stmt )->CursorSensitivity   = SQL_UNSPECIFIED;
    ( ( pODBCStmt ) stmt )->CursorType          = SQL_CURSOR_FORWARD_ONLY;
    ( ( pODBCStmt ) stmt )->AutoIPD             = SQL_FALSE;
    ( ( pODBCStmt ) stmt )->KeysetSize          = 0;
    ( ( pODBCStmt ) stmt )->MetaDataID          = SQL_FALSE;
    ( ( pODBCStmt ) stmt )->NoScan              = SQL_NOSCAN_OFF;
    ( ( pODBCStmt ) stmt )->QryTimeout          = 0;
    ( ( pODBCStmt ) stmt )->RetrieveData        = SQL_RD_DEFAULT;
    // set descriptor defaults
    _SQLSetARDFieldsDefault ( & ( ( ( pODBCStmt ) stmt )->ARD ), ( pODBCStmt ) stmt );
    _SQLSetAPDFieldsDefault ( & ( ( ( pODBCStmt ) stmt )->APD ), ( pODBCStmt ) stmt );
    _SQLSetIRDFieldsDefault ( & ( ( ( pODBCStmt ) stmt )->IRD ), ( pODBCStmt ) stmt );
    _SQLSetIPDFieldsDefault ( & ( ( ( pODBCStmt ) stmt )->IPD ), ( pODBCStmt ) stmt );
    // attach it to link list
    _SQLAttachStmt ( ( pODBCConn ) pConnHandle, stmt );
    // pass back to caller
    *pOutputHandlePtr = ( HSTMT ) stmt;
    // ???? debug log message
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "New stmt: %d", stmt ) );
    return SQL_SUCCESS;
}


// -----------------------------------------------------------------------
// to allocate a handle for env, conn or stmt
// -----------------------------------------------------------------------

RETCODE SQL_API SQLAllocHandle ( SQLSMALLINT pHandleType, SQLHANDLE pInputHandle, SQLHANDLE*  pOutputHandlePtr ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLAllocHandle called" ) );
    
    switch ( pHandleType ) {
        case SQL_HANDLE_ENV:
            return SQLAllocEnv ( pOutputHandlePtr );                        // allocate environment
            
        case SQL_HANDLE_DBC:
            return SQLAllocConnect ( pInputHandle, pOutputHandlePtr );      // allocate connection
            
        case SQL_HANDLE_STMT:
            return SQLAllocStmt ( pInputHandle, pOutputHandlePtr );         // allocate statement
            
        case SQL_HANDLE_DESC:
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQLAllocHandle - Explicit descriptor requested - not supported" ) );
            return SQL_ERROR;                                             // allocate a descriptor
    }
    
    return SQL_ERROR;
}


// -----------------------------------------------------------------------
// to free an existing statement handle
// -----------------------------------------------------------------------

RETCODE SQL_API SQLFreeStmt ( SQLHSTMT pStmt, SQLUSMALLINT pOption ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, "SQLFreeStmt called, %d with option %d", pStmt, pOption ) );
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );
    
    // check the operation to perform
    switch ( pOption ) {
        case SQL_UNBIND:
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLFreeStmt called for Col bindings" ) );
            return _SQLFreeStmtCols ( ( pODBCStmt ) pStmt );
            
        case SQL_RESET_PARAMS:
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLFreeStmt called for params" ) );
            return _SQLFreeStmtParams ( ( pODBCStmt ) pStmt );
            
        default:            // include SQL_CLOSE:
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLFreeStmt called for Result" ) );
            return _SQLFreeStmtResult ( ( pODBCStmt ) pStmt );
    }
}


// -----------------------------------------------------------------------
// to disconnect from sever is a state is maintained on a connection
// -----------------------------------------------------------------------

RETCODE SQL_API SQLDisconnect ( HDBC pHandle ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, "SQLDisconnect called" ) );
    __CHK_HANDLE ( pHandle, SQL_HANDLE_DBC, SQL_ERROR );
    _SQLFreeDiag ( _DIAGCONN ( pHandle ) );
    // free the connection
    RETCODE code =  _SQLDisconnect ( ( pODBCConn ) pHandle );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLDisconnect exited" ) );
    return code;
}

// -----------------------------------------------------------------------
// to free an existing connection handle
// -----------------------------------------------------------------------

RETCODE SQL_API SQLFreeConnect ( HDBC pHandle ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, "SQLFreeConnect called" ) );
    __CHK_HANDLE ( pHandle, SQL_HANDLE_DBC, SQL_ERROR );
    // free the connection
    RETCODE code =  _SQLFreeConnect ( ( pODBCConn ) pHandle );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLFreeConnect Exited" ) );
    return code;
}


// -----------------------------------------------------------------------
// to free an existing environment handle
// -----------------------------------------------------------------------

RETCODE SQL_API SQLFreeEnv ( HENV pHandle ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, "SQLFreeEnv called" ) );
    __CHK_HANDLE ( pHandle, SQL_HANDLE_ENV, SQL_ERROR );
    // free the env
    return _SQLFreeEnv ( ( pODBCEnv ) pHandle );
}

// -----------------------------------------------------------------------
// to free an environment, connection or stmt
// -----------------------------------------------------------------------

RETCODE SQL_API SQLFreeHandle ( SQLSMALLINT pHandleType, SQLHANDLE pHandle ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, "SQLFreeHandle called, Handle Type: %d, Handle: %d", pHandleType,
                              ( Long ) pHandle ) );
    __CHK_HANDLE ( pHandle, pHandleType, SQL_ERROR );
    
    // as per the handle type
    switch ( pHandleType ) {
        case SQL_HANDLE_ENV:
            return _SQLFreeEnv ( ( pODBCEnv ) pHandle );                 // free environment
            
        case SQL_HANDLE_DBC:
            return _SQLFreeConnect ( ( pODBCConn ) pHandle );            // free conenction
            
        case SQL_HANDLE_STMT:
            return _SQLFreeStmtAll ( ( pODBCStmt ) pHandle );            // free statement
            
        case SQL_HANDLE_DESC:
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQLFreeHandle called for descriptor" ) );
            return SQL_ERROR;                             // free descriptor
    }
    
    return SQL_ERROR;
}


// -----------------------------------------------------------------------
// to attach a new stmt to STMT link list in CONN
// -----------------------------------------------------------------------

static eGoodBad _SQLAttachStmt ( pODBCConn pConn, pODBCStmt pStmt ) {
    pODBCStmt     l;
    
    // precaution
    if ( pConn == NULL || pStmt == NULL )
    { return BAD; }
    
    // set conn as stmt container
    pStmt->Conn = pConn;
    
    // check if this is the first item
    if ( pConn->Stmts == NULL ) {
        // set as first and only item
        pConn->Stmts  = pStmt;
        pStmt->Prev   = NULL;
        pStmt->Next   = NULL;
        return GOOD;
    }
    
    // move to tail item
    for ( l = pConn->Stmts; l->Next != NULL; l = l->Next );
    
    // attach to tail
    l->Next         = pStmt;
    pStmt->Prev     = l;
    pStmt->Next     = NULL;
    return GOOD;
}

// -----------------------------------------------------------------------
// to detach an existing stmt from link list
// -----------------------------------------------------------------------

static eGoodBad _SQLDetachStmt ( pODBCConn pConn, pODBCStmt pStmt ) {
    // precaution
    if ( pConn == NULL || pStmt == NULL )
    { return BAD; }
    
    if ( pStmt->Prev )
    { ( pStmt->Prev )->Next = pStmt->Next; }        // attach prev to next
    
    else
    { pConn->Stmts = pStmt->Next; }                 // set head to next if any
    
    if ( pStmt->Next )
    { ( pStmt->Next )->Prev = pStmt->Prev; }        // set next to prev if any
    
    return GOOD;
}


// -----------------------------------------------------------------------
// free the cursor/results associated with the specified stmt
// -----------------------------------------------------------------------

static RETCODE SQL_API _SQLFreeStmtResult ( pODBCStmt pHandle ) {
    // note
    // assumes handle already checked
    
    // basic stmt
    if ( pHandle->Stmt ) {
        delete[] pHandle->Stmt;
        pHandle->Stmt = NULL;
    }
    
    // basic stmt
    pHandle->StmtLen    = 0;
    pHandle->Prepared   = FALSE;
    pHandle->CurRowsetStartRow      = NULL;                         // start of current rowset
    pHandle->CurRowsetStartRowPos   = 0;                            // absolute position
    pHandle->CurRowsetEndRow        = NULL;                         // end of current rowset
    pHandle->CurRowsetEndRowPos     = 0;                            // absolute position
    pHandle->RowCount       = 0;
    _SQLFreeIRDContent ( & ( pHandle->IRD ) );
    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// free all the cols bound to a specified stmt
// -----------------------------------------------------------------------

static RETCODE SQL_API _SQLFreeStmtCols ( pODBCStmt pHandle ) {
    // note
    // assumes handle already checked
    return _SQLFreeARDContent ( & ( pHandle->ARD ) );
}


// -----------------------------------------------------------------------
// free all the params bound to a particular stmt
// -----------------------------------------------------------------------

static RETCODE SQL_API _SQLFreeStmtParams ( pODBCStmt pHandle ) {
    // note always called as a sub-function so no diag reset
    // to be implemented along with
    // other details of parms
    return _SQLFreeAPDContent ( & ( pHandle->APD ) );
}


// -----------------------------------------------------------------------
// to free the stmt itself  & others associated items like ARD, APD etc
// -----------------------------------------------------------------------

static RETCODE SQL_API _SQLFreeStmtAll ( pODBCStmt pHandle ) {
    _SQLFreeDiag ( _DIAGSTMT ( pHandle ) );
    // note always called as a sub-function so no diag reset
    pODBCConn    conn;
    // extract the container connection
    conn = pHandle->Conn;
    
    // check if valid
    if ( conn == NULL )
    { return SQL_ERROR; }
    
    // pluck from link-list
    _SQLDetachStmt ( conn, pHandle );
    // clear/free results
    _SQLFreeStmtResult ( pHandle );
    // clear/free col bindings
    _SQLFreeStmtCols ( pHandle );
    // clear/free parms bindings
    _SQLFreeStmtParams ( pHandle );
    // now free the structure itself
    delete pHandle;
    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to all the stmts starting from the specified stmt
// -----------------------------------------------------------------------

static RETCODE SQL_API _SQLFreeStmts ( pODBCStmt pHandle ) {
    pODBCStmt    t, n;
    
    // loop to iterate the list
    for ( n = pHandle; n != NULL; ) {
        t = n;
        n = n->Next;
        _SQLFreeStmtAll ( t );
    }
    
    return SQL_SUCCESS;
}


// -----------------------------------------------------------------------
// to attach a new conn to CONN link list in ENV
// -----------------------------------------------------------------------

static eGoodBad _SQLAttachConn ( pODBCEnv pEnv, pODBCConn pConn ) {
    pODBCConn     l;
    
    // precaution
    if ( pEnv == NULL || pConn == NULL )
    { return BAD; }
    
    // set env as container for conn
    pConn->Env = pEnv;
    
    // check if this is the first item
    if ( pEnv->Conns == NULL ) {
        // set as first and only item
        pEnv->Conns   = pConn;
        pConn->Prev   = NULL;
        pConn->Next   = NULL;
        return GOOD;
    }
    
    // move to tail item
    for ( l = pEnv->Conns; l->Next != NULL; l = l->Next );
    
    // attach to tail
    l->Next         = pConn;
    pConn->Prev     = l;
    pConn->Next     = NULL;
    return GOOD;
}

// -----------------------------------------------------------------------
// to detach an existing item from ARD link-list
// -----------------------------------------------------------------------

static eGoodBad _SQLDetachConn ( pODBCEnv pEnv, pODBCConn pConn ) {
    // precaution
    if ( pEnv == NULL || pConn == NULL )
    { return BAD; }
    
    if ( pConn->Prev )
    { ( pConn->Prev )->Next = pConn->Next; }        // attach prev to next
    
    else
    { pEnv->Conns = pConn->Next; }                 // set head to next if any
    
    if ( pConn->Next )
    { ( pConn->Next )->Prev = pConn->Prev; }        // set next to prev if any
    
    return GOOD;
}

// -----------------------------------------------------------------------
// to free all info associated with a connection but not the conenction itself
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLDisconnect ( pODBCConn pHandle ) {
    pHandle->IsConnected = FALSE;
    
    if ( pHandle->ConnectStr ) {
        delete[] pHandle->ConnectStr;
        pHandle->ConnectStr = NULL;
    }
    
    if ( pHandle->Server ) {
        delete[] pHandle->Server;
        pHandle->Server = NULL;
    }
    
    if ( pHandle->Project ) {
        delete[] pHandle->Project;
        pHandle->Project = NULL;
    }
    
    if ( pHandle->UserName ) {
        delete[] pHandle->UserName;
        pHandle->UserName = NULL;
    }
    
    if ( pHandle->Password ) {
        delete[] pHandle->Password;
        pHandle->Password = NULL;
    }
    
    // free all associated statements
    if ( pHandle->Stmts ) {                                 // all stmts within connection
        _SQLFreeStmts ( pHandle->Stmts );
        pHandle->Stmts = NULL;
    }
    
    //free meta
    if ( pHandle->meta ) {
        pHandle->meta = NULL;
    }
    
    return SQL_SUCCESS;
}


// -----------------------------------------------------------------------
// to free a specifed connection, plucks from parent/list AND FREEs it
// -----------------------------------------------------------------------

static RETCODE SQL_API _SQLFreeConnect ( pODBCConn pHandle ) {
    _SQLFreeDiag ( _DIAGCONN ( pHandle ) );
    pODBCEnv     env;
    // disconnect
    _SQLDisconnect ( pHandle );
    // extract the container environment
    env = pHandle->Env;
    
    // check if valid
    if ( env == NULL ) { return SQL_ERROR; }
    
    // detach from link list
    _SQLDetachConn ( env, pHandle );
    // now free the structure itself
    delete pHandle;
    // reset
    pHandle = NULL;
    return SQL_SUCCESS;
}


// -----------------------------------------------------------------------
// to all the connections starting from the specified connection
// -----------------------------------------------------------------------

static RETCODE SQL_API _SQLFreeConnects ( pODBCConn pHandle ) {
    pODBCConn    t, n;
    
    // loop to iterate the list of connections
    for ( n = pHandle; n != NULL; ) {
        t = n;                      // save
        n = n->Next;                // get next
        _SQLFreeConnect ( t );      // free saved
    }
    
    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to free the specified env
// -----------------------------------------------------------------------

static RETCODE SQL_API _SQLFreeEnv ( pODBCEnv pHandle ) {
    // free diags if any
    _SQLFreeDiag ( _DIAGENV ( pHandle ) );
    
    // check if any connections are allocated
    if ( pHandle->Conns ) {
        //_SQLFreeConnects ( pHandle->Conns );                    // relase all connections
    }
    
    delete pHandle;
    return SQL_SUCCESS;
}

