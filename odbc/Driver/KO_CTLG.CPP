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
// File:    KO_CTLG.CPP
//
// Purpose: Contains catalog functions
//          Functions that allow collection of metadata/information
//          about the database are termed as catalog functions.
//          For example SQLTables allows you to get all the
//          databases, users on the server or tables in database.
//          Similarly SQLColumns allows you to get all the cols
//          in a table.
//
// Exported functions:
//                       SQLTables
//                       SQLColumns
//                       SQLSpecialColumns
//                       SQLStatistics
//                       SQLPrimaryKeys
//                       SQLForeignKeys
//                       SQLTablePrivileges
//                       SQLColumnPrivileges
//                       SQLProcedures
//                       SQLProcedureColumns
//
// ----------------------------------------------------------------------------

#include "stdafx.h"


// -----------------------------------------------------------------------
// to get list of catalog(database), schema(users), tables(tables)
// ------------------------------------------------------------------------

RETCODE SQL_API SQLTablesW ( SQLHSTMT pStmt,
                             SQLWCHAR* pCatalogName,
                             SQLSMALLINT pCatalogNameSize,
                             SQLWCHAR* pSchemaName,
                             SQLSMALLINT pSchemaNameSize,
                             SQLWCHAR* pTableName,
                             SQLSMALLINT pTableNameSize,
                             SQLWCHAR* pTableType,
                             SQLSMALLINT pTableTypeSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLTablesW is called " ) );
    unique_ptr <char[]> p1 ( wchar2char ( pCatalogName ) );
    unique_ptr <char[]> p2 ( wchar2char ( pSchemaName ) );
    unique_ptr <char[]> p3 ( wchar2char ( pTableName ) );
    unique_ptr <char[]> p4 ( wchar2char ( pTableType ) );
    return SQLTables ( pStmt, ( SQLCHAR* ) p1 . get (), pCatalogNameSize, ( SQLCHAR* ) p2 . get (), pSchemaNameSize,
                       ( SQLCHAR* ) p3 . get (), pTableNameSize, ( SQLCHAR* ) p4 . get (), pTableTypeSize );
}

RETCODE SQL_API SQLTables ( SQLHSTMT pStmt,
                            SQLCHAR* pCatalogName,
                            SQLSMALLINT pCatalogNameSize,
                            SQLCHAR* pSchemaName,
                            SQLSMALLINT pSchemaNameSize,
                            SQLCHAR* pTableName,
                            SQLSMALLINT pTableNameSize,
                            SQLCHAR* pTableType,
                            SQLSMALLINT pTableTypeSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLTables: Ctlg: %s, %d, Schema: %s, %d, Table: %s,%d, Type: %s, %d",
        pCatalogName, pCatalogNameSize, pSchemaName, pSchemaNameSize, pTableName, pTableNameSize, pTableType,
        pTableTypeSize ) );
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );
    std::unique_ptr <SQLResponse> p = SQLResponse::MakeResp4SQLTables ( ( ( pODBCStmt ) pStmt ) -> Conn -> meta . get () );

    if ( PutRespToStmt ( ( pODBCStmt ) pStmt, std::move ( p ) ) != GOOD )
    {
        return SQL_ERROR;
    }

    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to get the list of column names in specified tables
// ------------------------------------------------------------------------

RETCODE SQL_API SQLColumnsW ( SQLHSTMT pStmt,
                              SQLWCHAR* pCtlgName,
                              SQLSMALLINT pCtlgNameLen,
                              SQLWCHAR* pSchemaName,
                              SQLSMALLINT pSchemaNameLen,
                              SQLWCHAR* pTableName,
                              SQLSMALLINT pTableNameLen,
                              SQLWCHAR* pColumnName,
                              SQLSMALLINT pColumnNameLen )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLColumnsW is called" ) );
    unique_ptr <char[]> p1 ( wchar2char ( pCtlgName ) );
    unique_ptr <char[]> p2 ( wchar2char ( pSchemaName ) );
    unique_ptr <char[]> p3 ( wchar2char ( pTableName ) );
    unique_ptr <char[]> p4 ( wchar2char ( pColumnName ) );
    return SQLColumns ( pStmt, ( SQLCHAR* ) p1 . get (), pCtlgNameLen, ( SQLCHAR* ) p2 . get (), pSchemaNameLen,
                        ( SQLCHAR* ) p3 . get (), pTableNameLen, ( SQLCHAR* ) p4 . get (), pColumnNameLen );
}

RETCODE SQL_API SQLColumns ( SQLHSTMT pStmt,
                             SQLCHAR* pCtlgName,
                             SQLSMALLINT pCtlgNameLen,
                             SQLCHAR* pSchemaName,
                             SQLSMALLINT pSchemaNameLen,
                             SQLCHAR* pTableName,
                             SQLSMALLINT pTableNameLen,
                             SQLCHAR* pColumnName,
                             SQLSMALLINT pColumnNameLen )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLColumns called, Ctlg: %s, %d. Schema: %s, %d, Table: %s, %d, Col: %s, %d",
        pCtlgName, pCtlgNameLen, pSchemaName, pSchemaNameLen, pTableName, pTableNameLen, pColumnName, pColumnNameLen ) );
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );

    // Some application will bring '\\' into table name
    remove_char ( ( char* )pTableName, '\\' );

    // feed stmt structure with response
    std::unique_ptr <SQLResponse> p = SQLResponse::MakeResp4SQLColumns ( ( ( pODBCStmt ) pStmt ) -> Conn -> meta . get (),
                                                                         ( char* ) pTableName, ( char* ) pColumnName );

    if ( PutRespToStmt ( ( pODBCStmt ) pStmt, std::move ( p ) ) != GOOD )
    {
        return SQL_ERROR;
    }

    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to get the list of column names which make a row unqiue or r updateable
// ------------------------------------------------------------------------

SQLRETURN SQL_API SQLSpecialColumns ( SQLHSTMT pStmt,
                                      SQLUSMALLINT pIdentifierType,
                                      SQLCHAR* pCtlgName,
                                      SQLSMALLINT pCtlgNameLen,
                                      SQLCHAR* pSchemaName,
                                      SQLSMALLINT pSchemaNameLen,
                                      SQLCHAR* pTableName,
                                      SQLSMALLINT pTableNameLen,
                                      SQLUSMALLINT pScope,
                                      SQLUSMALLINT pNullable )

{
    // note
    // possible values for pIdentifierType are
    // SQL_BEST_ROWID ----- primary key columns
    // SQL_ROWVER --------- all updateable columns
    //
    // possible values for pScope are
    // SQL_SCOPE_CURROW
    // SQL_SCOPE_TRANSACTION
    // SQL_SCOPE_SESSION
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLSpecialColumns called, Ctlg: %s, %d. Schema: %s, %d, Table: %s, %d",
        pCtlgName, pCtlgNameLen, pSchemaName, pSchemaNameLen, pTableName, pTableNameLen ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLSpecialColumns not implemented" ) );
    return SQL_ERROR;
}

// -----------------------------------------------------------------------
// to get table and/or index statistics
// ------------------------------------------------------------------------

RETCODE SQL_API SQLStatistics ( SQLHSTMT pStmt,
                                SQLCHAR* pCtlgName,
                                SQLSMALLINT pCtlgNameLen,
                                SQLCHAR* pSchemaName,
                                SQLSMALLINT pSchemaNameLen,
                                SQLCHAR* pTableName,
                                SQLSMALLINT pTableNameLen,
                                SQLUSMALLINT pUnique,
                                SQLUSMALLINT pReserved )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLStatistics called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLStatistics not implemented" ) );
    return SQL_ERROR;
}

// -----------------------------------------------------------------------
// to get columns which make up the p-keys
// ------------------------------------------------------------------------
RETCODE SQL_API SQLPrimaryKeysW ( SQLHSTMT pStmt,
                                  SQLWCHAR* pCtlgName,
                                  SQLSMALLINT pCtlgNameLen,
                                  SQLWCHAR* pSchemaName,
                                  SQLSMALLINT pSchemaNameLen,
                                  SQLWCHAR* pTableName,
                                  SQLSMALLINT pTableNameLen )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLPrimaryKeysW called" ) );
    return SQLPrimaryKeys ( pStmt, NULL, NULL, NULL, NULL, NULL, NULL );
}


RETCODE SQL_API SQLPrimaryKeys ( SQLHSTMT pStmt,
                                 SQLCHAR* pCtlgName,
                                 SQLSMALLINT pCtlgNameLen,
                                 SQLCHAR* pSchemaName,
                                 SQLSMALLINT pSchemaNameLen,
                                 SQLCHAR* pTableName,
                                 SQLSMALLINT pTableNameLen )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLPrimaryKeys called" ) );
    std::unique_ptr <SQLResponse> p ( new SQLResponse () );

    if ( PutRespToStmt ( ( pODBCStmt ) pStmt, std::move ( p ) ) != GOOD )
    {
        return SQL_ERROR;
    }

    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to get foreign key information
// ------------------------------------------------------------------------
RETCODE SQL_API SQLForeignKeysW ( SQLHSTMT pStmt,
                                  SQLWCHAR* pPKCtlgName,
                                  SQLSMALLINT pPKCtlgNameLen,
                                  SQLWCHAR* pPKSchemaName,
                                  SQLSMALLINT pPKSchemaNameLen,
                                  SQLWCHAR* pPKTableName,
                                  SQLSMALLINT pPKTableNameLen,
                                  SQLWCHAR* pFKCtlgName,
                                  SQLSMALLINT pFKCtlgNameLen,
                                  SQLWCHAR* pFKSchemaName,
                                  SQLSMALLINT pFKSchemaNameLen,
                                  SQLWCHAR* pFKTableName,
                                  SQLSMALLINT pFKTableNameLen )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLForeignKeysW called" ) );
    unique_ptr <char[]> p1 ( wchar2char ( pPKCtlgName ) );
    unique_ptr <char[]> p2 ( wchar2char ( pPKSchemaName ) );
    unique_ptr <char[]> p3 ( wchar2char ( pPKTableName ) );
    unique_ptr <char[]> p4 ( wchar2char ( pFKCtlgName ) );
    unique_ptr <char[]> p5 ( wchar2char ( pFKSchemaName ) );
    unique_ptr <char[]> p6 ( wchar2char ( pFKTableName ) );
    return SQLForeignKeys ( pStmt,
                            ( SQLCHAR* ) p1 . get (),
                            pPKCtlgNameLen,
                            ( SQLCHAR* ) p2 . get (),
                            pPKSchemaNameLen,
                            ( SQLCHAR* ) p3 . get (),
                            pPKTableNameLen,
                            ( SQLCHAR* ) p4 . get (),
                            pFKCtlgNameLen,
                            ( SQLCHAR* ) p5 . get (),
                            pFKSchemaNameLen,
                            ( SQLCHAR* ) p6 . get (),
                            pFKTableNameLen );
}

RETCODE SQL_API SQLForeignKeys ( SQLHSTMT pStmt,
                                 SQLCHAR* pPKCtlgName,
                                 SQLSMALLINT pPKCtlgNameLen,
                                 SQLCHAR* pPKSchemaName,
                                 SQLSMALLINT pPKSchemaNameLen,
                                 SQLCHAR* pPKTableName,
                                 SQLSMALLINT pPKTableNameLen,
                                 SQLCHAR* pFKCtlgName,
                                 SQLSMALLINT pFKCtlgNameLen,
                                 SQLCHAR* pFKSchemaName,
                                 SQLSMALLINT pFKSchemaNameLen,
                                 SQLCHAR* pFKTableName,
                                 SQLSMALLINT pFKTableNameLen )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "SQLForeignKeys called pPKCtlgName: %s, pPKSchemaName : %s, pPKTableName: %s, pFKCtlgName: %s, pFKSchemaName: %s, pFKTableName: %s",
        pPKCtlgName, pPKSchemaName, pPKTableName, pFKCtlgName, pFKSchemaName, pFKTableName ) );
    std::unique_ptr <SQLResponse> p ( new SQLResponse () );

    if ( PutRespToStmt ( ( pODBCStmt ) pStmt, std::move ( p ) ) != GOOD )
    {
        return SQL_ERROR;
    }

    return SQL_SUCCESS;
}


// -----------------------------------------------------------------------
// to get a list of tables and the privileges associated with each table
// ------------------------------------------------------------------------

RETCODE SQL_API SQLTablePrivileges ( SQLHSTMT pStmt,
                                     SQLCHAR* pCtlgName,
                                     SQLSMALLINT pCtlgNameLen,
                                     SQLCHAR* pSchemaName,
                                     SQLSMALLINT pSchemaNameLen,
                                     SQLCHAR* pTableName,
                                     SQLSMALLINT pTableNameLen )

{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLTablePrivileges called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLTablePrivileges not implemented" ) );
    return SQL_ERROR;
}

// -----------------------------------------------------------------------
// to get a list of columns and associated privileges for the specified table
// ------------------------------------------------------------------------

RETCODE SQL_API SQLColumnPrivileges ( SQLHSTMT pStmt,
                                      SQLCHAR* pCtlgName,
                                      SQLSMALLINT pCtlgNameLen,
                                      SQLCHAR* pSchemaName,
                                      SQLSMALLINT pSchemaNameLen,
                                      SQLCHAR* pTableName,
                                      SQLSMALLINT pTableNameLen,
                                      SQLCHAR* pColumnName,
                                      SQLSMALLINT pColumnNameLen )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLColumnPrivileges called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLColumnPrivileges not implemented" ) );
    return SQL_ERROR;
}


// -----------------------------------------------------------------------
// to get a list of procedure names stored in a specific data source
// ------------------------------------------------------------------------


RETCODE SQL_API SQLProcedures ( SQLHSTMT pStmt,
                                SQLCHAR* pCtlgName,
                                SQLSMALLINT pCtlgNameLen,
                                SQLCHAR* pSchemaName,
                                SQLSMALLINT pSchemaNameLen,
                                SQLCHAR* pProcName,
                                SQLSMALLINT pProcNameLen )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLProcedures called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLProcedures not implemented" ) );
    return SQL_ERROR;
}


// -----------------------------------------------------------------------
// to get a list of procedure names stored in a specific data source
// ------------------------------------------------------------------------

RETCODE SQL_API SQLProcedureColumns ( SQLHSTMT pStmt,
                                      SQLCHAR* pCtlgName,
                                      SQLSMALLINT pCtlgNameLen,
                                      SQLCHAR* pSchemaName,
                                      SQLSMALLINT pSchemaNameLen,
                                      SQLCHAR* pProcName,
                                      SQLSMALLINT pProcNameLen,
                                      SQLCHAR* pColumnName,
                                      SQLSMALLINT pColumnNameLen )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLProceduresColumns called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLProceduresColumns not implemented" ) );
    return SQL_ERROR;
}

