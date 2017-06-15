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
// File:    KO_EXEC.CPP
//
// Purpose:     Contains function for executing queries.
//              After a request is prepared by the caller
//              control is passed to _SQLExecStmtFromReq.
//              This function
//              1. puts the request into the
//              statement structure. Right now this involves
//              putting the statement text to the structure
//              but it can be extended. (PutReqToStmt)
//              2. The actual request-response takes place
//              on REST call
//              3. The result is validated and again put
//              inside the statement structure for proper
//              access via IRD, resultdata and so on. (PutRespToStmt)
//
//
//              Either a qeuery can be executed directly
//              using SQLExecDirect or it can be executed
//              after it is prepared using SQLPrepare. This
//              is used in case mulitple queries r to be executed
//              with just a parameter change and the server
//              is capable of storing the query is a semi-processed
//              form providing for speed. Our SQLPrepare is nothing
//              but Execute only.
//
// Exported functions:
//                       SQLPrepare
//                       SQLExecute
//                       SQLExecDirect
//                       SQLCancel
//                       SQLEndTran
//                       SQLSetPos
//                       SQLCloseCursor
//
// ----------------------------------------------------------------------------
#include "stdafx.h"

#include "REST.h"


// --------------------- local functions prototypes -----------------------
static eGoodBad ProcessStmtForParams ( pODBCStmt pStmt, const wchar_t* pStmtText, Long pTextLength,
                                       wchar_t*& s );


// -----------------------------------------------------------------------
// to convert a stmt to an SZ string plus include param values
// -----------------------------------------------------------------------

static eGoodBad ProcessStmtForParams ( pODBCStmt pStmt, const wchar_t* pStmtText, Long pTextLength, wchar_t* & s ) {
    // note
    // since the param feature has not yet been implemented
    // this function just creates a zero-terminated version
    // of the stmt string
    Long t = 0;
    
    // calc stmt length
    if ( pStmtText )
    { t = ( pTextLength > 0 ) ? pTextLength : ( Long ) wcslen ( pStmtText ); }
    
    // allocate new space for stmt
    s = new wchar_t[t + 1];
    // make copy
    memcpy ( s, pStmtText, t * sizeof ( wchar_t ) );
    // SZ
    s[t] = 0;
    return GOOD;
}

// -----------------------------------------------------------------------
// to finalize a response to stmt structure
// -----------------------------------------------------------------------

eGoodBad PutRespToStmt ( pODBCStmt pStmt, std::unique_ptr<SQLResponse> resp ) {
    // check the response type
    if ( true ) {
        // TRANSFER  to stmt
        pStmt->CurRowsetStartRow      = NULL;                           // start of current rowset
        pStmt->CurRowsetStartRowPos   = 0;                              // absolute position
        pStmt->CurRowsetEndRow        = NULL;                           // end of current rowset
        pStmt->CurRowsetEndRowPos     = 0;                              // absolute position
        pStmt->RowCount         =  resp->results.size();
        pStmt->IRD.DescCount    = ( Word ) resp->columnMetas.size();
        pStmt->IRD.RowDesc      = std::move ( resp );                               // IRD
    }
    
    return GOOD;
}

// -----------------------------------------------------------------------
// to execute a statement given in form of rest query
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLExecStmtFromReq ( pODBCStmt pStmt, bool pPrepared ) {
    ////// this part should not be required if already prepared
    // release existing stmt contents
    //SQLFreeStmt ( pStmt, SQL_CLOSE );
    //////
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, "================================================================" ) );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, "start exec the query: " ) );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, pStmt->Stmt ) );

    std::unique_ptr<SQLResponse> p;
	wstring response;
    int status;

    try {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "start to call rest api" ) );
		response = requestQuery ( pStmt->Stmt, pStmt->Conn->Server, pStmt->Conn->ServerPort, pStmt->Conn->UserName, pStmt->Conn->Password,
                        pStmt->Conn->Project, pPrepared, &status );
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "received and uncompressed rest response:" ) );
		p = convertToSQLResponse(status, response);
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "parsed to SQLResponse" ) );
    }
    
    catch ( const exception& e ) {
        std::stringstream ss;
        ss << "The REST query request failed, the error message is: " << e.what();
        std::string s = ss.str();
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_ERROR, s.c_str() ) );
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "_SQLExecStmtFromReq", "01000", -1, ( char* ) s.c_str() );
        return SQL_ERROR;
    }
    
    // feed stmt structure with response, stmt will take charge of deleting it
    if ( p == NULL ||  p->isException == true || PutRespToStmt ( ( pODBCStmt ) pStmt, std::move ( p ) ) != GOOD ) {
        return SQL_ERROR;
    }
    
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, "Successfully done executing the query" ) );
    return SQL_SUCCESS;
}




// -----------------------------------------------------------------------
// to prepare a sql statement for executing
// -----------------------------------------------------------------------

RETCODE SQL_API SQLPrepare ( SQLHSTMT    pStmt,
                             SQLCHAR*    pStmtText,
                             SQLINTEGER  pTextLength ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLPrepare called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLPrepare not implemented, use SQLPrepareW" ) );
    return SQL_ERROR;
}

/*
     Kylin uses rest request for query executing, SQLPrepare does not do anything meaningful
*/
RETCODE SQL_API SQLPrepareW ( SQLHSTMT    pStmt,
                              SQLWCHAR*    pStmtText,
                              SQLINTEGER  pTextLength ) {

	

    wchar_t*      s;
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLPrepareW called on: %d", pStmt ) );
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );
    
    // precaution
    if ( pStmtText == NULL || ( pTextLength <= 0 && pTextLength != SQL_NTS ) ) {
        __ODBCPOPMSG ( _ODBCPopMsg ( "SQLPrepare - bad params" ) );
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "SQLPrepare", "01000", -1, "SQLPrepare - bad params" );
        return SQL_ERROR;
    }
    
    // MANAGE STMT CONTENT
    
    // convert to full request, with zero termination ( as well as params - later )
    if ( ProcessStmtForParams ( ( pODBCStmt ) pStmt, pStmtText, pTextLength, s ) == BAD )
    { return SQL_ERROR; }
    
    // release existing stmt contents
    SQLFreeStmt ( pStmt, SQL_CLOSE );
    // replace with new stmt string
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The query being prepared is :" ) );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, s ) );
    ( ( pODBCStmt ) pStmt )->Stmt = s;
    ( ( pODBCStmt ) pStmt )->StmtLen = pTextLength;
    // MARK as prepared
    // set the flag
    ( ( pODBCStmt ) pStmt )->Prepared = 1;


	return _SQLExecStmtFromReq((pODBCStmt)pStmt, 1);

    //return SQL_SUCCESS;
}


// -----------------------------------------------------------------------
// to execute a prepared statement
// -----------------------------------------------------------------------

RETCODE SQL_API SQLExecute ( HSTMT pStmt ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLExecute called on: %d", pStmt ) );
    Word        x;
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );
    
    // check if prepared
    if ( ( ( pODBCStmt ) pStmt )->Prepared != 1 ) {
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "SQLExecute", "01000", -1, "No prepared stmt" );
        return SQL_ERROR;
    }
    
    // excute the request
    x  = _SQLExecStmtFromReq ( ( pODBCStmt ) pStmt, 0 );
    return x;
}

// -----------------------------------------------------------------------
// to execute a sql statement directly
// -----------------------------------------------------------------------

RETCODE SQL_API SQLExecDirect ( SQLHSTMT    pStmt,
                                SQLCHAR*    pStmtText,
                                SQLINTEGER  pTextLength ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLExecDirect called, strlen is %d, pTextLength is %d",
                              strlen ( ( char* ) pStmtText ), pTextLength ) );
    unique_ptr<wchar_t[]> pStmtTextW ( char2wchar ( ( char* ) pStmtText ) );
    return SQLExecDirectW ( pStmt, ( SQLWCHAR* ) pStmtTextW.get(), pTextLength );
}

RETCODE SQL_API SQLExecDirectW ( SQLHSTMT    pStmt,
                                 SQLWCHAR*    pStmtText,
                                 SQLINTEGER  pTextLength ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLExecDirectW called on: %d", pStmt ) );
    wchar_t*      s;
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );
    
    // precaution
    if ( pStmtText == NULL || ( pTextLength <= 0 && pTextLength != SQL_NTS ) ) {
        __ODBCPOPMSG ( _ODBCPopMsg ( "SQLExecDirect - bad params" ) );
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "SQLExecDirectW", "01000", -1, "bad params" );
        return SQL_ERROR;
    }
    
    // convert to full request, with zero termination ( as well as params - later )
    if ( ProcessStmtForParams ( ( pODBCStmt ) pStmt, ( wchar_t* ) pStmtText, pTextLength, s ) == BAD )
    { return SQL_ERROR; }
    
    // release existing stmt contents
    SQLFreeStmt ( pStmt, SQL_CLOSE );
    // replace with new stmt string
    ( ( pODBCStmt ) pStmt )->Stmt = s;
    ( ( pODBCStmt ) pStmt )->StmtLen = pTextLength;
    // mark it as prepared
    ( ( pODBCStmt ) pStmt )->Prepared = 1;
    // execute
    RETCODE code = SQLExecute ( pStmt );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLExecDirectW exited on with %d", code ) );
    return code;
}

// ----------------------------------------------------------------------
// to cancel any asynchronous processing, or pending data request
// ----------------------------------------------------------------------

RETCODE SQL_API SQLCancel ( HSTMT pStmt ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, "SQLCancel called" ) );
    //if ( pStmt )
    //    // clear all previous diag info
    //{ _SQLFreeDiag ( & ( ( ( pODBCStmt ) pStmt )->Diag ) ); }
    return SQL_SUCCESS;
}


// ----------------------------------------------------------------------
// to commit/rollback all stmts on connection or connections on env
// ----------------------------------------------------------------------

RETCODE SQL_API SQLEndTran ( SQLSMALLINT     pHandleType,
                             SQLHANDLE       pHandle,
                             SQLSMALLINT     pCompletionType ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLEndTran called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLEndTran not implemented" ) );
    return SQL_ERROR;
}

// ----------------------------------------------------------------------
// to set cursor position in a rowset, allows an appl to refresh/update/delete data in rowset
// ----------------------------------------------------------------------

RETCODE SQL_API SQLSetPos ( SQLHSTMT        pStmt,
                            SQLUSMALLINT    pRowNumber,
                            SQLUSMALLINT    pOperation,
                            SQLUSMALLINT    pLockType ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLSetPos called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLSetPos not implemented" ) );
    return SQL_ERROR;
}

// ----------------------------------------------------------------------
// to close a cursor that has been opened on a statement and discards pending results.
// ----------------------------------------------------------------------

RETCODE SQL_API SQLCloseCursor ( SQLHSTMT pStmt ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLCloseCursor called" ) );
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );
    
    // check if there is some result
    if (
        ( ( pODBCStmt ) pStmt )->IRD.RowDesc != NULL ) {
        // free the results
        return SQLFreeStmt ( pStmt, SQL_CLOSE );
    }
    
    else {
        // error condition
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "SQLCloseCursor", "24000", -1, "Invalid cursor state" );
        return SQL_ERROR;
    }
}

// ----------------------------------------------------------------------
// to perform bulk insertions & bulk bookmark operations, like update, delete & fetch by bookmark.
// ----------------------------------------------------------------------

RETCODE SQL_API SQLBulkOperations ( SQLHSTMT        pStmt,
                                    SQLUSMALLINT    pOperation ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLBulkOperations called\n" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLBulkOperations not implemented" ) );
    return SQL_ERROR;
}
