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
// File:    KO_FETCH.CPP
//
// Purpose:     Contains function for fetching results.
//              As explained in the article ODBC has a
//              defined way in which the data is made available.
//
//              1. The client CAN obtain general details
//              about the result like num of rows etc using
//              SQLRowCount, SQLNumResultCols.
//
//              2. It obtains metadata about the result
//              more specifically info about columns like
//              size type etc using SQLColAttribute or
//              SQLDescribeCol.
//
//              3. Then it allocates and bind the buffer
//              specifying the other info like buffer size
//              etc for the cols it desires to extract using
//              SQLBindCol
//
//              4. Then it instructs the driver to feed these
//              buffers using SQLFetch or SQLExtendedFetch.
//
//              5. This is the general way. In case the data
//              involved is quite large, it can be extracted
//              piecemeal using SQLGetData. SQLPutData is the
//              funcion corresponding to SQLGetData to specify
//              large param values say storing images etc.
//              Since I have not implemented params or long
//              data read write in this sample, SQLPutData resides
//              in this file with SQLGetData.
//
//              All fetch occur via the local function _SQLFetch.
//              Internally the fetch is executed in the following
//              way.
//              1. The rowdesc from server (IRD) and appl (ARD )
//                 is obtained.
//              2. Main loop to fetch rowset number of rows. More
//                 than one row can be fetched at a time. The movement
//                 in resulset is done using _SQLFetchMoveNext.
//              3. For each row, loop through each ARD item to
//                 extract the col(s) required by the client.
//
//              SQLColConvert and associated funtions _SQLCopyCharData,
//              SQLCopyNumData, _SQLCopyDateTimeData r used to perform
//              necessary conversion between data as recived from the
//              server and data as required by the client.
//
// Exported functions:
//                       SQLColAttribute
//                       SQLDescribeCol
//                       SQLBindCol
//                       SQLNumResultCols
//                       SQLRowCount
//                       SQLFetch
//                       SQLExtendedFetch
//                       SQLFetchScroll
//                       SQLGetData
//                       SQLPutData
//                       SQLMoreResults
//                       SQLNativeSql
//
// ----------------------------------------------------------------------------
#include "stdafx.h"

#include "Dump.h"

// ------------------------- local functions -----------------------------
RETCODE SQL_API _SQLFetch ( pODBCStmt pStmt, Word pFetchOrientation, Long pFetchOffset, ULong* pRowCountPtr,
                            UWord* pRowStatusArray );
RETCODE SQL_API _SQLColConvert ( pODBCStmt pStmt, void* pTgtDataPtr, Long* pTgtDataSizePtr, CStrPtr pSrcColData,
                                 pARDItem pARDCol, bool isSigned );
RETCODE SQL_API _SQLFetchMoveNext ( pODBCStmt pStmt );
RETCODE SQL_API _SQLResetRowPos ( pODBCStmt pStmt );

SQLRowContent* GetIfExist ( std::vector <SQLRowContent*>& container, int index );

// -----------------------------------------------------------------------
// to get specific detail//attribute about a col returned from server --- FROM IRD
// kylin specific
// -----------------------------------------------------------------------


RETCODE _SQLColAttribute_basic ( SQLHSTMT pStmt,
                                 SQLUSMALLINT pColNum,
                                 SQLUSMALLINT pFldID,
                                 SQLPOINTER pDataPtr,
                                 SQLSMALLINT pDataSize,
                                 SQLSMALLINT* pDataSizePtr, // in bytes
                                 SQLPOINTER pNumValuePtr,// integer
                                 bool isANSI
)
{ //if returned data is numeric, feed this
    Long n;
    SQLResponse* ird;
    pIRDItem col;
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    // free diags
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );

    // precaution
    if ( pColNum == 0 )
    {
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "SQLColAttribute", "01000", -1, "bad params" );
        return SQL_ERROR;
    }

    // get the row descriptor obtained with response
    ird = ( ( ( pODBCStmt ) pStmt ) -> IRD ) . RowDesc . get ();

    // check
    if ( ird == NULL )
    {
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "SQLColAttribute", "01000", -1, "No resultset or no col descriptors" );
        return SQL_ERROR;
    }

    // find the xth element/col
    col = _SQLGetIRDItem ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), pColNum );

    // check
    if ( col == NULL )
    {
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "SQLColAttribute", "01000", -1, "Invalid col num" );
        return SQL_ERROR;
    }

    // get value from descriptor as per field type
    switch ( pFldID )
    {
            // numeric types clubbed together
        case SQL_DESC_AUTO_UNIQUE_VALUE : // is col auto-incrementing
        case SQL_DESC_CASE_SENSITIVE : // is col case-insensitive
        case SQL_DESC_TYPE : // verbose type
        case SQL_DESC_CONCISE_TYPE : // concise type
        case SQL_DESC_COUNT : // no.of highest bound column
        case SQL_DESC_LENGTH :
        case SQL_DESC_DISPLAY_SIZE :
        case SQL_DESC_OCTET_LENGTH :
        case SQL_DESC_FIXED_PREC_SCALE :
        case SQL_DESC_NULLABLE :
        case SQL_DESC_NUM_PREC_RADIX :
        case SQL_DESC_PRECISION :
        case SQL_DESC_SCALE :
        case SQL_DESC_SEARCHABLE :
        case SQL_DESC_UNNAMED :
        case SQL_DESC_UNSIGNED :
        case SQL_DESC_UPDATABLE :
            // added for Excel
        case SQL_COLUMN_LENGTH :
        case SQL_COLUMN_PRECISION :
        case SQL_COLUMN_SCALE :
            _SQLGetIRDItemField ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), col, pColNum, pFldID, pNumValuePtr, -1, NULL, isANSI );
            break;

            // char types clubbed together

        case SQL_DESC_BASE_TABLE_NAME : // table name for column
        case SQL_DESC_CATALOG_NAME : // database name
        case SQL_DESC_LITERAL_PREFIX :
        case SQL_DESC_LITERAL_SUFFIX :
        case SQL_DESC_LOCAL_TYPE_NAME :
        case SQL_DESC_TYPE_NAME :
        case SQL_DESC_SCHEMA_NAME :
        case SQL_DESC_TABLE_NAME :
            _SQLGetIRDItemField ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), col, pColNum, pFldID, pDataPtr, pDataSize,
                                  pDataSizePtr ? &n : NULL, isANSI );

            if ( pDataSizePtr )
            {
                *pDataSizePtr = ( Word ) n;
            }

            break;

        case SQL_DESC_BASE_COLUMN_NAME :
        case SQL_DESC_LABEL :
        case SQL_DESC_NAME :
            // ////
            // as a special case the name length may be required without the actual name
            //////
            StrPtr cname;
            Word cnamesize;

            if ( pDataPtr )
            {
                cname = ( StrPtr ) pDataPtr;
                cnamesize = pDataSize;
            }

            else
            {
                cname = new Char[256]; // arbitary
                cnamesize = 255;
            }

            _SQLGetIRDItemField ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), col, pColNum, pFldID, cname, cnamesize,
                                  pDataSizePtr ? &n : NULL, isANSI );

            if ( pDataPtr == NULL )
            {
                delete[] cname;
            }

            if ( pDataSizePtr )
            {
                *pDataSizePtr = ( Word ) n;
            }

            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQLColAttribute unknown attr, ColNum: %d, FldID: %d\n", pColNum, pFldID ) );
            return SQL_ERROR;
    }
    //unique_ptr<char[]> temp ( wchar2char ( ( wchar_t* ) pDataPtr ) );
    //__ODBCLOG(_ODBCLogMsg(LogLevel_INFO, "_SQLColAttribute_basic was called - Stmt:%d, ColNum:%d, FldId:%d, pDataStr:%s, pDataSize:%d, pDataSizePtr:%d(n:%d), pNumValPtr:%d",
    //	pStmt, pColNum, pFldID, temp.get(), pDataSize, pDataSizePtr, n, pNumValuePtr ? *(( Long* )pNumValuePtr):-1));
    return SQL_SUCCESS;
}


#ifdef _WIN64
RETCODE SQL_API SQLColAttributeW ( SQLHSTMT        pStmt,
                                   SQLUSMALLINT    pColNum,
                                   SQLUSMALLINT    pFldID,
                                   SQLPOINTER      pDataPtr,
                                   SQLSMALLINT     pDataSize,
                                   SQLSMALLINT*    pDataSizePtr,
                                   SQLLEN*     pNumValuePtr ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLColAttributeW called, ColNum: %d, FldID: %d", pColNum, pFldID ) );
    RETCODE code =  _SQLColAttribute_basic ( pStmt, pColNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, pNumValuePtr,
                                             false );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "the return code is %d", code ) );
    return code;
}

RETCODE SQL_API SQLColAttribute ( SQLHSTMT        pStmt,
                                  SQLUSMALLINT    pColNum,
                                  SQLUSMALLINT    pFldID,
                                  SQLPOINTER      pDataPtr,
                                  SQLSMALLINT     pDataSize,
                                  SQLSMALLINT*    pDataSizePtr,
                                  SQLLEN*      pNumValuePtr ) {
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLColAttributeW called, ColNum: %d, FldID: %d", pColNum, pFldID ) );
    RETCODE code =  _SQLColAttribute_basic ( pStmt, pColNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, pNumValuePtr,
                                             true );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "the return code is %d", code ) );
    return code;
}

#else
RETCODE SQL_API SQLColAttributeW ( SQLHSTMT pStmt,
                                   SQLUSMALLINT pColNum,
                                   SQLUSMALLINT pFldID,
                                   SQLPOINTER pDataPtr,
                                   SQLSMALLINT pDataSize,
                                   SQLSMALLINT* pDataSizePtr,
                                   SQLPOINTER pNumValuePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLColAttributeW called, ColNum: %d, FldID: %d", pColNum, pFldID ) );
    RETCODE code = _SQLColAttribute_basic ( pStmt, pColNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, pNumValuePtr,
                                            false );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "the return code is %d", code ) );
    return code;
}

RETCODE SQL_API SQLColAttribute ( SQLHSTMT pStmt,
                                  SQLUSMALLINT pColNum,
                                  SQLUSMALLINT pFldID,
                                  SQLPOINTER pDataPtr,
                                  SQLSMALLINT pDataSize,
                                  SQLSMALLINT* pDataSizePtr,
                                  SQLPOINTER pNumValuePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLColAttributeW called, ColNum: %d, FldID: %d", pColNum, pFldID ) );
    return _SQLColAttribute_basic ( pStmt, pColNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, pNumValuePtr, true );
}
#endif

// ----------------------------------------------------------------------
// to get the basic set of ARD col attributes, ie details recd. from server --- FROM IRD
// SQLDescribeCol returns the result descriptor �� column name,type, column size, decimal digits, and nullability (from msdn)
// kylin specific
// ----------------------------------------------------------------------

SQLRETURN SQL_API _SQLDescribeCol_basic ( SQLHSTMT pStmt,
                                          SQLUSMALLINT pColNum,
                                          void* pColNamePtr,
                                          SQLSMALLINT pColNameSize,
                                          SQLSMALLINT* pColNameSizePtr,
                                          SQLSMALLINT* pDataTypePtr,
                                          SQLULEN* pColSizePtr,
                                          SQLSMALLINT* pDecimalDigitsPtr,
                                          SQLSMALLINT* pNullablePtr,
                                          bool isANSI
)
{
    Long n;
    SQLResponse* ird;
    pIRDItem col;
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    // free diags
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );

    // precaution
    if ( pColNum == 0 )
    {
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "SQLDescribeCol", "01000", -1, "bad params" );
        return SQL_ERROR;
    }

    // get the row descriptor obtained with response
    ird = ( ( ( pODBCStmt ) pStmt ) -> IRD ) . RowDesc . get ();

    // check
    if ( ird == NULL )
    {
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "SQLDescribeCol", "01000", -1, "No resultset or no col descriptors" );
        return SQL_ERROR;
    }

    // find the xth element/col
    col = _SQLGetIRDItem ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), pColNum );

    // check
    if ( col == NULL )
    {
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "SQLDescribeCol", "01000", -1, "Invalid col num" );
        return SQL_ERROR;
    }

    // COL-NAME ie title
    if ( pColNamePtr )
    {
        _SQLGetIRDItemField ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), col, pColNum, SQL_DESC_BASE_COLUMN_NAME, pColNamePtr,
                              pColNameSize, pColNameSizePtr ? &n : NULL, isANSI );

        // here should return length of characters
        if ( pColNameSizePtr )
        {
            if ( isANSI )
            {
                *pColNameSizePtr = ( Word ) n;
            }
            else
            {
                *pColNameSizePtr = ( Word ) ( n / 2 );
            }
        }

        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, (wchar_t*)pColNamePtr ) );
    }

    // COL-DATA TYPE

    if ( pDataTypePtr )
    {
        _SQLGetIRDItemField ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), col, pColNum, SQL_DESC_CONCISE_TYPE, pDataTypePtr, -1,
                              NULL, isANSI );
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "data type: %d", *pDataTypePtr ) );
    }

    // COL-SIZE

    if ( pColSizePtr )
    {
        _SQLGetIRDItemField ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), col, pColNum, SQL_DESC_LENGTH, pColSizePtr, -1, NULL,
                              isANSI );
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "column size: %d", *pColSizePtr ) );
    }

    // COL-DECIMAL

    if ( pDecimalDigitsPtr )
    {
        _SQLGetIRDItemField ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), col, pColNum, SQL_DESC_SCALE, pDecimalDigitsPtr, -1, NULL,
                              isANSI );
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "decimal scale: %d", *pDecimalDigitsPtr ) );
    }

    // COL-NULLABLE

    if ( pNullablePtr )
    {
        _SQLGetIRDItemField ( & ( ( ( pODBCStmt ) pStmt ) -> IRD ), col, pColNum, SQL_DESC_NULLABLE, pNullablePtr, -1, NULL,
                              isANSI );
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "nullable: %d", *pNullablePtr ) );
    }

    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLDescribeCol returned" ) );
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLDescribeColW ( SQLHSTMT pStmt,
                                    SQLUSMALLINT pColNum,
                                    SQLWCHAR* pColNamePtr,
                                    SQLSMALLINT pColNameSize,
                                    SQLSMALLINT* pColNameSizePtr,
                                    SQLSMALLINT* pDataTypePtr,
                                    SQLULEN* pColSizePtr,
                                    SQLSMALLINT* pDecimalDigitsPtr,
                                    SQLSMALLINT* pNullablePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "SQLDescribeColW. Col: %d, ColNamePtr: %d, ColNameSize: %d, ColNameSizePtr: %d, DataTypePtr: %d, ColSizePtr: %d, DecDigitsPtr: %d, NullPtr: %d",
        pColNum, pColNamePtr, pColNameSize, pColNameSizePtr, pDataTypePtr, pColSizePtr, pDecimalDigitsPtr, pNullablePtr ) );
    return _SQLDescribeCol_basic ( pStmt, pColNum, pColNamePtr, pColNameSize, pColNameSizePtr, pDataTypePtr, pColSizePtr,
                                   pDecimalDigitsPtr, pNullablePtr, false );
}

SQLRETURN SQL_API SQLDescribeCol ( SQLHSTMT pStmt,
                                   SQLUSMALLINT pColNum,
                                   SQLCHAR* pColNamePtr,
                                   SQLSMALLINT pColNameSize,
                                   SQLSMALLINT* pColNameSizePtr,
                                   SQLSMALLINT* pDataTypePtr,
                                   SQLULEN* pColSizePtr,
                                   SQLSMALLINT* pDecimalDigitsPtr,
                                   SQLSMALLINT* pNullablePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "SQLDescribeCol. Col: %d, ColNamePtr: %d, ColNameSize: %d, ColNameSizePtr: %d, DataTypePtr: %d, ColSizePtr: %d, DecDigitsPtr: %d, NullPtr: %d",
        pColNum, pColNamePtr, pColNameSize, pColNameSizePtr, pDataTypePtr, pColSizePtr, pDecimalDigitsPtr, pNullablePtr ) );
    return _SQLDescribeCol_basic ( pStmt, pColNum, pColNamePtr, pColNameSize, pColNameSizePtr, pDataTypePtr, pColSizePtr,
                                   pDecimalDigitsPtr, pNullablePtr, true );
}

// -----------------------------------------------------------------------
// to bind a column to with details from application --- TO ARD
// kylin specific, no change required
// -----------------------------------------------------------------------

/*

    From msdn:

    SQLRETURN SQLBindCol(
    SQLHSTMT       StatementHandle,
    SQLUSMALLINT   ColumnNumber,
    SQLSMALLINT    TargetType,
    SQLPOINTER     TargetValuePtr,
    SQLLEN         BufferLength,
    SQLLEN *       StrLen_or_Ind);

*/
RETCODE SQL_API SQLBindCol ( SQLHSTMT pStmt,
                             SQLUSMALLINT pColNum,
                             SQLSMALLINT pDataType,
                             SQLPOINTER pDataPtr,
                             SQLLEN pDataSize,
                             SQLLEN* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLBindCol called, ColNum: %d, TgtType: %d, ValuePtr: %d, Capacity: %d",
        pColNum, pDataType, pDataPtr, pDataSize ) );
    pODBCARD ard; // application row descriptor
    pARDItem ardcol; // application row descriptor item
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    // free diags
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );
    // extract the appl. row descriptor from stmt
    ard = & ( ( ( pODBCStmt ) pStmt ) -> ARD );
    // get the specified column if already bound
    ardcol = _SQLGetARDItem ( ard, pColNum );

    // EXISTS

    if ( ardcol != NULL )
    {
        // check if total unbind is required
        if ( pDataPtr == NULL && pDataSizePtr == NULL )
        {
            // detach it from ARD link list
            _SQLDetachARDItem ( ard, ardcol );
            // free
            delete ardcol;
        }

        else
        {
            // unbind/rebind col details
            _SQLSetARDItemField ( ard, ardcol, pColNum, SQL_DESC_DATA_PTR, pDataPtr, -1 );
            _SQLSetARDItemField ( ard, ardcol, pColNum, SQL_DESC_CONCISE_TYPE, ( void* ) pDataType, -1 );
            _SQLSetARDItemField ( ard, ardcol, pColNum, SQL_DESC_LENGTH, ( void* ) pDataSize, -1 );
            _SQLSetARDItemField ( ard, ardcol, pColNum, SQL_DESC_OCTET_LENGTH_PTR, pDataSizePtr, -1 );
            // reset the source data type
            ardcol -> SrcDataType = 0;
        }

        return SQL_SUCCESS;
    }

    // DOES NOT EXIST

    // check for bad params
    if ( pDataPtr == NULL && pDataSizePtr == NULL )
    {
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "SQLBindCol", "01000", -1, "Bad params" );
        return SQL_ERROR;
    }

    // check for bad params
    else if ( pDataSize < 0 )
    {
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "SQLBindCol", "01000", -1, "Invalid buffer length" );
        return SQL_ERROR;
    }

    // CREATE
    // allocate a new col-item
    ardcol = new ARDItem;
    // reset
    _SQLSetARDItemFieldsDefault ( ardcol, pColNum );

    // set all values - bind
    _SQLSetARDItemField ( ard, ardcol, pColNum, SQL_DESC_DATA_PTR, pDataPtr, -1 );
    _SQLSetARDItemField ( ard, ardcol, pColNum, SQL_DESC_CONCISE_TYPE, ( void* ) pDataType, -1 );
    _SQLSetARDItemField ( ard, ardcol, pColNum, SQL_DESC_LENGTH, ( void* ) pDataSize, -1 );
    _SQLSetARDItemField ( ard, ardcol, pColNum, SQL_DESC_OCTET_LENGTH_PTR, pDataSizePtr, -1 );
    // attach it to link list
    _SQLAttachARDItem ( ard, ardcol );
    return SQL_SUCCESS;
}


// ---------------------------------------------------------------------
// to get the number of columns in result --- COUNTING ELEMENTS IN IRD.ROWDESC
// Kylin specific
// ---------------------------------------------------------------------

RETCODE SQL_API SQLNumResultCols ( SQLHSTMT pStmt, SQLSMALLINT* pColCountPtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLNumResultCols called" ) );
    SQLResponse* rowdesc;
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    // free diags
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );
    // caller safe
    * ( ( SQLSMALLINT* ) pColCountPtr ) = 0;
    // get the row desciptor
    rowdesc = ( ( pODBCStmt ) pStmt ) -> IRD . RowDesc . get ();

    if ( rowdesc == NULL )
    {
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "SQLNumResultCols", "01000", -1, "no resultset or IRD" );
        return SQL_ERROR;
    }

    // count the number of columns
    * ( ( SQLSMALLINT* ) pColCountPtr ) = ( SQLSMALLINT ) ( rowdesc -> columnMetas . size () );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLNumResultCols called returned: %d",
        * ( ( SQLSMALLINT* ) pColCountPtr ) ) );
    return SQL_SUCCESS;
}


// ----------------------------------------------------------------------
// to count the number of rows in the current result --- COUNTING ELEMENTS IN IRD
// ----------------------------------------------------------------------

RETCODE SQL_API SQLRowCount ( HSTMT pStmt, SQLLEN* pDataPtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLRowCount called" ) );
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    // free diags
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );
    *pDataPtr = ( ( pODBCStmt ) pStmt ) -> RowCount;
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLRowCount returned: %d", *pDataPtr ) );
    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to return the next row from the resultset
// -----------------------------------------------------------------------

RETCODE SQL_API SQLFetch ( HSTMT pStmt )
{
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    // free diags
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );
    // all fetch occur thru the local function _SQLFetch
    /*
        RETCODE SQL_API _SQLFetch ( pODBCStmt    pStmt,
        Word            pFetchOrientation,
        Long            pFetchOffset,
        ULong*          pRowCountPtr,
        UWord*          pRowStatusArray )
    */
    RETCODE ret = _SQLFetch ( ( pODBCStmt ) pStmt, SQL_FETCH_NEXT,
                              ( ( pODBCStmt ) pStmt ) -> ARD . RowArraySize > 0 ? ( ( pODBCStmt ) pStmt ) -> ARD . RowArraySize : 1,
                              ( ( pODBCStmt ) pStmt ) -> IRD . RowsProcessedPtr, ( ( pODBCStmt ) pStmt ) -> IRD . ArrayStatusPtr );

    if ( ret == SQL_NO_DATA )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, "Last row of current query has been fetched" ) );
    }

    return ret;
}

// -----------------------------------------------------------------------
// to fetch the specified rowset of data from the result set
// Version Introduced: ODBC 1.0 Standards Compliance: Deprecated  (from msdn)
// -----------------------------------------------------------------------

RETCODE SQL_API SQLExtendedFetch ( SQLHSTMT pStmt,
                                   SQLUSMALLINT pFetchOrientation,
                                   SQLINTEGER pFetchOffset,
                                   SQLUINTEGER* pRowCountPtr,
                                   SQLUSMALLINT* pRowStatusArray )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "SQLExtendedFetch called, Stmt: %d, FO: %d, Offset: %d, Rcount: %d, RowStatus: %d", pStmt, pFetchOrientation,
        pFetchOffset, pRowCountPtr, pRowStatusArray ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLExtendedFetch is not implemented   " ) );
    return SQL_ERROR;
    Long n;
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );
    // free diags
    _SQLFreeDiag ( _DIAGSTMT ( pStmt ) );

    // only fetch next supported
    if ( pFetchOrientation != SQL_FETCH_NEXT )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "SQLExtendedFetch option not supported, FetchOrientation: %d", pFetchOrientation ) );
        return SQL_ERROR;
    }

    // check if number of rows explicitly specified
    if ( pFetchOffset <= 0 )
    {
        n = ( ( pODBCStmt ) pStmt ) -> ARD . RowArraySize;
    }

    // use default rowset size as a fallback
    if ( n <= 0 )
    {
        n = 1;
    }

    return _SQLFetch ( ( pODBCStmt ) pStmt, pFetchOrientation, n, pRowCountPtr, pRowStatusArray );
}


// -----------------------------------------------------------------------
// to fetch the specified rowset of data from the result set
// -----------------------------------------------------------------------

RETCODE SQL_API SQLFetchScroll ( SQLHSTMT pStatementHandle,
                                 SQLSMALLINT pFetchOrientation,
                                 SQLINTEGER pFetchOffset )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLFetchScroll called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLFetchScroll not implemented" ) );
    return SQL_ERROR;
}


// -----------------------------------------------------------------------
// to send data for a parameter or column to the driver at statement execution time
// -----------------------------------------------------------------------

RETCODE SQL_API SQLPutData ( SQLHSTMT pStmt,
                             SQLPOINTER pDataPtr,
                             SQLINTEGER pDataSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLPutData called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLPutData not implemented" ) );
    return SQL_ERROR;
}

// -----------------------------------------------------------------------
// to iterate through multiple resultsets
// -----------------------------------------------------------------------

RETCODE SQL_API SQLMoreResults ( HSTMT pStmt )
{
    pODBCStmt odbcStmt = ( pODBCStmt )pStmt;

    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLMoreResults called, stmt handle %d with start-row %d and end-row %d, next handle is %d", 
        (long) odbcStmt, odbcStmt->CurRowsetStartRowPos, odbcStmt->CurRowsetEndRowPos, (long) odbcStmt -> Next) );

    // check if stmt been released already
    if (( ! odbcStmt -> Prepared) || ( ! odbcStmt -> Next ))
    {
	return SQL_NO_DATA;
    }
    else if ( odbcStmt -> IRD . RowDesc != NULL )
    {
        // ------- THIS CASE SHOULD NOT OCCUR ----------
        // check if position is currently unknown
        if ( odbcStmt -> CurRowsetStartRow == NULL && odbcStmt -> CurRowsetStartRowPos == 0 )
        {
            // position to first row ( both the pointers )
            if ( GetIfExist ( odbcStmt -> IRD . RowDesc -> results, 1 ) )
            {
                return SQL_SUCCESS;
            }
        }
        // -----------------------------------------------
        // position to next row if already position is known
        else if ( odbcStmt -> CurRowsetEndRow != NULL )
        {
            // position to next row
            if ( GetIfExist ( odbcStmt -> IRD . RowDesc -> results, odbcStmt -> CurRowsetEndRowPos ) )
            {
                return SQL_SUCCESS;
            }
        }
        return SQL_NO_DATA;
    }
    return SQL_ERROR;
}

// -----------------------------------------------------------------------
// to get a driver specific version of specified sql statement
// -----------------------------------------------------------------------

RETCODE SQL_API SQLNativeSql ( SQLHDBC pConn,
                               SQLCHAR* pInStmtText,
                               SQLINTEGER pInStmtTextLen,
                               SQLCHAR* pOutStmtText,
                               SQLINTEGER pOutStmtTextLen,
                               SQLINTEGER* pOutStmtTextLenPtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLNativeSql called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLNativeSql not implemented" ) );
    return SQL_ERROR;
}


// -----------------------------------------------------------------------
// to convert and transfer col data for application
// -----------------------------------------------------------------------

//mhb TODO, check if the sqltype defined here match from c#
RETCODE SQL_API _SQLColConvert ( pODBCStmt pStmt,
                                 void* pTgtDataPtr,
                                 Long* pTgtDataSizePtr,
                                 const wchar_t* pSrcColData,
                                 pARDItem pARDCol,
                                 bool isSigned )
{
    //check out this for SQL data type to C data type mapping
    //http://msdn.microsoft.com/en-us/library/ms714556(v=vs.85).aspx
    // note
    // this function actually determines the conversion
    // required to transfer the data
    Word pSrcDataType = pARDCol -> SrcDataType;
    Word pTgtDataType = pARDCol -> DataConciseType;
    Long pTgtDataSize = pARDCol -> DataSize;
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLColConvert called" ) );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The SrcDataType is %d, the TgtDataType is %d, the TgtDataSize is %d",
        pSrcDataType, pTgtDataType, pTgtDataSize ) );

    // TARGET TYPE IS LEFT TO OUR DRIVER
    // check if target type is open
    if ( pTgtDataType == SQL_DEFAULT )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "pTgtDataType is SQL_DEFAULT, use default type mapping." ) );

        // determine targettype based on data-source type
        // check out this http://msdn.microsoft.com/en-us/library/ms716298(v=vs.85).aspx for default type mapping
        switch ( pSrcDataType )
        {
            case SQL_CHAR :
                pTgtDataType = SQL_C_CHAR ;
                break;

            case SQL_VARCHAR :
                pTgtDataType = SQL_C_CHAR ;
                break;

            case SQL_WCHAR :
                pTgtDataType = SQL_C_WCHAR;
                break;

            case SQL_WVARCHAR :
                pTgtDataType = SQL_C_WCHAR;
                break;

            case SQL_DECIMAL :
                pTgtDataType = SQL_C_CHAR ;
                break;

            case SQL_BIT :
                pTgtDataType = SQL_C_BIT;
                break;

            case SQL_TINYINT :
                if ( isSigned )
                {
                    pTgtDataType = SQL_C_STINYINT ;
                }

                else
                {
                    pTgtDataType = SQL_C_UTINYINT ;
                }

                break;

            case SQL_SMALLINT :
                if ( isSigned )
                {
                    pTgtDataType = SQL_C_SSHORT ;
                }

                else
                {
                    pTgtDataType = SQL_C_USHORT ;
                }

                break;

            case SQL_INTEGER :
                if ( isSigned )
                {
                    pTgtDataType = SQL_C_SLONG ;
                }

                else
                {
                    pTgtDataType = SQL_C_ULONG ;
                }

                break;

            case SQL_BIGINT :
                if ( isSigned )
                {
                    pTgtDataType = SQL_C_SBIGINT ;
                }

                else
                {
                    pTgtDataType = SQL_C_UBIGINT ;
                }

                break;

            case SQL_FLOAT :
                pTgtDataType = SQL_C_FLOAT ;
                break;

            case SQL_DOUBLE :
                pTgtDataType = SQL_C_DOUBLE ;
                break;

            case SQL_TYPE_DATE :
                pTgtDataType = SQL_C_CHAR ;
                break;

            case SQL_TYPE_TIME :
                pTgtDataType = SQL_C_CHAR ;
                break;

            case SQL_TYPE_TIMESTAMP :
                pTgtDataType = SQL_C_CHAR ;
                break;

                //case SQL_C_SLONG:
                //case SQL_C_ULONG:               // unsigned long
                //case SQL_C_USHORT:
                //case SQL_C_SSHORT:
                //case SQL_NUMERIC:
                //case SQL_REAL:
                //  pTgtDataType = pSrcDataType;
                //  break;

            default :
                __ODBCPOPMSG ( _ODBCPopMsg ( "The data type %d not implemented", pSrcDataType ) );
                return SQL_ERROR;
                break;
        }
    }

    else
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "pTgtDataType is NOT SQL_DEFAULT, it is %d", pTgtDataType ) );
    }

    // TARGET TYPE IS CHAR
    // as an optimization, check if the application
    // or target data type is char. since the data from
    // server is already in char format. the data can
    // easily be transferred without incurring any
    // conversion overhead
    unique_ptr <char[]> pTextInAnsi ( wchar2char ( pSrcColData ) );

    // check if char type
    if ( pTgtDataType == SQL_CHAR || pTgtDataType == SQL_VARCHAR )
    {
        // only in case of src data being bool a conversion is required
        if ( pSrcDataType == SQL_BIT )
        {
            // prepare a converted single char bool string
            Char src[2];

            if ( pTextInAnsi . get () == NULL )
            {
                src[0] = '0';
            }

            else
            {
                src[0] = ( pTextInAnsi . get ()[0] == 'T' || pTextInAnsi . get ()[0] == '1' || pTextInAnsi . get ()[0] == 't' ) ? '1' : '0';
            }

            src[1] = 0;
            // transfer the bool string
            return _SQLCopyCharData ( _DIAGSTMT ( pStmt ), pTgtDataPtr, pARDCol -> DataSize, pTgtDataSizePtr, 32, src, -1 );
        }

        else
        {
            // transfer the string as it is
            return _SQLCopyCharData ( _DIAGSTMT ( pStmt ), pTgtDataPtr, pARDCol -> DataSize, pTgtDataSizePtr, 32, pTextInAnsi . get (),
                                                         -1 );
        }
    }
    else if ( pTgtDataType == SQL_WCHAR || pTgtDataType == SQL_WVARCHAR )
    {
        return _SQLCopyWCharDataW ( _DIAGSTMT ( pStmt ), pTgtDataPtr, pARDCol -> DataSize, pTgtDataSizePtr, 32, pSrcColData, -1 );
    }

    // TARGET TYPE IS NOT CHAR

    // try using a numeric conversion
    switch ( _SQLCopyNumData ( _DIAGSTMT ( pStmt ), pTgtDataPtr, pTgtDataType, pTextInAnsi . get (), pSrcDataType,
                                                  pTgtDataSizePtr ) )
    {
        case -1 :
            return SQL_ERROR;

        case 0 :
            return SQL_SUCCESS;

        default :
            break;
    }

    // try using a date/time conversion
    switch ( _SQLCopyDateTimeData ( _DIAGSTMT ( pStmt ), pTgtDataPtr, pTgtDataType, pTextInAnsi . get (), pSrcDataType ) )
    {
        case -1 :
            return SQL_ERROR;

        case 0 :
            return SQL_SUCCESS;

        default :
            break;
    }

    // try using SQL_BIT data type ie bool
    if ( pTgtDataType == SQL_BIT )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "the target data type is SQL_C_BIT" ) );

        // prepare a converted single char bool string
        if ( pTextInAnsi . get () == NULL )
        {
            * ( ( char* ) pTgtDataPtr ) = 0;
        }

        else
        {
            * ( ( char* ) pTgtDataPtr ) = ( pTextInAnsi . get ()[0] == 'T' || pTextInAnsi . get ()[0] == '1' || pTextInAnsi . get ()[0] == 't' ) ? 1 : 0;
        }

        return SQL_SUCCESS;
    }

    // error condition
    __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLColConvert - Unknown data type, Target: %d, Source: %d", pTgtDataType,
        pSrcDataType ) );
    _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "_SQLColConvert", "01000", -1, "Unknown data type, Target: %d, Source: %d",
                                    pTgtDataType, pSrcDataType );
    return SQL_ERROR;
}


// -----------------------------------------------------------------------
// to get the specified column data from
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLFetchCol ( pODBCStmt pStmt,
                               pARDItem pARDCol, //ard
                               SQLResponse* pRowDesc,// ird
                               SQLRowContent* pRowData )
{ //content
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLFetchCol called" ) );
    // note
    // this function checks the binding type and positions the pointer
    // for copying the data accordingly. It takes into account the
    // current row position in rowset, the initial min increment specified
    // by client and the size of the row or col buffer
    Long i;
    Long j;
    Long* tgtsizeptr; // target size ptr
    void* tgtdataptr; // target data ptr
    const wchar_t* srcdata; // source data
    SelectedColumnMeta* coldesc;
    // COMPUTE DATA AND SIZE PTR
    // get the row pos in current rowset
    i = ( pStmt -> CurRowsetEndRowPos - pStmt -> CurRowsetStartRowPos );
    // compute min increment
    j = ( pStmt -> ARD . BindOffsetPtr ) ? * ( pStmt -> ARD . BindOffsetPtr ) : 0;

    // check the binding type
    if ( pStmt -> ARD . BindTypeOrSize != SQL_BIND_BY_COLUMN )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "ARD bindtypeorsize not euqal to SQL_BIND_BY_COLUMN" ) );
        // note
        Long k;
        // compute row-size increment
        k = ( pStmt -> ARD . BindTypeOrSize );
        // compute target col and size ptr
        tgtdataptr = ( void* ) ( ( ( Char* ) ( pARDCol -> DataPtr ) ) + j + ( i * k ) );
        tgtsizeptr = ( Long* ) ( ( ( Char* ) ( pARDCol -> SizePtr ) ) + j + ( i * k ) );
    }

    // column-wise binding
    else
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "ARD bindtypeorsize euqal to SQL_BIND_BY_COLUMN" ) );
        // move both data and size ptr in the array
        //TODO find out where the pARDCol->DataSize if set
        tgtdataptr = ( void* ) ( ( ( Char* ) ( pARDCol -> DataPtr ) ) + j + ( i * pARDCol -> DataSize ) ); // use based on data type
        tgtsizeptr = ( Long* ) ( ( ( Char* ) ( pARDCol -> SizePtr ) ) + j + ( i * sizeof ( SQLLEN) ) );
    }

    // PRECAUTION

    if ( tgtdataptr )
    {
        * ( ( Char* ) tgtdataptr ) = 0;
    }

    if ( tgtsizeptr )
    {
        * ( ( Long* ) tgtsizeptr ) = 0;
    }

    // COLLECT AND CHECK
    // get col desc for specified col ( response )
    coldesc = pRowDesc -> columnMetas . at ( pARDCol -> ColNum - 1 );
    //if ( coldesc == NULL ) {
    //  _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "_SQLFetchCol", "01000", -1, pStmt->CurRowsetEndRowPos, pARDCol->ColNum, "column not found in resultset for specified index" );
    //  return SQL_SUCCESS_WITH_INFO;                 // no col for specified index
    //}
    // get the col data for specfied col ( response )
    srcdata = pRowData -> contents . at ( pARDCol -> ColNum - 1 ) . c_str ();

    //coldata = SOAPGetChildElemX ( pRowData, pARDCol->ColNum );
    //if ( coldata == NULL ) {

    //  _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "_SQLFetchCol", "01000", -1, pStmt->CurRowsetEndRowPos, pARDCol->ColNum, "column not found in resultset for specified index" );
    //  return SQL_SUCCESS_WITH_INFO;                 // no col for specified index
    //}

    // get col value as string
    //srcdata = SOAPGetElemText ( coldata );

    // NULL DATA                                            // note: a text of NULL indicates NULL data from server

    // check if data is NULL
    if ( srcdata == NULL || _wcsicmp ( srcdata, L"NULL" ) == 0 )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "srcdata is null" ) );

        // check if a size indicator is available
        if ( tgtsizeptr == NULL )
        {
            _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "_SQLFetchCol", "22002", -1, pStmt -> CurRowsetEndRowPos, pARDCol -> ColNum,
                                            "Indicator variable required but not supplied" );
            return SQL_SUCCESS_WITH_INFO;
        }

        // set to SQL_NULL_DATA
        else
        {
            // indicate null data
            * ( ( Long* ) tgtsizeptr ) = SQL_NULL_DATA;
            // added precaution for bad appl design
            /*  if ( tgtdataptr )
                memset ( tgtdataptr, 0, pARDCol->MaxSize );*/
            return SQL_SUCCESS;
        }
    }

    // check if info about src is also available in ARD col
    if ( pARDCol -> SrcDataType == 0 )
    {
        GetIRDColDescInfo ( coldesc, & ( pARDCol -> SrcDataType ), & ( pARDCol -> SrcDataPrecision ), & ( pARDCol -> SrcDataScale ), & ( pARDCol -> SrcDataSize ) );
    } // collect source data information in form comparable to appl

    // CONVERT AND TRANSFER
    //Important!!!
    //Notice the specification of different types
    //http://publib.boulder.ibm.com/infocenter/idshelp/v10/index.jsp?topic=/com.ibm.odbc.doc/odbc72.htm
    RETCODE ret = _SQLColConvert ( pStmt, tgtdataptr, tgtsizeptr, srcdata, pARDCol, coldesc -> isSigned );
    //char buffer[1024];
    //hexDump((char*)tgtdataptr,4,buffer,false);
    //__ODBCLOG(_ODBCLogMsg(LogLevel_DEBUG,buffer));
    //hexDump((char*)tgtdataptr,4,buffer,true);
    //__ODBCLOG(_ODBCLogMsg(LogLevel_DEBUG,buffer));
    return ret;
}


// -----------------------------------------------------------------------
// to move to the next row with relevant checks
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLFetchMoveNext ( pODBCStmt pStmt )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLFetch MoveNext is called" ) );

    // check if there is some response of type resultset
    if ( pStmt -> IRD . RowDesc != NULL )
    {
        // ------- THIS CASE SHOULD NOT OCCUR ----------

        // check if position is currently unknown
        if ( pStmt -> CurRowsetStartRow == NULL && pStmt -> CurRowsetStartRowPos == 0 )
        {
            // position to first row ( both the pointers )
            pStmt -> CurRowsetStartRowPos = 1;
            pStmt -> CurRowsetStartRow = GetIfExist ( pStmt -> IRD . RowDesc -> results, 1 );
            pStmt -> CurRowsetEndRowPos = 1;
            pStmt -> CurRowsetEndRow = GetIfExist ( pStmt -> IRD . RowDesc -> results, 1 );
        }

        // -----------------------------------------------

        // position to next row if already position is known
        else if ( pStmt -> CurRowsetEndRow != NULL )
        {
            // position to next row
            pStmt -> CurRowsetEndRowPos += 1;
            pStmt -> CurRowsetEndRow = GetIfExist ( pStmt -> IRD . RowDesc -> results, pStmt -> CurRowsetEndRowPos );
        }

        // finally check if there is some data found
        if ( pStmt -> CurRowsetEndRow == NULL )
        {
            // put in diag
            _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "", "01000", -1, "SQLFetch - no data" );
            return SQL_NO_DATA;
        }

        else
        {
            return SQL_SUCCESS;
        }
    }

    else
    {
        // error situation
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "_SQLFetchMoveNext", "01000", -1, "no resultset" );
        return SQL_ERROR;
    }
}

SQLRowContent* GetIfExist ( std::vector <SQLRowContent*>& container, int index )
{
    index = index - 1; //sql cardinals start at 1

    if ( index >= ( int ) container . size () )
    {
        return NULL;
    }

    else
    {
        return container . at ( index );
    }
}

// -----------------------------------------------------------------------
// to set the initial row positions for a fetch
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLResetRowPos ( pODBCStmt pStmt )
{
    // note
    // there r 2 row pointers one is the start row for the current fetch and
    // the other is the end row after the current fetch
    // this function brings them together and moves them to the first row
    // after the cur end row
    // a block of rows which is fetched in one go is ROWSET while the full
    // result is called RESULTSET

    // check if there is some response of type resultset
    if (
        pStmt -> IRD . RowDesc != NULL )
    {
        // check if position is currently unknown
        if ( pStmt -> CurRowsetEndRow == NULL && pStmt -> CurRowsetEndRowPos == 0 )
        {
            // position to first row ( both the pointers )
            pStmt -> CurRowsetEndRowPos = 1;
            pStmt -> CurRowsetEndRow = GetIfExist ( pStmt -> IRD . RowDesc -> results, pStmt -> CurRowsetEndRowPos );
        }

        // already positioned somewhere
        else if ( pStmt -> CurRowsetEndRow != NULL )
        {
            // position to next row
            pStmt -> CurRowsetEndRowPos += 1;
            pStmt -> CurRowsetEndRow = GetIfExist ( pStmt -> IRD . RowDesc -> results, pStmt -> CurRowsetEndRowPos );
        }

        // calibrate the first row with end row
        pStmt -> CurRowsetStartRow = pStmt -> CurRowsetEndRow;
        pStmt -> CurRowsetStartRowPos = pStmt -> CurRowsetEndRowPos;

        // finally check if there is some data found
        if ( pStmt -> CurRowsetStartRow == NULL )
        {
            // put in diag
            _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "", "01000", -1, "SQLFetch - no data" );
            return SQL_NO_DATA;
        }

        else
        {
            return SQL_SUCCESS;
        }
    }

    else
    {
        // error situation
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "_SQLResetRowPos", "01000", -1, "no resultset" );
        return SQL_ERROR;
    }
}


// -----------------------------------------------------------------------
// to return the next row from the resultset
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLFetch ( pODBCStmt pStmt,
                            Word pFetchOrientation,
                            Long pFetchOffset, //ARD.RowArraySize
                            ULong* pRowCountPtr, //IRD.RowsProcessedPtr
                            UWord* pRowStatusArray )
{ //ArrayStatusPtr
    // note
    // fetchoffset is treated as the number of rows to fetch
    bool flgNoData;
    Long i, n1, n2;
    RETCODE s;
    SQLRowContent* rowdata;
    SQLResponse* rowdesc;
    pODBCARD ard;
    pARDItem ardcol;

    // CALLER SAFE

    // caller safe for row fetched
    if ( pRowCountPtr )
    {
        *pRowCountPtr = 0;
    }

    // caller safe for each row status
    if ( pRowStatusArray )
        for ( i = 0; i < pFetchOffset; i ++ )
        {
            pRowStatusArray[i] = SQL_ROW_NOROW;
        }

    // RESET POSITION OR SET INITIAL POSITIONS

    // postions the row counter for fetch start
    if ( ( s = _SQLResetRowPos ( ( pODBCStmt ) pStmt ) ) != SQL_SUCCESS )
    {
        return s;
    }

    // COLLECT INFO to START
    // get the row desc - ird
    rowdesc = ( ( pODBCStmt ) pStmt ) -> IRD . RowDesc . get ();
    // get the row desc - ard
    ard = & ( ( ( pODBCStmt ) pStmt ) -> ARD );

    // MAIN LOOP to fetch rowset number of rows
    // loop to fetch the rows
    for ( i = 0 , n1 = 0 , n2 = 0 , flgNoData = FALSE; i < pFetchOffset && flgNoData == FALSE; i ++ )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "Get One Row:" ) );

        // check if first row or not
        if ( i != 0 )
        {
            // move to next row if not first time
            switch ( _SQLFetchMoveNext ( pStmt ) )
            {
                case SQL_NO_DATA :
                    flgNoData = TRUE; // can continue
                    continue;

                case SQL_ERROR :
                    return SQL_ERROR; // not continuing

                default : // case SQL_SUCCESS:
                    break;
            }
        }

        // get the current row data
        rowdata = pStmt -> CurRowsetEndRow;

        // LOOP to fetch cols of one row

        // loop to put data in all bound cols
        for ( ardcol = ard -> BindCols; ardcol != NULL; ardcol = ardcol -> Next )
        {
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "Get one column:" ) );
            // get data using _SQLFetchCol
            s = _SQLFetchCol ( pStmt, ardcol, rowdesc, rowdata );

            // update row status
            switch ( s )
            {
                case SQL_SUCCESS :
                    if ( pRowStatusArray && pRowStatusArray[i] == SQL_ROW_NOROW )
                    {
                        pRowStatusArray[i] = SQL_ROW_SUCCESS_WITH_INFO;
                    }

                    break;

                case SQL_SUCCESS_WITH_INFO :
                    ++ n1; // rows with info

                    if ( pRowStatusArray )
                    {
                        pRowStatusArray[i - 1] = SQL_ROW_SUCCESS_WITH_INFO;
                    }

                    break;

                default :
                    ++ n2; // no. of rows with error

                    if ( pRowStatusArray )
                    {
                        pRowStatusArray[i - 1] = SQL_ROW_ERROR;
                    }
            }
        }

        // update the number of rows fetched
        if ( pRowCountPtr )
        {
            *pRowCountPtr = i + 1;
        }
    }

    // check if no data
    if ( flgNoData == TRUE && i <= 0 )
    {
        return SQL_NO_DATA;
    }

    // check if all error
    else if ( i > 0 && n2 == i )
    {
        return SQL_ERROR;
    }

    // check if any success with info
    else if ( i > 0 && n1 > 0 )
    {
        return SQL_SUCCESS_WITH_INFO;
    }

    // all success
    else
    {
        return SQL_SUCCESS;
    }
}


// -----------------------------------------------------------------------
// to retrieve long data for a single column in the result set using multiple calls
// -----------------------------------------------------------------------

RETCODE SQL_API SQLGetData ( SQLHSTMT pStmt,
                             SQLUSMALLINT pColNum,
                             SQLSMALLINT pDataType,
                             SQLPOINTER pDataPtr,
                             SQLLEN pDataSize,
                             SQLLEN* pDataSizePtr )
{
    __CHK_HANDLE ( pStmt, SQL_HANDLE_STMT, SQL_ERROR );

    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetData called, ColNum: %d, TgtType: %d, ValuePtr: %d, Capacity: %d",
        pColNum, pDataType, pDataPtr, pDataSize ) );

    if ( pColNum < 1 || pColNum > ( ( pODBCStmt )pStmt ) -> IRD . DescCount )
    {
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "", "07009", -1, "Dynamic SQL error-invalid descriptor index" );
        return SQL_ERROR;
    }

    pODBCARD ard;
    pARDItem ardcol;
    SQLRowContent* rowdata;
    SQLResponse* rowdesc;
    SQLSMALLINT tgtPDataType;

    ard = & ( ( ( pODBCStmt ) pStmt ) -> ARD );
    rowdata = ( ( pODBCStmt )pStmt ) -> CurRowsetEndRow;
    rowdesc = ( ( pODBCStmt ) pStmt ) -> IRD . RowDesc . get ();

    if ( rowdata == NULL )
    {
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "", "HY010", -1, "CLI-specific condition-function sequence error" );
        return SQL_ERROR;
    }

    ardcol = _SQLGetARDItem ( ard, pColNum );
    if ( ardcol != NULL )
    {
        /* It's illegal to call SQLGetdata for a "bound" Column */
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "", "07009", -1, "dynamic SQL error-invalid descriptor index" );
        return SQL_ERROR;
    }

    // convert C data type to SQL data type
    switch ( pDataType )
    {
        case SQL_C_SBIGINT :
        case SQL_C_SLONG :
        case SQL_C_SSHORT :
        case SQL_C_STINYINT :
            tgtPDataType = pDataType - SQL_SIGNED_OFFSET;
            break;
        case SQL_C_ULONG :
        case SQL_C_USHORT :
        case SQL_C_UTINYINT :
        case SQL_C_UBIGINT :
            tgtPDataType = pDataType - SQL_UNSIGNED_OFFSET;
            break;
        default :
            tgtPDataType = pDataType;
            break;
    }

    // manually bind column information to output
    RETCODE ret = SQLBindCol ( pStmt, pColNum, tgtPDataType, pDataPtr, pDataSize, pDataSizePtr );
    if ( ret != SQL_SUCCESS )
    {
        return ret;
    }

    ardcol = _SQLGetARDItem ( ard, pColNum );
    ret = _SQLFetchCol ( ( pODBCStmt )pStmt, ardcol, rowdesc, rowdata );
    if ( ret != SQL_SUCCESS )
    {
        return ret;
    }
    _SQLDetachARDItem ( ard, ardcol );

    /*unique_ptr<char[]> temp2 ( wchar2char ( ( wchar_t* ) pDataPtr ) );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "Data1: %s", temp2.get()));
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "Size1: %d", *pDataSizePtr));*/

    return SQL_SUCCESS;
}

