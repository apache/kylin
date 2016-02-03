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

#include "Tests.h"
#include <vector>

#include "ColorPrint.h"
#include "fstream"

// ----- whether to ask user for DSN and connection parameters OR specify -----
//#define _CONNECT_WITH_PROMPT        1

// ------------- maximum length to be displayed for a column ------------------
#define _DISPLAY_MAX                128


// -------------------- macro to handle error situations ----------------------
#define ODBC_CHK_ERROR(hType,hValue,iStatus,szMsg)          \
    {\
        if ( status != SQL_SUCCESS ) {\
            ShowDiagMessages ( hType, hValue, iStatus, szMsg );\
        }\
        if ( status == SQL_ERROR ) {\
            goto Cleanup;\
        }\
    }

// ---------------------------------- structure -------------------------------
typedef struct BindColInfo
{
    SQLSMALLINT iColTitleSize; // size of column title
    wchar_t* szColTitle; // column title
    SQLLEN iColDisplaySize; // size to display
    void* szColData; // display buffer
    int iType;
    bool isSigned;
    SQLLEN indPtr; // size or null indicator
    BOOL fChar; // character col flag
    struct BindColInfo* next; // linked list
} BIND_COL_INFO;

// -------------------------- function prototypes -----------------------------
void ShowDiagMessages ( SQLSMALLINT hType, SQLHANDLE hValue, SQLRETURN iStatus, char* szMsg );

SQLRETURN CheckResults ( HSTMT hStmt, wchar_t* sql );
void FreeBindings ( BIND_COL_INFO* pBindColInfo );

int totalCount;
int successCount;
int failCount;
std::vector <wstring> failedQueries;

void validateOneQuery ( wchar_t* sql )
{
    Sleep ( 1000 );
    SQLRETURN status;
    SQLHANDLE hEnv = 0;
    SQLHANDLE hConn = 0;
    SQLHANDLE hStmt = 0;
    wchar_t szConnStrOut[1024];
    SQLSMALLINT x;
    // show query to be executed
    wprintf ( L"The query being validated: %ls \n", sql );
    // BEFORE U CONNECT
    // allocate ENVIRONMENT
    status = SQLAllocHandle ( SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv );
    // check for error
    ODBC_CHK_ERROR ( SQL_HANDLE_ENV, hEnv, status, "" );
    // set the ODBC version for behaviour expected
    status = SQLSetEnvAttr ( hEnv, SQL_ATTR_ODBC_VERSION, ( SQLPOINTER ) SQL_OV_ODBC3, 0 );
    // check for error
    ODBC_CHK_ERROR ( SQL_HANDLE_ENV, hEnv, status, "" );
    // allocate CONNECTION
    status = SQLAllocHandle ( SQL_HANDLE_DBC, hEnv, &hConn );
    // check for error
    ODBC_CHK_ERROR ( SQL_HANDLE_ENV, hEnv, status, "" );
#ifdef _WIN64
    // ----------- real connection takes place at this point
    // ----------- option 1: user is prompted for DSN & options
    status = SQLDriverConnect ( hConn, GetDesktopWindow(),
                                ( unsigned char* ) "",
                                SQL_NTS, szConnStrOut, 1024, &x,
                                SQL_DRIVER_COMPLETE );
#else
    status = SQLDriverConnectW ( hConn, GetDesktopWindow (),
                                 //L"DSN=testDSN;",
                                 L"DRIVER={KylinODBCDriver};PROJECT=default;UID=ADMIN;SERVER=http://localhost;PORT=80;",
                                 SQL_NTS, szConnStrOut, 1024, &x,
                                 SQL_DRIVER_COMPLETE );
#endif
    // check for error
    ODBC_CHK_ERROR ( SQL_HANDLE_DBC, hConn, status, "" );
    // CONGRATUALTIONS ---- u r connected to a DBMS via an ODBC driver
    // allocate STATEMENT
    status = SQLAllocHandle ( SQL_HANDLE_STMT, hConn, &hStmt );
    // check for error
    ODBC_CHK_ERROR ( SQL_HANDLE_DBC, hConn, status, "" );
    // execute the statement
    //status = SQLExecDirect ( hStmt, ( unsigned char* )argv[1], SQL_NTS );
    status = SQLExecDirectW ( hStmt, ( SQLWCHAR* ) sql, SQL_NTS );
    // check for error
    ODBC_CHK_ERROR ( SQL_HANDLE_STMT, hConn, status, "" );
    // RESULTS READY
    // show the full results row by row
    status = CheckResults ( hStmt, sql );
    totalCount++;

    if ( status == SQL_ERROR )
    {
        setPrintColorRED ();
        fputs ( "[FAIL]\n", stdout );
        resetPrintColor ();
        failCount++;
        failedQueries . push_back ( sql );
    }

    else if ( status == SQL_SUCCESS )
    {
        setPrintColorGreen ();
        fputs ( "[SUCCESS]\n", stdout );
        resetPrintColor ();
        successCount++;
    }

    // check for error
    ODBC_CHK_ERROR ( SQL_HANDLE_STMT, hStmt, status, "" );
Cleanup:

    if ( hStmt )
    {
        SQLFreeHandle ( SQL_HANDLE_STMT, hStmt );
    }

    if ( hConn )
    {
        SQLFreeHandle ( SQL_HANDLE_DBC, hConn );
    }

    if ( hEnv )
    {
        SQLFreeHandle ( SQL_HANDLE_ENV, hEnv );
    }

    return;
}

void validateQueries ( char* file )
{
    std::string line;
    std::ifstream infile ( file );

    while ( std::getline ( infile, line ) )
    {
        if ( line . size () < 5 )
        {
            continue;
        }

        unique_ptr <wchar_t[]> p ( char2wchar ( line . c_str () ) );
        validateOneQuery ( p . get () );
    }

    infile . close ();
}


bool isValueConsistent ( void* data, wstring& valueJ, int pSrcDataType, bool isSigned )
{
    fwprintf ( stdout, L"The value from the JDBC is : %s \n", valueJ . c_str () );

    switch ( pSrcDataType )
    {
        case SQL_BIT :
        {
            wstring tempW;

            if ( * ( char* ) data == 0 )
            {
                tempW = L"false";
            }

            else if ( * ( char* ) data == 1 )
            {
                tempW = L"true";
            }

            else
            {
                return false;
            }

            return tempW . compare ( valueJ ) == 0;
        }

        case SQL_CHAR :
        case SQL_VARCHAR :
        {
            string temp ( ( char* ) data );
            wstring tempW = string2wstring ( temp );
            fwprintf ( stdout, L"The value from the ODBC is : %s \n", tempW . c_str () );
            return tempW . compare ( valueJ ) == 0;
        }

        case SQL_WCHAR :
        case SQL_WVARCHAR :
        {
            wstring tempW ( ( wchar_t* ) data );
            fwprintf ( stdout, L"The value from the ODBC is : %s \n", tempW . c_str () );
            return tempW . compare ( valueJ ) == 0;
        }

        case SQL_DECIMAL :
        {
            string temp ( ( char* ) data );
            wstring tempW = string2wstring ( temp );
            fwprintf ( stdout, L"The value from the ODBC is : %s \n", tempW . c_str () );
            return tempW . compare ( valueJ ) == 0;
        }

        case SQL_TINYINT :
        {
            int v = 0;

            if ( isSigned )
            {
                v = * ( char* ) data;
            }

            else
            {
                v = * ( unsigned char* ) data;
            }

            char buffer[100];
            _itoa ( v, buffer, 10 );
            string temp ( buffer );
            wstring tempW = string2wstring ( temp );
            fwprintf ( stdout, L"The value from the ODBC is : %s \n", tempW . c_str () );
            return tempW . compare ( valueJ ) == 0;
        }

        case SQL_SMALLINT :
        {
            int v = 0;

            if ( isSigned )
            {
                v = * ( short* ) data;
            }

            else
            {
                v = * ( unsigned short* ) data;
            }

            char buffer[100];
            _itoa ( v, buffer, 10 );
            string temp ( buffer );
            wstring tempW = string2wstring ( temp );
            fwprintf ( stdout, L"The value from the ODBC is : %s \n", tempW . c_str () );
            return tempW . compare ( valueJ ) == 0;
        }

        case SQL_INTEGER :
        {
            __int64 v = 0;

            if ( isSigned )
            {
                v = * ( int* ) data;
            }

            else
            {
                v = * ( unsigned int* ) data;
            }

            char buffer[100];
            _i64toa ( v, buffer, 10 );
            string temp ( buffer );
            wstring tempW = string2wstring ( temp );
            fwprintf ( stdout, L"The value from the ODBC is : %s \n", tempW . c_str () );
            return tempW . compare ( valueJ ) == 0;
        }

        case SQL_BIGINT :
        {
            if ( isSigned )
            {
                __int64 v = 0;
                v = * ( __int64* ) data;
                char buffer[100];
                _i64toa ( v, buffer, 10 );
                string temp ( buffer );
                wstring tempW = string2wstring ( temp );
                fwprintf ( stdout, L"The value from the ODBC is : %s \n", tempW . c_str () );
                return tempW . compare ( valueJ ) == 0;
            }

            else
            {
                unsigned __int64 v = 0;
                v = * ( unsigned __int64* ) data;
                char buffer[100];
                _ui64toa ( v, buffer, 10 );
                string temp ( buffer );
                wstring tempW = string2wstring ( temp );
                fwprintf ( stdout, L"The value from the ODBC is : %s \n", tempW . c_str () );
                return tempW . compare ( valueJ ) == 0;
            }
        }

        case SQL_FLOAT :
        {
            float v = 0;
            v = * ( float* ) data;
            fwprintf ( stdout, L"The value from the ODBC is (float) : %9.9f \n", v );
            double x = ( v - _wtof ( valueJ . c_str () ) );
            return ( x > -0.0000001 ) && ( x < 0.0000001 ); // In Kylin float is treated like double, so it might be more accurate
        }

        case SQL_DOUBLE :
        {
            double v = * ( double* ) data;
            fwprintf ( stdout, L"The value from the ODBC is (double) : %9.9f\n ", v );
            return v == wcstod ( valueJ . c_str (), NULL );
        }

        case SQL_TYPE_DATE :
        {
            string temp ( ( char* ) data );
            wstring tempW = string2wstring ( temp );
            fwprintf ( stdout, L"The value from the ODBC is : %s \n", tempW . c_str () );
            return tempW . compare ( valueJ ) == 0;
        }

        case SQL_TYPE_TIMESTAMP :
        {
            string temp ( ( char* ) data );
            wstring tempW = string2wstring ( temp );
            fwprintf ( stdout, L"The value from the ODBC is : %s \n", tempW . c_str () );
            return tempW . compare ( valueJ ) == 0;
        }

        default :
            return false;
    }
}

// ----------------------------------------------------------------------------
// to validate the full results row by row
// ----------------------------------------------------------------------------
SQLRETURN CheckResults ( HSTMT hStmt, wchar_t* sql )
{
    //First directly call REST to get a JDBC version result to compare against
    std::unique_ptr <SQLResponse> response = restQuery ( sql, "http://localhost", 80, "ADMIN", "KADMIN", "default" );
    //Go with hStmt now
    int i, iCol;
    BIND_COL_INFO* head;
    BIND_COL_INFO* last;
    BIND_COL_INFO* curr;
    SQLRETURN status;
    SQLLEN cType;
    SQLSMALLINT iColCount;
    // initializations
    head = NULL;

    // ALLOCATE SPACE TO FETCH A COMPLETE ROW

    // get number of cols
    if ( ( status = SQLNumResultCols ( hStmt, &iColCount ) ) != SQL_SUCCESS )
    {
        return status;
    }

    // loop to allocate binding info structure
    for ( iCol = 1; iCol <= iColCount; iCol ++ )
    {
        // alloc binding structure
        curr = ( BIND_COL_INFO* ) calloc ( 1, sizeof ( BIND_COL_INFO) );

        if ( curr == NULL )
        {
            fprintf ( stderr, "Out of memory!\n" );
            return SQL_ERROR; // its not an ODBC error so no diags r required
        }

        memset ( curr, 0, sizeof ( BIND_COL_INFO) );

        // maintain link list
        if ( iCol == 1 )
        {
            head = curr;
        } // first col, therefore head of list

        else
        {
            last -> next = curr;
        } // attach

        last = curr; // tail

        // get column title size
        if ( ( status = SQLColAttributeW ( hStmt, iCol, SQL_DESC_NAME, NULL, 0, & ( curr -> iColTitleSize ),
                                           NULL ) ) != SQL_SUCCESS )
        {
            FreeBindings ( head );
            return status;
        }

        else
        {
            ++ curr -> iColTitleSize; // allow space for null char
        }

        // allocate buffer for title
        curr -> szColTitle = ( wchar_t* ) calloc ( 1, curr -> iColTitleSize * sizeof ( wchar_t) );

        if ( curr -> szColTitle == NULL )
        {
            FreeBindings ( head );
            fprintf ( stderr, "Out of memory!\n" );
            return SQL_ERROR; // its not an ODBC error so no diags r required
        }

        // get column title
        if ( ( status = SQLColAttributeW ( hStmt, iCol, SQL_DESC_NAME, curr -> szColTitle, curr -> iColTitleSize,
                                           & ( curr -> iColTitleSize ), NULL ) ) != SQL_SUCCESS )
        {
            FreeBindings ( head );
            return status;
        }

        //xxx
        // get col length
        if ( ( status = SQLColAttributeW ( hStmt, iCol, SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL,
                                           & ( curr -> iColDisplaySize ) ) ) != SQL_SUCCESS )
        {
            FreeBindings ( head );
            return status;
        }

        // arbitrary limit on display size
        if ( curr -> iColDisplaySize > _DISPLAY_MAX )
        {
            curr -> iColDisplaySize = _DISPLAY_MAX;
        }

        // allocate buffer for col data + NULL terminator
        curr -> szColData = ( void* ) calloc ( 1, 2 * ( curr -> iColDisplaySize + 1 ) * sizeof ( char) );

        if ( curr -> szColData == NULL )
        {
            FreeBindings ( head );
            fprintf ( stderr, "Out of memory!\n" );
            return SQL_ERROR; // its not an ODBC error so no diags r required
        }

        //xxx
        // get col type, not used now but can be checked to print value right aligned etcc
        if ( ( status = SQLColAttributeW ( hStmt, iCol, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, &cType ) ) != SQL_SUCCESS )
        {
            FreeBindings ( head );
            return status;
        }

        curr -> iType = cType;
        fprintf ( stdout, "The type for column %d is %d\n", iCol, cType );
        //xxx
        // get col type, not used now but can be checked to print value right aligned etcc
        SQLLEN unsignedV = 0;

        if ( ( status = SQLColAttributeW ( hStmt, iCol, SQL_DESC_UNSIGNED, NULL, 0, NULL, &unsignedV ) ) != SQL_SUCCESS )
        {
            FreeBindings ( head );
            return status;
        }

        curr -> isSigned = ( unsignedV == 1 ) ? false : true;
        fprintf ( stdout, "The column %d is signed ? %d\n", iCol, curr -> isSigned );
        // set col type indicator in struct
        curr -> fChar = ( cType == SQL_CHAR || cType == SQL_VARCHAR || cType == SQL_LONGVARCHAR ||
            cType == SQL_WCHAR || cType == SQL_WVARCHAR || cType == SQL_WLONGVARCHAR );
        fprintf ( stdout, "char flag is set to %d\n", curr -> fChar );
        fputs ( "\n", stdout );

        //xxx
        // bind the col buffer so that the driver feeds it with col value on every fetch and use generic char binding for very column
        if ( ( status = SQLBindCol ( hStmt, iCol, SQL_C_DEFAULT, ( SQLPOINTER ) curr -> szColData,
                                     2 * ( curr -> iColDisplaySize + 1 ) * sizeof ( char), & ( curr -> indPtr ) ) ) != SQL_SUCCESS )
        {
            FreeBindings ( head );
            return status;
        }
    }

    // loop to print all the rows one by one
    for ( i = 1; TRUE; i ++ )
    {
        // fetch the next row
        if ( ( status = SQLFetch ( hStmt ) ) == SQL_NO_DATA_FOUND )
        {
            break;
        } // no more rows so break

        // check for error
        else if ( status == SQL_ERROR )
        { // fetch failed
            FreeBindings ( head );
            return status;
        }

        for ( curr = head , iCol = 0; iCol < iColCount; iCol ++ , curr = curr -> next )
        {
            fprintf ( stdout, "Row Index: %d, Column Cardinal : %d\n", i - 1, iCol );

            if ( !isValueConsistent ( curr -> szColData, response -> results[i - 1] -> contents[iCol], curr -> iType, curr -> isSigned ) )
            {
                FreeBindings ( head );
                return SQL_ERROR;
            }

            fputs ( "\n", stdout );
        }
    }

    // free the allocated bindings
    FreeBindings ( head );
    return SQL_SUCCESS;
}


// ----------------------------------------------------------------------------
// to free the col info allocated by ShowFullResults
// ----------------------------------------------------------------------------

void FreeBindings ( BIND_COL_INFO* pBindColInfo )
{
    BIND_COL_INFO* next;

    // precaution
    if ( pBindColInfo )
    {
        do
        {
            // get the next col binding
            next = pBindColInfo -> next;

            // free any buffer for col title
            if ( pBindColInfo -> szColTitle )
            {
                free ( pBindColInfo -> szColTitle );
                pBindColInfo -> szColTitle = NULL;
            }

            // free any col data
            if ( pBindColInfo -> szColData )
            {
                free ( pBindColInfo -> szColData );
                pBindColInfo -> szColData = NULL;
            }

            // free the current binding
            free ( pBindColInfo );
            // make next the current
            pBindColInfo = next;
        }
        while ( pBindColInfo );
    }
}

// ----------------------------------------------------------------------------
// to show the ODBC diagnostic messages
// ----------------------------------------------------------------------------

void ShowDiagMessages ( SQLSMALLINT hType, SQLHANDLE hValue, SQLRETURN iStatus, char* szMsg )
{
    SQLSMALLINT iRec = 0;
    SQLINTEGER iError;
    SQLTCHAR szMessage[1024];
    SQLTCHAR szState[1024];
    // header
    fputs ( "\nDiagnostics:\n", stdout );

    // in case of an invalid handle, no message can be extracted
    if ( iStatus == SQL_INVALID_HANDLE )
    {
        fprintf ( stderr, "ODBC Error: Invalid handle!\n" );
        return;
    }

    // loop to get all diag messages from driver/driver manager
    while ( SQLGetDiagRec ( hType, hValue, ++ iRec, szState, &iError, szMessage,
                            ( SQLSMALLINT ) ( sizeof ( szMessage ) / sizeof ( SQLTCHAR) ), ( SQLSMALLINT* ) NULL ) == SQL_SUCCESS )
    {
        _ftprintf ( stderr, TEXT ( "[%5.5s] %s (%d)\n" ), szState, szMessage, iError );
    }

    // gap
    fputs ( "\n", stdout );
}

void crossValidate ()
{
    char* queryFile = "testqueries.txt";
    //char* queryFile = "c:\\foo.txt";
    fprintf ( stdout, "The test queries file location is: %s\n", queryFile );
    //validateOneQuery(L"SELECT * FROM classicmodels.new_table;");
    validateQueries ( queryFile );
    fprintf ( stdout, "The verify process is done.\n", queryFile );
    fprintf ( stdout, "Total queries: %d, Successful queries: %d, Failed queries: %d.\n", totalCount, successCount,
                    failCount );

    for ( vector <wstring>::iterator iter = failedQueries . begin (); iter != failedQueries . end (); ++iter )
    {
        fprintf ( stdout, wstring2string ( *iter ) . c_str () );
        fprintf ( stdout, "\n\n" );
    }
}

void validateSQLGetTypeInfo ()
{
    Sleep ( 1000 );
    SQLRETURN status;
    SQLHANDLE hEnv = 0;
    SQLHANDLE hConn = 0;
    SQLHANDLE hStmt = 0;
    wchar_t szConnStrOut[1024];
    SQLSMALLINT x;

    // BEFORE U CONNECT
    // allocate ENVIRONMENT
    status = SQLAllocHandle ( SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv );
    // check for error
    ODBC_CHK_ERROR ( SQL_HANDLE_ENV, hEnv, status, "" );
    // set the ODBC version for behaviour expected
    status = SQLSetEnvAttr ( hEnv, SQL_ATTR_ODBC_VERSION, ( SQLPOINTER ) SQL_OV_ODBC3, 0 );
    // check for error
    ODBC_CHK_ERROR ( SQL_HANDLE_ENV, hEnv, status, "" );
    // allocate CONNECTION
    status = SQLAllocHandle ( SQL_HANDLE_DBC, hEnv, &hConn );
    // check for error
    ODBC_CHK_ERROR ( SQL_HANDLE_ENV, hEnv, status, "" );
#ifdef _WIN64
    // ----------- real connection takes place at this point
    // ----------- option 1: user is prompted for DSN & options
    status = SQLDriverConnect ( hConn, GetDesktopWindow(),
                                ( unsigned char* ) "",
                                SQL_NTS, szConnStrOut, 1024, &x,
                                SQL_DRIVER_COMPLETE );
#else
    status = SQLDriverConnectW ( hConn, GetDesktopWindow (),
                                 //L"DSN=testDSN;",
                                 L"DRIVER={KylinODBCDriver};PROJECT=default;UID=ADMIN;SERVER=http://localhost;PORT=80;",
                                 SQL_NTS, szConnStrOut, 1024, &x,
                                 SQL_DRIVER_COMPLETE );
#endif
    // check for error
    ODBC_CHK_ERROR ( SQL_HANDLE_DBC, hConn, status, "" );
    // CONGRATUALTIONS ---- u r connected to a DBMS via an ODBC driver
    // allocate STATEMENT
    status = SQLAllocHandle ( SQL_HANDLE_STMT, hConn, &hStmt );
    // check for error
    ODBC_CHK_ERROR ( SQL_HANDLE_DBC, hConn, status, "" );
    // execute the statement
    //status = SQLExecDirect ( hStmt, ( unsigned char* )argv[1], SQL_NTS );
    status = SQLGetTypeInfoW ( hStmt, SQL_ALL_TYPES );
    // check for error
    ODBC_CHK_ERROR ( SQL_HANDLE_STMT, hConn, status, "" );
    // check for error
    ODBC_CHK_ERROR ( SQL_HANDLE_STMT, hStmt, status, "" );
Cleanup:

    if ( hStmt )
    {
        SQLFreeHandle ( SQL_HANDLE_STMT, hStmt );
    }

    if ( hConn )
    {
        SQLFreeHandle ( SQL_HANDLE_DBC, hConn );
    }

    if ( hEnv )
    {
        SQLFreeHandle ( SQL_HANDLE_ENV, hEnv );
    }

    return;
}

