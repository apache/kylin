#include "Tests.h"

void queryFlowTest() {
    SQLRETURN       status;
    SQLHANDLE       hEnv = 0;
    SQLHANDLE       hConn = 0;
    SQLHANDLE       hStmt = 0;
    wchar_t szConnStrOut[1024];
    SQLSMALLINT     x;
    // BEFORE U CONNECT
    // allocate ENVIRONMENT
    status = SQLAllocHandle ( SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv );
    // set the ODBC version for behaviour expected
    status = SQLSetEnvAttr ( hEnv,  SQL_ATTR_ODBC_VERSION, ( SQLPOINTER ) SQL_OV_ODBC3, 0 );
    // allocate CONNECTION
    status = SQLAllocHandle ( SQL_HANDLE_DBC, hEnv, &hConn );
    status = SQLDriverConnectW ( hConn, GetDesktopWindow(),
                                 L"DRIVER={KylinODBCDriver};PROJECT=default;UID=ADMIN;SERVER=http://localhost;PORT=80;",
                                 //L"DSN=testDSN",
                                 SQL_NTS, szConnStrOut, 1024, &x,
                                 SQL_DRIVER_PROMPT );
    // check for error
    //ODBC_CHK_ERROR(SQL_HANDLE_DBC,hConn,status,"");
    
    if ( hConn )
    { SQLFreeHandle ( SQL_HANDLE_DBC, hConn ); }
    
    if ( hEnv )
    { SQLFreeHandle ( SQL_HANDLE_ENV, hEnv ); }
    
    printf ( "finish" );
}
