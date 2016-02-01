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

void queryFlowTest ()
{
    SQLRETURN status;
    SQLHANDLE hEnv = 0;
    SQLHANDLE hConn = 0;
    SQLHANDLE hStmt = 0;
    wchar_t szConnStrOut[1024];
    SQLSMALLINT x;
    // BEFORE U CONNECT
    // allocate ENVIRONMENT
    status = SQLAllocHandle ( SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv );
    // set the ODBC version for behaviour expected
    status = SQLSetEnvAttr ( hEnv, SQL_ATTR_ODBC_VERSION, ( SQLPOINTER ) SQL_OV_ODBC3, 0 );
    // allocate CONNECTION
    status = SQLAllocHandle ( SQL_HANDLE_DBC, hEnv, &hConn );
    status = SQLDriverConnectW ( hConn, GetDesktopWindow (),
                                 L"DRIVER={KylinODBCDriver};PROJECT=default;UID=ADMIN;SERVER=http://localhost;PORT=80;",
                                 //L"DSN=testDSN",
                                 SQL_NTS, szConnStrOut, 1024, &x,
                                 SQL_DRIVER_PROMPT );
    // check for error
    //ODBC_CHK_ERROR(SQL_HANDLE_DBC,hConn,status,"");

    if ( hConn )
    {
        SQLFreeHandle ( SQL_HANDLE_DBC, hConn );
    }

    if ( hEnv )
    {
        SQLFreeHandle ( SQL_HANDLE_ENV, hEnv );
    }

    printf ( "finish" );
}

