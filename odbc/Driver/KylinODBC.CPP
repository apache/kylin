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


// ---------------------------------------------------------------------------------
//
// File:    GODBC32.CPP
//
// Author:  Hongbin Ma
//
// Purpose: Entry point for the Kylin ODBC
//          Contains the DllMain as entry point function
//          Contains log initialization-deinitialization function
//          Contains in commented form the deprecated functions
//          Contains more details about the driver as comments
//          Finally compiled into a DLL
//
// Note:    The driver is supposed to be an ODBC 3.0 driver.
//          If you're to study the code,
//          Take a look at KylinODBC.h to learn about the prerequisites
//
// ----------------------------------------------------------------------------------
#include "stdafx.h"

#include <stdio.h>


// ------------------------- global variables -------------------------
HINSTANCE ghInstDLL = 0;
Long gLogFile = -1;
Word gLogUsage = 0;


// -----------------------------------------------------------------------
// DLL enrty point
// -----------------------------------------------------------------------

BOOL WINAPI DllMain ( HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpvReserved )
{
    // note
    // key functions it needs to perform
    // 1. initialize the log system if in debug mode

    // Perform actions based on the reason for calling.
    switch ( fdwReason )
    {
        case DLL_PROCESS_ATTACH :
            // Initialize once for each new process.
            // Return FALSE to fail DLL load.
            OutputDebugString ( "Process Attached\n" ); // for DBMON
            // start log
            _ODBCLogStart ();
#ifdef _DEBUG
            MessageBox ( GetDesktopWindow(), "Entry into driver DLL - PROCESS_ATTACH",  "Kylin ODBC driver", MB_OK );
#endif
            break;

        case DLL_THREAD_ATTACH :
#ifdef _DEBUG
            // start log
            _ODBCLogStart ();
#endif
            // log msg
            OutputDebugString ( "Thread Attached\n" ); // for DBMON
            // Do thread-specific initialization.
            break;

        case DLL_THREAD_DETACH :
#ifdef _DEBUG
            // stop log
            _ODBCLogStop ( 0 );
#endif
            // Do thread-specific cleanup.
            break;

        case DLL_PROCESS_DETACH :
#ifdef _DEBUG
            // log msg
            //__ODBCLOG(_ODBCLogMsg(LogLevel_DEBUG, "Thread de-tach"));
            // stop log
            _ODBCLogStop ( 1 );
#endif
            // Perform any necessary cleanup.
            break;
    }

    // check if instance has not been saved
    if ( ghInstDLL == 0 )
    {
        ghInstDLL = hinstDLL;
    }

    return TRUE; // Successful DLL_PROCESS_ATTACH.
}


// -----------------------------------------------------------------------
// Notes
//
//
//
//
//
// -----------------------------------------------------------------------


// -----------------------------------------------------------------------
// Modules
//
// driver.DEF  -    Exported function. Required by linker to export the
//                  functions.
//
// KO_ALLOC.CPP -   Contains function for managing allocation and release
//                  of the three structures/handles - Environment,
//                  Connection and Statement. Statements r
//                  encapsulated inside connection while all connections
//                  inside environment.
//
// KO_ATTR.CPP  -   Contains the attribute/option set/get functions
//                  for environment, connection and statement
//                  also contains some other functions like setting of
//                  cursor name etc.
//
// KO_CONN.CPP  -   Contains the main connection functions more
//                  specifically SQLDriverConnect.
//
// KO_CTLG.CPP  -   Contains catalog functions
//                  Functions that allow collection of metadata/information
//                  about the database are termed as catalog functions.
//
// KO_DESC.CPP  -   Contains descriptor functions.
//                  As explained in the article, descriptors r the heart
//                  as far as driver design is concerned. Descriptors
//                  r essntially data structures to encapsulate one set/type of
//                  data associated with ODBC connection.
//
// KO_DIAG.CPP  -   Contains DIAG related functions.
//                  Contains debug/log related functions.
//                  The diagnostic functions work as per ODBC
//                  specification while the log functions
//                  are useful for debugging.
//
// KO_DTYPE.CPP -   Contains functions related to data types as per ODBC.
//                  For defining the type of data ODBC uses three params
//                  Concise Type, Verbose Type and DateTime Interval.
//                  All of these r intertwined, setting one of these
//                  influences the other two.
//
// KO_EXEC.CPP  -   Contains function for executing queries.
//                  After a request is prepared by the caller
//                  control is passed to _SQLExecStmtFromReq in this
//                  file. Alse SQLExecDirect, SQLExecute, SQLPrepare
//                  r implemented in this file
//
// KO_FETCH.CPP -   Contains function for fetching results.
//                  As explained in the article ODBC has a
//                  defined way in which the data is made available.
//                  Client specified the buffer, its size and return
//                  size placeholder. These are then used by the
//                  driver automatically when fetch funtions r
//                  called by the client
//
// KO_INFO.CPP  -   This file is dedicated to the single function SQLGetInfo.
//                  Almost all information about your driver, server
//                  and current state required by the client is obtained
//                  using this function.
//
// KO_PARAM.CPP -   Functions for implementing paramterized queries. Not
//                  implemented in this sample.
//
//
// KO_UTILS.CPP -   contains generic utility functions used across files/modules.
//                  Data conversion and copying functions w.r.t ODBC type params
//                  like its type, size, its size pointer, src data and src size etc.
//
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// Header Files
//
// KylinODBC.h      -   Main header file for driver
//
// SQL.H        -   Main ODBC file on Win32
//
// SQLEXT.H     -   Main ODBC file on Win32
//
// SQLTYPES.H   -   ODBC types
//
// -----------------------------------------------------------------------


// -----------------------------------------------------------------------
// ------------------------- deprecated functions ------------------------
// -----------------------------------------------------------------------


/**********

    RETCODE SQL_API SQLSetParam ( HSTMT arg0, UWORD arg1, SWORD arg2, SWORD arg3, UDWORD arg4, SWORD arg5, PTR arg6, UNALIGNED SDWORD * arg7)
    {
    // has been mapped to SQLBindParameter
    // hence not exported

    _LogMessage("SQLSetParam called");
    return(SQL_SUCCESS);
    }



    RETCODE SQL_API SQLError ( HENV arg0, HDBC arg1, HSTMT arg2, UCHAR * arg3, UNALIGNED SDWORD * arg4, UCHAR * arg5, SWORD arg6, UNALIGNED SWORD * arg7)
    {
    // has been mapped to SQLDiagRec
    // hence not exported

    _LogMessage("SQLError called");

    return(SQL_NO_DATA_FOUND);
    }

    RETCODE SQL_API SQLTransact ( HENV arg0, HDBC arg1, UWORD arg2 )
    {
    // has been mapped to SQLEndTran
    // hence not exported

    _LogMessage("SQLTransact called");
    return(SQL_SUCCESS);
    }



    RETCODE SQL_API SQLParamOptions  (HSTMT arg0,
    UDWORD arg1,
    UNALIGNED UDWORD * arg2)
    {
    // has been mapped to SQLSetStmtAttr
    // hence not exported

    _LogMessage("SQLParamOptions called");
    return(SQL_SUCCESS);
    }


    RETCODE SQL_API SQLSetScrollOptions  (HSTMT arg0,
    UWORD arg1,
    SDWORD arg2,
    UWORD arg3)
    {
    // has been mapped to SQLSetStmtAttr
    // hence not exported

    _LogMessage("SQLSetScrollOptions called");
    return(SQL_SUCCESS);
    }


    RETCODE SQL_API SQLBindParam  (SQLHSTMT arg0,
    SQLUSMALLINT arg1,
    SQLSMALLINT arg2,
    SQLSMALLINT arg3,
    SQLUINTEGER arg4,
    SQLSMALLINT arg5,
    SQLPOINTER arg6,
    SQLINTEGER * arg7)
    {
    // has been mapped to SQLBindParameter
    // hence not exported

    _LogMessage("SQLBindParam called");
    return(SQL_SUCCESS);
    }


    RETCODE SQL_API SQLSetConnectOption ( HDBC arg0, UWORD arg1, UDWORD arg2 )
    {
    // has been mapped to SQLSetConnectAttr
    // hence not exported

    _LogMessage("SQLSetConnectOption called\n");
    return(SQL_SUCCESS);
    }

    ///// SQLGetConnectOption /////

    RETCODE SQL_API SQLGetConnectOption ( HDBC arg0, UWORD arg1, PTR arg2 )
    {
    // has been mapped to SQLGetConnectAttr
    // hence not exported

    _LogMessage("SQLGetConnectOption called");
    return(SQL_SUCCESS);
    }


    ///// SQLGetStmtOption /////

    RETCODE SQL_API SQLGetStmtOption ( HSTMT arg0, UWORD arg1, PTR arg2 )
    {
    // has been mapped to SQLGetStmtAttr
    // hence not exported

    _LogMessage("SQLGetStmtOption called");
    return(SQL_SUCCESS);
    }


    ///// SQLSetStmtOption /////

    RETCODE SQL_API SQLSetStmtOption ( HSTMT arg0, UWORD arg1, UDWORD arg2 )
    {
    // has been mapped to SQLSetStmtAttr
    // hence not exported

    _LogMessage("SQLSetStmtOption called, Fld: %d, Value: %d", arg1, arg2 );
    return(SQL_SUCCESS);
    }




    RETCODE SQL_API SQLColAttributes ( HSTMT arg0, UWORD arg1, UWORD arg2, PTR arg3, SWORD arg4, UNALIGNED SWORD * arg5, UNALIGNED SDWORD * arg6 )
    {
    // has been mapped to SQLColAttribute
    // hence not exported

    _LogMessage("SQLColAttributes called - deprecated ????");
    return SQL_SUCCESS;
    }

    //////////////// DRIVER MANAGER

    RETCODE SQL_API SQLDrivers  (HENV arg0,
    UWORD arg1,
    UCHAR * arg2,
    SWORD arg3,
    SWORD * arg4,
    UCHAR * arg5,
    SWORD arg6,
    SWORD * arg7)
    {
    // implemented  solely only by driver manager
    // hence not exported

    _LogMessage("SQLDrivers called");
    return(SQL_SUCCESS);
    }


    RETCODE SQL_API SQLDataSources  (HENV arg0, UWORD arg1, UCHAR * arg2, SWORD arg3, SWORD * arg4, UCHAR * arg5, SWORD arg6, SWORD * arg7)
    {
    // implemented  solely only by driver manager
    // hence not exported

    _LogMessage("SQLDataSources called\n");
    return(SQL_SUCCESS);
    }


****************** */

