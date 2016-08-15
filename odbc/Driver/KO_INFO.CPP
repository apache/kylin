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
// File:    KO_INFO.CPP
//
// Purpose: This file is dedicated to the single function SQLGetInfo.
//          Almost all information about your driver, server
//          and current state required by the client is obtained
//          using this function. Most of the values need to be
//          changed to values as per your server. I believe
//          that you will not require to query your server for
//          most information, but u can do that if u like your
//          driver to be more flexible.
//
//          Some info like current database ofcourse needs to be
//          queried live from the server. Clients like MS Word,
//          SQL server use a whole bunch of info from this function.
//          You have to go thru each attribute and provide
//          the info.
//
//          Another function SQLGetTypeInfo is implemented in this
//          file. This function is a catalog function used to obtain
//          the data types supported by the server and the associated
//          details required by the client to interpret them correctly.
//          This function does an Rest call to the server to obtain the
//          details at the very beginning of SQLConnect
//
//
// Exported functions:
//                       SQLGetInfo
//                       SQLGetTypeInfo
//
// ----------------------------------------------------------------------------
#include "stdafx.h"
#include "JsonConverter.h"

// -----------------------------------------------------------------------
// to get driver related information
// -----------------------------------------------------------------------

RETCODE SQL_API SQLGetInfoW ( SQLHDBC pConn,
                              SQLUSMALLINT pInfoType,
                              SQLPOINTER pInfoValuePtr,
                              SQLSMALLINT pBufferLength,
                              SQLSMALLINT* pStringLengthPtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetInfoW called: Field: %d, Length: %d", pInfoType, pBufferLength ) );
    _SQLFreeDiag ( _DIAGCONN ( pConn ) );

    // check the info required
    // check this page for detailed http://msdn.microsoft.com/en-us/library/ms711681(v=vs.85).aspx
    switch ( pInfoType )
    {
        case SQL_COLUMN_ALIAS : //87 called
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            break;

        case SQL_CONVERT_FUNCTIONS : //48 called
        case SQL_CONVERT_BIGINT :
        case SQL_CONVERT_BINARY :
        case SQL_CONVERT_BIT :
        case SQL_CONVERT_CHAR :
        case SQL_CONVERT_GUID :
        case SQL_CONVERT_DATE :
        case SQL_CONVERT_DECIMAL :
        case SQL_CONVERT_DOUBLE :
        case SQL_CONVERT_FLOAT :
        case SQL_CONVERT_INTEGER :
        case SQL_CONVERT_INTERVAL_YEAR_MONTH :
        case SQL_CONVERT_INTERVAL_DAY_TIME :
        case SQL_CONVERT_LONGVARBINARY :
        case SQL_CONVERT_LONGVARCHAR :
        case SQL_CONVERT_NUMERIC :
        case SQL_CONVERT_REAL :
        case SQL_CONVERT_SMALLINT :
        case SQL_CONVERT_TIME :
        case SQL_CONVERT_TIMESTAMP :
        case SQL_CONVERT_TINYINT :
        case SQL_CONVERT_VARBINARY :
        case SQL_CONVERT_VARCHAR :
        case SQL_CONVERT_WCHAR :
        case SQL_CONVERT_WLONGVARCHAR :
        case SQL_CONVERT_WVARCHAR :
            break;

        case SQL_MAX_IDENTIFIER_LEN : //10005 called
            break;

        case SQL_ODBC_INTERFACE_CONFORMANCE : //152 called
            break;

        case SQL_SQL_CONFORMANCE : //118 called
            break;

            /*
                An SQLUINTEGER bitmask enumerating the SQL-92 datetime literals supported by the data source. Note that these are the datetime literals listed in the SQL-92 specification and are separate from the datetime literal escape clauses defined by ODBC. For more information about the ODBC datetime literal escape clauses, see Date, Time, and Timestamp Literals.
                A FIPS Transitional level�Cconformant driver will always return the "1" value in the bitmask for the bits in the following list. A value of "0" means that SQL-92 datetime literals are not supported.
                The following bitmasks are used to determine which literals are supported:
                SQL_DL_SQL92_DATESQL_DL_SQL92_TIMESQL_DL_SQL92_TIMESTAMPSQL_DL_SQL92_INTERVAL_YEARSQL_DL_SQL92_INTERVAL_MONTHSQL_DL_SQL92_INTERVAL_DAYSQL_DL_SQL92_INTERVAL_HOURSQL_DL_SQL92_INTERVAL_MINUTESQL_DL_SQL92_INTERVAL_SECONDSQL_DL_SQL92_INTERVAL_YEAR_TO_MONTHSQL_DL_SQL92_INTERVAL_DAY_TO_HOUR
                SQL_DL_SQL92_INTERVAL_DAY_TO_MINUTESQL_DL_SQL92_INTERVAL_DAY_TO_SECONDSQL_DL_SQL92_INTERVAL_HOUR_TO_MINUTESQL_DL_SQL92_INTERVAL_HOUR_TO_SECONDSQL_DL_SQL92_INTERVAL_MINUTE_TO_SECOND
            */
        case SQL_DATETIME_LITERALS : //119 called
            //assuming all datetime literals are supported
            * ( ( Long* ) pInfoValuePtr ) =
                SQL_DL_SQL92_DATE |
                SQL_DL_SQL92_TIME |
                SQL_DL_SQL92_TIMESTAMP |
                SQL_DL_SQL92_INTERVAL_YEAR |
                SQL_DL_SQL92_INTERVAL_MONTH |
                SQL_DL_SQL92_INTERVAL_DAY |
                SQL_DL_SQL92_INTERVAL_HOUR |
                SQL_DL_SQL92_INTERVAL_MINUTE |
                SQL_DL_SQL92_INTERVAL_SECOND |
                SQL_DL_SQL92_INTERVAL_YEAR_TO_MONTH |
                SQL_DL_SQL92_INTERVAL_DAY_TO_HOUR |
                SQL_DL_SQL92_INTERVAL_DAY_TO_MINUTE |
                SQL_DL_SQL92_INTERVAL_DAY_TO_SECOND |
                SQL_DL_SQL92_INTERVAL_HOUR_TO_MINUTE |
                SQL_DL_SQL92_INTERVAL_HOUR_TO_SECOND |
                SQL_DL_SQL92_INTERVAL_MINUTE_TO_SECOND ;
            break;

            /*
                An SQLUINTEGER bitmask enumerating the timestamp intervals supported by the driver and associated data source for the TIMESTAMPADD scalar function.
                The following bitmasks are used to determine which intervals are supported:
                SQL_FN_TSI_FRAC_SECONDSQL_FN_TSI_SECONDSQL_FN_TSI_MINUTESQL_FN_TSI_HOURSQL_FN_TSI_DAYSQL_FN_TSI_WEEKSQL_FN_TSI_MONTHSQL_FN_TSI_QUARTERSQL_FN_TSI_YEAR
                An FIPS Transitional level�Cconformant driver will always return a bitmask in which all of these bits are set.SQL_DATETIME_LITERALS(ODBC 3.0)
            */
        case SQL_TIMEDATE_ADD_INTERVALS : // 109 called
            * ( ( Long* ) pInfoValuePtr ) =
                SQL_FN_TSI_FRAC_SECOND |
                SQL_FN_TSI_SECOND |
                SQL_FN_TSI_MINUTE |
                SQL_FN_TSI_HOUR |
                SQL_FN_TSI_DAY |
                SQL_FN_TSI_WEEK |
                SQL_FN_TSI_MONTH |
                SQL_FN_TSI_QUARTER |
                SQL_FN_TSI_YEAR ;
            break;

            /*
                An SQLUINTEGER bitmask enumerating the timestamp intervals supported by the driver and associated data source for the TIMESTAMPDIFF scalar function.
                The following bitmasks are used to determine which intervals are supported:
                SQL_FN_TSI_FRAC_SECONDSQL_FN_TSI_SECONDSQL_FN_TSI_MINUTESQL_FN_TSI_HOURSQL_FN_TSI_DAYSQL_FN_TSI_WEEKSQL_FN_TSI_MONTHSQL_FN_TSI_QUARTERSQL_FN_TSI_YEAR
                An FIPS Transitional level�Cconformant driver will always return a bitmask in which all of these bits are set.
            */
        case SQL_TIMEDATE_DIFF_INTERVALS : //110 called
            * ( ( Long* ) pInfoValuePtr ) =
                SQL_FN_TSI_FRAC_SECOND |
                SQL_FN_TSI_SECOND |
                SQL_FN_TSI_MINUTE |
                SQL_FN_TSI_HOUR |
                SQL_FN_TSI_DAY |
                SQL_FN_TSI_WEEK |
                SQL_FN_TSI_MONTH |
                SQL_FN_TSI_QUARTER |
                SQL_FN_TSI_YEAR ;
            break;

        case SQL_AGGREGATE_FUNCTIONS : //169 called
            * ( ( Long* ) pInfoValuePtr ) = SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_DISTINCT | SQL_AF_MAX | SQL_AF_MIN |
                                                         SQL_AF_SUM ;
            break;

            /*
                An SQLUINTEGER bitmask enumerating the datetime scalar functions that are supported by the driver and the associated data source, as defined in SQL-92.
                The following bitmasks are used to determine which datetime functions are supported:
                SQL_SDF_CURRENT_DATESQL_SDF_CURRENT_TIMESQL_SDF_CURRENT_TIMESTAMP
            */
        case SQL_SQL92_DATETIME_FUNCTIONS : //155 called
            * ( ( Long* ) pInfoValuePtr ) =
                SQL_SDF_CURRENT_DATE |
                SQL_SDF_CURRENT_TIME |
                SQL_SDF_CURRENT_TIMESTAMP ;
            break;

        case SQL_SQL92_VALUE_EXPRESSIONS : //165 called
            break;

        case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS : //159 called
            break;

        case SQL_SQL92_STRING_FUNCTIONS : //164 called
            break;

        case SQL_SQL92_PREDICATES : //160 called
            break;

        case SQL_SQL92_RELATIONAL_JOIN_OPERATORS : //161 called
            break;

        case SQL_DRIVER_ODBC_VER : // 77 called
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "03.00", -1 );
            //*pStringLengthPtr = *pStringLengthPtr *2;
            break;

        case SQL_CURSOR_COMMIT_BEHAVIOR : //23 called
            //MessageBox ( GetDesktopWindow(), "SQL_CURSOR_COMMIT_BEHAVIOR", "SQLGetInfo", MB_OK );
            * ( ( short* ) pInfoValuePtr ) = SQL_CB_CLOSE;
            break;

        case SQL_CORRELATION_NAME : //74
            //MessageBox ( GetDesktopWindow(), "SQL_CORRELATION_NAME", "SQLGetInfo", MB_OK );
            * ( ( short* ) pInfoValuePtr ) = SQL_CN_ANY;
            break;

        case SQL_MAX_CONCURRENT_ACTIVITIES : // 1

            //MessageBox ( GetDesktopWindow(), "SQL_MAX_CONCURRENT_ACTIVITIES", "SQLGetInfo", MB_OK );
            if ( pInfoValuePtr )
            {
                * ( ( Word* ) pInfoValuePtr ) = 2;
            }

            break;

        case SQL_ODBC_API_CONFORMANCE : // 9

            //MessageBox ( GetDesktopWindow(), "SQL_ODBC_API_CONFORMANCE", "SQLGetInfo", MB_OK );
            if ( pInfoValuePtr )
            {
                * ( ( Word* ) pInfoValuePtr ) = SQL_OAC_NONE;
            } // for MS Access

            break;

        case SQL_DTC_TRANSITION_COST : // 1750
            break;

        case SQL_DATA_SOURCE_READ_ONLY : // 25
            //MessageBox ( GetDesktopWindow(), "SQL_DATA_SOURCE_READ_ONLY", "SQLGetInfo", MB_OK );
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "N", -1 );
            break;

        case SQL_DRIVER_NAME : // 6
            //MessageBox ( GetDesktopWindow(), "SQL_DRIVER_NAME", "SQLGetInfo", MB_OK );
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "driver.DLL", -1 );
            break;

        case SQL_SEARCH_PATTERN_ESCAPE : // 14
            //MessageBox ( GetDesktopWindow(), "SQL_SEARCH_PATTERN_ESCAPE", "SQLGetInfo", MB_OK );
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "\\", -1 );
            break;

        case SQL_NON_NULLABLE_COLUMNS : // 75
            //MessageBox ( GetDesktopWindow(), "SQL_NON_NULLABLE_COLUMNS", "SQLGetInfo", MB_OK );
            * ( ( short* ) pInfoValuePtr ) = SQL_NNC_NULL;
            break;

        case SQL_QUALIFIER_NAME_SEPARATOR : // 41 called
            //MessageBox ( GetDesktopWindow(), "SQL_QUALIFIER_NAME_SEPARATOR", "SQLGetInfo", MB_OK );
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, ".", -1 );
            break;

        case SQL_FILE_USAGE : // 84
            //MessageBox ( GetDesktopWindow(), "SQL_FILE_USAGE", "SQLGetInfo", MB_OK );
            * ( ( short* ) pInfoValuePtr ) = SQL_FILE_NOT_SUPPORTED;
            break;

        case SQL_QUALIFIER_TERM : // 42  // SQL_CATALOG_TERM: called
            //MessageBox ( GetDesktopWindow(), "SQL_QUALIFIER_TERM", "SQLGetInfo", MB_OK );
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "database", -1 );
            break;

        case SQL_OWNER_TERM : // 39 //called
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "owner", -1 );
            break;

        case SQL_TABLE_TERM : // 45 called
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "table", -1 );
            break;

        case SQL_CURSOR_ROLLBACK_BEHAVIOR : // 24 called
            //MessageBox ( GetDesktopWindow(), "SQL_CURSOR_ROLLBACK_BEHAVIOR", "SQLGetInfo", MB_OK );
            * ( ( short* ) pInfoValuePtr ) = SQL_CB_CLOSE;
            break;

        case SQL_DATA_SOURCE_NAME : // 2
            //MessageBox ( GetDesktopWindow(), "SQL_DATA_SOURCE_NAME", "SQLGetInfo", MB_OK );
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "GODBC", -1 );
            break;

        case 16 :
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "default", -1 );
            break;

        case SQL_PROCEDURES : // 21
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "N", -1 );
            break;

        case SQL_IDENTIFIER_QUOTE_CHAR : // 29  //called by tb
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "\"", -1 );
            break;

        case SQL_MAX_SCHEMA_NAME_LEN : // 32
            * ( ( short* ) pInfoValuePtr ) = 128;
            break;

        case SQL_USER_NAME : // 47
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "default", -1 );
            break;

        case SQL_POS_OPERATIONS : // 79
            // deprecated
            * ( ( Long* ) pInfoValuePtr ) = SQL_POS_POSITION;
            break;

        case SQL_STATIC_SENSITIVITY : // 83
            // deprecated
            * ( ( Long* ) pInfoValuePtr ) = SQL_SS_ADDITIONS;
            break;

        case SQL_LOCK_TYPES : // 78
            // deprecated
            * ( ( Long* ) pInfoValuePtr ) = SQL_LCK_NO_CHANGE;
            break;

        case SQL_GETDATA_EXTENSIONS : // 81 called
            * ( ( Long* ) pInfoValuePtr ) = SQL_GD_ANY_COLUMN;
            break;

        case SQL_TXN_ISOLATION_OPTION : // 72
            * ( ( Long* ) pInfoValuePtr ) = SQL_TXN_SERIALIZABLE;
            break;

        case SQL_BOOKMARK_PERSISTENCE : // 82
            * ( ( Long* ) pInfoValuePtr ) = 0;
            break;

        case SQL_SCROLL_OPTIONS : // 44
            * ( ( Long* ) pInfoValuePtr ) = SQL_SO_FORWARD_ONLY;
            break;

        case SQL_SCROLL_CONCURRENCY : // 43
            // deprecated
            * ( ( Long* ) pInfoValuePtr ) = SQL_SCCO_READ_ONLY;
            break;

        case SQL_DYNAMIC_CURSOR_ATTRIBUTES1 : // 144
            * ( ( Long* ) pInfoValuePtr ) = SQL_CA1_NEXT;
            break;

        case SQL_KEYSET_CURSOR_ATTRIBUTES1 : // 150
            * ( ( Long* ) pInfoValuePtr ) = SQL_CA1_NEXT;
            break;

        case SQL_STATIC_CURSOR_ATTRIBUTES1 : // 167
            * ( ( Long* ) pInfoValuePtr ) = SQL_CA1_NEXT;
            break;

        case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1 : // 146
            * ( ( Long* ) pInfoValuePtr ) = SQL_CA1_NEXT;
            break;

        case SQL_KEYSET_CURSOR_ATTRIBUTES2 : // 151
            * ( ( Long* ) pInfoValuePtr ) = SQL_CA2_READ_ONLY_CONCURRENCY;
            break;

        case SQL_STATIC_CURSOR_ATTRIBUTES2 : // 168
            * ( ( Long* ) pInfoValuePtr ) = SQL_CA2_READ_ONLY_CONCURRENCY;
            break;

        case SQL_NEED_LONG_DATA_LEN : // 111
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            break;

        case SQL_TXN_CAPABLE : // 46 called
            * ( ( Word* ) pInfoValuePtr ) = SQL_TC_NONE;
            break;

        case SQL_DEFAULT_TXN_ISOLATION : // 26
            * ( ( Long* ) pInfoValuePtr ) = SQL_TXN_READ_COMMITTED;
            break;

        case SQL_DBMS_NAME : // 17 called
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "MySQL", -1 );
            break;

        case SQL_ODBC_SQL_CONFORMANCE : // 15
            // deprecated
            * ( ( Word* ) pInfoValuePtr ) = SQL_OSC_MINIMUM;
            break;

        case SQL_INTEGRITY : // 73
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "N", -1 );
            break;

        case SQL_QUALIFIER_USAGE : // 92 called
            * ( ( Long* ) pInfoValuePtr ) = SQL_CU_DML_STATEMENTS | SQL_CU_PROCEDURE_INVOCATION | SQL_CU_TABLE_DEFINITION;
            break;

        case SQL_DBMS_VER : // 18 called
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "01.00.00000", -1 );
            break;

        case SQL_QUOTED_IDENTIFIER_CASE : // 93 called
            //*(( Word* )pInfoValuePtr) = SQL_IC_SENSITIVE;
            * ( ( Word* ) pInfoValuePtr ) = SQL_IC_UPPER;
            break;

        case SQL_MAX_CATALOG_NAME_LEN : // 34
            * ( ( Word* ) pInfoValuePtr ) = 128;
            break;

        case SQL_MAX_TABLE_NAME_LEN : // 35
            * ( ( Word* ) pInfoValuePtr ) = 128;
            break;

        case SQL_ACTIVE_CONNECTIONS : // 0
            * ( ( Word* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_ACTIVE_CONNECTIONS" ));
            break;

        case SQL_CATALOG_LOCATION : // 114
            * ( ( Word* ) pInfoValuePtr ) = SQL_CL_START;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_CATALOG_LOCATION" ));
            break;

        case SQL_CATALOG_NAME : // 10003
            break;

        case SQL_CONCAT_NULL_BEHAVIOR : // 22
            * ( ( Word* ) pInfoValuePtr ) = SQL_CB_NULL;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_CONCAT_NULL_BEHAVIOR" ));
            break;

        case SQL_GROUP_BY : // 88
            * ( ( Word* ) pInfoValuePtr ) = SQL_GB_GROUP_BY_EQUALS_SELECT;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_GROUP_BY" ));
            break;

        case SQL_IDENTIFIER_CASE : // 28
            * ( ( Word* ) pInfoValuePtr ) = SQL_IC_MIXED;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_IDENTIFIER_CASE" ));
            break;

        case SQL_MAX_INDEX_SIZE : // 102
            * ( ( Long* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_INDEX_SIZE" ));
            break;

        case SQL_MAX_ROW_SIZE : // 104
            * ( ( Long* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_ROW_SIZE" ));
            break;

        case SQL_MAX_ROW_SIZE_INCLUDES_LONG : // 103
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_ROW_SIZE_INCLUDES_LONG" ));
            break;

        case SQL_MAX_TABLES_IN_SELECT : // 106
            * ( ( Word* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_TABLES_IN_SELECT" ));
            break;

        case SQL_NULL_COLLATION : // 85
            * ( ( Word* ) pInfoValuePtr ) = SQL_NC_START;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_NULL_COLLATION" ));
            break;

        case SQL_ORDER_BY_COLUMNS_IN_SELECT : // 90
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "N", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_ORDER_BY_COLUMNS_IN_SELECT" ));
            break;

        case SQL_PROCEDURE_TERM : // 40
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "procedure", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_PROCEDURE_TERM" ));
            break;

        case SQL_OWNER_USAGE : // 91 called
            * ( ( Word* ) pInfoValuePtr ) = SQL_SU_DML_STATEMENTS | SQL_SU_TABLE_DEFINITION | SQL_SU_PRIVILEGE_DEFINITION;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_OWNER_USAGE" ));
            break;

        case SQL_SUBQUERIES : // 95
            * ( ( Long* ) pInfoValuePtr ) = SQL_SQ_CORRELATED_SUBQUERIES |
                SQL_SQ_COMPARISON |
                SQL_SQ_EXISTS |
                SQL_SQ_IN |
                SQL_SQ_QUANTIFIED;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_SUBQUERIES" ));
            break;

        case SQL_MULT_RESULT_SETS : // 36:
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MULT_RESULT_SETS" ));
            break;

        case SQL_SERVER_NAME : // 13
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16,
                                                   ( ( pODBCConn ) pConn ) -> Server ? ( ( pODBCConn ) pConn ) -> Server : "", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_SERVER_NAME" ));
            break;

        case SQL_ACCESSIBLE_PROCEDURES : // 20
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_ACCESSIBLE_PROCEDURES" ));
            break;

        case SQL_ACCESSIBLE_TABLES : // 19
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_ACCESSIBLE_TABLES" ));
            break;

        case SQL_OJ_CAPABILITIES : // 115 called
            * ( ( Long* ) pInfoValuePtr ) = SQL_OJ_LEFT |
                SQL_OJ_RIGHT |
                SQL_OJ_FULL |
                SQL_OJ_NESTED |
                SQL_OJ_NOT_ORDERED |
                SQL_OJ_INNER |
                SQL_OJ_ALL_COMPARISON_OPS;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_OJ_CAPABILITIES" ));
            break;

        case SQL_DRIVER_VER : // 7 called
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "01.00.00000", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_DRIVER_VER" ));
            break;

        case SQL_LIKE_ESCAPE_CLAUSE : // 113
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_LIKE_ESCAPE_CLAUSE" ));
            break;

        case SQL_SPECIAL_CHARACTERS : // 94 //called by tb
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "$_", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_SPECIAL_CHARACTERS" ));
            break;

        case SQL_MAX_COLUMNS_IN_GROUP_BY : // 97
            * ( ( Word* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_COLUMNS_IN_GROUP_BY" ));
            break;

        case SQL_MAX_COLUMNS_IN_INDEX : // 98
            * ( ( Word* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_COLUMNS_IN_INDEX" ));
            break;

        case SQL_MAX_COLUMNS_IN_ORDER_BY : // 99
            * ( ( Word* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_COLUMNS_IN_ORDER_BY" ));
            break;

        case SQL_MAX_COLUMNS_IN_SELECT : // 100
            * ( ( Word* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_COLUMNS_IN_SELECT" ));
            break;

        case SQL_MAX_COLUMNS_IN_TABLE : // 101
            * ( ( Word* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_COLUMNS_IN_TABLE" ));
            break;

        case SQL_NUMERIC_FUNCTIONS : // 49 called
            * ( ( Long* ) pInfoValuePtr ) = 0; // ???? set of standard numeric functions
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_NUMERIC_FUNCTIONS" ));
            break;

        case SQL_OUTER_JOINS : // 38
            _SQLCopyWCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            // __ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_OUTER_JOINS" ));
            break;

        case SQL_STRING_FUNCTIONS : // 50 called
            * ( ( Long* ) pInfoValuePtr ) = 0; // ???? set of standard string functions
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_STRING_FUNCTIONS" ));
            break;

        case SQL_SYSTEM_FUNCTIONS : // 51 called
            * ( ( Long* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_SYSTEM_FUNCTIONS" ));
            break;

            /*
                Note: The information type was introduced in ODBC 1.0; each bitmask is labeled with the version in which it was introduced.
                An SQLUINTEGER bitmask enumerating the scalar date and time functions supported by the driver and associated data source.
                The following bitmasks are used to determine which date and time functions are supported:
                SQL_FN_TD_CURRENT_DATE ODBC 3.0)SQL_FN_TD_CURRENT_TIME (ODBC 3.0)SQL_FN_TD_CURRENT_TIMESTAMP (ODBC 3.0)SQL_FN_TD_CURDATE (ODBC 1.0)SQL_FN_TD_CURTIME (ODBC 1.0) SQL_FN_TD_DAYNAME (ODBC 2.0)SQL_FN_TD_DAYOFMONTH (ODBC 1.0)SQL_FN_TD_DAYOFWEEK (ODBC 1.0)SQL_FN_TD_DAYOFYEAR (ODBC 1.0) SQL_FN_TD_EXTRACT (ODBC 3.0)SQL_FN_TD_HOUR (ODBC 1.0)SQL_FN_TD_MINUTE (ODBC 1.0)SQL_FN_TD_MONTH (ODBC 1.0)SQL_FN_TD_MONTHNAME (ODBC 2.0)SQL_FN_TD_NOW (ODBC 1.0)SQL_FN_TD_QUARTER (ODBC 1.0)SQL_FN_TD_SECOND (ODBC 1.0)SQL_FN_TD_TIMESTAMPADD (ODBC 2.0)SQL_FN_TD_TIMESTAMPDIFF (ODBC 2.0)SQL_FN_TD_WEEK (ODBC 1.0)SQL_FN_TD_YEAR (ODBC 1.0)
            */
        case SQL_TIMEDATE_FUNCTIONS : // 52 called
            * ( ( Long* ) pInfoValuePtr ) =
                //SQL_FN_TD_CURRENT_DATE |
                //SQL_FN_TD_CURRENT_TIME |
                SQL_FN_TD_CURRENT_TIMESTAMP |
                SQL_FN_TD_CURDATE |
                SQL_FN_TD_CURTIME |
                //SQL_FN_TD_DAYNAME |
                //SQL_FN_TD_DAYOFMONTH |
                //SQL_FN_TD_DAYOFWEEK |
                //SQL_FN_TD_DAYOFYEAR |
                SQL_FN_TD_EXTRACT
                //SQL_FN_TD_HOUR |
                //SQL_FN_TD_MINUTE |
                //SQL_FN_TD_MONTH |
                //SQL_FN_TD_MONTHNAME |
                //SQL_FN_TD_NOW |
                //SQL_FN_TD_QUARTER |
                //SQL_FN_TD_SECOND |
                //SQL_FN_TD_TIMESTAMPADD |
                //SQL_FN_TD_TIMESTAMPDIFF |
                //SQL_FN_TD_WEEK |
                //SQL_FN_TD_YEAR
                ;
            break;

        default :
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_ERROR,
                "Function Missing!!! SQLGetInfoW, Field: %d, DataPtr: %d, BufLen: %d, SizePtr: %d\n", pInfoType, pInfoValuePtr,
                pBufferLength, pStringLengthPtr ) );
    }

    return SQL_SUCCESS;
}


RETCODE SQL_API SQLGetInfo ( SQLHDBC pConn,
                             SQLUSMALLINT pInfoType,
                             SQLPOINTER pInfoValuePtr,
                             SQLSMALLINT pBufferLength,
                             SQLSMALLINT* pStringLengthPtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetInfo called: Field: %d, Length: %d", pInfoType, pBufferLength ) );
    _SQLFreeDiag ( _DIAGCONN ( pConn ) );

    // check the info required
    switch ( pInfoType )
    {
        case SQL_COLUMN_ALIAS : //87 called
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            break;

        case SQL_CONVERT_FUNCTIONS : //48 called
        case SQL_CONVERT_BIGINT :
        case SQL_CONVERT_BINARY :
        case SQL_CONVERT_BIT :
        case SQL_CONVERT_CHAR :
        case SQL_CONVERT_GUID :
        case SQL_CONVERT_DATE :
        case SQL_CONVERT_DECIMAL :
        case SQL_CONVERT_DOUBLE :
        case SQL_CONVERT_FLOAT :
        case SQL_CONVERT_INTEGER :
        case SQL_CONVERT_INTERVAL_YEAR_MONTH :
        case SQL_CONVERT_INTERVAL_DAY_TIME :
        case SQL_CONVERT_LONGVARBINARY :
        case SQL_CONVERT_LONGVARCHAR :
        case SQL_CONVERT_NUMERIC :
        case SQL_CONVERT_REAL :
        case SQL_CONVERT_SMALLINT :
        case SQL_CONVERT_TIME :
        case SQL_CONVERT_TIMESTAMP :
        case SQL_CONVERT_TINYINT :
        case SQL_CONVERT_VARBINARY :
        case SQL_CONVERT_VARCHAR :
        case SQL_CONVERT_WCHAR :
        case SQL_CONVERT_WLONGVARCHAR :
        case SQL_CONVERT_WVARCHAR :
            break;

        case SQL_MAX_IDENTIFIER_LEN : //10005 called
            break;

        case SQL_ODBC_INTERFACE_CONFORMANCE : //152 called
            break;

        case SQL_SQL_CONFORMANCE : //118 called
            break;

            /*
                An SQLUINTEGER bitmask enumerating the SQL-92 datetime literals supported by the data source. Note that these are the datetime literals listed in the SQL-92 specification and are separate from the datetime literal escape clauses defined by ODBC. For more information about the ODBC datetime literal escape clauses, see Date, Time, and Timestamp Literals.
                A FIPS Transitional level�Cconformant driver will always return the "1" value in the bitmask for the bits in the following list. A value of "0" means that SQL-92 datetime literals are not supported.
                The following bitmasks are used to determine which literals are supported:
                SQL_DL_SQL92_DATESQL_DL_SQL92_TIMESQL_DL_SQL92_TIMESTAMPSQL_DL_SQL92_INTERVAL_YEARSQL_DL_SQL92_INTERVAL_MONTHSQL_DL_SQL92_INTERVAL_DAYSQL_DL_SQL92_INTERVAL_HOURSQL_DL_SQL92_INTERVAL_MINUTESQL_DL_SQL92_INTERVAL_SECONDSQL_DL_SQL92_INTERVAL_YEAR_TO_MONTHSQL_DL_SQL92_INTERVAL_DAY_TO_HOUR
                SQL_DL_SQL92_INTERVAL_DAY_TO_MINUTESQL_DL_SQL92_INTERVAL_DAY_TO_SECONDSQL_DL_SQL92_INTERVAL_HOUR_TO_MINUTESQL_DL_SQL92_INTERVAL_HOUR_TO_SECONDSQL_DL_SQL92_INTERVAL_MINUTE_TO_SECOND
            */
        case SQL_DATETIME_LITERALS : //119 called
            //assuming all datetime literals are supported
            * ( ( Long* ) pInfoValuePtr ) =
                SQL_DL_SQL92_DATE |
                SQL_DL_SQL92_TIME |
                SQL_DL_SQL92_TIMESTAMP |
                SQL_DL_SQL92_INTERVAL_YEAR |
                SQL_DL_SQL92_INTERVAL_MONTH |
                SQL_DL_SQL92_INTERVAL_DAY |
                SQL_DL_SQL92_INTERVAL_HOUR |
                SQL_DL_SQL92_INTERVAL_MINUTE |
                SQL_DL_SQL92_INTERVAL_SECOND |
                SQL_DL_SQL92_INTERVAL_YEAR_TO_MONTH |
                SQL_DL_SQL92_INTERVAL_DAY_TO_HOUR |
                SQL_DL_SQL92_INTERVAL_DAY_TO_MINUTE |
                SQL_DL_SQL92_INTERVAL_DAY_TO_SECOND |
                SQL_DL_SQL92_INTERVAL_HOUR_TO_MINUTE |
                SQL_DL_SQL92_INTERVAL_HOUR_TO_SECOND |
                SQL_DL_SQL92_INTERVAL_MINUTE_TO_SECOND ;
            break;

            /*
                An SQLUINTEGER bitmask enumerating the timestamp intervals supported by the driver and associated data source for the TIMESTAMPADD scalar function.
                The following bitmasks are used to determine which intervals are supported:
                SQL_FN_TSI_FRAC_SECONDSQL_FN_TSI_SECONDSQL_FN_TSI_MINUTESQL_FN_TSI_HOURSQL_FN_TSI_DAYSQL_FN_TSI_WEEKSQL_FN_TSI_MONTHSQL_FN_TSI_QUARTERSQL_FN_TSI_YEAR
                An FIPS Transitional level�Cconformant driver will always return a bitmask in which all of these bits are set.SQL_DATETIME_LITERALS(ODBC 3.0)
            */
        case SQL_TIMEDATE_ADD_INTERVALS : // 109 called
            * ( ( Long* ) pInfoValuePtr ) =
                SQL_FN_TSI_FRAC_SECOND |
                SQL_FN_TSI_SECOND |
                SQL_FN_TSI_MINUTE |
                SQL_FN_TSI_HOUR |
                SQL_FN_TSI_DAY |
                SQL_FN_TSI_WEEK |
                SQL_FN_TSI_MONTH |
                SQL_FN_TSI_QUARTER |
                SQL_FN_TSI_YEAR ;
            break;

            /*
                An SQLUINTEGER bitmask enumerating the timestamp intervals supported by the driver and associated data source for the TIMESTAMPDIFF scalar function.
                The following bitmasks are used to determine which intervals are supported:
                SQL_FN_TSI_FRAC_SECONDSQL_FN_TSI_SECONDSQL_FN_TSI_MINUTESQL_FN_TSI_HOURSQL_FN_TSI_DAYSQL_FN_TSI_WEEKSQL_FN_TSI_MONTHSQL_FN_TSI_QUARTERSQL_FN_TSI_YEAR
                An FIPS Transitional level�Cconformant driver will always return a bitmask in which all of these bits are set.
            */
        case SQL_TIMEDATE_DIFF_INTERVALS : //110 called
            * ( ( Long* ) pInfoValuePtr ) =
                SQL_FN_TSI_FRAC_SECOND |
                SQL_FN_TSI_SECOND |
                SQL_FN_TSI_MINUTE |
                SQL_FN_TSI_HOUR |
                SQL_FN_TSI_DAY |
                SQL_FN_TSI_WEEK |
                SQL_FN_TSI_MONTH |
                SQL_FN_TSI_QUARTER |
                SQL_FN_TSI_YEAR ;
            break;

        case SQL_AGGREGATE_FUNCTIONS : //169 called
            * ( ( Long* ) pInfoValuePtr ) = SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_DISTINCT | SQL_AF_MAX | SQL_AF_MIN |
                                                         SQL_AF_SUM ;
            break;

            /*
                An SQLUINTEGER bitmask enumerating the datetime scalar functions that are supported by the driver and the associated data source, as defined in SQL-92.
                The following bitmasks are used to determine which datetime functions are supported:
                SQL_SDF_CURRENT_DATESQL_SDF_CURRENT_TIMESQL_SDF_CURRENT_TIMESTAMP
            */
        case SQL_SQL92_DATETIME_FUNCTIONS : //155 called
            * ( ( Long* ) pInfoValuePtr ) =
                SQL_SDF_CURRENT_DATE |
                SQL_SDF_CURRENT_TIME |
                SQL_SDF_CURRENT_TIMESTAMP ;
            break;

        case SQL_SQL92_VALUE_EXPRESSIONS : //165 called
            break;

        case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS : //159 called
            break;

        case SQL_SQL92_STRING_FUNCTIONS : //164 called
            break;

        case SQL_SQL92_PREDICATES : //160 called
            break;

        case SQL_SQL92_RELATIONAL_JOIN_OPERATORS : //161 called
            break;

        case SQL_DRIVER_ODBC_VER : // 77 called
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "03.00", -1 );
            break;

        case SQL_CURSOR_COMMIT_BEHAVIOR : //23 called
            //MessageBox ( GetDesktopWindow(), "SQL_CURSOR_COMMIT_BEHAVIOR", "SQLGetInfo", MB_OK );
            * ( ( short* ) pInfoValuePtr ) = SQL_CB_CLOSE;
            break;

        case SQL_CORRELATION_NAME : //74
            //MessageBox ( GetDesktopWindow(), "SQL_CORRELATION_NAME", "SQLGetInfo", MB_OK );
            * ( ( short* ) pInfoValuePtr ) = SQL_CN_ANY;
            break;

        case SQL_MAX_CONCURRENT_ACTIVITIES : // 1

            //MessageBox ( GetDesktopWindow(), "SQL_MAX_CONCURRENT_ACTIVITIES", "SQLGetInfo", MB_OK );
            if ( pInfoValuePtr )
            {
                * ( ( Word* ) pInfoValuePtr ) = 2;
            }

            break;

        case SQL_ODBC_API_CONFORMANCE : // 9

            //MessageBox ( GetDesktopWindow(), "SQL_ODBC_API_CONFORMANCE", "SQLGetInfo", MB_OK );
            if ( pInfoValuePtr )
            {
                * ( ( Word* ) pInfoValuePtr ) = SQL_OAC_NONE;
            } // for MS Access

            break;

        case SQL_DTC_TRANSITION_COST : // 1750
            break;

        case SQL_DATA_SOURCE_READ_ONLY : // 25
            //MessageBox ( GetDesktopWindow(), "SQL_DATA_SOURCE_READ_ONLY", "SQLGetInfo", MB_OK );
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "N", -1 );
            break;

        case SQL_DRIVER_NAME : // 6 //called by tb
            //MessageBox ( GetDesktopWindow(), "SQL_DRIVER_NAME", "SQLGetInfo", MB_OK );
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "driver.DLL", -1 );
            break;

        case SQL_SEARCH_PATTERN_ESCAPE : // 14
            //MessageBox ( GetDesktopWindow(), "SQL_SEARCH_PATTERN_ESCAPE", "SQLGetInfo", MB_OK );
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "\\", -1 );
            break;

        case SQL_NON_NULLABLE_COLUMNS : // 75
            //MessageBox ( GetDesktopWindow(), "SQL_NON_NULLABLE_COLUMNS", "SQLGetInfo", MB_OK );
            * ( ( short* ) pInfoValuePtr ) = SQL_NNC_NULL;
            break;

        case SQL_QUALIFIER_NAME_SEPARATOR : // 41 called
            //MessageBox ( GetDesktopWindow(), "SQL_QUALIFIER_NAME_SEPARATOR", "SQLGetInfo", MB_OK );
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, ".", -1 );
            break;

        case SQL_FILE_USAGE : // 84
            //MessageBox ( GetDesktopWindow(), "SQL_FILE_USAGE", "SQLGetInfo", MB_OK );
            * ( ( short* ) pInfoValuePtr ) = SQL_FILE_NOT_SUPPORTED;
            break;

        case SQL_QUALIFIER_TERM : // 42  // SQL_CATALOG_TERM: called
            //MessageBox ( GetDesktopWindow(), "SQL_QUALIFIER_TERM", "SQLGetInfo", MB_OK );
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "database", -1 );
            break;

        case SQL_OWNER_TERM : // 39 //called
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "owner", -1 );
            break;

        case SQL_TABLE_TERM : // 45 called
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "table", -1 );
            break;

        case SQL_CURSOR_ROLLBACK_BEHAVIOR : // 24 called
            //MessageBox ( GetDesktopWindow(), "SQL_CURSOR_ROLLBACK_BEHAVIOR", "SQLGetInfo", MB_OK );
            * ( ( short* ) pInfoValuePtr ) = SQL_CB_CLOSE;
            break;

        case SQL_DATA_SOURCE_NAME : // 2
            //MessageBox ( GetDesktopWindow(), "SQL_DATA_SOURCE_NAME", "SQLGetInfo", MB_OK );
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "GODBC", -1 );
            break;

        case 16 :
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "default", -1 );
            break;

        case SQL_PROCEDURES : // 21
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "N", -1 );
            break;

        case SQL_IDENTIFIER_QUOTE_CHAR : // 29  //called by tb
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "\"", -1 );
            break;

        case SQL_MAX_SCHEMA_NAME_LEN :
            * ( ( short* ) pInfoValuePtr ) = 128;
            break;

        case SQL_USER_NAME :
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "default", -1 );
            break;

        case SQL_POS_OPERATIONS : // 79
            // deprecated
            * ( ( Long* ) pInfoValuePtr ) = SQL_POS_POSITION;
            break;

        case SQL_STATIC_SENSITIVITY : // 83
            // deprecated
            * ( ( Long* ) pInfoValuePtr ) = SQL_SS_ADDITIONS;
            break;

        case SQL_LOCK_TYPES : // 78
            // deprecated
            * ( ( Long* ) pInfoValuePtr ) = SQL_LCK_NO_CHANGE;
            break;

        case SQL_GETDATA_EXTENSIONS : // 81 called
            * ( ( Long* ) pInfoValuePtr ) = SQL_GD_ANY_COLUMN;
            break;

        case SQL_TXN_ISOLATION_OPTION : // 72
            * ( ( Long* ) pInfoValuePtr ) = SQL_TXN_SERIALIZABLE;
            break;

        case SQL_BOOKMARK_PERSISTENCE : // 82
            * ( ( Long* ) pInfoValuePtr ) = 0;
            break;

        case SQL_SCROLL_OPTIONS : // 44
            * ( ( Long* ) pInfoValuePtr ) = SQL_SO_FORWARD_ONLY;
            break;

        case SQL_SCROLL_CONCURRENCY : // 43
            // deprecated
            * ( ( Long* ) pInfoValuePtr ) = SQL_SCCO_READ_ONLY;
            break;

        case SQL_DYNAMIC_CURSOR_ATTRIBUTES1 : // 144
            * ( ( Long* ) pInfoValuePtr ) = SQL_CA1_NEXT;
            break;

        case SQL_KEYSET_CURSOR_ATTRIBUTES1 : // 150
            * ( ( Long* ) pInfoValuePtr ) = SQL_CA1_NEXT;
            break;

        case SQL_STATIC_CURSOR_ATTRIBUTES1 : // 167
            * ( ( Long* ) pInfoValuePtr ) = SQL_CA1_NEXT;
            break;

        case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1 : // 146
            * ( ( Long* ) pInfoValuePtr ) = SQL_CA1_NEXT;
            break;

        case SQL_KEYSET_CURSOR_ATTRIBUTES2 : // 151
            * ( ( Long* ) pInfoValuePtr ) = SQL_CA2_READ_ONLY_CONCURRENCY;
            break;

        case SQL_STATIC_CURSOR_ATTRIBUTES2 : // 168
            * ( ( Long* ) pInfoValuePtr ) = SQL_CA2_READ_ONLY_CONCURRENCY;
            break;

        case SQL_NEED_LONG_DATA_LEN : // 111
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            break;

        case SQL_TXN_CAPABLE : // 46 called
            * ( ( Word* ) pInfoValuePtr ) = SQL_TC_NONE;
            break;

        case SQL_DEFAULT_TXN_ISOLATION : // 26
            * ( ( Long* ) pInfoValuePtr ) = SQL_TXN_READ_COMMITTED;
            break;

        case SQL_DBMS_NAME : // 17 called
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "MySQL", -1 );
            break;

        case SQL_ODBC_SQL_CONFORMANCE : // 15
            // deprecated
            * ( ( Word* ) pInfoValuePtr ) = SQL_OSC_MINIMUM;
            break;

        case SQL_INTEGRITY : // 73
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "N", -1 );
            break;

        case SQL_QUALIFIER_USAGE : // 92 called
            * ( ( Long* ) pInfoValuePtr ) = SQL_CU_DML_STATEMENTS | SQL_CU_PROCEDURE_INVOCATION | SQL_CU_TABLE_DEFINITION;
            break;

        case SQL_DBMS_VER : // 18 called
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "01.00.00000", -1 );
            break;

        case SQL_QUOTED_IDENTIFIER_CASE : // 93 called
            //*(( Word* )pInfoValuePtr) = SQL_IC_SENSITIVE;
            * ( ( Word* ) pInfoValuePtr ) = SQL_IC_UPPER;
            break;

        case SQL_MAX_CATALOG_NAME_LEN : // 34
            * ( ( Word* ) pInfoValuePtr ) = 128;
            break;

        case SQL_MAX_TABLE_NAME_LEN : // 35
            * ( ( Word* ) pInfoValuePtr ) = 128;
            break;

        case SQL_ACTIVE_CONNECTIONS : // 0
            * ( ( Word* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_ACTIVE_CONNECTIONS" ));
            break;

        case SQL_CATALOG_LOCATION : // 114
            * ( ( Word* ) pInfoValuePtr ) = SQL_CL_START;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_CATALOG_LOCATION" ));
            break;

        case SQL_CATALOG_NAME : // 10003
            break;

        case SQL_CONCAT_NULL_BEHAVIOR : // 22
            * ( ( Word* ) pInfoValuePtr ) = SQL_CB_NULL;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_CONCAT_NULL_BEHAVIOR" ));
            break;

        case SQL_GROUP_BY : // 88
            * ( ( Word* ) pInfoValuePtr ) = SQL_GB_GROUP_BY_EQUALS_SELECT;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_GROUP_BY" ));
            break;

        case SQL_IDENTIFIER_CASE : // 28
            * ( ( Word* ) pInfoValuePtr ) = SQL_IC_MIXED;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_IDENTIFIER_CASE" ));
            break;

        case SQL_MAX_INDEX_SIZE : // 102
            * ( ( Long* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_INDEX_SIZE" ));
            break;

        case SQL_MAX_ROW_SIZE : // 104
            * ( ( Long* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_ROW_SIZE" ));
            break;

        case SQL_MAX_ROW_SIZE_INCLUDES_LONG : // 103
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_ROW_SIZE_INCLUDES_LONG" ));
            break;

        case SQL_MAX_TABLES_IN_SELECT : // 106
            * ( ( Word* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_TABLES_IN_SELECT" ));
            break;

        case SQL_NULL_COLLATION : // 85
            * ( ( Word* ) pInfoValuePtr ) = SQL_NC_START;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_NULL_COLLATION" ));
            break;

        case SQL_ORDER_BY_COLUMNS_IN_SELECT : // 90
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "N", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_ORDER_BY_COLUMNS_IN_SELECT" ));
            break;

        case SQL_PROCEDURE_TERM : // 40
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "procedure", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_PROCEDURE_TERM" ));
            break;

        case SQL_OWNER_USAGE : // 91 called
            * ( ( Word* ) pInfoValuePtr ) = SQL_SU_DML_STATEMENTS | SQL_SU_TABLE_DEFINITION | SQL_SU_PRIVILEGE_DEFINITION;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_OWNER_USAGE" ));
            break;

        case SQL_SUBQUERIES : // 95
            * ( ( Long* ) pInfoValuePtr ) = SQL_SQ_CORRELATED_SUBQUERIES |
                SQL_SQ_COMPARISON |
                SQL_SQ_EXISTS |
                SQL_SQ_IN |
                SQL_SQ_QUANTIFIED;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_SUBQUERIES" ));
            break;

        case SQL_MULT_RESULT_SETS : // 36:
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MULT_RESULT_SETS" ));
            break;

        case SQL_SERVER_NAME : // 13
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16,
                                                  ( ( pODBCConn ) pConn ) -> Server ? ( ( pODBCConn ) pConn ) -> Server : "", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_SERVER_NAME" ));
            break;

        case SQL_ACCESSIBLE_PROCEDURES : // 20
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_ACCESSIBLE_PROCEDURES" ));
            break;

        case SQL_ACCESSIBLE_TABLES : // 19
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_ACCESSIBLE_TABLES" ));
            break;

        case SQL_OJ_CAPABILITIES : // 115 called
            * ( ( Long* ) pInfoValuePtr ) = SQL_OJ_LEFT |
                SQL_OJ_RIGHT |
                SQL_OJ_FULL |
                SQL_OJ_NESTED |
                SQL_OJ_NOT_ORDERED |
                SQL_OJ_INNER |
                SQL_OJ_ALL_COMPARISON_OPS;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_OJ_CAPABILITIES" ));
            break;

        case SQL_DRIVER_VER : // 7 called
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "01.00.00000", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_DRIVER_VER" ));
            break;

        case SQL_LIKE_ESCAPE_CLAUSE : // 113
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_LIKE_ESCAPE_CLAUSE" ));
            break;

        case SQL_SPECIAL_CHARACTERS : // 94 //called by tb
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "$_", -1 );
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_SPECIAL_CHARACTERS" ));
            break;

        case SQL_MAX_COLUMNS_IN_GROUP_BY : // 97
            * ( ( Word* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_COLUMNS_IN_GROUP_BY" ));
            break;

        case SQL_MAX_COLUMNS_IN_INDEX : // 98
            * ( ( Word* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_COLUMNS_IN_INDEX" ));
            break;

        case SQL_MAX_COLUMNS_IN_ORDER_BY : // 99
            * ( ( Word* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_COLUMNS_IN_ORDER_BY" ));
            break;

        case SQL_MAX_COLUMNS_IN_SELECT : // 100
            * ( ( Word* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_COLUMNS_IN_SELECT" ));
            break;

        case SQL_MAX_COLUMNS_IN_TABLE : // 101
            * ( ( Word* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_MAX_COLUMNS_IN_TABLE" ));
            break;

        case SQL_NUMERIC_FUNCTIONS : // 49 called
            * ( ( Long* ) pInfoValuePtr ) = 0; // ???? set of standard numeric functions
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_NUMERIC_FUNCTIONS" ));
            break;

        case SQL_OUTER_JOINS : // 38
            _SQLCopyCharData ( _DIAGCONN ( pConn ), pInfoValuePtr, pBufferLength, pStringLengthPtr, 16, "Y", -1 );
            // __ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_OUTER_JOINS" ));
            break;

        case SQL_STRING_FUNCTIONS : // 50 called
            * ( ( Long* ) pInfoValuePtr ) = 0; // ???? set of standard string functions
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_STRING_FUNCTIONS" ));
            break;

        case SQL_SYSTEM_FUNCTIONS : // 51 called
            * ( ( Long* ) pInfoValuePtr ) = 0;
            //__ODBCPOPMSG(_ODBCPopMsg("SQLGetInfo, SQL_SYSTEM_FUNCTIONS" ));
            break;

            /*
                Note: The information type was introduced in ODBC 1.0; each bitmask is labeled with the version in which it was introduced.
                An SQLUINTEGER bitmask enumerating the scalar date and time functions supported by the driver and associated data source.
                The following bitmasks are used to determine which date and time functions are supported:
                SQL_FN_TD_CURRENT_DATE ODBC 3.0)SQL_FN_TD_CURRENT_TIME (ODBC 3.0)SQL_FN_TD_CURRENT_TIMESTAMP (ODBC 3.0)SQL_FN_TD_CURDATE (ODBC 1.0)SQL_FN_TD_CURTIME (ODBC 1.0) SQL_FN_TD_DAYNAME (ODBC 2.0)SQL_FN_TD_DAYOFMONTH (ODBC 1.0)SQL_FN_TD_DAYOFWEEK (ODBC 1.0)SQL_FN_TD_DAYOFYEAR (ODBC 1.0) SQL_FN_TD_EXTRACT (ODBC 3.0)SQL_FN_TD_HOUR (ODBC 1.0)SQL_FN_TD_MINUTE (ODBC 1.0)SQL_FN_TD_MONTH (ODBC 1.0)SQL_FN_TD_MONTHNAME (ODBC 2.0)SQL_FN_TD_NOW (ODBC 1.0)SQL_FN_TD_QUARTER (ODBC 1.0)SQL_FN_TD_SECOND (ODBC 1.0)SQL_FN_TD_TIMESTAMPADD (ODBC 2.0)SQL_FN_TD_TIMESTAMPDIFF (ODBC 2.0)SQL_FN_TD_WEEK (ODBC 1.0)SQL_FN_TD_YEAR (ODBC 1.0)
            */
        case SQL_TIMEDATE_FUNCTIONS : // 52 called
            * ( ( Long* ) pInfoValuePtr ) =
                SQL_FN_TD_CURRENT_DATE |
                SQL_FN_TD_CURRENT_TIME |
                SQL_FN_TD_CURRENT_TIMESTAMP |
                SQL_FN_TD_CURDATE |
                SQL_FN_TD_CURTIME |
                SQL_FN_TD_DAYNAME |
                SQL_FN_TD_DAYOFMONTH |
                SQL_FN_TD_DAYOFWEEK |
                SQL_FN_TD_DAYOFYEAR |
                SQL_FN_TD_EXTRACT |
                SQL_FN_TD_HOUR |
                SQL_FN_TD_MINUTE |
                SQL_FN_TD_MONTH |
                SQL_FN_TD_MONTHNAME |
                SQL_FN_TD_NOW |
                SQL_FN_TD_QUARTER |
                SQL_FN_TD_SECOND |
                SQL_FN_TD_TIMESTAMPADD |
                SQL_FN_TD_TIMESTAMPDIFF |
                SQL_FN_TD_WEEK |
                SQL_FN_TD_YEAR ;
            break;

        default :
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_ERROR,
                "Function Missing!!! SQLGetInfo, Field: %d, DataPtr: %d, BufLen: %d, SizePtr: %d\n", pInfoType, pInfoValuePtr,
                pBufferLength, pStringLengthPtr ) );
    }

    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to get type related information, this communicates with the server
// -----------------------------------------------------------------------
RETCODE SQL_API _SQLGetTypeInfoBasic ( HSTMT pStmt, SWORD pDataType )
{
    __CHK_HANDLE(pStmt,SQL_HANDLE_STMT,SQL_ERROR);

    std::unique_ptr <SQLResponse> p;
    try
    {
        wstring info = U("{\"columnMetas\":[{\"isNullable\":0,\"displaySize\":10,\"label\":\"TYPE_NAME\",\"name\":\"TYPE_NAME\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":128,\"scale\":0,\"columnType\":12,\"columnTypeName\":\"VARCHAR(128)\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":0,\"displaySize\":5,\"label\":\"DATA_TYPE\",\"name\":\"DATA_TYPE\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":5,\"scale\":0,\"columnType\":5,\"columnTypeName\":\"SMALLINT\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":1,\"displaySize\":10,\"label\":\"COLUMN_SIZE\",\"name\":\"COLUMN_SIZE\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":10,\"scale\":0,\"columnType\":4,\"columnTypeName\":\"INTEGER\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":1,\"displaySize\":10,\"label\":\"LITERAL_PREFIX?\",\"name\":\"LITERAL_PREFIX?\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":128,\"scale\":0,\"columnType\":12,\"columnTypeName\":\"VARCHAR(128)\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":1,\"displaySize\":10,\"label\":\"LITERAL_SUFFIX?\",\"name\":\"LITERAL_SUFFIX?\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":128,\"scale\":0,\"columnType\":12,\"columnTypeName\":\"VARCHAR(128)\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":1,\"displaySize\":10,\"label\":\"CREATE_PARAMS?\",\"name\":\"CREATE_PARAMS?\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":128,\"scale\":0,\"columnType\":12,\"columnTypeName\":\"VARCHAR(128)\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":0,\"displaySize\":5,\"label\":\"NULLABLE\",\"name\":\"NULLABLE\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":5,\"scale\":0,\"columnType\":5,\"columnTypeName\":\"SMALLINT\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":0,\"displaySize\":5,\"label\":\"CASE_SENSITIVE?\",\"name\":\"CASE_SENSITIVE?\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":5,\"scale\":0,\"columnType\":5,\"columnTypeName\":\"SMALLINT\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":0,\"displaySize\":5,\"label\":\"SEARCHABLE?\",\"name\":\"SEARCHABLE?\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":5,\"scale\":0,\"columnType\":5,\"columnTypeName\":\"SMALLINT\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":1,\"displaySize\":5,\"label\":\"UNSIGNED_ATTRIBUTE??\",\"name\":\"UNSIGNED_ATTRIBUTE??\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":5,\"scale\":0,\"columnType\":5,\"columnTypeName\":\"SMALLINT\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":0,\"displaySize\":5,\"label\":\"FIXED_PREC_SCALE???\",\"name\":\"FIXED_PREC_SCALE???\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":5,\"scale\":0,\"columnType\":5,\"columnTypeName\":\"SMALLINT\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":1,\"displaySize\":5,\"label\":\"AUTO_UNIQUE_VALUE?\",\"name\":\"AUTO_UNIQUE_VALUE???\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":5,\"scale\":0,\"columnType\":5,\"columnTypeName\":\"SMALLINT\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":1,\"displaySize\":5,\"label\":\"LOCAL_TYPE_NAME???\",\"name\":\"LOCAL_TYPE_NAME???\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":128,\"scale\":0,\"columnType\":12,\"columnTypeName\":\"VARCHAR(128)\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":1,\"displaySize\":5,\"label\":\"MINIMUM_SCALE???\",\"name\":\"MINIMUM_SCALE???\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":5,\"scale\":0,\"columnType\":5,\"columnTypeName\":\"SMALLINT\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":1,\"displaySize\":5,\"label\":\"MAXIMUM_SCALE???\",\"name\":\"MAXIMUM_SCALE???\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":5,\"scale\":0,\"columnType\":5,\"columnTypeName\":\"SMALLINT\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":0,\"displaySize\":5,\"label\":\"SQL_DATA_TYPE?? ?\",\"name\":\"SQL_DATA_TYPE?? ?\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":5,\"scale\":0,\"columnType\":5,\"columnTypeName\":\"SMALLINT\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":1,\"displaySize\":5,\"label\":\"SQL_DATETIME_SUB?? ??\",\"name\":\"SQL_DATETIME_SUB?? ??\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":5,\"scale\":0,\"columnType\":5,\"columnTypeName\":\"SMALLINT\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":1,\"displaySize\":10,\"label\":\"NUM_PREC_RADIX?? ? ?\",\"name\":\"NUM_PREC_RADIX?? ? ?\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":10,\"scale\":0,\"columnType\":4,\"columnTypeName\":\"INTEGER\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false},{\"isNullable\":1,\"displaySize\":5,\"label\":\"INTERVAL_PRECISION?? ? ?\",\"name\":\"INTERVAL_PRECISION?? ? ?\",\"schemaName\":null,\"catelogName\":null,\"tableName\":null,\"precision\":5,\"scale\":0,\"columnType\":5,\"columnTypeName\":\"SMALLINT\",\"readOnly\":true,\"writable\":false,\"caseSensitive\":true,\"searchable\":false,\"currency\":false,\"signed\":true,\"autoIncrement\":false,\"definitelyWritable\":false}],\"results\":[[\"char\",\"1\",\"255\",\"'\",\"'\",null,\"1\",\"0\",\"3\",null,\"0\",null,\"char\",null,null,\"1\",null,null,null]],\"affectedRowCount\":0,\"isException\":false,\"exceptionMessage\":null,\"duration\":0,\"partial\":false}");
        web::json::value v = web::json::value::parse ( info );
        p = SQLResponseFromJSON ( v );
    }
    catch ( const exception& e )
    {
        std::stringstream ss;
        ss << "The error message is: " << e . what ();
        std::string s = ss . str ();
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_ERROR, s.c_str() ) );
        _SQLPutDiagRow ( SQL_HANDLE_STMT, pStmt, "SQLGetTypeInfoW", "01000", -1, ( char* ) s . c_str () );
        return SQL_ERROR;
    }

    if ( p == NULL || p -> isException == true || PutRespToStmt ( ( pODBCStmt ) pStmt, std::move ( p ) ) != GOOD )
    {
        return SQL_ERROR;
    }
    return SQL_SUCCESS;
}

RETCODE SQL_API SQLGetTypeInfo ( HSTMT pStmt, SWORD pDataType )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetTypeInfo called, Stmt: %d, DataType: %d", ( Long ) pStmt, pDataType ) );

    return _SQLGetTypeInfoBasic ( pStmt, pDataType );
}

RETCODE SQL_API SQLGetTypeInfoW ( HSTMT pStmt, SWORD pDataType )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetTypeInfoW called, Stmt: %d, DataType: %d", ( Long ) pStmt, pDataType ) );

    return _SQLGetTypeInfoBasic ( pStmt, pDataType );
}

// -----------------------------------------------------------------------
// to get function related information
// -----------------------------------------------------------------------

RETCODE SQL_API SQLGetFunctions ( SQLHDBC pConn, SQLUSMALLINT pFuncID, SQLUSMALLINT* pOutput )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetFunctions called" ) );
    // the functions is currently not being exported
    // bcoz of implemenation difficulties for
    // SQL_API_ODBC3_ALL_FUNCTIONS param
    // which requires an array to be built
    // to satisfy the functioning of the macro  SQL_FUNC_EXISTS.
    // in any case the driver manager provides for a proper
    // stub
    // SQLFUNCEXISTS ---- ( ( *(((UWORD*) (pfExists)) + ((uwAPI) >> 4)) & (1 << ((uwAPI) & 0x000F))) ? SQL_TRUE : SQL_FALSE )
    return SQL_SUCCESS;
}

