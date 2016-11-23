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
// File:    KO_DTYPE.CPP
//
// Purpose: Contains functions related to data types as per ODBC.
//          For defining the type of data ODBC uses three params
//          Concise Type, Verbose Type and DateTime Interval.
//          All of these r intertwined, setting one of these
//          influences the other two.
//
//          These r stored inside the descriptors and accessed via
//          descriptor functions defined in KO_DESC.CPP. These
//          functions in turn call the _SQLSetDataType function
//          from this file.
//
//          _SQLCheckDataType is to check if the data type recd. from
//          the server is valid or not.
//
//          GetIRDDataTypeDefaults is used to get the default values
//          for a data type. The string data type like CHAR, VARCHAR
//          is passed to this function and the default length, precision
//          nullability is obtained. This is then overriden by the caller
//          with the specific values recd. from the server
//
// ----------------------------------------------------------------------------

#include "stdafx.h"


// ---------------------- function prototypes --------------------------
eGoodBad _SQLCheckDataType ( Word pDataType );
eGoodBad _SQLCheckIntervalCode ( Word pIntervalCode );
eGoodBad _SQLSetDataType ( pODBCDiag pDiag, Word pFldID, Word pFldValue, Word* pVerboseDataType,
                           Word* pConciseDataType, Word* pDateTimeIntervalCode );
eGoodBad GetIRDDataTypeDefaults ( CStrPtr pDataType, Word* pSqlDataType, Word* pNativeDataType,
                                  Word* pPrecision, Word* pScale, Long* pLength, Long* pRadix );

// -----------------------------------------------------------------------
// to check if the specified type is a valid SQL data type
// -----------------------------------------------------------------------

eGoodBad _SQLCheckDataType ( Word pDataType )
{
    switch ( pDataType )
    {
        case SQL_CHAR :
        case SQL_VARCHAR :
        case SQL_WCHAR :
        case SQL_WVARCHAR :
        case SQL_C_SSHORT :
        case SQL_C_USHORT :
        case SQL_SMALLINT :
        case SQL_C_SLONG :
        case SQL_C_ULONG :
        case SQL_INTEGER :
        case SQL_NUMERIC :
        case SQL_DECIMAL :
        case SQL_FLOAT :
        case SQL_REAL :
        case SQL_DOUBLE :
        case SQL_TYPE_DATE :
        case SQL_TYPE_TIME :
        case SQL_TYPE_TIMESTAMP :
        case SQL_BIT :
        case SQL_DEFAULT :
        case SQL_BIGINT :
        case SQL_C_SBIGINT :
        case SQL_C_UBIGINT :
        case SQL_C_TINYINT :
        case SQL_C_STINYINT :
        case SQL_C_UTINYINT :
            return GOOD;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLCheckDataType: Unknown data type: %d", pDataType ) );
            return BAD;
    }
}


// -----------------------------------------------------------------------
// to check if the specified type is a valid interval code
// -----------------------------------------------------------------------

eGoodBad _SQLCheckIntervalCode ( Word pIntervalCode )
{
    // can check the interval code
    return GOOD;
}


// -----------------------------------------------------------------------
// to set the value of data type and interval codes in descriptors
// -----------------------------------------------------------------------

eGoodBad _SQLSetDataType ( pODBCDiag pDiag, Word pFldID, Word pFldValue, Word* pVerboseDataType,
                           Word* pConciseDataType, Word* pDateTimeIntervalCode )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLSetDataType is called, pFldID:%d, pFldValue:%d", pFldID, pFldValue ) );
    // note
    // the data type, concise data type, datetime interval code r interdependent
    // setting one of these changes the other.
    Word concise_type;
    Word verbose_type;
    Word datetime_interval;
    // initial values
    concise_type = pConciseDataType ? *pConciseDataType : 0;
    verbose_type = pVerboseDataType ? *pVerboseDataType : 0;
    datetime_interval = pDateTimeIntervalCode ? *pDateTimeIntervalCode : 0;

    // check if concise type has been specified
    if ( pFldID == SQL_DESC_CONCISE_TYPE )
    {
        // set the concise type itself first
        concise_type = pFldValue;

        // based on concise type set the verbose type and datetime interval
        switch ( concise_type )
        {
            case SQL_TYPE_DATE :
                //case SQL_C_TYPE_DATE:
                verbose_type = SQL_DATETIME;
                datetime_interval = SQL_CODE_DATE;
                break;

            case SQL_TYPE_TIME :
                //case SQL_C_TYPE_TIME:
                verbose_type = SQL_DATETIME;
                datetime_interval = SQL_CODE_TIME;
                break;

            case SQL_TYPE_TIMESTAMP :
                //case SQL_C_TYPE_TIMESTAMP:
                verbose_type = SQL_DATETIME;
                datetime_interval = SQL_CODE_TIMESTAMP;
                break;

            case SQL_INTERVAL_MONTH :
                //case SQL_C_INTERVAL_MONTH:
                verbose_type = SQL_INTERVAL;
                datetime_interval = SQL_CODE_MONTH;
                break;

            case SQL_INTERVAL_YEAR :
                //case SQL_C_INTERVAL_YEAR:
                verbose_type = SQL_INTERVAL;
                datetime_interval = SQL_CODE_YEAR;
                break;

            case SQL_INTERVAL_YEAR_TO_MONTH :
                //case SQL_C_INTERVAL_YEAR_TO_MONTH:
                verbose_type = SQL_INTERVAL;
                datetime_interval = SQL_CODE_YEAR_TO_MONTH;
                break;

            case SQL_INTERVAL_DAY :
                //case SQL_C_INTERVAL_DAY:
                verbose_type = SQL_INTERVAL;
                datetime_interval = SQL_CODE_DAY;
                break;

            case SQL_INTERVAL_HOUR :
                //case SQL_C_INTERVAL_HOUR:
                verbose_type = SQL_INTERVAL;
                datetime_interval = SQL_CODE_HOUR;
                break;

            case SQL_INTERVAL_MINUTE :
                //case SQL_C_INTERVAL_MINUTE:
                verbose_type = SQL_INTERVAL;
                datetime_interval = SQL_CODE_MINUTE;
                break;

            case SQL_INTERVAL_SECOND :
                //case SQL_C_INTERVAL_SECOND:
                verbose_type = SQL_INTERVAL;
                datetime_interval = SQL_CODE_SECOND;
                break;

            case SQL_INTERVAL_DAY_TO_HOUR :
                //case SQL_C_INTERVAL_DAY_TOHOUR:
                verbose_type = SQL_INTERVAL;
                datetime_interval = SQL_CODE_DAY_TO_HOUR;
                break;

            case SQL_INTERVAL_DAY_TO_MINUTE :
                //case SQL_C_INTERVAL_DAY_TO_MINUTE:
                verbose_type = SQL_INTERVAL;
                datetime_interval = SQL_CODE_DAY_TO_MINUTE;
                break;

            case SQL_INTERVAL_DAY_TO_SECOND :
                //case SQL_C_INTERVAL_DAY_TO_SECOND:
                verbose_type = SQL_INTERVAL;
                datetime_interval = SQL_CODE_DAY_TO_SECOND;
                break;

            case SQL_INTERVAL_HOUR_TO_MINUTE :
                //case SQL_C_INTERVAL_HOUR_TO_MINUTE:
                verbose_type = SQL_INTERVAL;
                datetime_interval = SQL_CODE_HOUR_TO_MINUTE;
                break;

            case SQL_INTERVAL_HOUR_TO_SECOND :
                //case SQL_C_INTERVAL_HOUR_TO_SECOND:
                verbose_type = SQL_INTERVAL;
                datetime_interval = SQL_CODE_HOUR_TO_SECOND;
                break;

            case SQL_INTERVAL_MINUTE_TO_SECOND :
                //case SQL_C_INTERVAL_MINUTE_TO_SECOND:
                verbose_type = SQL_INTERVAL;
                datetime_interval = SQL_CODE_MINUTE_TO_SECOND;
                break;

            default :

                // check if data type if valid
                if ( _SQLCheckDataType ( concise_type ) != GOOD )
                {
                    return BAD;
                }

                // concise type does not relate to datetime or interval
                // hence both concise and verbose type r equal
                verbose_type = concise_type;
                datetime_interval = 0;
        }
    }

    // check if verbose data type is being set
    else if ( pFldID == SQL_DESC_TYPE )
    {
        // set the verbose type itself first
        verbose_type = pFldValue;

        // based on verbose type & datetime interval code determine concise type
        switch ( verbose_type )
        {
            case SQL_INTERVAL :
                switch ( datetime_interval )
                {
                    case SQL_CODE_DATE :
                        concise_type = SQL_TYPE_DATE;
                        break;

                    case SQL_CODE_TIME :
                        concise_type = SQL_TYPE_TIME;
                        break;

                    case SQL_CODE_TIMESTAMP :
                        concise_type = SQL_TYPE_TIMESTAMP;
                        break;

                    default :
                        // interval should have been set
                        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetDataType: Interval code not yet set for SQL_INTERVAL" ) );
                        return BAD;
                }

                break;

            case SQL_DATETIME :
                switch ( datetime_interval )
                {
                    case SQL_CODE_MONTH :
                        concise_type = SQL_INTERVAL_MONTH;
                        break;

                    case SQL_CODE_YEAR :
                        concise_type = SQL_INTERVAL_YEAR;
                        break;

                    case SQL_CODE_YEAR_TO_MONTH :
                        concise_type = SQL_INTERVAL_YEAR_TO_MONTH;
                        break;

                    case SQL_CODE_DAY :
                        concise_type = SQL_INTERVAL_DAY;
                        break;

                    case SQL_CODE_HOUR :
                        concise_type = SQL_INTERVAL_HOUR;
                        break;

                    case SQL_CODE_MINUTE :
                        concise_type = SQL_INTERVAL_MINUTE;
                        break;

                    case SQL_CODE_SECOND :
                        concise_type = SQL_INTERVAL_SECOND;
                        break;

                    case SQL_CODE_DAY_TO_HOUR :
                        concise_type = SQL_INTERVAL_DAY_TO_HOUR;
                        break;

                    case SQL_CODE_DAY_TO_MINUTE :
                        concise_type = SQL_INTERVAL_DAY_TO_MINUTE;
                        break;

                    case SQL_CODE_DAY_TO_SECOND :
                        concise_type = SQL_INTERVAL_DAY_TO_SECOND;
                        break;

                    case SQL_CODE_HOUR_TO_MINUTE :
                        concise_type = SQL_INTERVAL_HOUR_TO_MINUTE;
                        break;

                    case SQL_CODE_HOUR_TO_SECOND :
                        concise_type = SQL_INTERVAL_HOUR_TO_SECOND;
                        break;

                    case SQL_CODE_MINUTE_TO_SECOND :
                        concise_type = SQL_INTERVAL_MINUTE_TO_SECOND;
                        break;

                    default :
                        // interval should have been set
                        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetDataType: Interval code not yet set for SQL_DATETIME" ) );
                        return BAD;
                }

                break;

            default :

                // check if data type if valid
                if ( _SQLCheckDataType ( verbose_type ) != GOOD )
                {
                    return BAD;
                }

                // verbose type does not relate to datetime or interval
                // hence both concise and verbose type r equal
                concise_type = verbose_type;
                datetime_interval = 0;
                break;
        }
    }

    else if ( pFldID == SQL_DESC_DATETIME_INTERVAL_CODE )
    {
        // check if date interval code is valid
        if ( _SQLCheckIntervalCode ( pFldValue ) != GOOD )
        {
            return BAD;
        }

        // set the datetime interval code, autonomously
        datetime_interval = pFldValue;
    }

    // unknown field to set
    else
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetDataType: Unknown field type" ) );
        return BAD;
    }

    // pass back values to caller
    if ( pVerboseDataType )
    {
        *pVerboseDataType = verbose_type;
    }

    if ( pConciseDataType )
    {
        *pConciseDataType = concise_type;
    }

    if ( pDateTimeIntervalCode )
    {
        *pDateTimeIntervalCode = datetime_interval;
    }

    return GOOD;
}

// -----------------------------------------------------------------------
// to get specific detail about a data type
// -----------------------------------------------------------------------

eGoodBad GetIRDDataTypeDefaults ( CStrPtr pDataType, Word* pSqlDataType, Word* pNativeDataType, Word* pPrecision,
                                  Word* pScale, Long* pLength, Long* pRadix )
{
    return BAD;
}

