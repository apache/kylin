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
// File:    KO_UTILS.CPP
//
// Notes:   contains generic utility functions used across files/modules.
//          Data conversion and copying functions w.r.t ODBC type params
//          like its type, size, its size pointer, src data and src size etc.
//
//          As explained in the article ODBC specifies a way in which
//          buffers r laid out along with their size and return length
//          placeholders. The functions _SQLCopyCharData, _SQLCopyNumData
//          and _SQLCopyDateTimeData have been designed to copy char, numeric
//          and date time data from src ( buffer,size,type ) to tgt ( buffer,
//          size, type ) whichever params r applicable.
//
// ----------------------------------------------------------------------------------
#include "stdafx.h"

// ------------------------- local functions ----------------------------
eGoodBad GetDateFromString ( const char* pDateStr, struct tagDATE_STRUCT* pDateStruct );
eGoodBad GetTimestampFromString ( const char* pDateStr, struct tagTIMESTAMP_STRUCT* pTimestampStruct );


// ----------------------------------------------------------------------
// to extract date from string assuming server format to be yyyy-mm-dd or yyyymmdd
// ----------------------------------------------------------------------

eGoodBad GetDateFromString ( const char* pDateStr, struct tagDATE_STRUCT* pDateStruct )
{
    char val[5];
    short x;
    short day, month;
    // length of source
    x = strlen ( pDateStr );

    // 10 byte date yyyy-mm-dd, 8 byte date yyyymmdd
    if ( x == 8 || x == 10 )
    {
        // calc  pos of day and month in string
        if ( x == 8 )
        {
            day = 6;
            month = 4;
        }

        else
        {
            day = 8;
            month = 5;
        }

        // convert day value
        pDateStruct -> day = atoi ( pDateStr + day );
        // copy and convert month
        strncpy ( val, pDateStr + month, 2 );
        val[2] = 0;
        pDateStruct -> month = atoi ( val );
        strncpy ( val, pDateStr, 4 );
        val[4] = 0;
        pDateStruct -> year = atoi ( val );
        return GOOD;
    }

    else
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "Invalid date string for conversion: %s", pDateStr ) );
        return BAD;
    }
}

//Timestamps in text files have to use the format yyyy-mm-dd or yyyy-mm-dd hh:mm:ss[.f...]
eGoodBad GetTimestampFromString ( const char* pStr, struct tagTIMESTAMP_STRUCT* pTimestampStruct )
{
    char val[10];
    short x;
    short day, month, hour, minute, second, frag;
    // length of source
    x = strlen ( pStr );
    const char* p = pStr;

    while ( ( *p != ' ' ) && ( p < pStr + x ) )
    {
        p++;
    }

    if ( ( p - pStr ) != 10 || ( x < 19 && x != 10 ) )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "Invalid timestamp string for conversion: %s", pStr ) );
        return BAD;
    }

    month = 5;
    day = 8;
    hour = 11;
    minute = 14;
    second = 17;
    frag = 20;
    // convert day value
    strncpy ( val, pStr + day, 2 );
    val[2] = 0;
    pTimestampStruct -> day = atoi ( val );
    // copy and convert month
    strncpy ( val, pStr + month, 2 );
    val[2] = 0;
    pTimestampStruct -> month = atoi ( val );
    //convert year
    strncpy ( val, pStr, 4 );
    val[4] = 0;
    pTimestampStruct -> year = atoi ( val );

    if ( x > 10 )
    {
        //convert hour
        strncpy ( val, pStr + hour, 2 );
        val[2] = 0;
        pTimestampStruct -> hour = atoi ( val );
        //convert minute
        strncpy ( val, pStr + minute, 2 );
        val[2] = 0;
        pTimestampStruct -> minute = atoi ( val );
        //convert second
        strncpy ( val, pStr + second, 2 );
        val[2] = 0;
        pTimestampStruct -> second = atoi ( val );
    }
    else
    {
        pTimestampStruct -> hour = 0;
        pTimestampStruct -> minute = 0;
        pTimestampStruct -> second = 0;
    }

    if ( x >= 21 )
    {
        pTimestampStruct -> fraction = atoi ( pStr + frag );
    }

    else
    {
        pTimestampStruct -> fraction = 0;
    }

    return GOOD;
}

// ----------------------------------------------------------------------
// to create a copy of char data to specified tgt buffer
// ----------------------------------------------------------------------

RETCODE SQL_API _SQLCopyCharData ( pODBCDiag pDiag, void* pTgtDataPtr, Long pDataBufSize, void* pSizePtr,
                                   Word pSizePtrSize, CStrPtr pSrcData, Long pSrcDataSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "_SQLCopyCharData called,pDataBufSize %d, the src is %s, strlen(src) %d, pSrcDataSize %d", pDataBufSize, pSrcData,
        strlen ( pSrcData ), pSrcDataSize ) );
    Long n;

    // caller safe
    if ( pTgtDataPtr )
    {
        * ( ( StrPtr ) pTgtDataPtr ) = 0;
    }

    // DATA SIZE

    // check source data to compute size
    if ( pSrcData && _stricmp ( ( StrPtr ) pSrcData, "NULL" ) != 0 )
    {
        n = ( pSrcDataSize < 0 ) ? strlen ( ( StrPtr ) pSrcData ) : pSrcDataSize;
    } // compute length based on whether null terminated

    else
    {
        n = 0;
    }

    // check if there is a holder for size
    if ( pSizePtr )
    {
        // set size as per ptr type 16-bt or 32-bit
        if ( pSizePtrSize == 16 )
        {
            * ( ( Word* ) pSizePtr ) = ( Word ) n;
        }

        else
        {
            * ( ( Long* ) pSizePtr ) = n;
        }
    }

    // check if src data but no size holder
    else if ( pSrcData )
    {
        // check if diag to be set
        if ( pDiag )
        {
            _SQLPutDiagRow ( pDiag, "_SQLCopyCharData", "01000", -1, "No holder for data size", NULL );
        }

        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLCopyCharData - No holder for data size" ) );
        return SQL_ERROR;
    }

    // check if there is a target holder
    if ( pTgtDataPtr )
    {
        // check if there is a source pointer
        if ( pSrcData )
        {
            // does all of it fit with null char
            if ( pDataBufSize >= n + 1 )
            {
                memcpy ( ( StrPtr ) pTgtDataPtr, pSrcData, n );
                ( ( StrPtr ) pTgtDataPtr )[n] = 0;
                __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLCopyCharData has been called, the string(not truncated) is %s",
                    pTgtDataPtr ) );
                return SQL_SUCCESS;
            }

            // all of it does not fit
            else
            {
                memcpy ( ( StrPtr ) pTgtDataPtr, pSrcData, pDataBufSize - 1 );
                ( ( StrPtr ) pTgtDataPtr )[pDataBufSize - 1] = 0;
                __ODBCLOG ( _ODBCLogMsg ( LogLevel_WARN, "_SQLCopyCharData has been called, the target string is (truncated) %s",
                    pTgtDataPtr ) );
                //return SQL_SUCCESS_WITH_INFO may cause error in tableau
                //if ( pDiag )
                //  _SQLPutDiagRow ( pDiag, "_SQLCopyCharData", "01000", -1, "string data truncated", NULL );
                //return SQL_SUCCESS_WITH_INFO;
                __ODBCLOG ( _ODBCLogMsg ( LogLevel_WARN, "string data truncated" ) );
                return SQL_SUCCESS;
            }
        }

        // tgt data but no src data
        else
        {
            // clear tgt
            * ( ( Char* ) pTgtDataPtr ) = 0;
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLCopyCharData has been called, the string is (empty) %s", pTgtDataPtr ) );
        }
    }

    return SQL_SUCCESS;
}

RETCODE SQL_API _SQLCopyWCharData ( pODBCDiag pDiag, void* pTgtDataPtr, Long pDataBufSize, void* pSizePtr,
                                    Word pSizePtrSize, CStrPtr pSrcData, Long pSrcDataSize, bool returnByteSize )
{
    unique_ptr <wchar_t[]> pWCS ( char2wchar ( pSrcData ) );
    return _SQLCopyWCharDataW ( pDiag, pTgtDataPtr, pDataBufSize, pSizePtr, pSizePtrSize, pWCS . get (), pSrcDataSize, returnByteSize );
}

//mhb added, for those ard that accept wchar
RETCODE SQL_API _SQLCopyWCharDataW ( pODBCDiag pDiag, void* pTgtDataPtr, Long pDataBufSize, void* pSizePtr,
                                     Word pSizePtrSize, const wchar_t* pSrcData, Long pSrcDataSize, bool returnByteSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLCopyWCharDataW called, pTgtDataPtr is null? %d, pSizePtr == null? %d",
        pTgtDataPtr == NULL, pSizePtr == NULL ) );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLCopyWCharDataW called, the src string is :" ) );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, pSrcData ) );
    Long n;

    // caller safe
    if ( pTgtDataPtr )
    {
        * ( ( wchar_t* ) pTgtDataPtr ) = 0;
    }

    // DATA SIZE

    // check source data to compute size
    if ( pSrcData && _wcsicmp ( pSrcData, L"NULL" ) != 0 )
    {
        n = ( pSrcDataSize < 0 ) ? wcslen ( pSrcData ) : pSrcDataSize;
    } // compute length based on whether null terminated

    else
    {
        n = 0;
    }

    // check if there is a holder for size
    if ( pSizePtr )
    {
        // set size as per ptr type 16-bt or 32-bit

        //should be number of characters
        Long pPtrSizeBuf = n;
        if ( returnByteSize )
        {
            pPtrSizeBuf = 2 * n;
        }

        if ( pSizePtrSize == 16 )
        {
            * ( ( Word* ) pSizePtr ) = ( Word ) pPtrSizeBuf;
        }

        else
        {
            * ( ( Long* ) pSizePtr ) = pPtrSizeBuf;
        }

        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "pSizePtr %d is set to %d", pSizePtr, * ( ( SQLLEN* ) pSizePtr ) ) );
    }

    // check if src data but no size holder
    else if ( pSrcData )
    {
        // check if diag to be set
        if ( pDiag )
        {
            _SQLPutDiagRow ( pDiag, "_SQLCopyWCharDataW", "01000", -1, "No holder for data size", NULL );
        }

        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLCopyWCharDataW - No holder for data size" ) );
        return SQL_ERROR;
    }

    // DATA

    // check if there is a target holder
    if ( pTgtDataPtr )
    {
        // check if there is a source pointer
        if ( pSrcData )
        {
            // does all of it fit with null char
            if ( pDataBufSize >= ( n + 1 ) )
            {
                memcpy ( ( StrPtr ) pTgtDataPtr, pSrcData, 2 * ( n + 1 ) );
                ( ( wchar_t* ) pTgtDataPtr )[n] = L'\0';
                //__ODBCLOG(_ODBCLogMsg(LogLevel_DEBUG,"_SQLCopyWCharDataW has been called, the target string(not truncated) is :"));
                //unique_ptr<char[]> temp2(wchar2char( (wchar_t*)pTgtDataPtr));
                //__ODBCLOG(_ODBCLogMsg(LogLevel_DEBUG,temp2.get()));
                return SQL_SUCCESS;
            }

            // all of it does not fit
            else
            {
                //if(pDataBufSize % 2 == 1)
                //  pDataBufSize -= 1;
                memcpy ( ( StrPtr ) pTgtDataPtr, pSrcData, 2 * ( pDataBufSize - 1 ) );
                ( ( wchar_t* ) pTgtDataPtr )[pDataBufSize - 1] = 0;
                //__ODBCLOG(_ODBCLogMsg(LogLevel_WARN,"_SQLCopyWCharDataW has been called, the target string is(truncated) :"));
                //unique_ptr<char[]> temp ( wchar2char ( ( wchar_t* ) pTgtDataPtr ) );
                //__ODBCLOG(_ODBCLogMsg(LogLevel_DEBUG, temp.get()));
                //return SQL_SUCCESS_WITH_INFO may cause error in tableau
                //if ( pDiag )
                //  _SQLPutDiagRow ( pDiag, "_SQLCopyWCharDataW", "01000", -1, "string data truncated", NULL );
                //return SQL_SUCCESS_WITH_INFO;
                __ODBCLOG ( _ODBCLogMsg ( LogLevel_WARN, "string data truncated" ) );
                return SQL_SUCCESS;
            }
        }

        // tgt data but no src data
        else
        {
            // clear tgt
            * ( ( wchar_t* ) pTgtDataPtr ) = 0;
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLCopyWCharDataW has been called, the string is (empty) %s",
                pTgtDataPtr ) );
        }
    }

    return SQL_SUCCESS;
}


// ----------------------------------------------------------------------
// to create a copy of numeric data to specified tgt buffer
// ----------------------------------------------------------------------


//TODO: it seems that the unsigned values are not treated specially
Word _SQLCopyNumData ( pODBCDiag pDiag, void* pTgtDataPtr, Word pTgtDataType, CStrPtr pSrcData, Word pSrcDataType,
                       Long* pTgtDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLCopyNumData called" ) );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The src is %s", pSrcData ) );
    // note
    // source data is received as character string
    // target data size indicates the type of int - 8bit, 16bit, 32bit, float, double etc
    // source data type is also recd. but is not being checked right now
    // this can be used to detrmine if the conversion is possible at all
    bool isnull;
    // check if source data is NULL
    isnull = ( !pSrcData || _stricmp ( pSrcData, "NULL" ) == 0 ) ? TRUE : 0;

    // check if target is there
    if ( pTgtDataPtr )
    {
        // check the data type
        switch ( pTgtDataType )
        {
            case SQL_C_UTINYINT :
                if ( !isnull )
                {
                    int i32;
                    i32 = atoi ( pSrcData );
                    * ( ( unsigned char* ) pTgtDataPtr ) = i32;
                }

                else
                {
                    memset ( pTgtDataPtr, 0, sizeof ( char) );
                }

                *pTgtDataSizePtr = sizeof ( char);
                __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The num is set to %d (unsigned char)",
                    * ( ( unsigned char* ) pTgtDataPtr ) ) );
                break;

            case SQL_C_STINYINT :
            case SQL_C_TINYINT :
                if ( !isnull )
                {
                    int i32;
                    i32 = atoi ( pSrcData );
                    * ( ( char* ) pTgtDataPtr ) = i32;
                }

                else
                {
                    memset ( pTgtDataPtr, 0, sizeof ( char) );
                }

                *pTgtDataSizePtr = sizeof ( char);
                __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The num is set to %d  (signed char)", * ( ( char* ) pTgtDataPtr ) ) );
                break;

            case SQL_C_USHORT : // unsigned short
                if ( !isnull )
                {
                    int i32;
                    i32 = atoi ( pSrcData );
                    * ( ( Word* ) pTgtDataPtr ) = i32;
                }

                else
                {
                    memset ( pTgtDataPtr, 0, sizeof ( Word) );
                }

                *pTgtDataSizePtr = sizeof ( Word);
                __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The num is set to %u (signed short)",
                    * ( ( unsigned short* ) pTgtDataPtr ) ) );
                break;

            case SQL_C_SHORT : // case i2
            case SQL_C_SSHORT : // signed short
                if ( !isnull )
                {
                    int i32;
                    i32 = atoi ( pSrcData );
                    * ( ( Word* ) pTgtDataPtr ) = i32;
                }

                else
                {
                    memset ( pTgtDataPtr, 0, sizeof ( Word) );
                }

                *pTgtDataSizePtr = sizeof ( Word);
                __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The num is set to %d (signed short)", * ( ( Word* ) pTgtDataPtr ) ) );
                break;

            case SQL_C_ULONG : // unsigned long
                if ( !isnull )
                {
                    unsigned long ui32;
                    ui32 = strtoul ( pSrcData, NULL, 10 );
                    * ( ( unsigned long* ) pTgtDataPtr ) = ui32;
                }

                else
                {
                    memset ( pTgtDataPtr, 0, sizeof ( unsigned long) );
                }

                *pTgtDataSizePtr = sizeof ( unsigned long);
                __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The num is set to %u (unsigned int)",
                    * ( ( unsigned long* ) pTgtDataPtr ) ) );
                break;

            case SQL_C_LONG : // case i4
            case SQL_C_SLONG : // signed long

                // ???? check src type
                if ( !isnull )
                {
                    long i32;
                    i32 = atol ( pSrcData );
                    * ( ( Long* ) pTgtDataPtr ) = i32;
                }

                else
                {
                    memset ( pTgtDataPtr, 0, sizeof ( Long) );
                }

                *pTgtDataSizePtr = sizeof ( Long);
                __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The num is set to %d (signed int)", * ( ( Long* ) pTgtDataPtr ) ) );
                break;

            case SQL_C_UBIGINT :
                if ( !isnull )
                {
                    unsigned __int64 x64;
                    x64 = _strtoui64 ( pSrcData, NULL, 10 );
                    * ( ( unsigned __int64* ) pTgtDataPtr ) = x64;
                }

                else
                {
                    memset ( pTgtDataPtr, 0, sizeof ( unsigned __int64) );
                }

                *pTgtDataSizePtr = sizeof ( unsigned __int64);
                __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The num is set to %I64u (unsigned big int)",
                    * ( ( unsigned __int64* ) pTgtDataPtr ) ) );
                break;

            case SQL_BIGINT :
            case SQL_C_SBIGINT :
                if ( !isnull )
                {
                    __int64 x64;
                    x64 = _strtoi64 ( pSrcData, NULL, 10 );
                    * ( ( __int64* ) pTgtDataPtr ) = x64;
                }

                else
                {
                    memset ( pTgtDataPtr, 0, sizeof ( __int64) );
                }

                *pTgtDataSizePtr = sizeof ( __int64);
                __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The num is set to %I64d (signed big int)",
                    * ( ( __int64* ) pTgtDataPtr ) ) );
                break;

                //case SQL_DECIMAL: //decimal type has a special struct
            case SQL_FLOAT :
            case SQL_C_FLOAT :
                if ( !isnull )
                {
                    // ???? check src type
                    double f;
                    f = atof ( pSrcData );
                    * ( ( float* ) pTgtDataPtr ) = ( float ) f;
                }

                else
                {
                    memset ( pTgtDataPtr, 0, sizeof ( float) );
                }

                *pTgtDataSizePtr = sizeof ( float);
                __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The num is set to %9.9f (float)", * ( ( float* ) pTgtDataPtr ) ) );
                break;

                //case SQL_REAL:
                //case SQL_NUMERIC:                           // case float

            case SQL_C_DOUBLE :
                if ( !isnull )
                {
                    // ???? check src type
                    char* e;
                    double d;

                    if ( pSrcDataType == SQL_BIT )
                    {
                        d = 1;
                    }

                    else
                    {
                        d = strtod ( pSrcData, &e );
                    }

                    * ( ( double* ) pTgtDataPtr ) = d;
                }

                else
                {
                    memset ( pTgtDataPtr, 0, sizeof ( double) );
                }

                *pTgtDataSizePtr = sizeof ( double);
                __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The num is set to %9.9f (double), with the pTgtDataSizePtr %d",
                    * ( ( double* ) pTgtDataPtr ), *pTgtDataSizePtr ) );
                break;

            default :
                return 1; // data type not understood
        }

        return 0; // successful, at least data type recognized
    }

    else
    {
        return -1;
    } // should not typically happen
}


// ----------------------------------------------------------------------
// to create a copy of date/time data to specified tgt buffer
// ----------------------------------------------------------------------

Word _SQLCopyDateTimeData ( pODBCDiag pDiag, void* pTgtDataPtr, Word pTgtDataType, CStrPtr pSrcData,
                            Word pSrcDataType )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLCopyDateTimeData called, with the src : %s", pSrcData ) );
    // note
    // source data is received as character string
    // source data type is also recd. but is not being checked right now
    // this can be used to detrmine if the conversion is possible at all
    bool isnull;
    // check if source data is NULL
    isnull = ( !pSrcData || _stricmp ( pSrcData, "NULL" ) == 0 ) ? TRUE : 0;

    // check if target is there
    if ( pTgtDataPtr )
    {
        // check the data size
        switch ( pTgtDataType )
        {
            case SQL_C_TYPE_DATE : // 91
            case SQL_C_DATE :

                // ???? check src type
                if ( !isnull )
                {
                    memset ( pTgtDataPtr, 0, sizeof ( struct tagDATE_STRUCT) );
                    GetDateFromString ( pSrcData, ( struct tagDATE_STRUCT* ) pTgtDataPtr );
                }

                break;

            case SQL_C_TYPE_TIME : // 92
            case SQL_C_TIME :
                //not suppporting Time
                return 1;

            case SQL_C_TYPE_TIMESTAMP : // 93
            case SQL_C_TIMESTAMP :

                // ???? check src type
                if ( !isnull )
                {
                    memset ( pTgtDataPtr, 0, sizeof ( struct tagTIMESTAMP_STRUCT) );
                    GetTimestampFromString ( pSrcData, ( struct tagTIMESTAMP_STRUCT* ) pTgtDataPtr );
                }

                break;

            default :
                return 1; // data type not understood
        }

        return 0; // successful, at least data type recognized
    }

    else
    {
        return -1;
    } // should not typically happen
}

