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
// File:        KylinODBC.h
//
// Purpose:     Main header for Kylin ODBC driver - GODBC.CPP
//
// Author:      Hongbin Ma
//
// Notes:       This file is the heart as far as the driver design is concerned.
//              Essentially it contains the following elements/structures
//              Environment       --- as --- struct ODBCEnv
//                                -- type -- ODBCEnv
//              Connection        --- as --- struct ODBCConn
//                                -- type -- ODBCConn
//              Statement         --- as --- struct ODBCStmt
//                                -- type -- ODBCStmt
//              Diag row & header --- as --- struct ODBCDiagRow/ODBCDiag
//                                -- type -- ODBCStmt
//              ARD item & header --- as --- struct ARDItem/ODBCARD
//                                -- type -- ARDItem/ODBCARD
//              IRD item & header --- as --- ODBCIRD
//                                -- type -- IRDItem/ODBCIRD
//              APD item & header --- as --- struct APDItem/ODBCAPD
//                                -- type -- APDItem/ODBCAPD
//              IPD item & header --- as --- struct IPDItem/ODBCIPD
//                                -- type -- IPDItem/ODBCIPD
//              and constants, macros and function prototypes
//
//              The descriptors( http://msdn.microsoft.com/en-us/library/ms716262(v=vs.85).aspx )
//              are very important design issue as mentioned
//              in the article. ( http://www.drdobbs.com/windows/odbc-driver-development/184416434?pgno=1,
//              or you can just find all the stuff in "ODBC Driver Development Tutorial"
//              at the project folder.)
//
//              Environment encapsulates connection which
//              enacapsulates statement which encapsulates all four descriptors.
//              Almost all IO is done via these structures. You must be absolutely
//              clear as to their purpose/
//
//              We use windows c++ rest SDK for calling rest service.
//              vist its homepage at http://casablanca.codeplex.com/.
//              For a tutorial of it, refer to http://www.drdobbs.com/windows/using-the-microsoft-c-rest-sdk/240164544
//              and http://www.drdobbs.com/windows/json-and-the-microsoft-c-rest-sdk/240164821. There is also an article
//              on how to link it into our driver: http://katyscode.wordpress.com/2014/04/01/how-to-statically-link-the-c-rest-sdk-casablanca/
//
//              For Rest SDK working smoothly in our driver, we adopt the static linking techniques as describled in
//              http://katyscode.wordpress.com/2014/04/01/how-to-statically-link-the-c-rest-sdk-casablanca/
//
// ---------------------------------------------------------------------------

// --------------------------- Windows ODBC includes --------------------------
#pragma once
#include "vld.h"
#include <windows.h>
#include <sqlext.h>
#include <odbcinst.h>

// -------------------- legacy types for basic data types ---------------------
#include "MsgTypes.h"
#include "windows.h"

#include <memory>

// -------------------- types ---------------------
typedef char Char;
typedef short Word;
typedef long Long;

typedef unsigned char UChar;
typedef unsigned short UWord;
typedef unsigned long ULong;

typedef Char* StrPtr;
typedef const Char* CStrPtr;

enum eGoodBad
{
    GOOD = 0,
    BAD
};

// ----------------------- predeclarations and typedefs ----------------------------
struct ODBCEnv; // environment
typedef struct ODBCEnv* pODBCEnv; // ENVIRONMENT as pointer type

struct ODBCConn; // connection
typedef struct ODBCConn* pODBCConn; // CONNECTION as pointer type

struct ODBCStmt; // statement
typedef struct ODBCStmt* pODBCStmt; // STATEMENT as pointer type


// --------------------------- descriptor types/signatures --------------------------
#define SQL_DESC_ARD            1
#define SQL_DESC_IRD            2
#define SQL_DESC_APD            3
#define SQL_DESC_IPD            4

// DIAGNOSTICS


// ----------------------------------------------------------------------------------
// DIAGNOSTIC ROW - diagnostic message holder and link list
// ----------------------------------------------------------------------------------

struct ODBCDiagRow
{
    Long NativeErrorCode;
    // SQL_DIAG_NATIVE, native error code, specific to the data source
    Long Row; // error row if applicable
    Long Col; // error col if applicable

    Char State[SQL_SQLSTATE_SIZE + 1]; // SQLSTATE --- state as per ODBC spec
    StrPtr Msg; // SQL_DIAG_MESSAGE_TEXT, message text

    struct ODBCDiagRow* Next; // next message
    struct ODBCDiagRow* Prev; // prev message
};

typedef struct ODBCDiagRow* pODBCDiagRow; // DIAGROW as pointer type


// ----------------------------------------------------------------------------------
// DIAGNOSTIC - diagnostics header & rows link-list holder holder
// ----------------------------------------------------------------------------------

struct ODBCDiag
{
    // header attributes
    Char Func[64]; // func/API that generated the messages

    // diagnostic rows link-list
    pODBCDiagRow DiagRows;
};


typedef struct ODBCDiag* pODBCDiag; // ODBCDIAG as pointer type


// ARD


// ----------------------------------------------------------------------------------
// APPL ROW DESC ITEM - to hold appl specified buffers & other details for one column
// ----------------------------------------------------------------------------------

struct ARDItem
{
    Word ColNum; // column number

    Word DataConciseType; // concise data type
    Word DataVerboseType; // basic data type
    Word DateTimeIntervalCode; // date-time interval code
    Long DateTimeIntervalPrec; // date-time precision
    Long NumPrecRadix; // 2 if approx num type or 10 exact num type
    Word Scale; // scale, right of decimal

    void* DataPtr; // col data
    Long DataSize; // size of data buffer
    Long* SizePtr; // actual data size
    Long* SizeIndPtr; // size indicator ptr

    /////// following r fed & used  at time of fetch for optimization

    Word SrcDataType; // src data type ( for optimization only )
    Long SrcDataSize; // size of source data
    Word SrcDataPrecision; // precision for source data
    Word SrcDataScale; // scale for source data

    ///////

    struct ARDItem* Next; // next col binding
    struct ARDItem* Prev; // prev col binding
};

typedef struct ARDItem* pARDItem; // ARD-Col as pointer type


// ----------------------------------------------------------------------------------
// APPL ROW DESC - to hold appl specified buffers & other details for columns
// ----------------------------------------------------------------------------------

struct ODBCARD
{
    Word Sign;

    // header fields
    Word AllocType; // SQL_DESC_ALLOC_TYPE
    ULong RowArraySize; // SQL_DESC_ARRAY_SIZE
    UWord* ArrayStatusPtr; // SQL_DESC_ARRAY_STATUS_PTR
    Long* BindOffsetPtr; // SQL_DESC_BIND_OFFSET_PTR
    Long BindTypeOrSize; // SQL_DESC_BIND_TYPE
    Word DescCount; // SQL_DESC_COUNT

    // rows for binding each col
    pARDItem BindCols; // col bindings

    // container
    pODBCStmt Stmt; // container statememt
};


//typedef struct ODBCARD       ODBCARD;                         // ARD data type
typedef struct ODBCARD* pODBCARD; // ARD as pointer type


// IRD

// ----------------------------------------------------------------------------------
// IMPL ROW DESC ITEM - to hold info about one IRD item
// ----------------------------------------------------------------------------------

typedef SelectedColumnMeta IRDItem; // IRD data type
typedef SelectedColumnMeta* pIRDItem; // IRD as pointer type

// ----------------------------------------------------------------------------------
// IMPL ROW DESC - to hold response col description from server
// ----------------------------------------------------------------------------------

struct ODBCIRD
{
    Word Sign;

    // header fields
    UWord* ArrayStatusPtr; // SQL_DESC_ARRAY_STATUS_PTR
    Word DescCount; // SQL_DESC_COUNT
    ULong* RowsProcessedPtr; // SQL_DESC_ROWS_PROCESSED_PTR, it is changed in _SQL_FETCH

    // rows for binding each col
    std::unique_ptr <SQLResponse> RowDesc; // Row descriptor

    // container
    pODBCStmt Stmt; // container statememt
};


typedef struct ODBCIRD* pODBCIRD; // IRD as pointer type


// APD

// ----------------------------------------------------------------------------------
// APPL PARM DESC ITEM - to hold appl specified buffers & other details for one param
// ----------------------------------------------------------------------------------

struct APDItem
{
    Word ParamNum; // parameter number

    Word DataConciseType; // data type
    Word DataVerboseType; // basic data type
    Word DateTimeIntervalCode; // date-time interval code ????
    Long DateTimeIntervalPrec; // date-time precision ????
    Long NumPrecRadix; // 2 if approx num type or 10 exact num type
    Word Scale; // scale, right of decimal

    void* DataPtr; // param data
    Long DataSize; // size of data
    Long* SizePtr; // size of data buffer
    Long* SizeIndPtr; // size indicator ptr

    struct APDItem* Next; // next parm binding
    struct APDItem* Prev; // prev parm binding
};

typedef struct APDItem* pAPDItem; // APD-Param as pointer type


// ----------------------------------------------------------------------------------
// APPL PARAM DESC - to hold appl specified buffers & other details for params
// ----------------------------------------------------------------------------------

struct ODBCAPD
{
    Word Sign;

    // header fields
    Word AllocType; // SQL_DESC_ALLOC_TYPE
    ULong RowArraySize; // SQL_DESC_ARRAY_SIZE
    UWord* ArrayStatusPtr; // SQL_DESC_ARRAY_STATUS_PTR
    Long* BindOffsetPtr; // SQL_DESC_BIND_OFFSET_PTR
    Long BindTypeOrSize; // SQL_DESC_BIND_TYPE
    Word DescCount; // SQL_DESC_COUNT

    ULong ParamSetSize;
    Long* ParamProcessedPtr;

    // rows for binding each col
    pAPDItem BindParams; // param bindings

    // container
    pODBCStmt Stmt; // container statememt
};


typedef struct ODBCAPD* pODBCAPD; // APD as pointer type


// IPD

// ----------------------------------------------------------------------------------
// IMPL PARM DESC ITEM - to hold impl buffers & other details for one param
// ----------------------------------------------------------------------------------

struct IPDItem
{
    Word ParamNum; // parameter number
    Word ParamType; // parameter type input/output
    Char ParamName[33]; // param name - arbitray/dummy

    Word DataConciseType; // data type
    Word DataVerboseType; // basic data type
    Word DateTimeIntervalCode; // date-time interval code ????
    Long DateTimeIntervalPrec; // date-time precision ????
    Long NumPrecRadix; // 2 if approx num type or 10 exact num type
    Word Scale; // scale, right of decimal

    void* DataPtr; // param data
    Long DataSize; // size of data buffer
    Word FixedPrecScale; // fixed precision scale
    Word Nullable; // is it nullable


    struct IPDItem* Next; // next parm binding
    struct IPDItem* Prev; // prev parm binding
};

typedef struct IPDItem* pIPDItem; // IPD-Param as pointer type


// ----------------------------------------------------------------------------------
// IMPL PARAM DESC - to hold impl buffers & other details for params
// ----------------------------------------------------------------------------------

struct ODBCIPD
{
    Word Sign;

    // header fields
    //  Word            AllocType;     (read-only)                          // SQL_DESC_ALLOC_TYPE
    UWord* ArrayStatusPtr; // SQL_DESC_ARRAY_STATUS_PTR
    Word DescCount; // SQL_DESC_COUNT
    ULong* RowsProcessedPtr; // SQL_DESC_ROWS_PROCESSED_PTR

    // rows for binding each col
    pIPDItem BindParams; // param bindings

    // container
    pODBCStmt Stmt; // container statememt
};


typedef struct ODBCIPD* pODBCIPD; // IPD as pointer type


// STATEMENT

// ----------------------------------------------------------------------------------
// STATEMENT - a single sql stmt scoped within a CONNECTION and part of link list
// ----------------------------------------------------------------------------------

struct ODBCStmt
{
    Word Sign; // structure signature

    // Attributes
    ULong AsyncEnable;
    ULong Concurrency;
    ULong CursorScroll;
    ULong CursorSensitivity;
    ULong CursorType;
    ULong AutoIPD;
    ULong KeysetSize;
    ULong MetaDataID;
    ULong NoScan;
    ULong QryTimeout;
    ULong RetrieveData;

    wchar_t* Stmt; // SQL statement
    Long StmtLen; // length of statement
    bool Prepared; // is it prepared

    // RESPONSE
    Word
    RespType; // Response type, like fault, result-set, count etc etc
    SQLRowContent* CurRowsetStartRow; // start of current rowset
    Long CurRowsetStartRowPos; // absolute position
    SQLRowContent* CurRowsetEndRow; // end of current rowset
    Long CurRowsetEndRowPos; // absolute position
    Long RowCount; // total number of rows

    // DESCRIPTORS
    ODBCARD ARD; // application row descriptor
    ODBCIRD IRD; // implementation row descriptor

    ODBCAPD APD; // application parm descriptor
    ODBCIPD IPD; // implementation parm descriptor

    // DIAG & LINKS
    ODBCDiag Diag; // diagnostic details

    pODBCConn Conn; // container connection

    // other stmt attributes
    // state of stmt if required separately

    struct ODBCStmt* Next; // next statement
    struct ODBCStmt* Prev; // prev statement
};


// CONNECTION or DBC

// ----------------------------------------------------------------------------------
// CONNECTION - a single connection to server scoped within environment & part of link-list
// ----------------------------------------------------------------------------------

struct ODBCConn
{
    Word Sign; // structure signature

    // Attributes
    ULong AccessMode;
    ULong AutoIPD;
    ULong AsyncEnable;
    ULong AutoCommit;
    ULong TimeOut;
    ULong LoginTimeOut;
    ULong MetaDataID;
    ULong ODBCCursors;
    ULong Window;
    ULong TxnIsolation;
    ULong MaxRows;
    ULong QueryTimeout;

    StrPtr ConnectStr; // connection string, the full connection string
    StrPtr Server; // server name/address
    ULong ServerPort; // server port
    StrPtr Project; // name of database
    StrPtr UserName; // user name
    StrPtr Password; // password

    bool IsConnected; // connection checked
    pODBCStmt Stmts; // all stmts within connection
    ODBCDiag Diag; // diagnostic details
    pODBCEnv Env; // environment

    struct ODBCConn* Next; // next connection
    struct ODBCConn* Prev; // prev connection

    std::unique_ptr <MetadataResponse> meta; // ACTUAL metadata returned from REST Server
};


// ENVIRONMENT

// ----------------------------------------------------------------------------------
// ENVIRONMENT - maximum window for an application into the ODBC driver
// ----------------------------------------------------------------------------------

struct ODBCEnv
{
    Word Sign; // structure signature

    pODBCConn Conns; // all connections within env

    ODBCDiag Diag; // diagnostic messages

    ULong AttrODBCVersion; // SQL_ATTR_ODBC_VERSION                200
    ULong AttrConnPooling; // SQL_ATTR_CONNECTION_POOLING          201
    ULong AttrCPMatch; // SQL_ATTR_CP_MATCH                    202
    ULong AttrOutputNTS; // SQL_ATTR_OUTPUT_NTS
};

// ----------------------------------------------------------------------------------
// Key-value pair - to hold a single key-value pair
// ----------------------------------------------------------------------------------

struct ODBCKV
{
    StrPtr key;
    StrPtr value;
};

// ------------------------- other global variables ---------------------------
extern HINSTANCE ghInstDLL;

extern Long gLogFile;
extern Word gLogUsage;

// -------------------------------- defines -----------------------------------
#define DEFAULT_PORT            443

// --------------------- defines for connection properties --------------------
#define     CONN_PROP_SERVER        100
#define     CONN_PROP_PORT          101
#define     CONN_PROP_PROJECT       102
#define     CONN_PROP_UID           103
#define     CONN_PROP_PWD           104

// ------------------------- defines for log system ---------------------------
#ifdef _DEBUG

#define __ODBCLOG(x)          (x)
#define __ODBCLOGSTART()      _ODBCLogStart()
#define __ODBCLOGEND(f)       _ODBCLogStop(f)

#else

#define __ODBCLOG(x)          (x)
#define __ODBCLOGSTART()
#define __ODBCLOGEND(f)

#endif

// ------------ allow popup in non-debug also for the time being --------------
#define __ODBCPOPMSG(x)       (x)

// ---------------------------- diag defines ----------------------------------
#define _DIAGSTMT(x)            ( &(((pODBCStmt)(x))->Diag))
#define _DIAGCONN(x)            ( &(((pODBCConn)(x))->Diag))
#define _DIAGENV(x)             ( &(((pODBCEnv)(x))->Diag))

// --------------------------- descriptor defines  ----------------------------
#define _DESCTYPE(x)            ( *(( Word* )x) )

// --------------------------- checking handles -------------------------------
#define __CHK_HANDLE(h,t,r)   { \
        if(!(h)) { __ODBCPOPMSG(_ODBCPopMsg("Null handle!!!")); return r; } \
        else if((h) && *(( Word* )(h)) != t ) { __ODBCPOPMSG(_ODBCPopMsg("Handle type does not match!!!")); return r; } \
    }

// ---------------------------- diag functions --------------------------------
pODBCDiagRow _SQLPutDiagRow ( pODBCDiag pDiag, StrPtr pFunc, StrPtr pState, Long pNativeErrorCode,
                              StrPtr pMsgArgs, va_list pArgs );
eGoodBad _SQLPutDiagRow ( SQLSMALLINT pHandleType, SQLHANDLE pHandle, StrPtr pFunc, StrPtr pState,
                          Long pNativeErrorCode, StrPtr pMsgArgs, ... );
eGoodBad _SQLPutDiagRow ( SQLSMALLINT pHandleType, SQLHANDLE pHandle, StrPtr pFunc, StrPtr pState,
                          Long pNativeErrorCode, Long pRow, Long pCol, StrPtr pMsgArgs, ... );
eGoodBad _SQLFreeDiag ( pODBCDiag pHandle );
pODBCDiagRow _SQLGetDiagRow ( pODBCDiag pHandle, Word pRowNum );

// -------------------------------- debug functions ---------------------------
enum LogLevel
{
    LogLevel_DEBUG,
    LogLevel_INFO,
    LogLevel_WARN,
    LogLevel_ERROR,
    LogLevel_FATAL
};

void _ODBCLogStart ( void );
void _ODBCLogStop ( int pForce );
void _ODBCLogMsg ( LogLevel level, const char* s, ... );
void _ODBCLogMsg ( LogLevel level, const wchar_t* textW );
void _ODBCPopMsg ( const char* s, ... );
void __ODBCPopMsg ( const char* s, ... );

// ------------------------------ data type functions -------------------------
Word _SQLColDataType ( CStrPtr* pDataType );
eGoodBad _SQLCheckDataType ( Word pDataType );
eGoodBad _SQLCheckIntervalCode ( Word pIntervalCode );
eGoodBad _SQLSetDataType ( pODBCDiag pDiag, Word pFldID, Word pFldValue, Word* pVerboseDataType,
                           Word* pConciseDataType, Word* pDateTimeIntervalCode );

// ---------------------------- descriptor functions --------------------------
pARDItem _SQLGetARDItem ( const pODBCARD pDesc, Word pRecNum );
pAPDItem _SQLGetAPDItem ( const pODBCAPD pDesc, Word pRecNum );
pIRDItem _SQLGetIRDItem ( const pODBCIRD pDesc, Word pRecNum );
pIPDItem _SQLGetIPDItem ( const pODBCIPD pDesc, Word pRecNum );

eGoodBad _SQLSetARDFieldsDefault ( pODBCARD pDesc, const pODBCStmt pStmt );
eGoodBad _SQLSetARDItemFieldsDefault ( pARDItem pDescItem, Word pRecNum );

eGoodBad _SQLSetAPDFieldsDefault ( pODBCAPD pDesc, const pODBCStmt pStmt );
eGoodBad _SQLSetAPDItemFieldsDefault ( pAPDItem pDescItem );

eGoodBad _SQLSetIRDFieldsDefault ( pODBCIRD pDesc, const pODBCStmt pStmt );
eGoodBad _SQLSetIRDItemFieldsDefault ( pIRDItem pDescItem );

eGoodBad _SQLSetIPDFieldsDefault ( pODBCIPD pDesc, const pODBCStmt pStmt );
eGoodBad _SQLSetIPDItemFieldsDefault ( pIPDItem pDescItem );

RETCODE SQL_API _SQLSetARDField ( pODBCARD pDesc, Word pFldID, const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLSetARDItemField ( pODBCARD pDesc, pARDItem pDescItem, Word pRecNum, Word pFldID,
                                      const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLGetARDField ( const pODBCARD pDesc, Word pFldID, void* pDataPtr, Long pDataSize,
                                  Long* pDataSizePtr );
RETCODE SQL_API _SQLGetARDItemField ( const pODBCARD pDesc, const pARDItem pDescItem, Word pRecNum,
                                      Word pFldID, void* pDataPtr, Long pDataSize, Long* pDataSizePtr );

RETCODE SQL_API _SQLSetAPDField ( pODBCAPD pDesc, Word pFldID, const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLSetAPDItemField ( pODBCAPD pDesc, pAPDItem pDescItem, Word pRecNum, Word pFldID,
                                      const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLGetAPDField ( const pODBCAPD pDesc, Word pFldID, void* pDataPtr, Long pDataSize,
                                  Long* pDataSizePtr );
RETCODE SQL_API _SQLGetAPDItemField ( const pODBCAPD pDesc, const pAPDItem pDescItem, Word pRecNum,
                                      Word pFldID, void* pDataPtr, Long pDataSize, Long* pDataSizePtr );

RETCODE SQL_API _SQLSetIRDField ( pODBCIRD pDesc, Word pFldID, const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLSetIRDItemField ( pODBCIRD pDesc, pIRDItem pDescItem, Word pRecNum, Word pFldID,
                                      const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLGetIRDField ( const pODBCIRD pDesc, Word pFldID, void* pDataPtr, Long pDataSize,
                                  Long* pDataSizePtr );
RETCODE SQL_API _SQLGetIRDItemField ( const pODBCIRD pDesc, const pIRDItem pDescItem, Word pRecNum,
                                      Word pFldID, void* pDataPtr, Long pDataSize, Long* pDataSizePtr, bool isANSI );

RETCODE SQL_API _SQLSetIPDField ( pODBCIPD pDesc, Word pFldID, const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLSetIPDItemField ( pODBCIPD pDesc, pIPDItem pDescItem, Word pRecNum, Word pFldID,
                                      const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLGetIPDField ( const pODBCIPD pDesc, Word pFldID, void* pDataPtr, Long pDataSize,
                                  Long* pDataSizePtr );
RETCODE SQL_API _SQLGetIPDItemField ( const pODBCIPD pDesc, const pIPDItem pDescItem, Word pRecNum,
                                      Word pFldID, void* pDataPtr, Long pDataSize, Long* pDataSizePtr );

RETCODE SQL_API _SQLFreeARDContent ( const pODBCARD pDesc );
RETCODE SQL_API _SQLFreeAPDContent ( const pODBCAPD pDesc );
RETCODE SQL_API _SQLFreeIRDContent ( const pODBCIRD pDesc );
RETCODE SQL_API _SQLFreeIPDContent ( const pODBCIPD pDesc );

eGoodBad _SQLAttachARDItem ( pODBCARD pDesc, pARDItem pDescItem );
eGoodBad _SQLDetachARDItem ( pODBCARD pDesc, pARDItem pDescItem );

eGoodBad GetIRDDataTypeDefaults ( CStrPtr pDataType, Word* pSqlDataType, Word* pNativeDataType,
                                  Word* pPrecision, Word* pScale, Long* pLength, Long* pRadix );
eGoodBad GetIRDColDescInfo ( SelectedColumnMeta* pColDesc, Word* pDataType, Word* pPrecision, Word* pScale,
                             Long* pLength );

// ------------------------- execution function -------------------------------
RETCODE SQL_API _SQLExecStmtFromReq ( pODBCStmt pStmt, bool pPrepared );
eGoodBad PutRespToStmt ( pODBCStmt pStmt, std::unique_ptr <SQLResponse> resp );

// -------------------------- utility functions -------------------------------
RETCODE SQL_API _SQLCopyCharData ( pODBCDiag pDiag, void* pDataPtr, Long pDataBufSize, void* pSizePtr,
                                   Word pSizePtrSize, CStrPtr pSrcData, Long pSrcDataSize );
RETCODE SQL_API _SQLCopyWCharData ( pODBCDiag pDiag, void* pDataPtr, Long pDataBufSize, void* pSizePtr,
                                    Word pSizePtrSize, CStrPtr pSrcData, Long pSrcDataSize, bool returnByteSize = true );
RETCODE SQL_API _SQLCopyWCharDataW ( pODBCDiag pDiag, void* pDataPtr, Long pDataBufSize, void* pSizePtr,
                                     Word pSizePtrSize, const wchar_t* pSrcData, Long pSrcDataSize, bool returnByteSize = true );
Word _SQLCopyNumData ( pODBCDiag pDiag, void* pTgtDataPtr, Word pTgtDataType, CStrPtr pSrcData,
                       Word pSrcDataType, Long* pTgtDataSizePtr );
Word _SQLCopyDateTimeData ( pODBCDiag pDiag, void* pTgtDataPtr, Word pTgtDataType, CStrPtr pSrcData,
                            Word pSrcDataType );

// -------------------------- DSN config functions -------------------------------
eGoodBad SetConnProp ( pODBCConn pConn, Word pPropID, void* pPropValue );
eGoodBad LoadODBCINIDataToConn ( pODBCConn pConn );
eGoodBad CvtStrToKeyValues ( CStrPtr pStr, Word pMaxLen, Word* pNumPair, struct ODBCKV** pKV );
RETCODE TryFetchMetadata ( pODBCConn pConn );
RETCODE TryAuthenticate ( pODBCConn pConn );
void SetCurrentDSN ( char* connStr, char* logFunc );
bool FindInKeyValues ( CStrPtr pKey, CStrPtr pValue, struct ODBCKV* pKV, Word pItems, Word* pPosition );
void FreeGenODBCKeyValues ( ODBCKV* keyvalues, int pairs );
RETCODE SQL_API _SQLDisconnect ( pODBCConn pHandle );

