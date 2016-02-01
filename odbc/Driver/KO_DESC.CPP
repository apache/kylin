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
// File:    KO_DESC.CPP
//
// Purpose: Contains descriptor functions.
//          As explained in the article, descriptors r the heart
//          as far as driver design is concerned. Descriptors
//          r essntially data structures to encapsulate one type of
//          data. Say results from server. Say col specification from
//          application and so. All the four descriptors (ARD,IRD,APD,IPD)
//          r encapsulated inside the statement structure.
//
//          The local functions provided for each descriptor include
//          1. maintaining the link list                   _SQLAttachXXXItem/_SQLDetachXXXItem
//          2. getting an desc item from link list         _SQLGetXXXItem
//          3. setting default values in desc header/item  _SQLSetXXXFieldsDefault/_SQLSetXXXItemFieldsDefault
//          4. setting values in desc header/item          _SQLSetXXXField/_SQLSetXXXItemField
//          5. getting values from desc header/item        _SQLGetXXXField/_SQLGetXXXItemField
//          where XXX is the descriptor type ARD,IRD,APD,IPD
//
//          These functions are used internally by most functions
//          like SQLBindCol, SQLColAttribiute and so on to maintain
//          info in the descriptor structures.
//
//          The exposed functions allow the client to directly
//          manipulate the descriptor values, and these functions
//          also use these local functions only.
//          But use of the exposed descriptor function is not very
//          common in the ODBC parlance.
//
// Exported functions:
//                       SQLGetDescField
//                       SQLSetDescField
//                       SQLGetDescRec
//                       SQLSetDescRec
//                       SQLCopyDesc
//
// ----------------------------------------------------------------------------
#include "stdafx.h"


// ------------------------ local functions -----------------------------------
pAPDItem _SQLGetAPDItem ( const pODBCAPD pDesc, Word pRecNum );
pIPDItem _SQLGetIPDItem ( const pODBCIPD pDesc, Word pRecNum );
pARDItem _SQLGetARDItem ( const pODBCARD pDesc, Word pRecNum );
pIRDItem _SQLGetIRDItem ( const pODBCIRD pDesc, Word pRecNum );

eGoodBad _SQLSetAPDFieldsDefault ( pODBCAPD pDesc, const pODBCStmt pStmt );
eGoodBad _SQLSetAPDItemFieldsDefault ( pAPDItem pDescItem );

eGoodBad _SQLSetIPDFieldsDefault ( pODBCIPD pDesc, const pODBCStmt pStmt );
eGoodBad _SQLSetIPDItemFieldsDefault ( pIPDItem pDescItem );

eGoodBad _SQLSetARDFieldsDefault ( pODBCARD pDesc, const pODBCStmt pStmt );
eGoodBad _SQLSetARDItemFieldsDefault ( pARDItem pDescItem, Word pRecNum );

eGoodBad _SQLSetIRDFieldsDefault ( pODBCIRD pDesc, const pODBCStmt pStmt );
eGoodBad _SQLSetIRDItemFieldsDefault ( pIRDItem pDescItem );

RETCODE SQL_API _SQLSetAPDField ( pODBCAPD pDesc, Word pFldID, const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLSetAPDItemField ( pODBCAPD pDesc, pAPDItem pDescItem, Word pRecNum, Word pFldID,
                                      const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLGetAPDField ( const pODBCAPD pDesc, Word pFldID, void* pDataPtr, Long pDataSize,
                                  Long* pDataSizePtr );
RETCODE SQL_API _SQLGetAPDItemField ( const pODBCAPD pDesc, const pAPDItem pDescItem, Word pRecNum,
                                      Word pFldID, void* pDataPtr, Long pDataSize, Long* pDataSizePtr );

RETCODE SQL_API _SQLSetIPDField ( pODBCIPD pDesc, Word pFldID, const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLSetIPDItemField ( pODBCIPD pDesc, pIPDItem pDescItem, Word pRecNum, Word pFldID,
                                      const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLGetIPDField ( const pODBCIPD pDesc, Word pFldID, void* pDataPtr, Long pDataSize,
                                  Long* pDataSizePtr );
RETCODE SQL_API _SQLGetIPDItemField ( const pODBCIPD pDesc, const pIPDItem pDescItem, Word pRecNum,
                                      Word pFldID, void* pDataPtr, Long pDataSize, Long* pDataSizePtr );

RETCODE SQL_API _SQLSetARDField ( pODBCARD pDesc, Word pFldID, const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLSetARDItemField ( pODBCARD pDesc, pARDItem pDescItem, Word pRecNum, Word pFldID,
                                      const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLGetARDField ( const pODBCARD pDesc, Word pFldID, void* pDataPtr, Long pDataSize,
                                  Long* pDataSizePtr );
RETCODE SQL_API _SQLGetARDItemField ( const pODBCARD pDesc, const pARDItem pDescItem, Word pRecNum,
                                      Word pFldID, void* pDataPtr, Long pDataSize, Long* pDataSizePtr );

RETCODE SQL_API _SQLSetIRDField ( pODBCIRD pDesc, Word pFldID, const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLSetIRDItemField ( pODBCIRD pDesc, pIRDItem pDescItem, Word pRecNum, Word pFldID,
                                      const void* pDataPtr, Long pDataSize );
RETCODE SQL_API _SQLGetIRDField ( const pODBCIRD pDesc, Word pFldID, void* pDataPtr, Long pDataSize,
                                  Long* pDataSizePtr );
RETCODE SQL_API _SQLGetIRDItemField ( const pODBCIRD pDesc, const pIRDItem pDescItem, Word pRecNum,
                                      Word pFldID, void* pDataPtr, Long pDataSize, Long* pDataSizePtr, bool isANSI );

RETCODE SQL_API _SQLFreeAPDContent ( const pODBCAPD pDesc );
RETCODE SQL_API _SQLFreeIPDContent ( const pODBCIPD pDesc );
RETCODE SQL_API _SQLFreeARDContent ( const pODBCARD pDesc );
RETCODE SQL_API _SQLFreeIRDContent ( const pODBCIRD pDesc );

eGoodBad _SQLAttachARDItem ( pODBCARD pDesc, pARDItem pDescItem );
eGoodBad _SQLDetachARDItem ( pODBCARD pDesc, pARDItem pDescItem );


// -----------------------------------------------------------------------
// to get value of specified descriptor field
// -----------------------------------------------------------------------

RETCODE _SQLGetDescField_basic ( SQLHDESC pDesc,
                                 SQLSMALLINT pRecNum,
                                 SQLSMALLINT pFldID,
                                 SQLPOINTER pDataPtr,
                                 SQLINTEGER pDataSize,
                                 SQLINTEGER* pDataSizePtr,
                                 bool isANSI )
{
    bool invalidfld;
    bool headerfld;
    Word desctype;

    // precaution
    if ( pDesc == NULL || pDataPtr == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "SQLGetDescField - invalid params" ) );
        return SQL_ERROR;
    }

    // initializations
    invalidfld = FALSE;
    headerfld = TRUE;
    desctype = _DESCTYPE ( pDesc );

    // check if descriptor is valid & reset diags
    switch ( desctype )
    {
        case SQL_DESC_APD :
            _SQLFreeDiag ( & ( ( ( pODBCAPD ) pDesc ) -> Stmt -> Diag ) );
            break;

        case SQL_DESC_IPD :
            _SQLFreeDiag ( & ( ( ( pODBCIPD ) pDesc ) -> Stmt -> Diag ) );
            break;

        case SQL_DESC_ARD :
            _SQLFreeDiag ( & ( ( ( pODBCARD ) pDesc ) -> Stmt -> Diag ) );
            break;

        case SQL_DESC_IRD :
            _SQLFreeDiag ( & ( ( ( pODBCIRD ) pDesc ) -> Stmt -> Diag ) );
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQLGetDescField - Invalid descriptor handle" ) );
            return SQL_ERROR;
    }

    // HEADER FIELDS

    // check the field type - header field
    switch ( pFldID )
    {
        case SQL_DESC_ALLOC_TYPE :

            /*****
                SQLSMALLINT
                ARD: R          APD: R          IRD: R          IPD: R
                ARD: SQL_DESC_ALLOC_AUTO/SQL_DESC_ALLOC_USER
                APD: SQL_DESC_ALLOC_AUTO/SQL_DESC_ALLOC_USER
                IRD: SQL_DESC_ALLOC_AUTO
                IPD: SQL_DESC_ALLOC_AUTO
            ********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDField ( ( pODBCAPD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IPD :
                    return _SQLGetIPDField ( ( pODBCIPD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDField ( ( pODBCARD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDField ( ( pODBCIRD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );
            }

            break;

        case SQL_DESC_ARRAY_SIZE :

            /*********
                SQLUINTEGER
                ARD: R/W        APD: R/W        IRD: Unused     IPD: Unused
                ARD:[1]         APD:[1]         IRD: Unused     IPD: Unused
            ********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDField ( ( pODBCAPD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDField ( ( pODBCARD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_ARRAY_STATUS_PTR :

            /**********
                SQLUSMALLINT*
                ARD: R/W        APD: R/W        IRD: R/W        IPD: R/W
                ARD: Null ptr   APD: Null ptr   IRD: Null ptr   IPD: Null ptr
            **********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDField ( ( pODBCAPD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IPD :
                    return _SQLGetIPDField ( ( pODBCIPD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDField ( ( pODBCARD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDField ( ( pODBCIRD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );
            }

            break;

        case SQL_DESC_BIND_OFFSET_PTR :

            /**********
                SQLINTEGER*
                ARD: R/W        APD: R/W        IRD: Unused     IPD: Unused
                ARD: Null ptr   APD: Null ptr   IRD: Unused     IPD: Unused
            **********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDField ( ( pODBCAPD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDField ( ( pODBCARD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_BIND_TYPE :

            /**********
                SQLINTEGER
                ARD: R/W        APD: R/W        IRD: Unused     IPD: Unused
                ARD: SQL_BIND_BY_COLUMN
                APD: SQL_BIND_BY_COLUMN         IRD: Unused     IPD: Unused
            *********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDField ( ( pODBCAPD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDField ( ( pODBCARD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_COUNT :

            /**********
                SQLSMALLINT
                ARD: R/W        APD: R/W        IRD: R          IPD: R/W
                ARD: 0          APD: 0          IRD: D          IPD: 0
            **********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDField ( ( pODBCAPD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IPD :
                    return _SQLGetIPDField ( ( pODBCIPD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDField ( ( pODBCARD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDField ( ( pODBCIRD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );
            }

            break;

        case SQL_DESC_ROWS_PROCESSED_PTR :

            /**********
                SQLUINTEGER*
                ARD: Unused         APD: Unused     IRD: R/W        IPD: R/W
                ARD: Unused         APD: Unused     IRD: Null ptr   IPD: Null ptr
            ***********/
            switch ( desctype )
            {
                case SQL_DESC_IPD :
                    return _SQLGetIPDField ( ( pODBCIPD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDField ( ( pODBCIRD ) pDesc, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                default :
                    invalidfld = TRUE;
            }

            break;

        default :
            headerfld = FALSE;
    }

    // check if not an header field
    if ( headerfld == TRUE )
    {
        return SQL_SUCCESS;
    }

    // check if invalid field
    if ( invalidfld == TRUE )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "SQLGetDescField - Invalid field %d for descriptor %d", pFldID, desctype ) );
        return SQL_ERROR;
    }

    // check the field type - record field
    switch ( pFldID )
    {
        case SQL_DESC_AUTO_UNIQUE_VALUE :

            /***********
                SQLINTEGER
                ARD: Unused         APD: Unused         IRD: R      IPD: Unused
                ARD: Unused         APD: Unused         IRD: D      IPD: Unused
            *********/
            switch ( desctype )
            {
                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_BASE_COLUMN_NAME :

            /**********
                SQLCHAR *
                ARD: Unused         APD: Unused         IRD: R          IPD: Unused
                ARD: Unused         APD: Unused         IRD: D          IPD: Unused
            **********/
            switch ( desctype )
            {
                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_BASE_TABLE_NAME :

            /********
                SQLCHAR *
                ARD: Unused         APD: Unused         IRD: R          IPD: Unused
                ARD: Unused         APD: Unused         IRD: D          IPD: Unused
            *******/
            switch ( desctype )
            {
                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_CASE_SENSITIVE :

            /**********
                SQLINTEGER
                ARD: Unused         APD: Unused         IRD: R          IPD: R
                ARD: Unused         APD: Unused         IRD: D          IPD: D
            *******/
            switch ( desctype )
            {
                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_CATALOG_NAME :

            /**********
                SQLCHAR*
                ARD: Unused         APD: Unused         IRD: R          IPD: Unused
                ARD: Unused         APD: Unused         IRD: D          IPD: Unused
            ********/
            switch ( desctype )
            {
                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_CONCISE_TYPE :

            /**********
                SQLSMALLINT
                ARD: R/W            APD: R/W            IRD: R          IPD: R/W
                ARD: SQL_C_DEFAULT  APD: SQL_C_DEFAULT  IRD: D          IPD: ND
            *********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDItemField ( ( pODBCARD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );
            }

            break;

        case SQL_DESC_DATA_PTR :

            /***********
                SQLPOINTER
                ARD: R/W            APD: R/W            IRD: Unused     IPD: Unused
                ARD: Null ptr       APD: Null ptr       IRD: Unused     IPD: Unused
            ************/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDItemField ( ( pODBCARD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_DATETIME_INTERVAL_CODE :

            /**********
                SQLSMALLINT
                ARD: R/W            APD: R/W            IRD: R          IPD: R/W
                ARD: ND             APD: ND             IRD: D          IPD: ND
            ********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDItemField ( ( pODBCARD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );
            }

            break;

        case SQL_DESC_DATETIME_INTERVAL_PRECISION :

            /*********
                SQLINTEGER
                ARD: R/W            APD: R/W            IRD: R          IPD: R/W
                ARD: ND             APD: ND             IRD: D          IPD: ND
            **********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDItemField ( ( pODBCARD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );
            }

            break;

        case SQL_DESC_DISPLAY_SIZE :

            /*********
                SQLINTEGER
                ARD: Unused         APD: Unused         IRD: R          IPD: Unused
                ARD: Unused         APD: Unused         IRD: D          IPD: Unused
            **********/
            switch ( desctype )
            {
                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_FIXED_PREC_SCALE :

            /*********
                SQLSMALLINT
                ARD: Unused         APD: Unused         IRD: R          IPD: R
                ARD: Unused         APD: Unused         IRD: D          IPD: D[1]
            *********/
            switch ( desctype )
            {
                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_INDICATOR_PTR :

            /**********
                SQLINTEGER *
                ARD: R/W            APD: R/W            IRD: Unused         IPD: Unused
                ARD: Null ptr       APD: Null ptr       IRD: Unused         IPD: Unused
            ********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDItemField ( ( pODBCARD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_LABEL :

            /*********
                SQLCHAR *
                ARD: Unused         APD: Unused         IRD: R          IPD: Unused
                ARD: Unused         APD: Unused         IRD: D          IPD: Unused
            ***********/
            switch ( desctype )
            {
                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_LENGTH :

            /********
                SQLUINTEGER
                ARD: R/W            APD: R/W            IRD: R          IPD: R/W
                ARD: ND             APD: ND             IRD: D          IPD: ND
            ********/
            switch ( desctype )
            {
                case SQL_DESC_ARD :
                    return _SQLGetARDItemField ( ( pODBCARD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_APD :
                    return _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );
            }

            break;

        case SQL_DESC_LITERAL_PREFIX :

            /**********
                SQLCHAR*
                ARD: Unused         APD: Unused         IRD: R          IPD: Unused
                ARD: Unused         APD: Unused         IRD: D          IPD: Unused
            **********/

            // a read-only IRD field
            switch ( desctype )
            {
                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_LITERAL_SUFFIX :

            /**********
                SQLCHAR*
                ARD: Unused         APD: Unused         IRD: R          IPD: Unused
                ARD: Unused         APD: Unused         IRD: D          IPD: Unused
            **********/
            switch ( desctype )
            {
                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_LOCAL_TYPE_NAME :

            /***********
                SQLCHAR *
                ARD: Unused         APD: Unused         IRD: R          IPD: R
                ARD: Unused         APD: Unused         IRD: D          IPD: D
            ***********/
            switch ( desctype )
            {
                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_NAME :

            /***********
                SQLCHAR*
                ARD: Unused         APD: Unused         IRD: R          IPD: R/W
                ARD: ND             APD: ND             IRD: D          IPD: ND
            *********/
            switch ( desctype )
            {
                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_NULLABLE :

            /**********
                SQLSMALLINT
                ARD: Unused         APD: Unused         IRD: R          IPD: R
                ARD: ND             APD: ND             IRD: D          IPD: ND
            ***********/
            switch ( desctype )
            {
                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_NUM_PREC_RADIX :

            /*********
                SQLINTEGER
                ARD: R/W            APD: R/W            IRD: R          IPD: R/W
                ARD: ND             APD: ND             IRD: D          IPD: ND
            ********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDItemField ( ( pODBCARD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );
            }

            break;

        case SQL_DESC_OCTET_LENGTH :

            /**********
                SQLINTEGER
                ARD: R/W            APD: R/W            IRD: R          IPD: R/W
                ARD: ND             APD: ND             IRD: D          IPD: ND
            *********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDItemField ( ( pODBCARD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );
            }

            break;

        case SQL_DESC_OCTET_LENGTH_PTR :

            /******
                SQLINTEGER*
                ARD: R/W            APD: R/W             IRD: Unused    IPD: Unused
                ARD: Null ptr       APD: Null ptr        IRD: Unused    IPD: Unused
            *********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDItemField ( ( pODBCARD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_PARAMETER_TYPE :

            /*********
                SQLSMALLINT
                ARD: Unused         APD: Unused         IRD: Unused     IPD: R/W
                ARD: Unused         APD: Unused         IRD: Unused     IPD: D=SQL_PARAM_INPUT
            *********/
            switch ( desctype )
            {
                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_PRECISION :

            /*********
                SQLSMALLINT
                ARD: R/W            APD: R/W            IRD: R          IPD: R/W
                ARD: ND             APD: ND             IRD: D          IPD: ND
            ********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDItemField ( ( pODBCARD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );
            }

            break;

        case SQL_DESC_ROWVER :

            /*********
                SQLSMALLINT
                ARD: Unused         APD: Unused         IRD: R          IPD: R
                ARD: Unused         APD: Unused         IRD: ND         IPD: ND
            ********/
            switch ( desctype )
            {
                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_SCALE :

            /********
                SQLSMALLINT
                ARD: R/W            APD: R/W            IRD: R          IPD: R/W
                ARD: ND             APD: ND             IRD: D          IPD: ND
            *********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDItemField ( ( pODBCARD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );
            }

            break;

        case SQL_DESC_SCHEMA_NAME :

            /*********
                SQLCHAR*
                ARD: Unused         APD: Unused         IRD: R          IPD: Unused
                ARD: Unused         APD: Unused         IRD: D          IPD: Unused
            ********/
            switch ( desctype )
            {
                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_SEARCHABLE :

            /*********
                SQLCHAR*
                ARD: Unused         APD: Unused         IRD: R          IPD: Unused
                ARD: Unused         APD: Unused         IRD: D          IPD: Unused
            ********/
            switch ( desctype )
            {
                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_TABLE_NAME :

            /********
                SQLCHAR*
                ARD: Unused         APD: Unused         IRD: R          IPD: Unused
                ARD: Unused         APD: Unused         IRD: D          IPD: Unused
            ********/
            switch ( desctype )
            {
                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_TYPE :

            /*********
                SQLSMALLINT
                ARD: R/W            APD: R/W            IRD: R          IPD: R/W
                ARD: SQL_C_DEFAULT  APD: SQL_C_DEFAULT  IRD: D          IPD: ND
            *********/
            switch ( desctype )
            {
                case SQL_DESC_APD :
                    return _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_ARD :
                    return _SQLGetARDItemField ( ( pODBCARD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );
            }

            break;

        case SQL_DESC_TYPE_NAME :

            /*********
                SQLCHAR *
                ARD: Unused         APD: Unused         IRD: R          IPD: R
                ARD: Unused         APD: Unused         IRD: D          IPD: D
            **********/
            switch ( desctype )
            {
                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_UNNAMED :

            /********
                SQLSMALLINT
                ARD: Unused         APD: Unused         IRD: R          IPD: R/W
                ARD: ND             APD: ND             IRD: D          IPD: ND
            ********/
            switch ( desctype )
            {
                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        case SQL_DESC_UNSIGNED :

            /*********
                SQLSMALLINT
                ARD: Unused         APD: Unused         IRD: R          IPD: R
                ARD: Unused         APD: Unused         IRD: D          IPD: D
            ********/
            switch ( desctype )
            {
                case SQL_DESC_IPD :
                    return _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr );

                case SQL_DESC_IRD :
                    return _SQLGetIRDItemField ( ( pODBCIRD ) pDesc, NULL, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI );

                default :
                    invalidfld = TRUE;
            }

            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQLGetDescField - Unknown field %d", pFldID ) );
            return SQL_ERROR;
    }

    // check if invalid field
    if ( invalidfld == TRUE )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "SQLGetDescField - Invalid field %d for descriptor %d", pFldID, desctype ) );
        return SQL_ERROR;
    }

    return SQL_SUCCESS;
}


RETCODE SQL_API SQLGetDescFieldW ( SQLHDESC pDesc,
                                   SQLSMALLINT pRecNum,
                                   SQLSMALLINT pFldID,
                                   SQLPOINTER pDataPtr,
                                   SQLINTEGER pDataSize,
                                   SQLINTEGER* pDataSizePtr )

{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "SQLGetDescFieldW called, Desc: %d, Type: %d, Recnum: %d, Fld: %d, DataPtr: %d, DataSize: %d, DataSizePtr: %d", pDesc,
        ( pDesc ) ? * ( ( short* ) pDesc ) : 0, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr ) );
    return _SQLGetDescField_basic ( pDesc, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, false );
}

RETCODE SQL_API SQLGetDescField ( SQLHDESC pDesc,
                                  SQLSMALLINT pRecNum,
                                  SQLSMALLINT pFldID,
                                  SQLPOINTER pDataPtr,
                                  SQLINTEGER pDataSize,
                                  SQLINTEGER* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "SQLGetDescField called, Desc: %d, Type: %d, Recnum: %d, Fld: %d, DataPtr: %d, DataSize: %d, DataSizePtr: %d", pDesc,
        ( pDesc ) ? * ( ( short* ) pDesc ) : 0, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr ) );
    return _SQLGetDescField_basic ( pDesc, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, true );
}

// -----------------------------------------------------------------------
// to set a single field in a desc record
// -----------------------------------------------------------------------
RETCODE SQL_API SQLSetDescFieldW ( SQLHDESC pDesc,
                                   SQLSMALLINT pRecNum,
                                   SQLSMALLINT pFldID,
                                   SQLPOINTER pDataPtr,
                                   SQLINTEGER pDataSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "SQLSetDescFieldW called, Desc: %d, Type: %d, Recnum: %d, Fld: %d, DataPtr: %d, DataSize: %d", pDesc,
        ( pDesc ) ? * ( ( short* ) pDesc ) : ( short ) pDesc, pRecNum, pFldID, pDataPtr, pDataSize ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLSetDescFieldW Not implemented" ) );
    return SQL_ERROR;
}

RETCODE SQL_API SQLSetDescField ( SQLHDESC pDesc,
                                  SQLSMALLINT pRecNum,
                                  SQLSMALLINT pFldID,
                                  SQLPOINTER pDataPtr,
                                  SQLINTEGER pDataSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "SQLSetDescField called, Desc: %d, Type: %d, Recnum: %d, Fld: %d, DataPtr: %d, DataSize: %d", pDesc,
        ( pDesc ) ? * ( ( short* ) pDesc ) : ( short ) pDesc, pRecNum, pFldID, pDataPtr, pDataSize ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLSetDescField Not implemented" ) );
    return SQL_ERROR;
}


// -----------------------------------------------------------------------
// to get multiple fields from a desc record, at one shot
// -----------------------------------------------------------------------

RETCODE SQL_API SQLGetDescRec ( SQLHDESC pDesc,
                                SQLSMALLINT pRecNum,
                                SQLCHAR* pName,
                                SQLSMALLINT pNameSize,
                                SQLSMALLINT* pNameSizePtr,
                                SQLSMALLINT* pTypePtr,
                                SQLSMALLINT* pSubTypePtr,
                                SQLINTEGER* pLengthPtr,
                                SQLSMALLINT* pPrecisionPtr,
                                SQLSMALLINT* pScalePtr,
                                SQLSMALLINT* pNullablePtr )

{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLGetDescRec called Desc: %d, DescType: %d, RecNum: %d", pDesc,
        pDesc ? * ( ( short* ) pDesc ) : 0, pRecNum ) );
    Word desctype;
    Long i;

    // precaution
    if ( pDesc == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "SQLGetDescRec - invalid params" ) );
        return SQL_ERROR;
    }

    // initializations
    desctype = _DESCTYPE ( pDesc );

    switch ( desctype )
    {
        case SQL_DESC_APD :
            pAPDItem apditem;
            _SQLFreeDiag ( & ( ( ( pODBCAPD ) pDesc ) -> Stmt -> Diag ) );
            // get item from APD
            apditem = _SQLGetAPDItem ( ( pODBCAPD ) pDesc, pRecNum );

            // check if item located
            if ( apditem == NULL )
            {
                __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetDescRec - invalid APD item" ) );
                return SQL_ERROR;
            }

            // get the fields
            _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, apditem, pRecNum, SQL_DESC_NAME, pName, pNameSize, &i );
            *pNameSizePtr = ( Word ) i;
            _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, apditem, pRecNum, SQL_DESC_TYPE, pTypePtr, -1, NULL );
            _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, apditem, pRecNum, SQL_DESC_DATETIME_INTERVAL_CODE, pSubTypePtr, -1,
                                  NULL );
            _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, apditem, pRecNum, SQL_DESC_OCTET_LENGTH, pLengthPtr, -1, NULL );
            _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, apditem, pRecNum, SQL_DESC_PRECISION, pPrecisionPtr, -1, NULL );
            _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, apditem, pRecNum, SQL_DESC_SCALE, pScalePtr, -1, NULL );
            _SQLGetAPDItemField ( ( pODBCAPD ) pDesc, apditem, pRecNum, SQL_DESC_NULLABLE, pNullablePtr, -1, NULL );
            break;

        case SQL_DESC_IPD :
            pIPDItem ipditem;
            _SQLFreeDiag ( & ( ( ( pODBCIPD ) pDesc ) -> Stmt -> Diag ) );
            // get item from IPD
            ipditem = _SQLGetIPDItem ( ( pODBCIPD ) pDesc, pRecNum );

            // check if item located
            if ( ipditem == NULL )
            {
                __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetDescRec - invalid IPD item" ) );
                return SQL_ERROR;
            }

            // set the fields
            _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, ipditem, pRecNum, SQL_DESC_NAME, pName, pNameSize, &i );
            *pNameSizePtr = ( Word ) i;
            _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, ipditem, pRecNum, SQL_DESC_TYPE, pTypePtr, -1, NULL );
            _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, ipditem, pRecNum, SQL_DESC_DATETIME_INTERVAL_CODE, pSubTypePtr, -1,
                                  NULL );
            _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, ipditem, pRecNum, SQL_DESC_OCTET_LENGTH, pLengthPtr, -1, NULL );
            _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, ipditem, pRecNum, SQL_DESC_PRECISION, pPrecisionPtr, -1, NULL );
            _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, ipditem, pRecNum, SQL_DESC_SCALE, pScalePtr, -1, NULL );
            _SQLGetIPDItemField ( ( pODBCIPD ) pDesc, ipditem, pRecNum, SQL_DESC_NULLABLE, pNullablePtr, -1, NULL );
            break;

        case SQL_DESC_ARD :
            pARDItem arditem;
            _SQLFreeDiag ( & ( ( ( pODBCARD ) pDesc ) -> Stmt -> Diag ) );
            // get item from ARD
            arditem = _SQLGetARDItem ( ( pODBCARD ) pDesc, pRecNum );

            // check if item located
            if ( arditem == NULL )
            {
                __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetDescRec - invalid ARD item" ) );
                return SQL_ERROR;
            }

            // get the fields
            _SQLGetARDItemField ( ( pODBCARD ) pDesc, arditem, pRecNum, SQL_DESC_NAME, pName, pNameSize, &i );
            *pNameSizePtr = ( Word ) i;
            _SQLGetARDItemField ( ( pODBCARD ) pDesc, arditem, pRecNum, SQL_DESC_TYPE, pTypePtr, -1, NULL );
            _SQLGetARDItemField ( ( pODBCARD ) pDesc, arditem, pRecNum, SQL_DESC_DATETIME_INTERVAL_CODE, pSubTypePtr, -1,
                                  NULL );
            _SQLGetARDItemField ( ( pODBCARD ) pDesc, arditem, pRecNum, SQL_DESC_OCTET_LENGTH, pLengthPtr, -1, NULL );
            _SQLGetARDItemField ( ( pODBCARD ) pDesc, arditem, pRecNum, SQL_DESC_PRECISION, pPrecisionPtr, -1, NULL );
            _SQLGetARDItemField ( ( pODBCARD ) pDesc, arditem, pRecNum, SQL_DESC_SCALE, pScalePtr, -1, NULL );
            _SQLGetARDItemField ( ( pODBCARD ) pDesc, arditem, pRecNum, SQL_DESC_NULLABLE, pNullablePtr, -1, NULL );
            break;

        case SQL_DESC_IRD :
            _SQLFreeDiag ( & ( ( ( pODBCIRD ) pDesc ) -> Stmt -> Diag ) );

            // fall thru

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "SQLSetDescRec - Invalid descriptor handle" ) );
            return SQL_ERROR;
    }

    return SQL_SUCCESS;
}


// -----------------------------------------------------------------------
// to set multiple fields in a desc record, at one shot
// -----------------------------------------------------------------------

RETCODE SQL_API SQLSetDescRec ( SQLHDESC pDesc,
                                SQLSMALLINT pRecNum,
                                SQLSMALLINT pType,
                                SQLSMALLINT pSubType,
                                SQLINTEGER pLength,
                                SQLSMALLINT pPrecision,
                                SQLSMALLINT pScale,
                                SQLPOINTER pDataPtr,
                                SQLINTEGER* pDataSizePtr,
                                SQLINTEGER* pDataIndPtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLSetDescRec called Desc: %d, DescType: %d, RecNum: %d", pDesc,
        pDesc ? * ( ( short* ) pDesc ) : 0, pRecNum ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLGetDescRec  Not implemented" ) );
    return SQL_ERROR;
}


// -----------------------------------------------------------------------
// to copy descriptor information from one descriptor handle to another
// -----------------------------------------------------------------------

RETCODE SQL_API SQLCopyDesc ( SQLHDESC pSrcDescHandle,
                              SQLHDESC pTgtDescHandle )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLCopyDesc called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLCopyDesc not implemented" ) );
    return SQL_ERROR;
}


/////////////////////// TO GET A DESC ITEM


// -----------------------------------------------------------------------
// to get a particular ARD item
// -----------------------------------------------------------------------

pARDItem _SQLGetARDItem ( const pODBCARD pDesc, Word pRecNum )
{
    pARDItem i;

    // loop to see if binding for that col already exists
    for ( i = pDesc -> BindCols; i != NULL && i -> ColNum != pRecNum; i = i -> Next );

    // return the ARD item
    return i;
}


// -----------------------------------------------------------------------
// to get a particular IPD item
// -----------------------------------------------------------------------

pAPDItem _SQLGetAPDItem ( const pODBCAPD pDesc, Word pRecNum )
{
    pAPDItem i;

    // loop to see if binding for that col already exists
    for ( i = pDesc -> BindParams; i != NULL && i -> ParamNum != pRecNum; i = i -> Next );

    // return the ARD item
    return i;
}


// -----------------------------------------------------------------------
// to get a particular IRD item, mhb
// -----------------------------------------------------------------------

pIRDItem _SQLGetIRDItem ( const pODBCIRD pDesc, Word pRecNum )
{
    // check if descriptor is valid
    if ( pDesc == NULL || pDesc -> RowDesc == NULL )
    {
        return NULL;
    }

    return pDesc -> RowDesc -> columnMetas . at ( pRecNum - 1 );
}


// -----------------------------------------------------------------------
// to get a particular IPD item
// -----------------------------------------------------------------------

pIPDItem _SQLGetIPDItem ( const pODBCIPD pDesc, Word pRecNum )
{
    pIPDItem i;

    // loop to see if binding for that col already exists
    for ( i = pDesc -> BindParams; i != NULL && i -> ParamNum != pRecNum; i = i -> Next );

    // return the ARD item
    return i;
}


/////////////////////// DEFAULTS


// -----------------------------------------------------------------------
// to set default values of APD header
// -----------------------------------------------------------------------

eGoodBad _SQLSetAPDFieldsDefault ( pODBCAPD pDesc, const pODBCStmt pStmt )
{
    // reset all
    memset ( pDesc, 0, sizeof ( ODBCAPD) );
    // set explicit defaults
    pDesc -> Sign = SQL_DESC_APD;
    pDesc -> Stmt = pStmt;
    pDesc -> AllocType = SQL_DESC_ALLOC_AUTO;
    pDesc -> RowArraySize = 1;
    pDesc -> BindTypeOrSize = SQL_BIND_BY_COLUMN;
    return GOOD;
}

// -----------------------------------------------------------------------
// to set default values of APD item
// -----------------------------------------------------------------------

eGoodBad _SQLSetAPDItemFieldsDefault ( pAPDItem pDescItem )
{
    // reset all
    memset ( pDescItem, 0, sizeof ( APDItem) );
    // set explicit defaults
    pDescItem -> DataConciseType = SQL_C_DEFAULT;
    pDescItem -> DataVerboseType = SQL_C_DEFAULT;
    return GOOD;
}

// -----------------------------------------------------------------------
// to set default values of IPD header
// -----------------------------------------------------------------------

eGoodBad _SQLSetIPDFieldsDefault ( pODBCIPD pDesc, const pODBCStmt pStmt )
{
    // reset all
    memset ( pDesc, 0, sizeof ( ODBCIPD) );
    // set explicit defaults
    pDesc -> Sign = SQL_DESC_IPD;
    pDesc -> Stmt = pStmt;
    return GOOD;
}

// -----------------------------------------------------------------------
// to set default values of IPD item
// -----------------------------------------------------------------------

eGoodBad _SQLSetIPDItemFieldsDefault ( pIPDItem pDescItem )
{
    // reset all
    memset ( pDescItem, 0, sizeof ( IPDItem) );
    // set explicit defaults
    pDescItem -> ParamType = SQL_PARAM_INPUT;
    pDescItem -> DataConciseType = SQL_DEFAULT;
    pDescItem -> DataVerboseType = SQL_DEFAULT;
    return GOOD;
}

// -----------------------------------------------------------------------
// to set default values of ARD header
// -----------------------------------------------------------------------

eGoodBad _SQLSetARDFieldsDefault ( pODBCARD pDesc, const pODBCStmt pStmt )
{
    // reset all
    memset ( pDesc, 0, sizeof ( ODBCARD) );
    // set explicit defaults
    pDesc -> Sign = SQL_DESC_ARD;
    pDesc -> Stmt = pStmt;
    pDesc -> AllocType = SQL_DESC_ALLOC_AUTO;
    pDesc -> RowArraySize = 1;
    pDesc -> BindTypeOrSize = SQL_BIND_BY_COLUMN;
    return GOOD;
}

// -----------------------------------------------------------------------
// to set default values of ARD item
// -----------------------------------------------------------------------

eGoodBad _SQLSetARDItemFieldsDefault ( pARDItem pDescItem, Word pRecNum )
{
    // reset
    memset ( pDescItem, 0, sizeof ( ARDItem) );
    // set explicit defaults
    pDescItem -> ColNum = pRecNum;
    pDescItem -> DataConciseType = SQL_C_DEFAULT;
    pDescItem -> DataVerboseType = SQL_C_DEFAULT;
    return GOOD;
}


// -----------------------------------------------------------------------
// to set default values of IRD header
// -----------------------------------------------------------------------

eGoodBad _SQLSetIRDFieldsDefault ( pODBCIRD pDesc, const pODBCStmt pStmt )
{
    // reset all
    memset ( pDesc, 0, sizeof ( ODBCIRD) );
    // set explicit defaults
    pDesc -> Sign = SQL_DESC_IRD;
    pDesc -> Stmt = pStmt;
    return GOOD;
}

// -----------------------------------------------------------------------
// to set default values of IRD item
// -----------------------------------------------------------------------

eGoodBad _SQLSetIRDItemFieldsDefault ( pIRDItem pDescItem )
{
    return GOOD;
}


//////////////////////// GET AND SET FIELD VALUES

/////////////////////// APD

// -----------------------------------------------------------------------
// to set a field value in APD
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLSetAPDField ( pODBCAPD pDesc, Word pFldID, const void* pDataPtr, Long pDataSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLSetAPDField called, pDesc: %d, Fld: %d, DataPtr: %d, DataSize: %d",
        pDesc, pFldID, pDataPtr, pDataSize ) );

    // precaution
    if ( pDesc == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetAPDField - invalid params" ) );
        return SQL_ERROR;
    }

    switch ( pFldID )
    {
        case SQL_DESC_ARRAY_SIZE :
            pDesc -> RowArraySize = ( ULong ) pDataPtr;
            break;

        case SQL_DESC_ARRAY_STATUS_PTR :
            pDesc -> ArrayStatusPtr = ( UWord* ) pDataPtr;
            break;

        case SQL_DESC_BIND_OFFSET_PTR :
            pDesc -> BindOffsetPtr = ( Long* ) pDataPtr;
            break;

        case SQL_DESC_BIND_TYPE :
            pDesc -> BindTypeOrSize = ( Long ) pDataPtr;
            break;

        case SQL_DESC_COUNT :
            // ???? requires that all descriptors which r above the specified
            // value are freed
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetAPDField - SQL_DESC_COUNT not implemented" ) );
            return SQL_ERROR;
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetAPDField - unknown field (%d)", pFldID ) );
            return SQL_ERROR;
            break;
    }

    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to set a field value in APD item
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLSetAPDItemField ( pODBCAPD pDesc, pAPDItem pDescItem, Word pRecNum, Word pFldID,
                                      const void* pDataPtr, Long pDataSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "_SQLSetAPDItemField called, pDesc: %d, pDescItem: %d, Recnum: %d, Fld: %d, DataPtr: %d, DataSize: %d", pDesc,
        pDescItem, pRecNum, pFldID, pDataPtr, pDataSize ) );
    pAPDItem item;

    // precaution
    if ( pDesc == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetAPDItemField - invalid params" ) );
        return SQL_ERROR;
    }

    // check if item has not been explicitly specified
    if ( pDescItem == NULL )
    {
        // get item from APD
        item = _SQLGetAPDItem ( pDesc, pRecNum );

        // check if item located
        if ( item == NULL )
        {
            // as a patch for SQL server it is temporarily ignoring the error
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLSetAPDItemField - invalid item" ) );
            return SQL_SUCCESS;
            /////////// actual action is as follows /////////
            //__ODBCPOPMSG(_ODBCPopMsg("_SQLGetAPDItemField - invalid item"));
            //return SQL_ERROR;
            /////////////////////////////////////////////////
        }
    }

    else
    {
        item = pDescItem;
    }

    // as per required field
    switch ( pFldID )
    {
        case SQL_DESC_CONCISE_TYPE :
            _SQLSetDataType ( & ( pDesc -> Stmt -> Diag ), SQL_DESC_CONCISE_TYPE, ( Word ) pDataPtr, & ( item -> DataVerboseType ),
                              & ( item -> DataConciseType ), & ( item -> DateTimeIntervalCode ) );
            break;

        case SQL_DESC_DATA_PTR :
            item -> DataPtr = ( void* ) pDataPtr;
            break;

        case SQL_DESC_DATETIME_INTERVAL_CODE :
            _SQLSetDataType ( & ( pDesc -> Stmt -> Diag ), SQL_DESC_DATETIME_INTERVAL_CODE, ( Word ) pDataPtr,
                              & ( item -> DataVerboseType ), & ( item -> DataConciseType ), & ( item -> DateTimeIntervalCode ) );
            break;

        case SQL_DESC_DATETIME_INTERVAL_PRECISION :
            item -> DateTimeIntervalPrec = ( Long ) pDataPtr;
            break;

        case SQL_DESC_INDICATOR_PTR :
            item -> SizeIndPtr = ( Long* ) pDataPtr;
            break;

        case SQL_DESC_LENGTH :
        case SQL_DESC_OCTET_LENGTH :
            item -> DataSize = ( Long ) pDataPtr;
            break;

        case SQL_DESC_NUM_PREC_RADIX :
            item -> NumPrecRadix = ( Long ) pDataPtr;
            break;

        case SQL_DESC_OCTET_LENGTH_PTR :
            item -> SizePtr = ( Long* ) pDataPtr;
            break;

        case SQL_DESC_PRECISION :
            item -> DataSize = ( Word ) pDataPtr;
            break;

        case SQL_DESC_SCALE :
            item -> Scale = ( Word ) pDataPtr;
            break;

        case SQL_DESC_TYPE :
            _SQLSetDataType ( & ( pDesc -> Stmt -> Diag ), SQL_DESC_TYPE, ( Word ) pDataPtr, & ( item -> DataVerboseType ),
                              & ( item -> DataConciseType ), & ( item -> DateTimeIntervalCode ) );
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetAPDItemField - unknown field (%d)", pFldID ) );
            return SQL_ERROR;
            break;
    }

    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to get a field value from APD
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLGetAPDField ( const pODBCAPD pDesc, Word pFldID, void* pDataPtr, Long pDataSize,
                                  Long* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "_SQLGetAPDField called, pDesc: %d, Fld: %d, DataPtr: %d, DataSize: %d, DataSizePtr: %d", pDesc, pFldID, pDataPtr,
        pDataSize, pDataSizePtr ) );

    // precaution
    if ( pDesc == NULL || pDataPtr == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "SQLGetAPDField - invalid params" ) );
        return SQL_ERROR;
    }

    // as per required field
    switch ( pFldID )
    {
        case SQL_DESC_ALLOC_TYPE :
            * ( ( Word* ) pDataPtr ) = pDesc -> AllocType;
            break;

        case SQL_DESC_ARRAY_SIZE :
            * ( ( ULong* ) pDataPtr ) = pDesc -> RowArraySize;
            break;

        case SQL_DESC_ARRAY_STATUS_PTR :
            * ( ( UWord** ) pDataPtr ) = pDesc -> ArrayStatusPtr;
            break;

        case SQL_DESC_BIND_OFFSET_PTR :
            * ( ( Long** ) pDataPtr ) = pDesc -> BindOffsetPtr;
            break;

        case SQL_DESC_BIND_TYPE :
            * ( ( Long* ) pDataPtr ) = pDesc -> BindTypeOrSize;
            break;

        case SQL_DESC_COUNT :
            * ( ( Word* ) pDataPtr ) = pDesc -> DescCount;
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetAPDField - unknown field (%d)", pFldID ) );
            return SQL_ERROR;
            break;
    }

    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to get a field value from an APD item
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLGetAPDItemField ( const pODBCAPD pDesc, const pAPDItem pDescItem, Word pRecNum,
                                      Word pFldID, void* pDataPtr, Long pDataSize, Long* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "_SQLGetAPDItemField called, pDesc: %d, pDescItem: %d, Recnum: %d, Fld: %d, DataPtr: %d, DataSize: %d, DataSizePtr: %d",
        pDesc, pDescItem, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr ) );
    pAPDItem item;

    // precaution
    if ( pDesc == NULL || pDataPtr == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetAPDItemField - invalid params" ) );
        return SQL_ERROR;
    }

    // check if item has not been specified directly
    if ( pDescItem == NULL )
    {
        // get item from IPD
        item = _SQLGetAPDItem ( pDesc, pRecNum );

        // check if item located
        if ( item == NULL )
        {
            // as a patch fro SQL server it is temporarily ignoring the error
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLGetAPDItemField - invalid item" ) );
            return SQL_SUCCESS;
            /////////// actual action is as follows /////////
            //__ODBCPOPMSG(_ODBCPopMsg("_SQLGetAPDItemField - invalid item"));
            //return SQL_ERROR;
            /////////////////////////////////////////////////
        }
    }

    else
    {
        item = pDescItem;
    }

    // as per required field
    switch ( pFldID )
    {
        case SQL_DESC_CONCISE_TYPE :
            * ( ( Word* ) pDataPtr ) = item -> DataConciseType;
            break;

        case SQL_DESC_DATA_PTR :
            * ( ( Long* ) pDataPtr ) = ( Long ) ( item -> DataPtr );
            break;

        case SQL_DESC_DATETIME_INTERVAL_CODE :
            * ( ( Word* ) pDataPtr ) = item -> DateTimeIntervalCode;
            break;

        case SQL_DESC_DATETIME_INTERVAL_PRECISION :
            * ( ( Long* ) pDataPtr ) = item -> DateTimeIntervalPrec;
            break;

        case SQL_DESC_INDICATOR_PTR :
            * ( ( Long** ) pDataPtr ) = item -> SizeIndPtr;
            break;

        case SQL_DESC_LENGTH :
        case SQL_DESC_OCTET_LENGTH :
            * ( ( Long* ) pDataPtr ) = item -> DataSize;
            break;

        case SQL_DESC_NUM_PREC_RADIX :
            * ( ( Long* ) pDataPtr ) = item -> NumPrecRadix;
            break;

        case SQL_DESC_OCTET_LENGTH_PTR :
            * ( ( Long** ) pDataPtr ) = item -> SizePtr;
            break;

        case SQL_DESC_PRECISION :
            * ( ( Word* ) pDataPtr ) = ( Word ) ( item -> DataSize );
            break;

        case SQL_DESC_SCALE :
            * ( ( Word* ) pDataPtr ) = ( Word ) ( item -> Scale );
            break;

        case SQL_DESC_TYPE :
            * ( ( Word* ) pDataPtr ) = item -> DataVerboseType;
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetAPDItemField - unknown field (%d)", pFldID ) );
            return SQL_ERROR;
            break;
    }

    return SQL_SUCCESS;
}


/////////////////////// IPD


// -----------------------------------------------------------------------
// to set a field value in IPD
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLSetIPDField ( pODBCIPD pDesc, Word pFldID, const void* pDataPtr, Long pDataSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLSetIPDField called, pDesc: %d, Fld: %d, DataPtr: %d, DataSize: %d",
        pDesc, pFldID, pDataPtr, pDataSize ) );

    // precaution
    if ( pDesc == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetIPDField - invalid params" ) );
        return SQL_ERROR;
    }

    // as per required field
    switch ( pFldID )
    {
        case SQL_DESC_ALLOC_TYPE :
            // assumes automatic alloc by driver as appl alloc
            // is not allowed as of now
            break;

        case SQL_DESC_ARRAY_STATUS_PTR :
            pDesc -> ArrayStatusPtr = ( UWord* ) pDataPtr;
            break;

        case SQL_DESC_COUNT :
            // ???? requires that all descriptors which r above the specified
            // value are freed. not implemented as of now
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetIPDField - SQL_DESC_COUNT not implemented" ) );
            return SQL_ERROR;
            break;

        case SQL_DESC_ROWS_PROCESSED_PTR :
            pDesc -> RowsProcessedPtr = ( ULong* ) pDataPtr;
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetIPDField - unknown field (%d)", pFldID ) );
            return SQL_ERROR;
            break;
    }

    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to set a field value in IPD item
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLSetIPDItemField ( pODBCIPD pDesc, pIPDItem pDescItem, Word pRecNum, Word pFldID,
                                      const void* pDataPtr, Long pDataSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "_SQLSetIPDItemField called, pDesc: %d, pDescItem: %d, Recnum: %d, Fld: %d, DataPtr: %d, DataSize: %d", pDesc,
        pDescItem, pRecNum, pFldID, pDataPtr, pDataSize ) );
    pIPDItem item;

    // precaution
    if ( pDesc == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetIPDItemField - invalid params" ) );
        return SQL_ERROR;
    }

    // check if item has not been specified directly
    if ( pDescItem == NULL )
    {
        // get item from IRD
        item = _SQLGetIPDItem ( pDesc, pRecNum );

        // check if item located
        if ( item == NULL )
        {
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetIPDItemField - invalid item" ) );
            return SQL_ERROR;
        }
    }

    else
    {
        item = pDescItem;
    }

    // as per required field
    switch ( pFldID )
    {
        case SQL_DESC_CONCISE_TYPE :
            _SQLSetDataType ( & ( pDesc -> Stmt -> Diag ), SQL_DESC_CONCISE_TYPE, ( Word ) pDataPtr, & ( item -> DataVerboseType ),
                              & ( item -> DataConciseType ), & ( item -> DateTimeIntervalCode ) );
            break;

        case SQL_DESC_DATETIME_INTERVAL_CODE :
            _SQLSetDataType ( & ( pDesc -> Stmt -> Diag ), SQL_DESC_DATETIME_INTERVAL_CODE, ( Word ) pDataPtr,
                              & ( item -> DataVerboseType ), & ( item -> DataConciseType ), & ( item -> DateTimeIntervalCode ) );
            break;

        case SQL_DESC_DATETIME_INTERVAL_PRECISION :
            item -> DateTimeIntervalPrec = ( Long ) pDataPtr;
            break;

        case SQL_DESC_LENGTH :
        case SQL_DESC_OCTET_LENGTH :
            item -> DataSize = ( Long ) pDataPtr;
            break;

        case SQL_DESC_NAME :
            if ( pDataPtr && strlen ( ( Char* ) pDataPtr ) <= 32 )
            {
                strcpy ( item -> ParamName, ( Char* ) pDataPtr );
            }

            else
            {
                item -> ParamName[0] = 0;
            }

        case SQL_DESC_NUM_PREC_RADIX :
            item -> NumPrecRadix = ( Long ) pDataPtr;
            break;

        case SQL_DESC_PARAMETER_TYPE :
            item -> ParamType = ( Word ) pDataPtr;
            break;

        case SQL_DESC_PRECISION :
            item -> DataSize = ( Word ) pDataPtr;
            break;

        case SQL_DESC_SCALE :
            item -> Scale = ( Word ) pDataPtr;
            break;

        case SQL_DESC_TYPE :
            _SQLSetDataType ( & ( pDesc -> Stmt -> Diag ), SQL_DESC_TYPE, ( Word ) pDataPtr, & ( item -> DataVerboseType ),
                              & ( item -> DataConciseType ), & ( item -> DateTimeIntervalCode ) );
            break;

        case SQL_DESC_UNNAMED :
            // dummy, is related to SQL_DESC_NAME
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetIPDItemField - unknown field (%d)", pFldID ) );
            return SQL_ERROR;
            break;
    }

    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to get a field value from IPD
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLGetIPDField ( const pODBCIPD pDesc, Word pFldID, void* pDataPtr, Long pDataSize,
                                  Long* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "_SQLGetIPDField called, pDesc: %d, Fld: %d, DataPtr: %d, DataSize: %d, DataSizePtr: %d", pDesc, pFldID, pDataPtr,
        pDataSize, pDataSizePtr ) );

    // precaution
    if ( pDesc == NULL || pDataPtr == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetIPDField - invalid params" ) );
        return SQL_ERROR;
    }

    // as per required field
    switch ( pFldID )
    {
        case SQL_DESC_ALLOC_TYPE :
            * ( ( Word* ) pDataPtr ) = SQL_DESC_ALLOC_AUTO;
            break;

        case SQL_DESC_ARRAY_STATUS_PTR :
            * ( ( UWord** ) pDataPtr ) = pDesc -> ArrayStatusPtr;
            break;

        case SQL_DESC_COUNT :
            * ( ( Word* ) pDataPtr ) = pDesc -> DescCount;
            break;

        case SQL_DESC_ROWS_PROCESSED_PTR :
            * ( ( ULong** ) pDataPtr ) = pDesc -> RowsProcessedPtr;
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetIPDField - unknown field (%d)", pFldID ) );
            return SQL_ERROR;
            break;
    }

    return SQL_SUCCESS;
}


// -----------------------------------------------------------------------
// to get a field value from an IPD item
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLGetIPDItemField ( const pODBCIPD pDesc, const pIPDItem pDescItem, Word pRecNum,
                                      Word pFldID, void* pDataPtr, Long pDataSize, Long* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "_SQLGetIPDItemField called, pDesc: %d, pDescItem: %d, Recnum: %d, Fld: %d, DataPtr: %d, DataSize: %d, DataSizePtr: %d",
        pDesc, pDescItem, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr ) );
    pIPDItem item;

    // precaution
    if ( pDesc == NULL || pDataPtr == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetIPDItemField - invalid params" ) );
        return SQL_ERROR;
    }

    // check if item has not been specified directly
    if ( pDescItem == NULL )
    {
        // get item from IPD
        item = _SQLGetIPDItem ( pDesc, pRecNum );

        // check if item located
        if ( item == NULL )
        {
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetIPDItemField - invalid item" ) );
            return SQL_ERROR;
        }
    }

    else
    {
        item = pDescItem;
    }

    // as per required field
    switch ( pFldID )
    {
        case SQL_DESC_CASE_SENSITIVE :
            * ( ( Long* ) pDataPtr ) = SQL_FALSE; // whether a param is case-sensitive
            break;

        case SQL_DESC_CONCISE_TYPE :
            * ( ( Word* ) pDataPtr ) = item -> DataConciseType;
            break;

        case SQL_DESC_DATETIME_INTERVAL_CODE :
            * ( ( Word* ) pDataPtr ) = item -> DateTimeIntervalCode;
            break;

        case SQL_DESC_DATETIME_INTERVAL_PRECISION :
            * ( ( Long* ) pDataPtr ) = item -> DateTimeIntervalPrec;
            break;

        case SQL_DESC_FIXED_PREC_SCALE :
            * ( ( Word* ) pDataPtr ) = item -> FixedPrecScale;
            break;

        case SQL_DESC_LENGTH :
        case SQL_DESC_OCTET_LENGTH :
            * ( ( Long* ) pDataPtr ) = item -> DataSize;
            break;

        case SQL_DESC_TYPE_NAME :
        case SQL_DESC_LOCAL_TYPE_NAME :
            // ???? there is no param type string defined in IPD as of now
            _SQLCopyCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 16, "", -1 );
            break;

        case SQL_DESC_NAME :
            _SQLCopyCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 16, item -> ParamName, -1 );
            break;

        case SQL_DESC_NULLABLE :
            * ( ( Word* ) pDataPtr ) = item -> Nullable;
            break;

        case SQL_DESC_NUM_PREC_RADIX :
            * ( ( Long* ) pDataPtr ) = item -> NumPrecRadix;
            break;

        case SQL_DESC_PARAMETER_TYPE :
            * ( ( Word* ) pDataPtr ) = item -> ParamType;
            break;

        case SQL_DESC_PRECISION :
            * ( ( Word* ) pDataPtr ) = ( Word ) ( item -> DataSize );
            break;

        case SQL_DESC_ROWVER :
            * ( ( Word* ) pDataPtr ) = SQL_FALSE; // assumes that all cols r not auto-updating
            break;

        case SQL_DESC_SCALE :
            * ( ( Word* ) pDataPtr ) = item -> Scale;
            break;

        case SQL_DESC_TYPE :
            * ( ( Word* ) pDataPtr ) = item -> DataVerboseType;
            break;

        case SQL_DESC_UNNAMED :
            * ( ( Word* ) pDataPtr ) = SQL_NAMED;
            break;

        case SQL_DESC_UNSIGNED :
            * ( ( Word* ) pDataPtr ) = SQL_FALSE;
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetIPDItemField - unknown field (%d)", pFldID ) );
            return SQL_ERROR;
            break;
    }

    return SQL_SUCCESS;
}


/////////////////////// ARD

// -----------------------------------------------------------------------
// to set a field value in ARD header
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLSetARDField ( pODBCARD pDesc, Word pFldID, const void* pDataPtr, Long pDataSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLSetARDField called, pDesc: %d, Fld: %d, DataPtr: %d, DataSize: %d",
        pDesc, pFldID, pDataPtr, pDataSize ) );

    // precaution
    if ( pDesc == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetARDField - invalid params" ) );
        return SQL_ERROR;
    }

    // as per required field
    switch ( pFldID )
    {
        case SQL_DESC_ARRAY_SIZE :
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "ARD RowArraySize is set to %d", ( ULong ) pDataPtr ) );
            pDesc -> RowArraySize = ( ULong ) pDataPtr;
            break;

        case SQL_DESC_ARRAY_STATUS_PTR :
            pDesc -> ArrayStatusPtr = ( UWord* ) pDataPtr;
            break;

        case SQL_DESC_BIND_OFFSET_PTR :
            pDesc -> BindOffsetPtr = ( Long* ) pDataPtr;
            break;

        case SQL_DESC_BIND_TYPE :
            pDesc -> BindTypeOrSize = ( Long ) pDataPtr;
            break;

        case SQL_DESC_COUNT :
            // ???? requires that all descriptors which r above the specified
            // value are freed
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetARDField - SQL_DESC_COUNT not implemented" ) );
            return SQL_ERROR;
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetARDField - unknown field (%d)", pFldID ) );
            return SQL_ERROR;
            break;
    }

    return SQL_SUCCESS;
}


// -----------------------------------------------------------------------
// to set a field value in ARD item
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLSetARDItemField ( pODBCARD pDesc, pARDItem pDescItem, Word pRecNum, Word pFldID,
                                      const void* pDataPtr, Long pDataSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "_SQLSetARDItemField called, pDesc: %d, pDescItem: %d, Recnum: %d, Fld: %d, DataPtr: %d, DataSize: %d", pDesc,
        pDescItem, pRecNum, pFldID, pDataPtr, pDataSize ) );
    pARDItem item;

    // precaution
    if ( pDesc == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetARDItemField - invalid params" ) );
        return SQL_ERROR;
    }

    // check if item has not been specified directly
    if ( pDescItem == NULL )
    {
        // get item from ARD
        item = _SQLGetARDItem ( pDesc, pRecNum );

        // check if item located
        if ( item == NULL )
        {
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetARDItemField - invalid item" ) );
            return SQL_ERROR;
        }
    }

    else
    {
        item = pDescItem;
    }

    // as per required field
    switch ( pFldID )
    {
        case SQL_DESC_CONCISE_TYPE :
            _SQLSetDataType ( & ( pDesc -> Stmt -> Diag ), SQL_DESC_CONCISE_TYPE, ( Word ) pDataPtr, & ( item -> DataVerboseType ),
                              & ( item -> DataConciseType ), & ( item -> DateTimeIntervalCode ) );
            break;

        case SQL_DESC_DATA_PTR :
            item -> DataPtr = ( void* ) pDataPtr;
            break;

        case SQL_DESC_DATETIME_INTERVAL_CODE :
            _SQLSetDataType ( & ( pDesc -> Stmt -> Diag ), SQL_DESC_DATETIME_INTERVAL_CODE, ( Word ) pDataPtr,
                              & ( item -> DataVerboseType ), & ( item -> DataConciseType ), & ( item -> DateTimeIntervalCode ) );
            break;

        case SQL_DESC_DATETIME_INTERVAL_PRECISION :
            item -> DateTimeIntervalPrec = ( Long ) pDataPtr;
            break;

        case SQL_DESC_INDICATOR_PTR :
            item -> SizeIndPtr = ( Long* ) pDataPtr;
            break;

        case SQL_DESC_LENGTH :
        case SQL_DESC_OCTET_LENGTH :
            item -> DataSize = ( Long ) pDataPtr;
            break;

        case SQL_DESC_NUM_PREC_RADIX :
            item -> NumPrecRadix = ( Long ) pDataPtr;
            break;

        case SQL_DESC_OCTET_LENGTH_PTR : // 1004
            item -> SizePtr = ( Long* ) pDataPtr;
            break;

        case SQL_DESC_PRECISION :
            item -> DataSize = ( Word ) pDataPtr; // bytes required for numeric type
            break;

        case SQL_DESC_SCALE :
            item -> Scale = ( Word ) pDataPtr;
            break;

        case SQL_DESC_TYPE :
            _SQLSetDataType ( & ( pDesc -> Stmt -> Diag ), SQL_DESC_TYPE, ( Word ) pDataPtr, & ( item -> DataVerboseType ),
                              & ( item -> DataConciseType ), & ( item -> DateTimeIntervalCode ) );
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetARDItemField - unknown field (%d)", pFldID ) );
            return SQL_ERROR;
            break;
    }

    return SQL_SUCCESS;
}


// -----------------------------------------------------------------------
// to get a field value from ARD
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLGetARDField ( const pODBCARD pDesc, Word pFldID, void* pDataPtr, Long pDataSize,
                                  Long* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "_SQLGetARDField called, pDesc: %d, Fld: %d, DataPtr: %d, DataSize: %d, DataSizePtr: %d", pDesc, pFldID, pDataPtr,
        pDataSize, pDataSizePtr ) );

    // precaution
    if ( pDesc == NULL || pDataPtr == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetARDField - invalid params" ) );
        return SQL_ERROR;
    }

    // as per required field
    switch ( pFldID )
    {
        case SQL_DESC_ALLOC_TYPE :
            * ( ( Word* ) pDataPtr ) = pDesc -> AllocType;
            break;

        case SQL_DESC_ARRAY_SIZE :
            * ( ( ULong* ) pDataPtr ) = pDesc -> RowArraySize;
            break;

        case SQL_DESC_ARRAY_STATUS_PTR :
            * ( ( UWord** ) pDataPtr ) = pDesc -> ArrayStatusPtr;
            break;

        case SQL_DESC_BIND_OFFSET_PTR :
            * ( ( Long** ) pDataPtr ) = pDesc -> BindOffsetPtr;
            break;

        case SQL_DESC_BIND_TYPE :
            * ( ( Long* ) pDataPtr ) = pDesc -> BindTypeOrSize;
            break;

        case SQL_DESC_COUNT :
            * ( ( Word* ) pDataPtr ) = pDesc -> DescCount;
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetARDField - unknown field (%d)", pFldID ) );
            return SQL_ERROR;
            break;
    }

    return SQL_SUCCESS;
}


// -----------------------------------------------------------------------
// to get a field value from an ARD item
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLGetARDItemField ( const pODBCARD pDesc, const pARDItem pDescItem, Word pRecNum,
                                      Word pFldID, void* pDataPtr, Long pDataSize, Long* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "_SQLGetIPDItemField called, pDesc: %d, pDescItem: %d, Recnum: %d, Fld: %d, DataPtr: %d, DataSize: %d, DataSizePtr: %d",
        pDesc, pDescItem, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr ) );
    pARDItem item;

    // precaution
    if ( pDesc == NULL || pDataPtr == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetARDItemField - invalid params" ) );
        return SQL_ERROR;
    }

    // check if item has not been specified directly
    if ( pDescItem == NULL )
    {
        // get item from ARD
        item = _SQLGetARDItem ( pDesc, pRecNum );

        // check if item located
        if ( item == NULL )
        {
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetARDField - invalid item" ) );
            return SQL_ERROR;
        }
    }

    else
    {
        item = pDescItem;
    }

    // as per required field
    switch ( pFldID )
    {
        case SQL_DESC_CONCISE_TYPE :
            * ( ( Word* ) pDataPtr ) = item -> DataConciseType;
            break;

        case SQL_DESC_DATA_PTR :
            * ( ( Long* ) pDataPtr ) = ( Long ) ( item -> DataPtr );
            break;

        case SQL_DESC_DATETIME_INTERVAL_CODE :
            * ( ( Word* ) pDataPtr ) = item -> DateTimeIntervalCode;
            break;

        case SQL_DESC_DATETIME_INTERVAL_PRECISION :
            * ( ( Long* ) pDataPtr ) = item -> DateTimeIntervalPrec;
            break;

        case SQL_DESC_INDICATOR_PTR :
            * ( ( Long** ) pDataPtr ) = item -> SizeIndPtr;
            break;

        case SQL_DESC_LENGTH :
        case SQL_DESC_OCTET_LENGTH :
            * ( ( Long* ) pDataPtr ) = item -> DataSize;
            break;

        case SQL_DESC_NUM_PREC_RADIX :
            * ( ( Long* ) pDataPtr ) = item -> NumPrecRadix;
            break;

        case SQL_DESC_OCTET_LENGTH_PTR :
            * ( ( Long** ) pDataPtr ) = item -> SizePtr;
            break;

        case SQL_DESC_PRECISION :
            * ( ( Word* ) pDataPtr ) = ( Word ) ( item -> DataSize );
            break;

        case SQL_DESC_SCALE :
            * ( ( Word* ) pDataPtr ) = ( Word ) ( item -> Scale );
            break;

        case SQL_DESC_TYPE :
            * ( ( Word* ) pDataPtr ) = item -> DataVerboseType;
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetARDItemField - unknown field (%d)", pFldID ) );
            return SQL_ERROR;
            break;
    }

    return SQL_SUCCESS;
}


/////////////////////// IRD


// -----------------------------------------------------------------------
// to set a field value in IRD item
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLSetIRDField ( pODBCIRD pDesc, Word pFldID, const void* pDataPtr, Long pDataSize )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "_SQLSetIRDField called, pDesc: %d, Fld: %d, DataPtr: %d, DataSize: %d",
        pDesc, pFldID, pDataPtr, pDataSize ) );

    // precaution
    if ( pDesc == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetIRDField - invalid params" ) );
        return SQL_ERROR;
    }

    // as per required field
    switch ( pFldID )
    {
        case SQL_DESC_ARRAY_STATUS_PTR :
            pDesc -> ArrayStatusPtr = ( UWord* ) pDataPtr;
            break;

        case SQL_DESC_ROWS_PROCESSED_PTR :
            pDesc -> RowsProcessedPtr = ( ULong* ) pDataPtr;
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLSetIRDField - unknown field (%d)", pFldID ) );
            return SQL_ERROR;
            break;
    }

    return SQL_SUCCESS;
}

/**********
    // -----------------------------------------------------------------------
    // to set a field value in IRD item
    // -----------------------------------------------------------------------

    RETCODE SQL_API _SQLSetIRDItemField ( pODBCIRD pDesc, pIRDItem pDescItem, Word pRecNum, Word pFldID, const void* pDataPtr, Long pDataSize )
    {
    __ODBCPOPMSG(_ODBCPopMsg("_SQLSetIRDItemField - unknown field (%d)", pFldID));

    return SQL_SUCCESS;
    }
**********/

// -----------------------------------------------------------------------
// to get a field value from IRD
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLGetIRDField ( const pODBCIRD pDesc, Word pFldID, void* pDataPtr, Long pDataSize,
                                  Long* pDataSizePtr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "_SQLGetIRDField called, pDesc: %d, Fld: %d, DataPtr: %d, DataSize: %d, DataSizePtr: %d", pDesc, pFldID, pDataPtr,
        pDataSize, pDataSizePtr ) );

    // precaution
    if ( pDesc == NULL || pDataPtr == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetIRDField - invalid params" ) );
        return SQL_ERROR;
    }

    // as per required field
    switch ( pFldID )
    {
        case SQL_DESC_ALLOC_TYPE :
            * ( ( Word* ) pDataPtr ) = SQL_DESC_ALLOC_AUTO;
            break;

        case SQL_DESC_ARRAY_STATUS_PTR :
            * ( ( UWord** ) pDataPtr ) = pDesc -> ArrayStatusPtr;
            break;

        case SQL_DESC_COUNT :
            * ( ( Word* ) pDataPtr ) = pDesc -> DescCount;
            break;

        case SQL_DESC_ROWS_PROCESSED_PTR :
            * ( ( ULong** ) pDataPtr ) = pDesc -> RowsProcessedPtr;
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetIRDField - unknown field (%d)", pFldID ) );
            return SQL_ERROR;
            break;
    }

    return SQL_SUCCESS;
}

static bool isApproximateNumerical ( int type )
{
    //According to this article:
    //Data Types in SQL Statements
    //http://developer.mimer.com/documentation/Mimer_SQL_Reference_Manual/Syntax_Rules4.html
    if ( type == ( int ) ODBCTypes::ODBC_Float ||
        type == ( int ) ODBCTypes::ODBC_Real ||
        type == ( int ) ODBCTypes::ODBC_Double )
    {
        return true;
    }

    return false;
}


// -----------------------------------------------------------------------
// to get a field value from an IRD item, mhb
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLGetIRDItemField ( const pODBCIRD pDesc, const pIRDItem pDescItem, Word pRecNum,
                                      Word pFldID, void* pDataPtr, Long pDataSize, Long* pDataSizePtr, bool isANSI )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG,
        "_SQLGetIRDItemField called, pDesc: %d, pDescItem: %d, Recnum: %d, Fld: %d, DataPtr: %d, DataSize: %d, DataSizePtr: %d, isANSI: %d",
        pDesc, pDescItem, pRecNum, pFldID, pDataPtr, pDataSize, pDataSizePtr, isANSI ) );
    CStrPtr s;
    pIRDItem item;

    // precaution
    if ( pDesc == NULL || pDataPtr == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetIRDItemField - invalid params" ) );
        return SQL_ERROR;
    }

    // check if item has not been specified directly
    if ( pDescItem == NULL )
    {
        // get item from IRD
        item = _SQLGetIRDItem ( pDesc, pRecNum );

        // check if item located
        if ( item == NULL )
        {
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetIRDItemField - invalid item" ) );
            return SQL_ERROR;
        }
    }

    else
    {
        item = pDescItem;
    }

    // as per required field
    switch ( pFldID )
    {
        case SQL_DESC_AUTO_UNIQUE_VALUE :
            * ( ( Long* ) pDataPtr ) = pDescItem -> isAutoIncrement; // whether a col is auto-incrementing
            break;

        case SQL_DESC_LABEL : //18
            s = pDescItem -> label . c_str ();

            // transfer col label
            if ( isANSI )
            {
                _SQLCopyCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, ( ( s ) ? s : "" ), -1 );
            }

            else
            {
                _SQLCopyWCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, ( ( s ) ? s : "" ), -1 );
            }

            break;

        case SQL_DESC_NAME : //1011
            s = pDescItem -> label . c_str ();
            if ( !s ) s = pDescItem -> name . c_str ();

            // transfer col desc
            if ( isANSI )
            {
                _SQLCopyCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, ( ( s ) ? s : "" ), -1 );
            }

            else
            {
                _SQLCopyWCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, ( ( s ) ? s : "" ), -1 );
            }

            break;

        case SQL_DESC_BASE_COLUMN_NAME : //22
            s = pDescItem -> name . c_str ();

            // transfer col name
            if ( isANSI )
            {
                _SQLCopyCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, ( ( s ) ? s : "" ), -1 );
            }

            else
            {
                _SQLCopyWCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, ( ( s ) ? s : "" ), -1 );
            }

            break;

        case SQL_DESC_BASE_TABLE_NAME : //23
            if ( isANSI )
            {
                _SQLCopyCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, pDescItem -> tableName . c_str (), -1 );
            }

            else
            {
                _SQLCopyWCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, pDescItem -> tableName . c_str (), -1 );
            }

            break;

        case SQL_DESC_CASE_SENSITIVE : //12
            * ( ( Long* ) pDataPtr ) = pDescItem -> isCaseSensitive; // whether a col is case-sensitive
            break;

        case SQL_DESC_CATALOG_NAME : //17
            s = pDescItem -> catelogName . c_str ();

            if ( isANSI )
            {
                _SQLCopyCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 16, s, -1 );
            }

            else
            {
                _SQLCopyWCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 16, s, -1 );
            }

            break;

        case SQL_DESC_TYPE : //1002
        case SQL_DESC_CONCISE_TYPE : //2
            * ( ( SQLLEN* ) pDataPtr ) = pDescItem -> columnType;
            break;

        case SQL_DESC_DATETIME_INTERVAL_CODE : //1007
            * ( ( Word* ) pDataPtr ) = 0;
            break;

        case SQL_DESC_DATETIME_INTERVAL_PRECISION : //26
            * ( ( Long* ) pDataPtr ) = 0;
            break;

        case SQL_COLUMN_LENGTH : //3
        case SQL_DESC_LENGTH : //1003
        case SQL_DESC_DISPLAY_SIZE : //6
            if ( isANSI )
            {
                * ( ( Long* ) pDataPtr ) = pDescItem -> displaySize;
            }

            else
            {
                * ( ( Long* ) pDataPtr ) = pDescItem -> displaySize * 2;
            }

            break;

        case SQL_DESC_OCTET_LENGTH : //1013
            if ( isANSI )
            {
                * ( ( Long* ) pDataPtr ) = pDescItem -> displaySize;
            }

            else
            {
                * ( ( Long* ) pDataPtr ) = pDescItem -> displaySize * 2;
            }

            break;

        case SQL_DESC_FIXED_PREC_SCALE : //9

            //SQL_TRUE if the column has a fixed precision and nonzero scale that are data source�Cspecific.
            //SQL_FALSE if the column does not have a fixed precision and nonzero scale that are data source�Cspecific.
            if ( isApproximateNumerical ( pDescItem -> columnType ) )
            {
                * ( ( Long* ) pDataPtr ) = SQL_FALSE;
            }

            else
            {
                * ( ( Long* ) pDataPtr ) = SQL_TRUE;
            }

            break;

        case SQL_DESC_LITERAL_PREFIX : //27
        case SQL_DESC_LITERAL_SUFFIX : //28

            // assumes prefix and suffix to be single quote
            if ( isANSI )
            {
                _SQLCopyCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, "\"", -1 );
            }

            else
            {
                _SQLCopyWCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, "\"", -1 );
            }

            break;

        case SQL_DESC_TYPE_NAME : //14
        case SQL_DESC_LOCAL_TYPE_NAME : //29
            s = pDescItem -> columnTypeName . c_str ();

            // trasnfer col type name
            if ( isANSI )
            {
                _SQLCopyCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, s, -1 );
            }

            else
            {
                _SQLCopyWCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, s, -1 );
            }

            break;

        case SQL_DESC_NULLABLE : //1008
            if ( pDescItem -> isNullable == 1 )
            {
                * ( ( Word* ) pDataPtr ) = SQL_NULLABLE;
                break;
            }

            else if ( pDescItem -> isNullable == 0 )
            {
                * ( ( Word* ) pDataPtr ) = SQL_NO_NULLS;
                break;
            }

            // fall thru
            * ( ( Word* ) pDataPtr ) = SQL_NULLABLE_UNKNOWN;
            break;

        case SQL_DESC_NUM_PREC_RADIX : //32

            //If the data type in the SQL_DESC_TYPE field is an approximate numeric data type,
            //this SQLINTEGER field contains a value of 2 because the SQL_DESC_PRECISION field contains the number of bits.
            //If the data type in the SQL_DESC_TYPE field is an exact numeric data type,
            //this field contains a value of 10 because the SQL_DESC_PRECISION field contains the number of decimal digits.
            //This field is set to 0 for all non-numeric data types.
            if ( isApproximateNumerical ( pDescItem -> columnType ) )
            {
                * ( ( Long* ) pDataPtr ) = 2;
            }

            else
            {
                * ( ( Long* ) pDataPtr ) = 10;
            }

            break;

        case SQL_COLUMN_PRECISION : //4
        case SQL_DESC_PRECISION : //1005
            * ( ( Long* ) pDataPtr ) = pDescItem -> precision;
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "precision is returned as %i ", * ( ( Word* ) pDataPtr ) ) );
            break;

        case SQL_DESC_ROWVER : //35
            // assumes that all cols r not auto-updating like TIMESTAMP
            * ( ( Word* ) pDataPtr ) = SQL_FALSE;
            break;

        case SQL_COLUMN_SCALE : //5
        case SQL_DESC_SCALE : //1006
            * ( ( Long* ) pDataPtr ) = pDescItem -> scale;
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "scale is returned as %i ", * ( ( Word* ) pDataPtr ) ) );
            break;

        case SQL_DESC_SCHEMA_NAME : //16
            s = pDescItem -> schemaName . c_str ();

            if ( isANSI )
            {
                _SQLCopyCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, s, -1 );
            }

            else
            {
                _SQLCopyWCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, s, -1 );
            }

            break;

        case SQL_DESC_SEARCHABLE : //13
            * ( ( Long* ) pDataPtr ) = SQL_PRED_SEARCHABLE;
            break;

        case SQL_DESC_TABLE_NAME : //15
            s = pDescItem -> tableName . c_str ();

            if ( isANSI )
            {
                _SQLCopyCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, s, -1 );
            }

            else
            {
                _SQLCopyWCharData ( _DIAGSTMT ( pDesc->Stmt ), pDataPtr, pDataSize, pDataSizePtr, 32, s, -1 );
            }

            break;

        case SQL_DESC_UNNAMED : //1012
            * ( ( Long* ) pDataPtr ) = SQL_NAMED;
            break;

        case SQL_DESC_UNSIGNED : //8
            * ( ( Long* ) pDataPtr ) = !pDescItem -> isSigned;
            break;

        case SQL_DESC_UPDATABLE : //10
            * ( ( Long* ) pDataPtr ) = SQL_ATTR_READONLY;
            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLGetIRDItemField - unknown field (%d)", pFldID ) );
            return SQL_ERROR;
            break;
    }

    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "get item returned" ) );
    return SQL_SUCCESS;
}


///////////////////// FREE

// -----------------------------------------------------------------------
// to free/release the content of APD
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLFreeAPDContent ( pODBCAPD pDesc )
{
    // since params have not been implemented
    // it assumes that no params have been allocated
    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to free/release the content of IPD
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLFreeIPDContent ( pODBCIPD pDesc )
{
    // since params have not been implemented
    // it assumes that no params have been allocated
    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to free/release the content of ARD
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLFreeARDContent ( pODBCARD pDesc )
{
    pARDItem ardcol; // application row descriptor item

    // check if there r any bound cols in ARD
    if ( pDesc -> BindCols )
    {
        // loop to free all the bound cols
        while ( pDesc -> BindCols )
        {
            // get the first ard-col
            ardcol = pDesc -> BindCols;
            // detach it from ARD link list
            _SQLDetachARDItem ( pDesc, ardcol );
            // free
            delete ardcol;
        }
    }

    // reset the highest descriptor count
    pDesc -> DescCount = 0;
    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to free/release the content of IRD
// -----------------------------------------------------------------------

RETCODE SQL_API _SQLFreeIRDContent ( pODBCIRD pDesc )
{
    pDesc -> DescCount = 0;

    if ( pDesc -> RowDesc )
    {
        pDesc -> RowDesc = NULL;
    }

    return SQL_SUCCESS;
}


////////////////////// ATTACH/DETACH

// -----------------------------------------------------------------------
// to attach a new item to ARD link list
// -----------------------------------------------------------------------

eGoodBad _SQLAttachARDItem ( pODBCARD pDesc, pARDItem pDescItem )
{
    // note
    // this function also helps in maintaining highest desc number
    pARDItem l;

    // precaution
    if ( pDesc == NULL || pDescItem == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLAttachARDItem - invalid params" ) );
        return BAD;
    }

    // check if this is the first item
    if ( pDesc -> BindCols == NULL )
    {
        // set as first and only item
        pDesc -> BindCols = pDescItem;
        pDescItem -> Prev = NULL;
        pDescItem -> Next = NULL;
        return GOOD;
    }

    // move to tail item
    for ( l = pDesc -> BindCols; l -> Next != NULL; l = l -> Next );

    // attach to tail
    l -> Next = pDescItem;
    pDescItem -> Prev = l;
    pDescItem -> Next = NULL;

    // maintain highest desc number
    if ( pDesc -> DescCount < pDescItem -> ColNum )
    {
        pDesc -> DescCount = pDescItem -> ColNum;
    }

    return GOOD;
}

// -----------------------------------------------------------------------
// to detach an existing item from ARD link-list
// -----------------------------------------------------------------------

eGoodBad _SQLDetachARDItem ( pODBCARD pDesc, pARDItem pDescItem )
{
    // note
    // this function also helps in maintaining highest desc number

    // precaution
    if ( pDesc == NULL || pDescItem == NULL )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "_SQLDetachARDItem - invalid params" ) );
        return BAD;
    }

    if ( pDescItem -> Prev )
    {
        ( pDescItem -> Prev ) -> Next = pDescItem -> Next;
    } // attach prev to next

    else
    {
        pDesc -> BindCols = pDescItem -> Next;
    } // set head to next if any

    if ( pDescItem -> Next )
    {
        ( pDescItem -> Next ) -> Prev = pDescItem -> Prev;
    } // set next to prev if any

    // maintain highest desc number
    if ( pDesc -> DescCount == pDescItem -> ColNum )
    {
        Word i;
        pARDItem p;

        // loop to find the highest number and set that
        for ( i = 0 , p = pDesc -> BindCols; p != NULL; p = p -> Next )
            if ( p -> ColNum > i )
            {
                i = p -> ColNum;
            }

        // set the highest count to this column
        pDesc -> DescCount = i;
    }

    return GOOD;
}

//////////////////// UTILITY functions

// -----------------------------------------------------------------------
// to get converted col descriptor information
// -----------------------------------------------------------------------

eGoodBad GetIRDColDescInfo ( SelectedColumnMeta* pColDesc, Word* pDataType, Word* pPrecision, Word* pScale,
                             Long* pLength )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "GetIRDColDescInfo called" ) );

    // precaution
    if ( !pColDesc )
    {
        return BAD;
    }

    // caller safe
    if ( pDataType )
    {
        *pDataType = 0;
    }

    if ( pPrecision )
    {
        *pPrecision = 0;
    }

    if ( pScale )
    {
        *pScale = 0;
    }

    if ( pLength )
    {
        *pLength = 0;
    }

    if ( pDataType )
    {
        *pDataType = pColDesc -> columnType;
    }

    if ( pPrecision )
    {
        *pPrecision = pColDesc -> precision;
    }

    if ( pScale )
    {
        *pScale = pColDesc -> scale;
    }

    if ( pLength )
    {
        *pLength = pColDesc -> displaySize;
    }

    return GOOD;
}

