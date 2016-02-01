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
// File:    KO_CONN.CPP
//
// Purpose: Contains the main connection functions more
//          specifically SQLDriverConnect.
//
//          Only SQLDriverConnect with DSN specified is supported now.
//          The DSN must have been set properly with ODBCAD.exe
//
//          Most functions in this file either to support the dialog box or
//          help in manipulation or parsing of key-value pairs
//
// Exported functions:
//                       SQLDriverConnect
//                       SQLConnect
//                       SQLBrowseConnect
//
// ----------------------------------------------------------------------------
#include "stdafx.h"

#include <stdio.h>
#include <resource.h>

#include "REST.h"

// ------------------------------ local defines -------------------------------
#define MAX_KEYS_STR_LEN       1024                 // arbitray for loading keys from DSN file
#define MAX_CONN_STR_LEN       2048                 // arbitray for building key-values from DSN
#define KV_BLOCK_SIZE          5                    // arbitray size for a set of key-value pairs

#define PROTOCOL_HTTPS			"https"
#define PROTOCOL_HTTP			"http"
#define PROTOCOL_SEP			"://"
#define PORT_HTTPS_DEFAULT		"443"
#define PORT_HTTP_DEFAULT		"80"

// ------------------------ local callback functions  -------------------------
INT_PTR CALLBACK DlgDSNCfg1Proc ( HWND hDlg, UINT uMsg, WPARAM wParam, LPARAM lParam );

// ----------------------------- local functions ------------------------------
static eGoodBad CreateAndSetConnProp ( pODBCConn pConn, Word pPropID, void* pPropValue );

static eGoodBad PutDataToDlgDSNCfg1 ( pODBCConn pConn, HWND hDlg );

static eGoodBad GetDataFromDlgDSNCfg1 ( HWND hDlg, pODBCConn pConn );

static Word PromptForConnInfo ( SQLHDBC pConn );

static eGoodBad LoadKeyValuesfromFileDSN ( pODBCConn pConn, CStrPtr pDSNName, Word* pNumPair,
                                           struct ODBCKV** pKV );

static bool AddKVToConnStr ( StrPtr pKey, StrPtr pValue, Word* iPos, StrPtr pStrConn, Word pMaxLen );
static bool BuildConnStr ( char* pStrConn, Word pMaxLen, pODBCConn pConn, struct ODBCKV* KVInput,
                           Word iKVInputPairs, struct ODBCKV* KVFileDSN, Word iKVFileDSNPairs );

static const char* supportedProtocols[] = { PROTOCOL_HTTPS, PROTOCOL_HTTP };
static const char* defaultPorts[] = { PORT_HTTPS_DEFAULT, PORT_HTTP_DEFAULT };

// -----------------------------------------------------------------------
// to set a specified property in the connection structure
// -----------------------------------------------------------------------

eGoodBad SetConnProp ( pODBCConn pConn, Word pPropID, void* pPropValue )
{
    // note
    // this function does not create a copy of char data
    // it just transfers the pointer
    // numeric data is assumed to be a pointer

    // check property
    switch ( pPropID )
    {
        case CONN_PROP_SERVER :

            // check if a new value has to be put
            if ( pPropValue )
            {
                copyTrimmed ( & ( ( char* ) pConn -> Server ), ( char* ) pPropValue );
            }

            if ( pConn -> Server == NULL || strlen ( pConn -> Server ) == 0 )
            {
                __ODBCPopMsg ( "Server cannot be empty" );
                return BAD;
            }

            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The server is set to %s", pConn->Server ) );
            break;

        case CONN_PROP_PORT :

            // numeric values are passed as pointer to value
            if ( pPropValue )
            {
                pConn -> ServerPort = * ( ( ULong* ) pPropValue );
            }

            if ( pConn -> ServerPort == 0 )
            {
                __ODBCPopMsg ( "ServerPort cannot be 0" );
                return BAD;
            }

            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The port is set to %d", pConn->ServerPort ) );
            break;

        case CONN_PROP_UID :

            // check if a new value has to be put
            if ( pPropValue )
            {
                copyTrimmed ( & ( ( char* ) pConn -> UserName ), ( char* ) pPropValue );
            }

            if ( pConn -> UserName == NULL || strlen ( pConn -> UserName ) == 0 )
            {
                __ODBCPopMsg ( "UserName cannot be empty" );
                return BAD;
            }

            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The uid is set to %s", pConn->UserName ) );
            break;

        case CONN_PROP_PWD :

            // check if a new value has to be put
            if ( pPropValue )
            {
                copyTrimmed ( & ( ( char* ) pConn -> Password ), ( char* ) pPropValue );
            }

            if ( pConn -> Password == NULL || strlen ( pConn -> Password ) == 0 )
            {
                __ODBCPopMsg ( "Password cannot be empty" );
                return BAD;
            }

            break;

        case CONN_PROP_PROJECT :

            // check if a new value has to be put
            if ( pPropValue )
            {
                copyTrimmed ( & ( ( char* ) pConn -> Project ), ( char* ) pPropValue );
            }

            if ( pConn -> Project == NULL || strlen ( pConn -> Project ) == 0 )
            {
                __ODBCPopMsg ( "Project cannot be empty" );
                return BAD;
            }

            break;

        default :
            __ODBCPOPMSG ( _ODBCPopMsg ( "Bad connection property" ) );
            return BAD;
    }

    return GOOD;
}


// -----------------------------------------------------------------------
// to create copy of a value and then set it in the struct
// -----------------------------------------------------------------------

static eGoodBad CreateAndSetConnProp ( pODBCConn pConn, Word pPropID, void* pPropValue )
{
    bool IsPropStr;

    // precaution
    if ( !pConn )
    {
        return BAD;
    }

    // determine the prop type
    switch ( pPropID )
    {
        case CONN_PROP_PORT : // port is stored as a number
            IsPropStr = FALSE;
            break;

        default :
            IsPropStr = TRUE;
            break;
    }

    // check property type
    if ( IsPropStr )
    {
        Word x;
        unique_ptr <char[]> s = NULL;
        // find length of property
        x = pPropValue ? strlen ( ( StrPtr ) pPropValue ) : 0;

        // check if something
        if ( x > 0 )
        {
            // create copy of property
            s = make_unique_str ( x );
            // store
            strcpy ( s . get (), ( StrPtr ) pPropValue );
        }

        // now set the property
        return SetConnProp ( pConn, pPropID, s . get () );
    }

    else
    {
        Long v;
        // convert value to integer
        v = ( pPropValue ) ? atoi ( ( StrPtr ) pPropValue ) : 0;
        // now set the property
        return SetConnProp ( pConn, pPropID, &v );
    }
}

// -----------------------------------------------------------------------
// to provide the default data to DSN config dialog 1
// -----------------------------------------------------------------------

static eGoodBad PutDataToDlgDSNCfg1 ( pODBCConn pConn, HWND hDlg )
{
    BOOL x;

    // precaution
    if ( !pConn || !hDlg )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "PutDataToDlgDSNCfg1 - Bad params" ) );
        return BAD;
    }

    // server name/IP
    if ( pConn -> Server )
    {
        x = SetDlgItemText ( hDlg, IDC_SERVER, pConn -> Server );
    }

    else
    {
        x = SetDlgItemText ( hDlg, IDC_SERVER, "" );
    }

    if ( !x )
    {
        return BAD;
    }

    // server port
    if ( pConn -> ServerPort )
    {
        x = SetDlgItemInt ( hDlg, IDC_PORT, pConn -> ServerPort, FALSE );
    }

    else
    {
        x = SetDlgItemInt ( hDlg, IDC_PORT, DEFAULT_PORT, FALSE );
    }

    if ( !x )
    {
        return BAD;
    }

    // user name
    if ( pConn -> UserName )
    {
        x = SetDlgItemText ( hDlg, IDC_UID, pConn -> UserName );
    }

    else
    {
        x = SetDlgItemText ( hDlg, IDC_UID, "" );
    }

    if ( !x )
    {
        return BAD;
    }

    // password
    if ( pConn -> Password )
    {
        x = SetDlgItemText ( hDlg, IDC_PWD, pConn -> Password );
    }

    else
    {
        x = SetDlgItemText ( hDlg, IDC_PWD, "" );
    }

    if ( !x )
    {
        return BAD;
    }

    return GOOD;
}


// -----------------------------------------------------------------------
// to fetch the data from the dialog 1 into the conn-struct
// -----------------------------------------------------------------------

static eGoodBad GetDataFromDlgDSNCfg1 ( HWND hDlg, pODBCConn pConn )
{
    Long x;
    std::unique_ptr <char[]> n = NULL;
    eGoodBad status;

    // note
    // no error handling is currently being done for
    // GetDlgItemText/GetDlgItemInt/SetConnProp
    // generally should not be a problem

    // precaution
    if ( !pConn || !hDlg )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "GetDataFromDlgDSNCfg1 - Bad params" ) );
        return BAD;
    }

    ////// server name/IP
    // get length of input text
    x = SendDlgItemMessage ( hDlg, IDC_SERVER, EM_LINELENGTH, 0, 0 );

    if ( x > 0 )
    {
        n = make_unique_str ( x + 8 ); // allocate space for holding the text

        char serverStrBuf[256];
        GetDlgItemText ( hDlg, IDC_SERVER, serverStrBuf, x + 1 ); // get text from dialog

        HWND hwndCombo = GetDlgItem ( hDlg, IDC_PROTOCOL );
        int ItemIndex = SendMessage ( hwndCombo, ( UINT ) CB_GETCURSEL, ( WPARAM ) 0, ( LPARAM ) 0 );

        strcpy ( n . get (), supportedProtocols[ItemIndex] );
        strcat ( n . get (), PROTOCOL_SEP );
        strcat ( n . get (), serverStrBuf );
    }

    else
    {
        n = NULL;
    } // no input

    // set value in struct
    status = SetConnProp ( pConn, CONN_PROP_SERVER, n . get () );

    if ( status == BAD )
    {
        return BAD;
    }

    /////  Port
    // get value
    x = GetDlgItemInt ( hDlg, IDC_PORT, NULL, FALSE );
    // set value in struct
    status = SetConnProp ( pConn, CONN_PROP_PORT, &x );

    if ( status == BAD )
    {
        return BAD;
    }

    ////// User name
    // get length
    x = SendDlgItemMessage ( hDlg, IDC_UID, EM_LINELENGTH, 0, 0 );

    if ( x > 0 )
    {
        // allocate space
        n = make_unique_str ( x ); // allocate space for holding the text
        GetDlgItemText ( hDlg, IDC_UID, n . get (), x + 1 );
    }

    else
    {
        n = NULL;
    }

    // set value in struct
    status = SetConnProp ( pConn, CONN_PROP_UID, n . get () );

    if ( status == BAD )
    {
        return BAD;
    }

    ////// Password
    // get length
    x = SendDlgItemMessage ( hDlg, IDC_PWD, EM_LINELENGTH, 0, 0 );

    if ( x > 0 )
    {
        // allocate space
        n = make_unique_str ( x ); // allocate space for holding the text
        GetDlgItemText ( hDlg, IDC_PWD, n . get (), x + 1 );
    }

    else
    {
        n = NULL;
    }

    // set value in struct
    status = SetConnProp ( pConn, CONN_PROP_PWD, n . get () );

    if ( status == BAD )
    {
        return BAD;
    }

    return GOOD;
}


// --------------------------------------------------------------------------
// call back for DSN config dialog 1
// --------------------------------------------------------------------------

INT_PTR CALLBACK DlgDSNCfg1Proc ( HWND hDlg, UINT uMsg, WPARAM wParam, LPARAM lParam )
{
    pODBCConn pgConn = NULL;
    eGoodBad status = GOOD;
    RETCODE ret = SQL_SUCCESS;

    switch ( uMsg )
    {
        case WM_INITDIALOG :
        {
            // init protocol list
            HWND hwndCombo = GetDlgItem ( hDlg, IDC_PROTOCOL );
            SendMessage ( hwndCombo, CB_ADDSTRING, 0, reinterpret_cast <LPARAM> ( ( LPCTSTR )supportedProtocols[0] ) );
            SendMessage ( hwndCombo, CB_ADDSTRING, 0, reinterpret_cast <LPARAM> ( ( LPCTSTR )supportedProtocols[1] ) );
            SendMessage ( hwndCombo, CB_SETCURSEL, 0, 0 );

            // store the structure for future use
            SetWindowLongPtr ( hDlg, DWLP_USER, lParam );
            // initialize the dialog with data from conn struct
            PutDataToDlgDSNCfg1 ( ( pODBCConn ) lParam, hDlg );
            // set focus automatically
            return TRUE;
        }
        case WM_COMMAND :
            switch ( LOWORD ( wParam ) )
            {
                case IDC_PROTOCOL :
                {
                    switch ( HIWORD ( wParam ) )
                    {
                        case CBN_SELCHANGE :
                        {
                            HWND hwndCombo = GetDlgItem ( hDlg, IDC_PROTOCOL );
                            int portIndex = SendMessage ( hwndCombo, ( UINT ) CB_GETCURSEL, ( WPARAM ) 0, ( LPARAM ) 0 );
                            if ( SetDlgItemText ( hDlg, IDC_PORT, defaultPorts[portIndex] ) )
                            {
                                return TRUE;
                            }

                            return FALSE;
                        }
                        default :
                            break;
                    }
                    break;
                }

                case IDC_CONNECT :
                {
                    HWND hwndCombo = GetDlgItem ( hDlg, IDC_COMBO1 );
                    HWND hwndOK = GetDlgItem ( hDlg, IDOK );
                    // fetch all information from controls & feed to struct
                    pgConn = ( pODBCConn ) GetWindowLongPtr ( hDlg, DWLP_USER );
                    status = GetDataFromDlgDSNCfg1 ( hDlg, pgConn );

                    if ( status == BAD )
                    {
                        //Blank input, already popped message
                        return FALSE;
                    }

                    ret = TryAuthenticate ( pgConn );

                    if ( ret == SQL_ERROR )
                    {
                        //validation of data & other prompts goes here
                        __ODBCPopMsg ( "Username/Password not authorized, or server out of service." );
                        return FALSE;
                    }

                    //passed verification
                    EnableWindow ( hwndCombo, TRUE );

                    try
                    {
                        std::vector <string> projects;
                        restListProjects ( pgConn -> Server, pgConn -> ServerPort, pgConn -> UserName, pgConn -> Password, projects );

                        // reload project list
                        SendMessage ( hwndCombo, CB_RESETCONTENT, 0, 0 );
                        for ( unsigned int i = 0; i < projects . size (); ++i )
                        {
                            SendMessage ( hwndCombo, ( UINT ) CB_ADDSTRING, ( WPARAM ) 0, ( LPARAM ) projects . at ( i ) . c_str () );
                        }

                        SendMessage ( hwndCombo, CB_SETCURSEL, ( WPARAM ) 0, ( LPARAM ) 0 );
                    }

                    catch ( exception& e )
                    {
                        __ODBCPopMsg ( e . what () );
                        return FALSE;
                    }

                    EnableWindow ( hwndOK, TRUE );
                    return TRUE;
                }

                case IDOK :
                {
                    pgConn = ( pODBCConn ) GetWindowLongPtr ( hDlg, DWLP_USER );
                    HWND hwndCombo = GetDlgItem ( hDlg, IDC_COMBO1 );
                    int ItemIndex = SendMessage ( ( HWND ) hwndCombo, ( UINT ) CB_GETCURSEL,
                                                  ( WPARAM ) 0, ( LPARAM ) 0 );
                    TCHAR projectName[256];
                    ( TCHAR ) SendMessage ( ( HWND ) hwndCombo, ( UINT ) CB_GETLBTEXT,
                                            ( WPARAM ) ItemIndex, ( LPARAM ) projectName );
                    SetConnProp ( pgConn, CONN_PROP_PROJECT, projectName );
                    //last trial with project given
                    ret = TryFetchMetadata ( pgConn );

                    if ( ret == SQL_ERROR )
                    {
                        //validation of data & other prompts goes here
                        __ODBCPopMsg ( "Something went wrong with your selected project" );
                        return FALSE;
                    }

                    EndDialog ( hDlg, wParam );
                    return TRUE;
                }

                    // Fall through, do not break or return
                case IDCANCEL :
                    // indicate end with control id as return value
                    EndDialog ( hDlg, wParam );
                    return TRUE;
            }
    }

    return FALSE;
}


// -----------------------------------------------------------------------
// to get connection info from user
// -----------------------------------------------------------------------

static Word PromptForConnInfo ( SQLHDBC pConn )
{
    int i;
    // invoke dialog to fetch info
    i = DialogBoxParam ( ghInstDLL, MAKEINTRESOURCE ( IDD_DSN_CFG1 ), NULL, DlgDSNCfg1Proc, ( LPARAM ) pConn );

    // check status
    switch ( i )
    {
        case IDOK :
            return 1; // complete

        default :
            return 0; // user-cancelled
    }
}


// -----------------------------------------------------------------------
// to split a given string into key value pairs separated with semi-colon
// -----------------------------------------------------------------------

eGoodBad CvtStrToKeyValues ( CStrPtr pStr, Word pMaxLen, Word* pNumPair, struct ODBCKV** pKV )
{
    bool flgError;
    Word x;
    Word pairs;
    Word i, len;
    struct ODBCKV* kvtemp;
    struct ODBCKV* kv;
    // caller safe
    *pNumPair = 0;
    *pKV = NULL;
    // local initializations
    kvtemp = NULL;
    kv = NULL;

    // main loop to split the strings into key values
    for ( pairs = 0 , i = 0 , len = ( pMaxLen > 0 ) ? pMaxLen : strlen ( pStr ) , flgError = FALSE; i < len &&
          !flgError; pairs ++ )
    {
        // find the length of key
        for ( x = 0; pStr[i] != '=' && i < len; x ++ , i ++ );

        // check if a valid key found
        if ( x <= 0 )
        {
            flgError = TRUE; // error condition
            continue;
        }

        // allocate a new record ie key-value if required
        if ( kv == NULL || pairs % KV_BLOCK_SIZE == 0 )
        {
            // allocate more records
            kvtemp = new struct ODBCKV[pairs + KV_BLOCK_SIZE ];
            memset ( kvtemp, 0, sizeof ( struct ODBCKV) * ( pairs + KV_BLOCK_SIZE ) );

            // transfer the old ones into this new one
            if ( kv )
            {
                memcpy ( kvtemp, kv, sizeof ( struct ODBCKV) * pairs );
                delete[] kv;
                kv = NULL;
            }

            // now start using the new one
            kv = kvtemp;
            kvtemp = NULL;
        }

        // create key in current row
        kv[pairs] . key = new Char[x + 1];
        // put key
        strncpy ( kv[pairs] . key, pStr + ( i - x ), x );
        kv[pairs] . key[x] = 0;
        // move ahead to ignore equals sign
        ++ i;

        // find the length of value
        if ( strcmp ( kv[pairs] . key, "PWD" ) != 0 )
        {
            for ( x = 0; pStr[i] != ';' && i < len; x ++ , i ++ );
        }

        else
        {
            //There may exist ; in PWD
            for ( x = 0; i < len; x ++ , i ++ )
            {
                if ( strnicmp ( &pStr[i], ";SERVER=", 8 ) == 0 )
                {
                    break;
                }
            }
        }

        // check if a non-empty value found
        if ( x > 0 )
        {
            // create value in current row
            kv[pairs] . value = new Char[x + 1];
            // put value
            strncpy ( kv[pairs] . value, pStr + ( i - x ), x );
            kv[pairs] . value[x] = 0;
        }

        // move ahead to ignore the semi-colon at end of key-value
        ++ i;
    }

    // check for error condition
    if ( flgError )
    {
        // clean up
        if ( kv )
        {
            delete[] kv;
            kv = NULL;
        }

        return BAD; // error condition
    }

    else
    {
        *pNumPair = pairs;
        *pKV = kv;
        return GOOD;
    }
}

void FreeGenODBCKeyValues ( ODBCKV* keyvalues, int pairs )
{
    for ( int i = 0; i < pairs; ++i )
    {
        if ( keyvalues[i] . key )
        {
            delete[] keyvalues[i] . key ;
        }

        if ( keyvalues[i] . value )
        {
            delete[] keyvalues[i] . value;
        }
    }
}


// -----------------------------------------------------------------------
// to find a particular key and/or value in key-value pair list
// -----------------------------------------------------------------------

bool FindInKeyValues ( CStrPtr pKey, CStrPtr pValue, struct ODBCKV* pKV, Word pItems, Word* pPosition )
{
    Word i;
    bool flgMatch;

    // loop to traverse the list
    for ( i = 0 , flgMatch = FALSE; i < pItems; i ++ )
    {
        // match key
        flgMatch = ( pKey && _stricmp ( pKey, pKV[i] . key ) == 0 );

        // match value
        if ( pValue && pKV[i] . value )
        {
            flgMatch = ( _stricmp ( pValue, pKV[i] . value ) == 0 );
        }

        // break if match
        if ( flgMatch )
        {
            break;
        }
    }

    // check if found
    if ( flgMatch )
    {
        if ( pPosition )
        {
            *pPosition = i;
        }

        return TRUE;
    }

    return FALSE;
}


// -----------------------------------------------------------------------
// to load key value pairs from a File DSN
// -----------------------------------------------------------------------

static eGoodBad LoadKeyValuesfromFileDSN ( pODBCConn pConn, CStrPtr pDSNName, Word* pNumPair,
                                           struct ODBCKV** pKV )
{
    //Never called
    throw - 1;
}


// -----------------------------------------------------------------------
// to add a key-value pair to specified conn string
// -----------------------------------------------------------------------

static bool AddKVToConnStr ( StrPtr pKey, StrPtr pValue, Word* iPos, StrPtr pStrConn, Word pMaxLen )
{
    Word i, j;

    // precaution
    if ( !pKey )
    {
        return FALSE;
    }

    // get length of key and value
    i = strlen ( pKey );
    j = ( pValue ) ? strlen ( pValue ) : 0;

    // check if both can be added along with equal sign & semi-colon
    if ( *iPos + i + j + 2 <= pMaxLen )
    {
        strcat ( pStrConn, pKey );
        strcat ( pStrConn, "=" );

        if ( pValue )
        {
            strcat ( pStrConn, pValue );
        }

        strcat ( pStrConn, ";" );
        ( *iPos ) = ( *iPos ) + i + j + 2; // re-position
        return TRUE;
    }

    return FALSE;
}


// -----------------------------------------------------------------------
// to build the out-connection string
// -----------------------------------------------------------------------

static bool BuildConnStr ( char* pStrConn, Word pMaxLen, pODBCConn pConn, struct ODBCKV* KVInput,
                           Word iKVInputPairs, struct ODBCKV* KVFileDSN, Word iKVFileDSNPairs )
{
    Word iPos = 0;
    Char p[32]; // arbitary for string port number as string
    // initializations
    memset ( pStrConn, 0, pMaxLen );
    // convert port number to string
    _itoa ( pConn -> ServerPort, p, 10 );

    // transfer all strings from struct
    if ( !AddKVToConnStr ( "DRIVER", "{KylinODBCDriver}", &iPos, pStrConn, pMaxLen ) )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "AddKVToConnStr failed in %s", "Driver" ) );
        return FALSE;
    }

    if ( !AddKVToConnStr ( "SERVER", pConn -> Server, &iPos, pStrConn, pMaxLen ) )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "AddKVToConnStr failed in %s", "Server" ) );
        return FALSE;
    }

    if ( !AddKVToConnStr ( "PROJECT", pConn -> Project, &iPos, pStrConn, pMaxLen ) )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "AddKVToConnStr failed in %s", "Project" ) );
        return FALSE;
    }

    if ( !AddKVToConnStr ( "PORT", p, &iPos, pStrConn, pMaxLen ) )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "AddKVToConnStr failed in %s", "Port" ) );
        return FALSE;
    }

    if ( !AddKVToConnStr ( "UID", pConn -> UserName, &iPos, pStrConn, pMaxLen ) )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "AddKVToConnStr failed in %s", "Uid" ) );
        return FALSE;
    }

    if ( !AddKVToConnStr ( "PWD", pConn -> Password, &iPos, pStrConn, pMaxLen ) )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "AddKVToConnStr failed in %s", "Pwd" ) );
        return FALSE;
    }

    return TRUE;
}

// -----------------------------------------------------------------------
// to connect to the driver
// -----------------------------------------------------------------------
RETCODE SQL_API SQLDriverConnectW ( SQLHDBC hdbc,
                                    SQLHWND hwnd,
                                    SQLWCHAR* szConnStrIn,
                                    SQLSMALLINT cchConnStrIn,
                                    SQLWCHAR* szConnStrOut,
                                    SQLSMALLINT cchConnStrOutMax,
                                    SQLSMALLINT* pcchConnStrOut,
                                    SQLUSMALLINT fDriverCompletion )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLDriverConnectW called, cchConnStrIn %d, cchConnStrOutMax %d, wcslen %d",
        cchConnStrIn, cchConnStrOutMax, wcslen ( szConnStrIn ) ) );
    int inStrLength = wcslen ( szConnStrIn ) + 1;
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The inStr Length is : %d", inStrLength ) );
    unique_ptr <char[]> pInStr ( new char[inStrLength] );
    unique_ptr <char[]> pOutStr ( new char[cchConnStrOutMax + 1] );
    wchar2char ( szConnStrIn, pInStr . get (), inStrLength );
    //__ODBCLOG(_ODBCLogMsg(LogLevel_DEBUG,"The inStr is : %s",pInStr.get()));
    SQLSMALLINT outStrLength = 0;
    RETCODE code = SQLDriverConnect ( hdbc, hwnd, ( SQLCHAR* ) pInStr . get (), cchConnStrIn, ( SQLCHAR* ) pOutStr . get (),
                                      cchConnStrOutMax, &outStrLength, fDriverCompletion );

    if ( code == SQL_ERROR )
    {
        return code;
    }

    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "pcchConnStrOut null? %d, cchConnStrOutMax > 0 ? %d, szConnStrOut null? %d",
        pcchConnStrOut == NULL, cchConnStrOutMax > 0 , szConnStrOut == NULL ) );

    if ( cchConnStrOutMax > 0 && pcchConnStrOut && szConnStrOut )
    {
        char2wchar ( pOutStr . get (), szConnStrOut, ( int ) cchConnStrOutMax );
        *pcchConnStrOut = wcslen ( szConnStrOut );
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "(W)The Length of Out Conn Str is %d", *pcchConnStrOut ) );
    }

    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "the ret code is %d", code ) );
    return code;
}

RETCODE SQL_API SQLDriverConnect ( SQLHDBC pConn,
                                   SQLHWND pWndHandle,
                                   SQLCHAR* pInConnStr,
                                   SQLSMALLINT pInConnStrLen,
                                   SQLCHAR* pOutConnStr,
                                   SQLSMALLINT pOutConnStrLen,
                                   SQLSMALLINT* pOutConnStrLenPtr,
                                   SQLUSMALLINT pDriverCompletion )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The window handle is %d, the driver completion flag is %d", pWndHandle,
        pDriverCompletion ) );
    pODBCConn pgConn = ( pODBCConn ) pConn;
    bool f;
    bool flgDriver, flgDSN; // flags for knowing if these key present in string
    bool flgServer, flgPort, flgUID, flgPWD, flgProj; // flags for knowing if these key present in string
    Word i, n;
    Word iKVInputPairs; // no of key value pairs as input
    Word iDriverPos, iDSNPos; // ??? can be eliminated by optimization of code
    Word iServerPos, iPortPos, iUIDPos, iPWDPos, iProjPos; // ??? can be eliminated by optimization of code
    struct ODBCKV* KVInput; // key value as input via  function param
    struct ODBCKV* KV; // generic, temp

    if ( !pInConnStr )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "SQLDriverConnect: pInConnStr is required" ) );
        return SQL_ERROR;
    }

    else
    {
        //__ODBCLOG(_ODBCLogMsg(LogLevel_DEBUG,"The passed-in Connection Str is %s",(char*)pInConnStr));
    }

    __CHK_HANDLE ( pConn, SQL_HANDLE_DBC, SQL_ERROR );
    _SQLFreeDiag ( _DIAGCONN ( pConn ) );

    // caller safe
    if ( pOutConnStr )
    {
        *pOutConnStr = 0;
    }

    if ( pOutConnStrLenPtr )
    {
        *pOutConnStrLenPtr = 0;
    }

    // initializations
    KVInput = NULL;
    flgServer = FALSE;
    flgPort = FALSE;
    flgUID = FALSE;
    flgPWD = FALSE;
    flgProj = FALSE;

    // check if an in-string has been specified
    if ( pInConnStr )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "Parsing the in str" ) );

        // split into key-value pairs
        if ( CvtStrToKeyValues ( ( StrPtr ) pInConnStr, pInConnStrLen, &iKVInputPairs, &KVInput ) != GOOD )
        {
            return SQL_ERROR;
        }

        // first check if dsn keyword is present
        flgDSN = FindInKeyValues ( "DSN", NULL, KVInput, iKVInputPairs, &iDSNPos );
        // look for driver only if DSN is absent else Driver is always ignored
        flgDriver = ( flgDSN ) ? FALSE : FindInKeyValues ( "DRIVER", NULL, KVInput, iKVInputPairs, &iDriverPos );

        // if DSN is to be used, fetch its set of key values
        if ( flgDSN )
        {
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The in str is a dsn string" ) );
            //connect by dsn
            SetCurrentDSN ( ( char* ) pInConnStr, "SQLDriverConnect" );

            if ( LoadODBCINIDataToConn ( pgConn ) != GOOD )
            {
                return SQL_ERROR;
            }
        }

        else if ( flgDriver )
        {
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The in str is a driver string" ) );
            /************* debug
                for (  i = 0, n = iKVInputPairs, KV = KVInput; i < n; i++ )
                fprintf ( stderr, "Index: %d, Key: %s, Value: %s\n", i, KV[i].key ? KV[i].key : "(nokey)", KV[i].value ? KV[i].value : "(no value)" );
            *********/

            // loop to parse both input key-values and DSN key-values & feed into struct
            for ( i = 0 , n = iKVInputPairs , KV = KVInput; i < n; i++ )
            {
                if ( !flgServer )
                {
                    flgServer = FindInKeyValues ( "SERVER", NULL, KV, n, &iServerPos );

                    if ( flgServer )
                    {
                        CreateAndSetConnProp ( ( pODBCConn ) pConn, CONN_PROP_SERVER, KV[iServerPos] . value );
                    }
                }

                if ( !flgPort )
                {
                    flgPort = FindInKeyValues ( "PORT", NULL, KV, n, &iPortPos );

                    if ( flgPort )
                    {
                        CreateAndSetConnProp ( ( pODBCConn ) pConn, CONN_PROP_PORT, KV[iPortPos] . value );
                    }
                }

                if ( !flgUID )
                {
                    flgUID = FindInKeyValues ( "UID", NULL, KV, n, &iUIDPos );

                    if ( flgUID )
                    {
                        CreateAndSetConnProp ( ( pODBCConn ) pConn, CONN_PROP_UID, KV[iUIDPos] . value );
                        __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, "Log in as User : %s ", KV[iUIDPos].value ) );
                    }
                }

                if ( !flgPWD )
                {
                    flgPWD = FindInKeyValues ( "PWD", NULL, KV, n, &iPWDPos );

                    if ( flgPWD )
                    {
                        CreateAndSetConnProp ( ( pODBCConn ) pConn, CONN_PROP_PWD, KV[iPWDPos] . value );
                    }
                }

                if ( !flgProj )
                {
                    flgProj = FindInKeyValues ( "PROJECT", NULL, KV, n, &iProjPos );

                    if ( flgProj )
                    {
                        CreateAndSetConnProp ( ( pODBCConn ) pConn, CONN_PROP_PROJECT, KV[iProjPos] . value );
                    }
                }
            }
        }

        else
        {
            _SQLPutDiagRow ( SQL_HANDLE_DBC, pConn, "SQLDriverConnectW", "HY000", 1045, "Only DSN or driver connect is allowed" );
            __ODBCPOPMSG ( _ODBCPopMsg ( "Only DSN or driver connect is allowed, instead of %s", pInConnStr ) );
            return SQL_ERROR;
        }

        FreeGenODBCKeyValues ( KVInput, iKVInputPairs );
        delete[] KVInput;
    }

    else if ( pDriverCompletion == SQL_DRIVER_NOPROMPT )
    { // check if no-prompt forced
        __ODBCPOPMSG ( _ODBCPopMsg ( "No connection string && no prompt specified" ) );
        _SQLPutDiagRow ( SQL_HANDLE_DBC, pConn, "SQLDriverConnectW", "HY000", 1045,
                                       "Access denied. (using UID: NO , using password: NO)" );
        return SQL_ERROR;
    }

    RETCODE ret;

    // check if prompt required ie any info is missing
    if ( flgDriver && ( !flgServer || !flgPort || !flgUID || !flgPWD || !flgProj ) )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, "Connection info imcomplete, prompt for input..." ) );

        if ( flgUID && !flgPWD && pDriverCompletion == SQL_DRIVER_NOPROMPT )
        {
            _SQLPutDiagRow ( SQL_HANDLE_DBC, pConn, "SQLDriverConnectW", "HY000", 1045,
                                           "Access denied for user 'root'@'kylin-tableau-clean.com' (using password: NO)" );
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_ERROR,
                "UID present but PWD absent, guessing it's on Tableau Server, return SQL ERROR" ) );
            return SQL_ERROR;
        }

        //connect by driver
        // fetch entire connection information thru dialogs
        switch ( PromptForConnInfo ( pConn ) )
        {
            case 0 : // user-cancelled
                return SQL_NO_DATA_FOUND;

            default :
                break;
        }

        ret = SQL_SUCCESS;
    }

    else
    {
        ret = TryFetchMetadata ( pgConn ) ;

        if ( ret == SQL_ERROR )
        {
            return ret;
        }
    }

    // OUT CONN STRING
    // build the out-connection string if required
    if ( pOutConnStr && pOutConnStrLen > 0 && pOutConnStrLenPtr )
    {
        if ( flgDriver )
        {
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "Building out str..." ) );
            // build the out conn string using key value pairs
            f = BuildConnStr ( ( StrPtr ) pOutConnStr, pOutConnStrLen, ( pODBCConn ) pConn, NULL, 0, NULL, 0 );

            if ( !f )
            {
                _SQLPutDiagRow ( SQL_HANDLE_DBC, pConn, "SQLDriverConnectW", "HY000", 1045, "Out connection string not complete" );
            }
        }

        else
        {
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "Copy in str to out str" ) );
            strcpy ( ( char* ) pOutConnStr, ( char* ) pInConnStr );
        }

        *pOutConnStrLenPtr = strlen ( ( StrPtr ) pOutConnStr );
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "The Length of Out Conn Str is %d", *pOutConnStrLenPtr ) );
    }

    else
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "skip writing to the out put string" ) );
    }

    return ret;
}


RETCODE TryFetchMetadata ( pODBCConn pgConn )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "start loading metadata..." ) );

    try
    {
        pgConn -> meta = std::move ( restGetMeta ( pgConn -> Server, pgConn -> ServerPort, pgConn -> UserName, pgConn -> Password,
                                                   pgConn -> Project ) );
    }

    catch ( const exception& e )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_ERROR, "The REST request failed to get metadata" ) );
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_ERROR, e.what() ) );
        _SQLPutDiagRow ( SQL_HANDLE_DBC, pgConn, "SQLDriverConnect", "HY000", 1045, "Access denied. (using password: NO)" );
        return SQL_ERROR;
    }

    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "End loading metadata" ) );
    return SQL_SUCCESS;
}

RETCODE TryAuthenticate ( pODBCConn pgConn )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "Start authenticating.." ) );

    try
    {
        bool authenticated = restAuthenticate ( pgConn -> Server, pgConn -> ServerPort, pgConn -> UserName, pgConn -> Password );

        if ( !authenticated )
        {
            throw exception ( "Username/Password incorrect." );
        }
    }

    catch ( const exception& e )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_ERROR, "The REST request failed to authenticate." ) );
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_ERROR, e.what() ) );
        _SQLPutDiagRow ( SQL_HANDLE_DBC, pgConn, "SQLDriverConnect", "HY000", 1045, "Access denied. (using password: NO)" );
        return SQL_ERROR;
    }

    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "End authenticating" ) );
    return SQL_SUCCESS;
}

// -----------------------------------------------------------------------
// to connect to the server using standard parameters
// -----------------------------------------------------------------------

RETCODE SQL_API SQLConnect ( SQLHDBC pConn,
                             SQLCHAR* pServerName,
                             SQLSMALLINT pServerNameLen,
                             SQLCHAR* pUserName,
                             SQLSMALLINT pUserNameLen,
                             SQLCHAR* pPassword,
                             SQLSMALLINT pPasswordLen )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLConnect called" ) );
    __CHK_HANDLE ( pConn, SQL_HANDLE_DBC, SQL_ERROR );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLConnect - not implemented, use SQLDriverConnect" ) );
    return SQL_ERROR;
    _SQLFreeDiag ( _DIAGCONN ( pConn ) );
    return ( SQL_SUCCESS );
}

SQLRETURN SQL_API SQLConnectW ( SQLHDBC hdbc,
                                SQLWCHAR* szDSN,
                                SQLSMALLINT cchDSN,
                                SQLWCHAR* szUID,
                                SQLSMALLINT cchUID,
                                SQLWCHAR* szAuthStr,
                                SQLSMALLINT cchAuthStr
)
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLConnectW called" ) );
    __CHK_HANDLE ( hdbc, SQL_HANDLE_DBC, SQL_ERROR );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLConnectW - not implemented, use SQLDriverConnectW" ) );
    return SQL_ERROR;
    _SQLFreeDiag ( _DIAGCONN ( hdbc ) );
    return ( SQL_SUCCESS );
}


// -----------------------------------------------------------------------
// to connect in multiple iterations
// -----------------------------------------------------------------------

RETCODE SQL_API SQLBrowseConnect ( SQLHDBC pConn,
                                   SQLCHAR* InConnectionString,
                                   SQLSMALLINT StringLength1,
                                   SQLCHAR* OutConnectionString,
                                   SQLSMALLINT BufferLength,
                                   SQLSMALLINT* StringLength2Ptr )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "SQLBrowseConnect called" ) );
    __ODBCPOPMSG ( _ODBCPopMsg ( "SQLBrowseConnect - not implemented, use SQLDriverConnect" ) );
    return SQL_ERROR;
}

