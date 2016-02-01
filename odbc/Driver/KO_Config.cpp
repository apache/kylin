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


#include "stdafx.h"

#include "StringUtils.h"

#include <stdio.h>
#include <resource.h>
#include <REST.h>

#define SERVERKEY "SERVER"
#define PORTKEY "PORT"
#define UIDKEY "UID"
#define PWDKEY "PWD"
#define PROJECTKEY "PROJECT"

#define BUFFERSIZE 256

#define INITFILE "ODBC.INI"
#define INSTINIFILE "ODBCINST.INI"

#define DRIVER_NAME "KylinODBCDriver"
#define DRIVER_DEFAULT_LOCATION "C:\\Program Files (x86)\\kylinolap\\KylinODBCDriver\\driver.dll"

#define PROTOCOL_HTTPS "https"
#define PROTOCOL_HTTP "http"
#define PROTOCOL_SEP "://"
#define PORT_HTTPS_DEFAULT "443"
#define PORT_HTTP_DEFAULT "80"

static char currentDSN[BUFFERSIZE];

static const char* supportedProtocols[] = { PROTOCOL_HTTPS, PROTOCOL_HTTP };
static const char* defaultPorts[] = { PORT_HTTPS_DEFAULT, PORT_HTTP_DEFAULT };

static int GetValueFromODBCINI ( char* section, char* key, char* defaultValue, char* buffer, int bufferSize,
                                 char* initFileName )
{
    return SQLGetPrivateProfileString ( section, key, defaultValue, buffer, bufferSize, initFileName );
}

static BOOL SetValueInODBCINI ( char* section, char* key, char* newValue, char* initFileName )
{
    return SQLWritePrivateProfileString ( section, key, newValue, initFileName );
}

static BOOL AddDSN ( char* dsnName )
{
    return SQLWritePrivateProfileString ( "ODBC Data Sources", dsnName, "KylinODBCDriver", INITFILE );
}

static BOOL RemoveDSN ( char* dsnName )
{
    BOOL temp = true;
    temp = SQLWritePrivateProfileString ( dsnName, NULL, NULL, INITFILE );
    return SQLWritePrivateProfileString ( "ODBC Data Sources", dsnName, NULL, INITFILE ) && temp;
}

void SetCurrentDSN ( char* connStr, char* logFunc )
{
    currentDSN[0] = '\0';
    Word pairCount = 0;
    Word index = 0;
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "%s  : lparam: %s", logFunc, connStr ) );
    ODBCKV* pKV = NULL;

    if ( connStr == NULL || CvtStrToKeyValues ( connStr, -1, &pairCount, &pKV ) != GOOD )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "%s: failed to parse the attribute string %s", logFunc, connStr ) );
    }

    else
    {
        if ( FindInKeyValues ( "DSN", NULL, pKV, pairCount, &index ) != true )
        {
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "%s: failed to find the DSN attribute in %s", logFunc, connStr ) );
        }

        else
        {
            strcpy ( currentDSN, pKV[index] . value );
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "%s: success to set the currentDSN: %s", logFunc, currentDSN ) );
        }

        FreeGenODBCKeyValues ( pKV, pairCount );
        delete[] pKV;
    }
}

static eGoodBad LoadODBCINIDataToDlgDSNCfg2 ( HWND hDlg )
{
    BOOL x;
    char buffer[BUFFERSIZE];

    // precaution
    if ( !hDlg )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "LoadODBCINIDataToDlgDSNCfg2 - Bad params: hDlg is NULL" ) );
        return BAD;
    }

    // DSN name
    x = SetDlgItemText ( hDlg, IDC_DSNNAME, currentDSN );

    if ( !x )
    {
        return BAD;
    }

    // server name/IP and protocol
    GetValueFromODBCINI ( currentDSN, SERVERKEY, "", buffer, BUFFERSIZE, INITFILE );

    int protocol = 0;
    int hostOffset = 0;
    if ( buffer[0] == 'h' && buffer[1] == 't' && buffer[2] == 't' && buffer[3] == 'p' )
    {
        if ( buffer[4] == ':' && buffer[5] == '/' && buffer[6] == '/' )
        {
            protocol = 1;
            hostOffset = 7;
        }
        else if ( buffer[4] == 's' && buffer[5] == ':' && buffer[6] == '/' && buffer[7] == '/' )
        {
            hostOffset = 8;
        }
    }
    HWND hwndCombo = GetDlgItem ( hDlg, IDC_PROTOCOL );
    SendMessage ( hwndCombo, CB_SETCURSEL, protocol, 0 );

    strcpy ( buffer, buffer + hostOffset );
    x = SetDlgItemText ( hDlg, IDC_SERVER, buffer );
    if ( !x )
    {
        return BAD;
    }

    // server port
    char* defaultPort = PORT_HTTPS_DEFAULT;
    if ( protocol == 1 )
    {
        defaultPort = PORT_HTTP_DEFAULT;
    }
    GetValueFromODBCINI ( currentDSN, PORTKEY, defaultPort, buffer, BUFFERSIZE, INITFILE );

    int portTemp = atoi ( buffer );
    if ( portTemp == 0 )
    {
        portTemp = atoi ( defaultPort );
    }

    x = SetDlgItemInt ( hDlg, IDC_PORT, portTemp, FALSE );

    if ( !x )
    {
        return BAD;
    }

    // user name
    GetValueFromODBCINI ( currentDSN, UIDKEY, "", buffer, BUFFERSIZE, INITFILE );
    x = SetDlgItemText ( hDlg, IDC_UID, buffer );

    if ( !x )
    {
        return BAD;
    }

    // password
    GetValueFromODBCINI ( currentDSN, PWDKEY, "", buffer, BUFFERSIZE, INITFILE );
    x = SetDlgItemText ( hDlg, IDC_PWD, buffer );

    if ( !x )
    {
        return BAD;
    }

    return GOOD;
}

static eGoodBad RetriveDlgData ( HWND hDlg, char* newDSN, char* serverStr, char* uidStr, char* pwdStr, long* port )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "Start retrieving the configs..." ) );
    Long x;

    if ( !hDlg )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "RetriveDlgData - Bad params: hDlg is NULL" ) );
        return BAD;
    }

    x = SendDlgItemMessage ( hDlg, IDC_DSNNAME, EM_LINELENGTH, 0, 0 ); // get text from dialog

    if ( x > 0 )
    {
        GetDlgItemText ( hDlg, IDC_DSNNAME, newDSN, BUFFERSIZE ); // get text from dialog
    }

    else
    {
        newDSN[0] = '\0';
    }

    ////// server name/IP
    // get length of input text

    x = SendDlgItemMessage ( hDlg, IDC_SERVER, EM_LINELENGTH, 0, 0 );
    if ( x > 0 )
    {
        char serverStrBuf[BUFFERSIZE - 8];
        GetDlgItemText ( hDlg, IDC_SERVER, serverStrBuf, BUFFERSIZE ); // get text from dialog

        HWND hwndCombo = GetDlgItem ( hDlg, IDC_PROTOCOL );
        int ItemIndex = SendMessage ( hwndCombo, ( UINT ) CB_GETCURSEL, ( WPARAM ) 0, ( LPARAM ) 0 );
        strcpy ( serverStr, supportedProtocols[ItemIndex] );
        strcat ( serverStr, PROTOCOL_SEP );
        strcat ( serverStr, serverStrBuf );
    }
    else
    {
        serverStr[0] = '\0';
    }
    /////  Port
    // get value
    *port = GetDlgItemInt ( hDlg, IDC_PORT, NULL, FALSE );

    ////// User name
    // get length
    x = SendDlgItemMessage ( hDlg, IDC_UID, EM_LINELENGTH, 0, 0 );

    if ( x > 0 )
    {
        // allocate space
        GetDlgItemText ( hDlg, IDC_UID, uidStr, BUFFERSIZE );
    }

    else
    {
        uidStr[0] = '\0';
    }

    ////// Password
    // get length
    x = SendDlgItemMessage ( hDlg, IDC_PWD, EM_LINELENGTH, 0, 0 );

    if ( x > 0 )
    {
        GetDlgItemText ( hDlg, IDC_PWD, pwdStr, BUFFERSIZE );
    }

    else
    {
        pwdStr[0] = '\0';
    }

    trimwhitespace ( newDSN );
    trimwhitespace ( serverStr );
    trimwhitespace ( uidStr );
    trimwhitespace ( pwdStr );

    if ( strlen ( newDSN ) == 0 )
    {
        __ODBCPopMsg ( "DSN name cannot be empty" );
        return BAD;
    }

    if ( strlen ( serverStr ) == 0 )
    {
        __ODBCPopMsg ( "Server cannot be empty" );
        return BAD;
    }

    if ( strlen ( uidStr ) == 0 )
    {
        __ODBCPopMsg ( "Username cannot be empty" );
        return BAD;
    }

    if ( strlen ( pwdStr ) == 0 )
    {
        __ODBCPopMsg ( "Password cannot be empty" );
        return BAD;
    }

    if ( port == 0 )
    {
        __ODBCPopMsg ( "Port cannot be 0" );
        return BAD;
    }

    return GOOD;
}

static pODBCConn createConn ()
{
    pODBCConn conn;
    // allocate a conn
    conn = new ODBCConn;
    // clear the conn attributes
    memset ( conn, 0, sizeof ( ODBCConn) );
    // set the handle signature
    ( ( pODBCConn ) conn ) -> Sign = SQL_HANDLE_DBC;
    // default values
    ( ( pODBCConn ) conn ) -> AccessMode = SQL_MODE_READ_ONLY;
    ( ( pODBCConn ) conn ) -> AutoIPD = SQL_FALSE;
    ( ( pODBCConn ) conn ) -> AsyncEnable = SQL_ASYNC_ENABLE_OFF;
    ( ( pODBCConn ) conn ) -> AutoCommit = SQL_AUTOCOMMIT_ON;
    ( ( pODBCConn ) conn ) -> TimeOut = 0;
    ( ( pODBCConn ) conn ) -> LoginTimeOut = 0;
    ( ( pODBCConn ) conn ) -> MetaDataID = SQL_FALSE;
    ( ( pODBCConn ) conn ) -> ODBCCursors = SQL_CUR_USE_DRIVER;
    ( ( pODBCConn ) conn ) -> Window = NULL;
    ( ( pODBCConn ) conn ) -> TxnIsolation = 0;
    ( ( pODBCConn ) conn ) -> MaxRows = 0;
    ( ( pODBCConn ) conn ) -> QueryTimeout = 0;
    ( ( pODBCConn ) conn ) -> Server = new char[BUFFERSIZE];
    ( ( pODBCConn ) conn ) -> UserName = new char[BUFFERSIZE];
    ( ( pODBCConn ) conn ) -> Password = new char[BUFFERSIZE];
    ( ( pODBCConn ) conn ) -> Project = new char[BUFFERSIZE];
    return conn;
}

static eGoodBad testGetMetadata ( char* serverStr, char* uidStr, char* pwdStr, long port, char* project )
{
    pODBCConn conn = createConn ();
    strcpy ( ( ( pODBCConn ) conn ) -> Server, serverStr );
    strcpy ( ( ( pODBCConn ) conn ) -> UserName, uidStr );
    strcpy ( ( ( pODBCConn ) conn ) -> Password, pwdStr );
    strcpy ( ( ( pODBCConn ) conn ) -> Project, project );
    ( ( pODBCConn ) conn ) -> ServerPort = port;
    RETCODE ret = TryFetchMetadata ( conn );
    _SQLFreeDiag ( _DIAGCONN ( conn ) );
    // disconnect
    _SQLDisconnect ( conn );
    // now free the structure itself
    delete conn;

    if ( ret == SQL_ERROR )
    {
        //validation of data & other prompts goes here
        __ODBCPopMsg ( "Username/Password not authorized, or server out of service." );
        return BAD;
    }

    return GOOD;
}

static eGoodBad testConnection ( char* serverStr, char* uidStr, char* pwdStr, long port )
{
    pODBCConn conn = createConn ();
    strcpy ( ( ( pODBCConn ) conn ) -> Server, serverStr );
    strcpy ( ( ( pODBCConn ) conn ) -> UserName, uidStr );
    strcpy ( ( ( pODBCConn ) conn ) -> Password, pwdStr );
    ( ( pODBCConn ) conn ) -> ServerPort = port;
    RETCODE ret = TryAuthenticate ( conn );
    _SQLFreeDiag ( _DIAGCONN ( conn ) );
    // disconnect
    _SQLDisconnect ( conn );
    // now free the structure itself
    delete conn;

    if ( ret == SQL_ERROR )
    {
        //validation of data & other prompts goes here
        __ODBCPopMsg ( "Username/Password not authorized, or server out of service." );
        return BAD;
    }

    return GOOD;
}

static eGoodBad SaveConfigToODBCINI ( char* newDSN, char* serverStr, char* uidStr, char* pwdStr, long port,
                                      char* projectStr )
{
    char portStrBuffer[BUFFERSIZE];
    SetValueInODBCINI ( newDSN, SERVERKEY, serverStr, INITFILE );
    SetValueInODBCINI ( newDSN, PORTKEY, _itoa ( port, portStrBuffer, 10 ), INITFILE );
    SetValueInODBCINI ( newDSN, UIDKEY, uidStr, INITFILE );
    SetValueInODBCINI ( newDSN, PWDKEY, pwdStr, INITFILE );
    SetValueInODBCINI ( newDSN, PROJECTKEY, projectStr, INITFILE );

    //If a new dsn name comes, add a new entry in regedit
    if ( _stricmp ( newDSN, currentDSN ) != 0 )
    {
        AddDSN ( newDSN );

        //it is a dsn renaming
        if ( strlen ( currentDSN ) != 0 )
        {
            RemoveDSN ( currentDSN );
        }
    }

    strcpy ( currentDSN, newDSN );
    char temp[BUFFERSIZE];
    GetValueFromODBCINI ( DRIVER_NAME, "Driver", DRIVER_DEFAULT_LOCATION, temp, BUFFERSIZE, INSTINIFILE );
    SetValueInODBCINI ( currentDSN, "Driver", temp, INITFILE );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "Finish saving the configurations to ODBC INI" ) );
    return GOOD;
}

static eGoodBad RetriveDlgDataToODBCINI ( HWND hDlg, bool onlyTest )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "Start retrieving the configurations to ODBC INI" ) );
    Long x, port;
    char newDSN[BUFFERSIZE];
    char serverStr[BUFFERSIZE];
    char uidStr[BUFFERSIZE];
    char pwdStr[BUFFERSIZE];
    char portStrBuffer[BUFFERSIZE];

    if ( !hDlg )
    {
        __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "RetriveDlgDataToODBCINI - Bad params: hDlg is NULL" ) );
        return BAD;
    }

    x = SendDlgItemMessage ( hDlg, IDC_DSNNAME, EM_LINELENGTH, 0, 0 ); // get text from dialog

    if ( x > 0 )
    {
        GetDlgItemText ( hDlg, IDC_DSNNAME, newDSN, BUFFERSIZE ); // get text from dialog
    }

    else
    {
        newDSN[0] = '\0';
    }

    ////// server name/IP
    // get length of input text
    x = SendDlgItemMessage ( hDlg, IDC_SERVER, EM_LINELENGTH, 0, 0 );

    if ( x > 0 )
    {
        GetDlgItemText ( hDlg, IDC_SERVER, serverStr, BUFFERSIZE ); // get text from dialog
    }

    else
    {
        serverStr[0] = '\0';
    }

    /////  Port
    // get value
    port = GetDlgItemInt ( hDlg, IDC_PORT, NULL, FALSE );
    ////// User name
    // get length
    x = SendDlgItemMessage ( hDlg, IDC_UID, EM_LINELENGTH, 0, 0 );

    if ( x > 0 )
    {
        // allocate space
        GetDlgItemText ( hDlg, IDC_UID, uidStr, BUFFERSIZE );
    }

    else
    {
        uidStr[0] = '\0';
    }

    ////// Password
    // get length
    x = SendDlgItemMessage ( hDlg, IDC_PWD, EM_LINELENGTH, 0, 0 );

    if ( x > 0 )
    {
        GetDlgItemText ( hDlg, IDC_PWD, pwdStr, BUFFERSIZE );
    }

    else
    {
        pwdStr[0] = '\0';
    }

    trimwhitespace ( newDSN );
    trimwhitespace ( serverStr );
    trimwhitespace ( uidStr );
    trimwhitespace ( pwdStr );

    if ( strlen ( newDSN ) == 0 )
    {
        __ODBCPopMsg ( "DSN name cannot be empty" );
        return BAD;
    }

    if ( strlen ( serverStr ) == 0 )
    {
        __ODBCPopMsg ( "Server cannot be empty" );
        return BAD;
    }

    if ( strlen ( uidStr ) == 0 )
    {
        __ODBCPopMsg ( "Username cannot be empty" );
        return BAD;
    }

    if ( strlen ( pwdStr ) == 0 )
    {
        __ODBCPopMsg ( "Password cannot be empty" );
        return BAD;
    }

    if ( port == 0 )
    {
        __ODBCPopMsg ( "Port cannot be 0" );
        return BAD;
    }

    if ( onlyTest )
    {
        pODBCConn conn;
        // allocate a conn
        conn = new ODBCConn;
        // clear the conn attributes
        memset ( conn, 0, sizeof ( ODBCConn) );
        // set the handle signature
        ( ( pODBCConn ) conn ) -> Sign = SQL_HANDLE_DBC;
        // default values
        ( ( pODBCConn ) conn ) -> AccessMode = SQL_MODE_READ_ONLY;
        ( ( pODBCConn ) conn ) -> AutoIPD = SQL_FALSE;
        ( ( pODBCConn ) conn ) -> AsyncEnable = SQL_ASYNC_ENABLE_OFF;
        ( ( pODBCConn ) conn ) -> AutoCommit = SQL_AUTOCOMMIT_ON;
        ( ( pODBCConn ) conn ) -> TimeOut = 0;
        ( ( pODBCConn ) conn ) -> LoginTimeOut = 0;
        ( ( pODBCConn ) conn ) -> MetaDataID = SQL_FALSE;
        ( ( pODBCConn ) conn ) -> ODBCCursors = SQL_CUR_USE_DRIVER;
        ( ( pODBCConn ) conn ) -> Window = NULL;
        ( ( pODBCConn ) conn ) -> TxnIsolation = 0;
        ( ( pODBCConn ) conn ) -> MaxRows = 0;
        ( ( pODBCConn ) conn ) -> QueryTimeout = 0;
        ( ( pODBCConn ) conn ) -> Server = new char[BUFFERSIZE];
        ( ( pODBCConn ) conn ) -> UserName = new char[BUFFERSIZE];
        ( ( pODBCConn ) conn ) -> Password = new char[BUFFERSIZE];
        strcpy ( ( ( pODBCConn ) conn ) -> Server, serverStr );
        strcpy ( ( ( pODBCConn ) conn ) -> UserName, uidStr );
        strcpy ( ( ( pODBCConn ) conn ) -> Password, pwdStr );
        ( ( pODBCConn ) conn ) -> ServerPort = port;
        RETCODE ret = TryAuthenticate ( conn );
        _SQLFreeDiag ( _DIAGCONN ( conn ) );
        // disconnect
        _SQLDisconnect ( conn );
        // now free the structure itself
        delete conn;

        if ( ret == SQL_ERROR )
        {
            //validation of data & other prompts goes here
            __ODBCPopMsg ( "Username/Password not authorized, or server out of service." );
            return BAD;
        }

        return GOOD;
    }

    SetValueInODBCINI ( newDSN, SERVERKEY, serverStr, INITFILE );
    SetValueInODBCINI ( newDSN, PORTKEY, _itoa ( port, portStrBuffer, 10 ), INITFILE );
    SetValueInODBCINI ( newDSN, UIDKEY, uidStr, INITFILE );
    SetValueInODBCINI ( newDSN, PWDKEY, pwdStr, INITFILE );

    //If a new dsn name comes, add a new entry in regedit
    if ( _stricmp ( newDSN, currentDSN ) != 0 )
    {
        AddDSN ( newDSN );

        //it is a dsn renaming
        if ( strlen ( currentDSN ) != 0 )
        {
            RemoveDSN ( currentDSN );
        }
    }

    strcpy ( currentDSN, newDSN );
    char temp[BUFFERSIZE];
    GetValueFromODBCINI ( DRIVER_NAME, "Driver", DRIVER_DEFAULT_LOCATION, temp, BUFFERSIZE, INSTINIFILE );
    SetValueInODBCINI ( currentDSN, "Driver", temp, INITFILE );
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "Finish saving the configurations to ODBC INI" ) );
    return GOOD;
}


eGoodBad LoadODBCINIDataToConn ( pODBCConn pConn )
{
    Long x;
    char buffer[BUFFERSIZE];
    int c;

    // note
    // no error handling is currently being done for
    // GetDlgItemText/GetDlgItemInt/SetConnProp
    // generally should not be a problem

    // precaution
    if ( !pConn )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "GetDataFromDlgDSNCfg1 - Bad params: pConn is NULL" ) );
        return BAD;
    }

    ////// server name/IP
    c = GetValueFromODBCINI ( currentDSN, SERVERKEY, "", buffer, BUFFERSIZE, INITFILE );

    if ( c <= 0 )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "Please config the Kylin DSN in odbcad.exe before using it." ) );
        return BAD;
    }

    // set value in struct
    SetConnProp ( pConn, CONN_PROP_SERVER, buffer );
    /////  Port
    c = GetValueFromODBCINI ( currentDSN, PORTKEY, "", buffer, BUFFERSIZE, INITFILE );

    if ( c <= 0 )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "Please config the Kylin DSN in odbcad.exe before using it." ) );
        return BAD;
    }

    x = atoi ( buffer );
    // set value in struct
    SetConnProp ( pConn, CONN_PROP_PORT, &x );
    ////// User name
    c = GetValueFromODBCINI ( currentDSN, UIDKEY, "", buffer, BUFFERSIZE, INITFILE );

    if ( c <= 0 )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "Please config the Kylin DSN in odbcad.exe before using it." ) );
        return BAD;
    }

    // set value in struct
    SetConnProp ( pConn, CONN_PROP_UID, buffer );
    ////// Password
    c = GetValueFromODBCINI ( currentDSN, PWDKEY, "", buffer, BUFFERSIZE, INITFILE );

    if ( c <= 0 )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "Please config the Kylin DSN in odbcad.exe before using it." ) );
        return BAD;
    }

    // set value in struct
    SetConnProp ( pConn, CONN_PROP_PWD, buffer );
    ////// Project
    c = GetValueFromODBCINI ( currentDSN, PROJECTKEY, "", buffer, BUFFERSIZE, INITFILE );

    if ( c <= 0 )
    {
        __ODBCPOPMSG ( _ODBCPopMsg ( "Please config the Kylin DSN in odbcad.exe before using it." ) );
        return BAD;
    }

    // set value in struct
    SetConnProp ( pConn, CONN_PROP_PROJECT, buffer );
    return GOOD;
}

INT_PTR CALLBACK DlgDSNCfg2Proc ( HWND hDlg, UINT uMsg, WPARAM wParam, LPARAM lParam )
{
    char* attributes = ( char* ) lParam;
    Long port;
    char newDSN[BUFFERSIZE];
    char serverStr[BUFFERSIZE];
    char uidStr[BUFFERSIZE];
    char pwdStr[BUFFERSIZE];

    switch ( uMsg )
    {
        case WM_INITDIALOG :
        {
            // init protocol list

            HWND hwndCombo = GetDlgItem ( hDlg, IDC_PROTOCOL );
            SendMessage ( hwndCombo, CB_ADDSTRING, 0, reinterpret_cast <LPARAM> ( ( LPCTSTR )supportedProtocols[0] ) );
            SendMessage ( hwndCombo, CB_ADDSTRING, 0, reinterpret_cast <LPARAM> ( ( LPCTSTR )supportedProtocols[1] ) );
            SendMessage ( hwndCombo, CB_SETCURSEL, 0, 0 );

            SetCurrentDSN ( attributes, "DlgDSNCfg2Proc" );
            // store the structure for future use
            SetWindowLongPtr ( hDlg, DWLP_USER, lParam );

            // initialize the dialog with data from REGEDIT
            if ( LoadODBCINIDataToDlgDSNCfg2 ( hDlg ) != GOOD )
            {
                return false;
            }

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
                case IDC_BTEST :
                {
                    if ( RetriveDlgData ( hDlg, newDSN, serverStr, uidStr, pwdStr, &port ) == GOOD )
                    {
                        if ( testConnection ( serverStr, uidStr, pwdStr, port ) == GOOD )
                        {
                            HWND hwndCombo = GetDlgItem ( hDlg, IDC_COMBO1 );
                            HWND hwndOK = GetDlgItem ( hDlg, IDOK );
                            //passed verification
                            EnableWindow ( hwndCombo, TRUE );

                            try
                            {
                                std::vector <string> projects;
                                restListProjects ( serverStr, port, uidStr, pwdStr, projects );

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
                                stringstream ss;
                                ss << "Getting project list failed with error: " << e . what ();
                                __ODBCPopMsg ( ss . str () . c_str () );
                                return FALSE;
                            }

                            EnableWindow ( hwndOK, TRUE );
                            return TRUE;
                        }

                        else
                        {
                            __ODBCPopMsg ( "Connection failed." );
                        }
                    }

                    else
                    {
                        __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, "RetriveDlgData failed." ));
                    }

                    return FALSE;
                }

                case IDOK :
                {
                    HWND hwndCombo = GetDlgItem ( hDlg, IDC_COMBO1 );
                    int ItemIndex = SendMessage ( ( HWND ) hwndCombo, ( UINT ) CB_GETCURSEL,
                                                  ( WPARAM ) 0, ( LPARAM ) 0 );
                    TCHAR projectName[256];
                    ( TCHAR ) SendMessage ( ( HWND ) hwndCombo, ( UINT ) CB_GETLBTEXT,
                                            ( WPARAM ) ItemIndex, ( LPARAM ) projectName );

                    if ( RetriveDlgData ( hDlg, newDSN, serverStr, uidStr, pwdStr, &port ) == GOOD )
                    {
                        if ( testGetMetadata ( serverStr, uidStr, pwdStr, port, projectName ) == GOOD )
                        {
                            SaveConfigToODBCINI ( newDSN, serverStr, uidStr, pwdStr, port, projectName );
                            EndDialog ( hDlg, wParam );
                            return TRUE;
                        }
                    }

                    return FALSE;
                }

                case IDCANCEL :
                    // indicate end with control id as return value
                    EndDialog ( hDlg, wParam );
                    return TRUE;
            }
    }

    return FALSE;
}


BOOL INSTAPI ConfigDSN ( HWND hwndParent, WORD fRequest, LPCSTR lpszDriver, LPCSTR lpszAttributes )
{
    __ODBCLOG ( _ODBCLogMsg ( LogLevel_DEBUG, "ConfigDSN %s is called %s, the fRequest is: %d", lpszDriver, lpszAttributes,
        fRequest ) );

    if ( fRequest == ODBC_REMOVE_DSN )
    {
        SetCurrentDSN ( ( char* ) lpszAttributes, "ConfigDSN" );

        if ( strlen ( currentDSN ) <= 0 )
        {
            __ODBCPOPMSG ( _ODBCPopMsg ( "The DSN name is not defined in the connection string!" ) );
            return false;
        }

        BOOL ret = TRUE;
        ret = RemoveDSN ( currentDSN );

        if ( !ret )
        {
            __ODBCPOPMSG ( _ODBCPopMsg ( "The DSN is not found, removal failed!" ) );
            return false;
        }

        return true;
    }

    //else is ODBC_CONFIG_DSN or ODBC_ADD_DSN
    int i;
    i = DialogBoxParam ( ghInstDLL, MAKEINTRESOURCE ( IDD_DSN_CFG2 ), NULL, DlgDSNCfg2Proc, ( LPARAM ) lpszAttributes );

    // check status
    switch ( i )
    {
        case IDOK :
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, "User click OK button on DSN config" ) );
            return true; // complete

        default :
            __ODBCLOG ( _ODBCLogMsg ( LogLevel_INFO, "User click Cancel button on DSN config" ) );
            return false; // user-cancelled
    }

    return true;
}

