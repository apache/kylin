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


#include <cpprest/http_client.h>
#include <cpprest/filestream.h>
#include <cpprest/json.h>
#include <cpprest/uri.h>
#include <string>
#include <windows.h>
#include "Base64.h"
#include "StringUtils.h"
#include "REST.h"
#include "Gzip.h"
#include "QueryCache.h"
#include "JsonConverter.h"

#include <ctime>
#include <fcntl.h>
#include <io.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdarg.h>


using namespace utility;
using namespace web::http;
using namespace web::http::client;
using namespace concurrency::streams;
using namespace web;
using namespace web::json;

void printLog ( const char* msg )
{
    time_t now = time ( 0 );
    struct tm tstruct;
    char buffer[100];
    tstruct = *localtime ( &now );
    strftime ( buffer, 100, "%Y-%m-%d.%X", &tstruct );
    printf ( buffer );
    printf ( "\n" );
    printf ( msg );
    printf ( "\n" );
}

/// <summary>
/// Find the longest length
/// </summary>
/// <param name="results"></param>
/// <param name="column"></param>
/// <returns></returns>
int ScanForLength ( std::vector <SQLRowContent*> results, int column )
{
    int max = 0;

    for ( auto p = results . begin (); p < results . end (); p++ )
    {
        SQLRowContent* result = *p;
        int length = result -> contents[column] . size ();

        if ( length > max )
        {
            max = length;
        }
    }

    return max;
}

/// <summary>
/// Scale is Maximum number of digits to the right of the decimal point.
/// Find the largest scale.
/// </summary>
/// <param name="results"></param>
/// <param name="column"></param>
/// <returns></returns>
int ScanForScale ( std::vector <SQLRowContent*> results, int column )
{
    int max = 0;

    for ( auto p = results . begin (); p < results . end (); p++ )
    {
        SQLRowContent* result = *p;
        int length = result -> contents[column] . size ();
        int dotLocation = result -> contents[column] . find ( L"." );

        if ( dotLocation != string::npos )
        {
            int scale = length - 1 - dotLocation;

            if ( scale > max )
            {
                max = scale;
            }
        }
    }

    return max;
}


void overwrite ( SQLResponse* res )
{
    for ( int i = 0; i < ( int ) res -> columnMetas . size (); ++i )
    {
        SelectedColumnMeta* meta = res -> columnMetas[i];
        ODBCTypes t = ( ODBCTypes ) meta -> columnType;
        int scale = 0;
        int length = 0;

        switch ( t )
        {
            case ODBCTypes::ODBC_Numeric :
            case ODBCTypes::ODBC_Decimal :
            case ODBCTypes::ODBC_Double :
            case ODBCTypes::ODBC_Real :
            case ODBCTypes::ODBC_Float :
                scale = ScanForScale ( res -> results, i );
                meta -> scale = scale;
                meta -> scale = 4;
                break;

            case ODBCTypes::ODBC_Char :
            case ODBCTypes::ODBC_VarChar :
            case ODBCTypes::ODBC_LongVarChar :
            case ODBCTypes::ODBC_WChar :
            case ODBCTypes::ODBC_WVarChar :
            case ODBCTypes::ODBC_WLongVarChar :
            case ODBCTypes::ODBC_DateTime :
            case ODBCTypes::ODBC_Type_Date :
            case ODBCTypes::ODBC_Type_Time :
            case ODBCTypes::ODBC_Type_Timestamp :
                length = ScanForLength ( res -> results, i );
                meta -> displaySize = length;
                meta -> precision = length;
                break;

            default :
                break;
        }
    }
}

std::wstring completeServerStr ( char* serverStr, long port )
{
    //concat the whole server string
    char completeServerAddr[256];
    char portSuffix[10];
    sprintf ( portSuffix, ":%d", port );

    if ( strstr ( serverStr, "https://" ) == serverStr ||
        strstr ( serverStr, "http://" ) == serverStr )
    {
        sprintf ( completeServerAddr, "%s", serverStr );
    }

    else
    {
        // by default use https
        sprintf ( completeServerAddr, "https://%s", serverStr );
    }

    if ( strstr ( serverStr, portSuffix ) == NULL )
    {
        strcat ( completeServerAddr, portSuffix );
    }

    return string2wstring ( std::string ( completeServerAddr ) );
}


http_request makeRequest ( const char* username, const char* passwd, const wchar_t* uriStr, http::method method )
{
    http_request request;
    char s[128];
    sprintf ( s, "%s:%s", username, passwd );
    std::string b64 = base64_encode ( ( unsigned char const* ) s, strlen ( s ) );
    request . set_method ( method );
    request . set_request_uri ( uri ( uri::encode_uri ( uriStr ) ) );
    request . headers () . add ( header_names::authorization, string2wstring ( "Basic " + b64 ) );
    request . headers () . add ( header_names::content_type, "application/json" );
    return request;
}

bool restAuthenticate ( char* serverAddr, long port, char* username, char* passwd )
{
    wstring serverAddrW = completeServerStr ( serverAddr, port );
    http_client_config config;
    config . set_timeout ( utility::seconds ( 300 ) );
    http_client session ( serverAddrW, config );
    //can get project list only when correct username/password is given
    http_request request = makeRequest ( username, passwd, L"/kylin/api/projects", methods::GET );
    http_response response = session . request ( request ) . get ();

    if ( response . status_code () == status_codes::OK )
    {
        return true;
    }

    else
    {
        return false;
    }
}

void restListProjects ( char* serverAddr, long port, char* username, char* passwd, std::vector <string>& holder )
{
    wstring serverAddrW = completeServerStr ( serverAddr, port );
    http_client_config config;
    config . set_timeout ( utility::seconds ( 300 ) );
    http_client session ( serverAddrW, config );
    http_request request = makeRequest ( username, passwd, L"/kylin/api/projects", methods::GET );
    http_response response = session . request ( request ) . get ();

    if ( response . status_code () == status_codes::OK )
    {
        web::json::value projects = response . extract_json () . get ();

        for ( auto iter = projects . as_array () . begin (); iter != projects . as_array () . end (); ++iter )
        {
            holder . push_back ( wstring2string ( ( *iter )[U ( "name" )] . as_string () ) );
        }

        if ( holder . size () == 0 )
        {
            throw exception ( "There is no project available in this server" );
        }
    }

    else if ( response . status_code () == status_codes::InternalError )
    {
        std::unique_ptr <ErrorMessage> em = ErrorMessageFromJSON ( response . extract_json () . get () );
        string errorMsg = wstring2string ( em -> msg );
        throw exception ( errorMsg . c_str () );
    }

    else
    {
        throw exception ( "REST request(listproject) Invalid Response status code : " + response . status_code () );
    }
}

std::unique_ptr <MetadataResponse> restGetMeta ( char* serverAddr, long port, char* username, char* passwd,
                                                 char* project )
{
    wstring serverAddrW = completeServerStr ( serverAddr, port );
    http_client_config config;
    config . set_timeout ( utility::seconds ( 300 ) );
    http_client session ( serverAddrW, config );
    std::wstringstream wss;
    wss << L"/kylin/api/tables_and_columns" << L"?project=" << project;
    http_request request = makeRequest ( username, passwd, wss . str () . c_str (), methods::GET );
    http_response response = session . request ( request ) . get ();

    if ( response . status_code () == status_codes::OK )
    {
        return MetadataResponseFromJSON ( response . extract_json () . get () );
    }

    else if ( response . status_code () == status_codes::Unauthorized )
    {
        throw exception ( "Username/Password Unauthorized." );
    }

    else if ( response . status_code () == status_codes::InternalError )
    {
        std::unique_ptr <ErrorMessage> em = ErrorMessageFromJSON ( response . extract_json () . get () );
        string errorMsg = wstring2string ( em -> msg );
        throw exception ( errorMsg . c_str () );
    }

    else
    {
        throw exception ( "REST request(getmeta) Invalid Response status code : " + response . status_code () );
    }
}

wstring cookQuery ( wchar_t* p )
{
    wchar_t* q = new wchar_t[wcslen ( p ) + 1];
    wcscpy ( q, p );

    for ( int i = 0; i < ( int ) wcslen ( q ); i++ )
    {
        if ( q[i] == '\r' || q[i] == '\n' || q[i] == '\t' )
        {
            q[i] = ' ';
        }
    }

    wstring ret ( q );
    delete[] q;
    size_t pos = 0;

    for ( size_t pos = 0;; pos += 2 )
    {
        pos = ret . find ( L"\"", pos );

        if ( pos == wstring::npos )
        {
            break;
        }

        ret . insert ( pos, L"\\" );
    }

    return ret;
}

wstring getBodyString ( http_response& response )
{
    bool isGzipped = response . headers () . has ( L"Content-Encoding" );

    if ( isGzipped )
    {
        isGzipped = false;
        http_headers::iterator iterator = response . headers () . find ( L"Content-Encoding" );

        if ( iterator != response . headers () . end () )
        {
            wstring contentEncoding = iterator -> second;

            if ( contentEncoding . find ( L"gzip" ) != std::string::npos )
            {
                isGzipped = true;
            }
        }
    }

    container_buffer <std::string> bodyBuffer;
    response . body () . read_to_end ( bodyBuffer ) . get ();
    const std::string& raw = bodyBuffer . collection ();
    std::string uncompressed;

    if ( isGzipped )
    {
        bool decompressStatus = gzipInflate ( raw, uncompressed );

        if ( !decompressStatus )
        {
            throw exception ( "gzip decompress failed" );
        }
    }

    else
    {
        uncompressed = raw;
    }

    //convert the string from utf8 to wchar
    int size_needed = ::MultiByteToWideChar ( CP_UTF8, 0, ( char* ) uncompressed . c_str (), uncompressed . size (), NULL, 0 );
    std::wstring ret ( size_needed, 0 );
    ::MultiByteToWideChar ( CP_UTF8, 0, ( char* ) uncompressed . c_str (), uncompressed . size (), &ret[0], size_needed );
    return ret;
}

std::unique_ptr <SQLResponse> restQuery ( wchar_t* rawSql, char* serverAddr, long port, char* username,
                                          char* passwd,
                                          char* project )
{
    //using local cache to intercept probing queries
    std::unique_ptr <SQLResponse> cachedQueryRes = loadCache ( rawSql );

    if ( cachedQueryRes != NULL )
    {
        return cachedQueryRes;
    }

    //real requesting
    wstring serverAddrW = completeServerStr ( serverAddr, port );
    http_client_config config;
    config . set_timeout ( utility::seconds ( 36000 ) );
    http_client session ( serverAddrW, config );
    http_request request = makeRequest ( username, passwd, L"/kylin/api/query", methods::POST );
    wstring sql = cookQuery ( rawSql );
    std::wstringstream wss;
    wss << L"{ \"acceptPartial\": false, \"project\" : \"" << project << L"\", " << " \"sql\" : \"" << sql << L"\" }" ;
    request . set_body ( wss . str (), L"application/json" );
    request . headers () . add ( header_names::accept_encoding, "gzip,deflate" );
    http::status_code status;
    http_response response;

    try
    {
        response = session . request ( request ) . get ();
        status = response . status_code ();
    }

    catch ( std::exception& e )
    {
        std::stringstream ss;
        ss << "An exception is throw Error message: " << e . what ();
        throw exception ( ss . str () . c_str () );
    }

    wstring ret = getBodyString ( response );

    if ( status == status_codes::OK )
    {
        //convert to json
        web::json::value actualRes = web::json::value::parse ( ret );
        std::unique_ptr <SQLResponse> r = SQLResponseFromJSON ( actualRes );

        if ( r -> isException == true )
        {
            string expMsg = wstring2string ( r -> exceptionMessage );
            throw exception ( expMsg . c_str () );
        }

        overwrite ( r . get () );
        return r;
    }

    else if ( status == status_codes::InternalError )
    {
        std::unique_ptr <ErrorMessage> em = ErrorMessageFromJSON ( web::json::value::parse ( ret ) );
        string expMsg = wstring2string ( em -> msg );
        throw exception ( expMsg . c_str () );
    }

    else
    {
        throw exception ( "Unknown exception in rest query with return code " + status );
    }

    return NULL;
}

