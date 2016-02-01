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


#include <iostream>
#include <stdlib.h>
#include <string>

#include "atlbase.h"
#include "atlstr.h"
#include "comutil.h"


#include "StringUtils.h"
#include "Base64.h"

using namespace std;

std::unique_ptr <char[]> str_base64_encode ( char* raw )
{
    trimwhitespace ( raw );
    string encStr = base64_encode ( ( const unsigned char* ) raw, strlen ( raw ) );
    std::unique_ptr <char[]> temp ( new char[encStr . length () + 1] );
    strcpy ( temp . get (), encStr . c_str () );
    return temp;
}

std::unique_ptr <char[]> str_base64_decode ( char* enc )
{
    string s ( enc );
    string decStr = base64_decode ( s );
    std::unique_ptr <char[]> temp ( new char[decStr . length () + 1] );
    strcpy ( temp . get (), decStr . c_str () );
    return temp;
}

void trimwhitespace ( char* str )
{
    if ( str == NULL || strlen ( str ) == 0 )
    {
        return;
    }

    char* start = str;
    char* end;

    // Trim leading space
    while ( isspace ( *start ) )
    {
        start++;
    }

    if ( *start == 0 )
    { // All spaces?
        str[0] = '\0';
        return;
    }

    // Trim trailing space
    end = start + strlen ( start ) - 1;

    while ( end > start && isspace ( *end ) )
    {
        end--;
    }

    // Write new null terminator
    * ( end + 1 ) = 0;
    memmove ( str, start, end - start + 2 );
}

void copyTrimmed ( char** dest, char* src )
{
    // check if previous value exists
    if ( *dest )
    {
        delete[] ( *dest );
        *dest = NULL;
    }

    *dest = new char[strlen ( src ) + 1];
    strcpy ( *dest, src );
    trimwhitespace ( *dest );
}

std::unique_ptr <wchar_t[]> char2wchar ( char* orig )
{
    if ( orig == NULL )
    {
        return NULL;
    }

    size_t newsize = strlen ( orig ) + 1;
    std::unique_ptr <wchar_t[]> wcstring ( new wchar_t[newsize] );
    size_t convertedChars = 0;
    mbstowcs_s ( &convertedChars, wcstring . get (), newsize, orig, _TRUNCATE );
    return wcstring;
}

std::unique_ptr <wchar_t[]> char2wchar ( const char* orig )
{
    if ( orig == NULL )
    {
        return NULL;
    }

    size_t newsize = strlen ( orig ) + 1;
    std::unique_ptr <wchar_t[]> wcstring ( new wchar_t[newsize] );
    size_t convertedChars = 0;
    mbstowcs_s ( &convertedChars, wcstring . get (), newsize, orig, _TRUNCATE );
    return wcstring;
}

//specifying the destination
void char2wchar ( char* orig, wchar_t* dest, int destBufferLength )
{
    if ( orig == NULL )
    {
        return;
    }

    if ( destBufferLength > 0 )
    {
        if ( destBufferLength <= ( int ) strlen ( orig ) )
        {
            throw - 1;
        }
    }

    size_t newsize = strlen ( orig ) + 1;
    size_t convertedChars = 0;
    mbstowcs_s ( &convertedChars, dest, newsize, orig, _TRUNCATE );
}

std::unique_ptr <char[]> wchar2char ( wchar_t* orig )
{
    if ( orig == NULL )
    {
        return NULL;
    }

    size_t origsize = wcslen ( orig ) + 1;
    size_t convertedChars = 0;
    const size_t newsize = origsize;
    std::unique_ptr <char[]> nstring ( new char[newsize] );
    wcstombs_s ( &convertedChars, nstring . get (), newsize, orig, _TRUNCATE );
    return nstring;
}

std::unique_ptr <char[]> wchar2char ( const wchar_t* orig )
{
    if ( orig == NULL )
    {
        return NULL;
    }

    size_t origsize = wcslen ( orig ) + 1;
    size_t convertedChars = 0;
    const size_t newsize = origsize;
    std::unique_ptr <char[]> nstring ( new char[newsize] );
    wcstombs_s ( &convertedChars, nstring . get (), newsize, orig, _TRUNCATE );
    return nstring;
}

void wchar2char ( wchar_t* orig, char* dest, int destBufferLength )
{
    if ( orig == NULL )
    {
        return;
    }

    if ( destBufferLength > 0 )
    {
        if ( destBufferLength <= ( int ) wcslen ( orig ) )
        {
            throw - 1;
        }
    }

    size_t origsize = wcslen ( orig ) + 1;
    size_t convertedChars = 0;
    const size_t newsize = origsize;
    wcstombs_s ( &convertedChars, dest, newsize, orig, _TRUNCATE );
}


std::wstring string2wstring ( std::string& orig )
{
    std::wstring ws;
    ws . assign ( orig . begin (), orig . end () );
    return ws;
}

std::string wstring2string ( std::wstring& orig )
{
    std::string s;
    s . assign ( orig . begin (), orig . end () );
    return s;
}

std::unique_ptr <char[]> make_unique_str ( int size )
{
    return std::unique_ptr <char[]> ( new char[size + 1] );
}

void remove_char ( char* src, const char tgt )
{
    char* fp = src;
    while ( *src )
    {
        if ( *src != tgt )
        {
            *fp = *src;
            fp++;
        }
        src++;
    }
    *fp = '\0' ;
}

