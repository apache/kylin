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


#include <assert.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>

void displayHexRecord ( char* data, int count, int record_length )
{
    int i;

    for ( i = 0; i < count; i++ )
    {
        printf ( "%02x ", data[i] & 0xff );
    }

    for ( ; i < record_length; i++ )
    {
        printf ( "	" );
    }

    printf ( ": " );

    for ( i = 0; i < count; i++ )
    {
        if ( isgraph ( data[i] ) )
        {
            putchar ( data[i] );
        }

        else
        {
            putchar ( '.' );
        }
    }

    putchar ( '\n' );
}

void bufferHexRecord ( char* data, int count, int record_length, char* buffer )
{
    int i;

    for ( i = 0; i < count; i++ )
    {
        sprintf ( buffer + strlen ( buffer ), "%02x ", data[i] & 0xff );
    }

    for ( ; i < record_length; i++ )
    {
        sprintf ( buffer + strlen ( buffer ), " " );
    }

    sprintf ( buffer + strlen ( buffer ), ": " );

    for ( i = 0; i < count; i++ )
    {
        if ( isgraph ( data[i] ) )
        {
            buffer[strlen ( buffer ) + 1] = '\0';
            buffer[strlen ( buffer )] = data[i];
        }

        else
        {
            buffer[strlen ( buffer ) + 1] = '\0';
            buffer[strlen ( buffer )] = '.';
        }
    }

    buffer[strlen ( buffer ) + 1] = '\0';
    buffer[strlen ( buffer )] = '\n';
}

//dump 16 * lines bytes, readable test stored in buffer, reserve 100 bytes in buffer for one line
void hexDump ( char* data, int lines, char* buffer, bool forward )
{
    if ( !forward )
    {
        data -= lines * 16;
    }

    buffer[0] = '\0';

    for ( int i = 0; i < lines; ++i )
    {
        sprintf ( buffer + strlen ( buffer ), "%10d  ", data );
        bufferHexRecord ( data, 16, 16, buffer );
        data += 16;
    }
}

