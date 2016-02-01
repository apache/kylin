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

#include <stdio.h>
#include <windows.h>

#include "ColorPrint.h"

CONSOLE_SCREEN_BUFFER_INFO consoleInfo;
WORD saved_attributes;
bool init = false;

void initLocals ( HANDLE hConsole )
{
    /* Save current attributes */
    GetConsoleScreenBufferInfo ( hConsole, &consoleInfo );
    saved_attributes = consoleInfo . wAttributes;
    init = true;
}

void setPrintColorRED ()
{
    HANDLE hConsole = GetStdHandle ( STD_OUTPUT_HANDLE );

    if ( !init )
    {
        initLocals ( hConsole );
    }

    SetConsoleTextAttribute ( hConsole, FOREGROUND_RED );
}

void setPrintColorGreen ()
{
    HANDLE hConsole = GetStdHandle ( STD_OUTPUT_HANDLE );

    if ( !init )
    {
        initLocals ( hConsole );
    }

    SetConsoleTextAttribute ( hConsole, FOREGROUND_GREEN );
}

void resetPrintColor ()
{
    if ( !init )
    {
        throw - 1;
    }

    HANDLE hConsole = GetStdHandle ( STD_OUTPUT_HANDLE );
    /* Restore original attributes */
    SetConsoleTextAttribute ( hConsole, saved_attributes );
}

