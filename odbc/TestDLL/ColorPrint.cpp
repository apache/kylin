#include <stdio.h>
#include <windows.h>

#include "ColorPrint.h"

CONSOLE_SCREEN_BUFFER_INFO consoleInfo;
WORD saved_attributes;
bool init = false;

void initLocals ( HANDLE hConsole ) {
    /* Save current attributes */
    GetConsoleScreenBufferInfo ( hConsole, &consoleInfo );
    saved_attributes = consoleInfo.wAttributes;
    init = true;
}

void setPrintColorRED() {
    HANDLE hConsole = GetStdHandle ( STD_OUTPUT_HANDLE );
    
    if ( !init ) {
        initLocals ( hConsole );
    }
    
    SetConsoleTextAttribute ( hConsole, FOREGROUND_RED );
}

void setPrintColorGreen() {
    HANDLE hConsole = GetStdHandle ( STD_OUTPUT_HANDLE );
    
    if ( !init ) {
        initLocals ( hConsole );
    }
    
    SetConsoleTextAttribute ( hConsole, FOREGROUND_GREEN );
}

void resetPrintColor() {
    if ( !init )
    { throw - 1; }
    
    HANDLE hConsole = GetStdHandle ( STD_OUTPUT_HANDLE );
    /* Restore original attributes */
    SetConsoleTextAttribute ( hConsole, saved_attributes );
}