#include "Tests.h"

void report ( const char* msg ) {
    throw exception ( msg );
}

void report() {
    throw exception();
}