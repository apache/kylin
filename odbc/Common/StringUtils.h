#pragma once

#include <memory>
#include <string>


//UTIL
std::unique_ptr<wchar_t[]> char2wchar ( char* orig );
std::unique_ptr<wchar_t[]> char2wchar ( const char* orig );
void char2wchar ( char* orig, wchar_t* dest, int destLength );

std::unique_ptr<char[]> wchar2char ( wchar_t* orig );
std::unique_ptr<char[]> wchar2char ( const wchar_t* orig );
void wchar2char ( wchar_t* orig, char* dest, int destLength );

std::wstring string2wstring ( std::string& orig );
std::string wstring2string ( std::wstring& orig );

void trimwhitespace ( char* str );
void copyTrimmed ( char** dest, char* src );

std::unique_ptr<char[]> str_base64_encode ( char* raw );
std::unique_ptr<char[]> str_base64_decode ( char* enc );

std::unique_ptr<char[]> make_unique_str ( int size );