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


#pragma once

#include <memory>
#include <string>


//UTIL
std::unique_ptr <wchar_t[]> char2wchar ( char* orig );
std::unique_ptr <wchar_t[]> char2wchar ( const char* orig );
void char2wchar ( char* orig, wchar_t* dest, int destLength );

std::unique_ptr <char[]> wchar2char ( wchar_t* orig );
std::unique_ptr <char[]> wchar2char ( const wchar_t* orig );
void wchar2char ( wchar_t* orig, char* dest, int destLength );

std::wstring string2wstring ( std::string& orig );
std::string wstring2string ( std::wstring& orig );

void trimwhitespace ( char* str );
void copyTrimmed ( char** dest, char* src );

std::unique_ptr <char[]> str_base64_encode ( char* raw );
std::unique_ptr <char[]> str_base64_decode ( char* enc );

std::unique_ptr <char[]> make_unique_str ( int size );

void remove_char ( char* src, const char tgt );

