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

#include "vld.h"
#include "MsgTypes.h"
#include <xstring>


//REST
bool restAuthenticate ( char* serverAddr, long port, char* username, char* passwd );

void restListProjects ( char* serverAddr, long port, char* username, char* passwd, std::vector <string>& holder );

std::unique_ptr <MetadataResponse> restGetMeta ( char* serverAddr, long port, char* username, char* passwd,
                                                 char* project );

std::unique_ptr <SQLResponse> convertToSQLResponse ( int status,
										  wstring responseStr );

wstring requestQuery ( wchar_t* rawSql, char* serverAddr, long port, char* username,
                                          char* passwd,
                                          char* project,
										  bool isPrepare,
										  int* statusFlag);