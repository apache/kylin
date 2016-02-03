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

#define prod_KServerAddr ""
#define prod_KPort 443

#define KServerAddr "http://localhost"
#define KPort 80
#define KUserName "ADMIN"
#define KPassword "KADMIN"
#define KDefaultProject "default"

#include <conio.h>
#include <stdio.h>
#include <tchar.h>
#include <stdlib.h>
#include <string>
#include <memory>
#include <windows.h>
#include <sqlext.h> // required for ODBC calls

#include <iostream>
#include <REST.h>

void report ();
void report ( const char* msg );

void simpleQueryTest ();
void queryFlowTest ();
void restAPITest ();
void crossValidate ();
void validateSQLGetTypeInfo ();

