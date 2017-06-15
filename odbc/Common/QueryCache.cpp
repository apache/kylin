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


#include "QueryCache.h"
#include <cpprest/http_client.h>
#include <cpprest/filestream.h>
#include <cpprest/json.h>
#include <cpprest/uri.h>
#include <regex>
#include <map>
#include <queue>
#include <mutex>
#include "Dump.h"
#include "JsonConverter.h"

using namespace std;

map<wstring, wstring> queryMap;
queue<wstring> queryQueue;
const int cacheSize = 20;
mutex cacheMutex;

const wchar_t* loadCache ( const wchar_t* query )
{
    lock_guard<mutex> lock(cacheMutex);
    wstring queryString = wstring(query);
    queryString.erase(remove_if(queryString.begin(), queryString.end(), ::isspace), queryString.end());
    map<wstring, wstring>::iterator it = queryMap.find(queryString);
    if (it != queryMap.end()) 
    {
        return it->second.c_str();
    }
    return NULL;
}

void storeCache (const wchar_t* query, const wchar_t* result)
{
    lock_guard<mutex> lock(cacheMutex);
    wstring queryString = wstring(query);
    queryString.erase(remove_if(queryString.begin(), queryString.end(), ::isspace), queryString.end());
    
    map<wstring, wstring>::iterator it = queryMap.find(queryString);
    if (it != queryMap.end()) 
    {
        return;
    }

    if (queryQueue.size() >= cacheSize) 
    {
        wstring head = queryQueue.front();
        queryQueue.pop();
        queryMap.erase(head);
    }

    queryQueue.push(queryString);
    queryMap[queryString] = result;
}
