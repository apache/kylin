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
#include "Dump.h"
#include "JsonConverter.h"


const wchar_t* alwaysFailQueries[7] = {
    L"(\\s)*CREATE(\\s)*LOCAL(\\s)*TEMPORARY(\\s)*TABLE(\\s)*\"XTableau_B_Connect\"(\\s)*\\((\\s)*\"COL\"(\\s)*INTEGER(\\s)*\\)(\\s)*ON(\\s)*COMMIT(\\s)*PRESERVE(\\s)*ROWS(\\s)*",
    L"(\\s)*DROP(\\s)*TABLE(\\s)*\"XTableau_B_Connect\"(\\s)*",
    L"(\\s)*SELECT(\\s)*TOP(\\s)*1(\\s)*\"COL\"(\\s)*FROM(\\s)*\\(SELECT(\\s)*1(\\s)*AS(\\s)*\"COL\"(\\s)*\\)(\\s)*AS(\\s)*\"CHECKTOP\"(\\s)*",
    L"(\\s)*SELECT(\\s)*\"SUBCOL\"(\\s)*AS(\\s)*\"COL\"(\\s)*FROM(\\s)*\\((\\s)*SELECT(\\s)*1(\\s)*AS(\\s)*\"SUBCOL\"(\\s)*\\)(\\s)*\"SUBQUERY\"(\\s)*GROUP(\\s)*BY(\\s)*1(\\s)*",
    L"(\\s)*SELECT(\\s)*\"SUBCOL\"(\\s)*AS(\\s)*\"COL\"(\\s)*FROM(\\s)*\\((\\s)*SELECT(\\s)*1(\\s)*AS(\\s)*\"SUBCOL\"(\\s)*\\)(\\s)*\"SUBQUERY\"(\\s)*GROUP(\\s)*BY(\\s)*\"COL\"(\\s)*",
    L"(\\s)*INSERT(\\s)*INTO(\\s)*\"XTableau_C_Connect\"(\\s)*SELECT(\\s)*\\*(\\s)*FROM(\\s)*\\(SELECT(\\s)*1(\\s)*AS(\\s)*COL(\\s)*\\)(\\s)*AS(\\s)*CHECKTEMP(\\s)*LIMIT(\\s)*1(\\s)*",
    L"(\\s)*DROP(\\s)*TABLE(\\s)*\"XTableau_C_Connect\"(\\s)*"
};

const wchar_t* alwaysSuccessQueries[3] = {
    L"(\\s)*SELECT(\\s)*1(\\s)*",
    L"(\\s)*SELECT(\\s)*\"COL\"(\\s)*FROM(\\s)*\\(SELECT(\\s)*1(\\s)*AS(\\s)*\"COL\"\\)(\\s)*AS(\\s)*\"SUBQUERY\"(\\s)*",
    L"(\\s)*SELECT(\\s)*\"COL\"(\\s)*FROM(\\s)*\\(SELECT(\\s)*1(\\s)*AS(\\s)*\"COL\"\\)(\\s)*AS(\\s)*\"CHECKTOP\"(\\s)*LIMIT(\\s)*1(\\s)*"
};

const wchar_t* alwaysSuccessResults[3] = {
    L"{\"columnMetas\":[{\"isNullable\":2,\"displaySize\":11,\"label\":\"COL\",\"name\":\"COL\",\"schemaName\":\"\",\"catelogName\":\"\",\"tableName\":\"\",\"precision\":10,\"scale\":0,\"columnType\":4,\"columnTypeName\":\"int4\",\"writable\":true,\"caseSensitive\":false,\"autoIncrement\":false,\"searchable\":true,\"currency\":false,\"signed\":true,\"definitelyWritable\":false,\"readOnly\":false}],\"results\":[[\"1\"]],\"isResultsFlatten\":false,\"flattenResult\":null,\"flattenResultOriginalSize\":0,\"cubes\":null,\"affectedRowCount\":0,\"isException\":false,\"exceptionMessage\":null,\"duration\":0.002,\"partial\":false}",
    L"{\"columnMetas\":[{\"isNullable\":2,\"displaySize\":11,\"label\":\"COL\",\"name\":\"COL\",\"schemaName\":\"\",\"catelogName\":\"\",\"tableName\":\"\",\"precision\":10,\"scale\":0,\"columnType\":4,\"columnTypeName\":\"int4\",\"writable\":true,\"caseSensitive\":false,\"autoIncrement\":false,\"searchable\":true,\"currency\":false,\"signed\":true,\"definitelyWritable\":false,\"readOnly\":false}],\"results\":[[\"1\"]],\"isResultsFlatten\":false,\"flattenResult\":null,\"flattenResultOriginalSize\":0,\"cubes\":null,\"affectedRowCount\":0,\"isException\":false,\"exceptionMessage\":null,\"duration\":0.002,\"partial\":false}",
    L"{\"columnMetas\":[{\"isNullable\":2,\"displaySize\":11,\"label\":\"COL\",\"name\":\"COL\",\"schemaName\":\"\",\"catelogName\":\"\",\"tableName\":\"\",\"precision\":10,\"scale\":0,\"columnType\":4,\"columnTypeName\":\"int4\",\"writable\":true,\"caseSensitive\":false,\"autoIncrement\":false,\"searchable\":true,\"currency\":false,\"signed\":true,\"definitelyWritable\":false,\"readOnly\":false}],\"results\":[[\"1\"]],\"isResultsFlatten\":false,\"flattenResult\":null,\"flattenResultOriginalSize\":0,\"cubes\":null,\"affectedRowCount\":0,\"isException\":false,\"exceptionMessage\":null,\"duration\":0.002,\"partial\":false}"
};

int findQuery ( const wchar_t* sql, const wchar_t** regexs, int size )
{
    for ( int i = 0; i < size; ++i )
    {
        std::tr1::wregex rgx ( regexs[i], regex_constants::icase );
        bool match = std::tr1::regex_search ( sql, rgx );

        if ( match )
        {
            return i;
        }
    }

    return -1;
}

int findInAlwaysSuccessQuery ( const wchar_t* sql )
{
    return findQuery ( sql, alwaysSuccessQueries, sizeof ( alwaysSuccessQueries ) / sizeof ( wchar_t*) );
}

int findInAlwaysFailQuery ( const wchar_t* sql )
{
    return findQuery ( sql, alwaysFailQueries, sizeof ( alwaysFailQueries ) / sizeof ( wchar_t*) );
}

unique_ptr <SQLResponse> loadCache ( const wchar_t* query )
{
    int index = 0;

    if ( findInAlwaysFailQuery ( query ) >= 0 )
    {
        throw exception ( "Unsupported SQL" );
    }

    else if ( ( index = findInAlwaysSuccessQuery ( query ) ) >= 0 )
    {
        web::json::value v = web::json::value::parse ( alwaysSuccessResults[index] );
        std::unique_ptr <SQLResponse> r = SQLResponseFromJSON ( v );
        //overwrite(r.get());
        return r;
    }

    return NULL;
}

