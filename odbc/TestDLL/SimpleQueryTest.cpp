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

#include "Tests.h"

void simpleQueryTest ()
{
    //Intercept query test
    {
		int status;
		wstring s = requestQuery ( L"SELECT 1", KServerAddr, KPort, KUserName, KPassword, KDefaultProject, false, &status );
		std::unique_ptr <SQLResponse> y = convertToSQLResponse(status, s);

        if ( ( int ) y -> results . size () != 1 )
        {
            report ();
        }
    }
    //Ungzipped Query Test
    {
		int status;
		wstring s = requestQuery ( L"select cal_dt from test_kylin_fact limit 1", KServerAddr, KPort, KUserName, KPassword, KDefaultProject, false, &status );
		std::unique_ptr <SQLResponse> y = convertToSQLResponse(status, s);

        if ( ( int ) y -> results . size () != 1 )
        {
            report ();
        }
    }
    //zipped Query Test
    {
		int status;
		wstring s = requestQuery ( L"select * from test_kylin_fact limit 12", KServerAddr, KPort, KUserName, KPassword, KDefaultProject, false, &status );
		std::unique_ptr <SQLResponse> y = convertToSQLResponse(status, s);

        if ( ( int ) y -> results . size () != 12 )
        {
            report ();
        }
    }
}

