#!/usr/bin/python
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This is python unittest used in smoke-test.sh, aim to testing query via rest APIs.

import unittest
import requests
import json
import glob
import time


class testQuery(unittest.TestCase):
    base_url = "http://sandbox:7070/kylin/api"
    headers = {
        'content-type': "application/json",
        'authorization': "Basic QURNSU46S1lMSU4=",
        'cache-control': "no-cache"
    }

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testQuery(self):

        sql_files = glob.glob('sql/*.sql')
        index = 0
        query_url = testQuery.base_url + "/query"
        for sql_file in sql_files:
            index += 1
            sql_statement = ''
            sql_statement_lines = open(sql_file).readlines()
            for sql_statement_line in sql_statement_lines:
                if not sql_statement_line.startswith('--'):
                    sql_statement += sql_statement_line.strip() + ' '
            payload = "{\"sql\": \"" + sql_statement.strip() + "\", \"offset\": 0, \"limit\": \"50000\", \"acceptPartial\":false, \"project\":\"learn_kylin\"}"
            print 'Test Query #' + str(index) + ': \n' + sql_statement
            response = requests.request("POST", query_url, data=payload, headers=testQuery.headers)

            self.assertEqual(response.status_code, 200, 'Query failed.')
            actual_result = json.loads(response.text)
            print 'Query duration: ' + str(actual_result['duration']) + 'ms'
            del actual_result['duration']
            del actual_result['hitExceptionCache']
            del actual_result['storageCacheUsed']
            del actual_result['totalScanCount']
            del actual_result['totalScanBytes']

            expect_result = json.loads(open(sql_file[:-4] + '.json').read().strip())
            self.assertEqual(actual_result, expect_result, 'Query result does not equal.')

    def testQueryPushDown(self):
        sql_files = glob.glob('sql/*.sql')
        index = 0
        url = testQuery.base_url + "/cubes/kylin_sales_cube/disable"
        status_code = 0
        try_time = 1
        while status_code != 200 and try_time <= 3:
            print 'Disable cube, try_time = ' + str(try_time)
            try:
                response = requests.request("PUT", url, headers=testQuery.headers)
                status_code = response.status_code
            except:
                status_code = 0
                pass
            if status_code != 200:
                time.sleep(10)
                try_time += 1

        self.assertEqual(status_code, 200, 'Disable cube failed.')

        # Sleep 3 seconds to ensure cache wiped while do query pushdown
        time.sleep(3)

        query_url = testQuery.base_url + "/query"
        for sql_file in sql_files:
            index += 1
            sql_statement = ''
            sql_statement_lines = open(sql_file).readlines()
            for sql_statement_line in sql_statement_lines:
                if not sql_statement_line.startswith('--'):
                    sql_statement += sql_statement_line.strip() + ' '
            payload = "{\"sql\": \"" + sql_statement.strip() + "\", \"offset\": 0, \"limit\": \"50000\", \"acceptPartial\":false, \"project\":\"learn_kylin\"}"
            print 'Test Query #' + str(index) + ': \n' + sql_statement
            response = requests.request("POST", query_url, data=payload, headers=testQuery.headers)

            self.assertEqual(response.status_code, 200, 'Query failed.')

            actual_result = json.loads(response.text)
            print actual_result
            print 'Query duration: ' + str(actual_result['duration']) + 'ms'
            del actual_result['duration']
            del actual_result['hitExceptionCache']
            del actual_result['storageCacheUsed']
            del actual_result['totalScanCount']
            del actual_result['totalScanBytes']
            del actual_result['columnMetas']

            expect_result = json.loads(open(sql_file[:-4] + '.json').read().strip())
            del expect_result['columnMetas']
            expect_result['cube'] = ''
            expect_result['pushDown'] = True
            self.assertEqual(actual_result, expect_result, 'Query pushdown\'s result does not equal with expected result.')


if __name__ == '__main__':
    print 'Test Query for Kylin sample.'
    unittest.main()
