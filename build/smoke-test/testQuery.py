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


class testQuery(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testQuery(self):
        base_url = "http://sandbox:7070/kylin/api"
        url = base_url + "/query"
        headers = {
            'content-type': "application/json",
            'authorization': "Basic QURNSU46S1lMSU4=",
            'cache-control': "no-cache"
        }

        sql_files = glob.glob('sql/*.sql')
        index = 0
        for sql_file in sql_files:
            index += 1
            sql_fp = open(sql_file)
            sql_statement = ''
            sql_statement_lines = open(sql_file).readlines()
            for sql_statement_line in sql_statement_lines:
                if not sql_statement_line.startswith('--'):
                    sql_statement += sql_statement_line.strip() + ' '
            payload = "{\"sql\": \"" + sql_statement.strip() + "\", \"offset\": 0, \"limit\": \"50000\", \"acceptPartial\":false, \"project\":\"learn_kylin\"}"
            print 'Test Query #' + str(index) + ': \n' + sql_statement
            response = requests.request("POST", url, data=payload, headers=headers)

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


if __name__ == '__main__':
    print 'Test Query for Kylin sample.'
    unittest.main()
