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
# This is python unittest used in smoke-test.sh, aim to testing diagnosis via rest APIs.

import unittest
import requests

class testDiag(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testDiag(self):
        url = "http://sandbox:7070/kylin/api/diag/project/learn_kylin/download"
        headers = {
            'content-type': "application/json",
            'authorization': "Basic QURNSU46S1lMSU4=",
            'cache-control': "no-cache"
        }

        response = requests.get(url, headers = headers)
        self.assertEqual(response.status_code, 200, 'Diagnosis failed.')


if __name__ == '__main__':
    print 'Test Diagnosis for Kylin sample.'
    unittest.main()
