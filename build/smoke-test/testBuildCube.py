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
# This is python unittest used in smoke-test.sh, aim to testing building cubes via rest APIs.

import unittest
import requests
import json
import time


class testBuildCube(unittest.TestCase):

    _base_url = "http://sandbox:7070/kylin/api"

    _headers = {
        'content-type': "application/json",
        'authorization': "Basic QURNSU46S1lMSU4=",
        'cache-control': "no-cache"
    }

    _clone_cube_url = _base_url + "/cubes/kylin_sales_cube/clone"

    def setUp(self):
        self.clone_cube("kylin_sales_cube_spark", "SPARK")
        self.clone_cube("kylin_sales_cube_flink", "FLINK")

    def tearDown(self):
        pass

    def clone_cube(self, cube_name, engine_type):
        payload = {'project': 'learn_kylin',
                   'cubeName': cube_name}
        response = requests.request("PUT", self._clone_cube_url, json=payload, headers=self._headers)
        self.assertEqual(response.status_code, 200, 'Clone cube : ' + cube_name + ' failed.')
        update_engine_url = self._base_url + "/cubes/" + cube_name + "/engine/" + engine_type
        response = requests.request("PUT", update_engine_url, headers=self._headers)
        self.assertEqual(response.status_code, 200, 'Update engine type of cube : ' + cube_name + ' failed.')

    def singleBuild(self, cube_name):

        url = self._base_url + "/cubes/" + cube_name + "/rebuild"

        # reload metadata before build cubes
        cache_response = requests.request("PUT", self._base_url + "/cache/all/all/update", headers=self._headers)
        self.assertEqual(cache_response.status_code, 200, 'Metadata cache not refreshed.')

        payload = "{\"startTime\": 1325376000000, \"endTime\": 1456790400000, \"buildType\":\"BUILD\"}"
        status_code = 0
        try_time = 1
        while status_code != 200 and try_time <= 3:
            print 'Submit build job, try_time = ' + str(try_time)
            try:
                response = requests.request("PUT", url, data=payload, headers=self._headers)
                status_code = response.status_code
            except:
                status_code = 0
                pass
            if status_code != 200:
                time.sleep(60)
                try_time += 1

        self.assertEqual(status_code, 200, 'Build job submitted failed.')

        if status_code == 200:
            print 'Build job is submitted...'
            job_response = json.loads(response.text)
            job_uuid = job_response['uuid']
            job_url = self._base_url + "/jobs/" + job_uuid
            job_response = requests.request("GET", job_url, headers=self._headers)

            self.assertEqual(job_response.status_code, 200, 'Build job information fetched failed.')

            job_info = json.loads(job_response.text)
            job_status = job_info['job_status']
            try_time = 1
            while job_status in ('RUNNING', 'PENDING') and try_time <= 30:
                print 'Wait for job complete, try_time = ' + str(try_time)
                try:
                    job_response = requests.request("GET", job_url, headers=self._headers)
                    job_info = json.loads(job_response.text)
                    job_status = job_info['job_status']
                except:
                    job_status = 'UNKNOWN'
                    pass
                if job_status in ('RUNNING', 'PENDING', 'UNKNOWN'):
                    time.sleep(60)
                    try_time += 1

            self.assertEquals(job_status, 'FINISHED', 'Build cube failed, job status is ' + job_status)
            print 'Job complete.'

    def testBuild(self):
        self.singleBuild("kylin_sales_cube_spark")
        self.singleBuild("kylin_sales_cube_flink")
        self.singleBuild("kylin_sales_cube")
        # wait for kylin_sales_cube to READY
        time.sleep(10)


if __name__ == '__main__':
    print 'Test Build Cube for Kylin sample.'
    unittest.main()
