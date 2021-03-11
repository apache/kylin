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

from getgauge.python import step
import os
import json
import pytest
from kylin_utils import util
from kylin_utils import equals
from kylin_utils import shell
import re
import time


@step("automated happy path, check query using <sql_file>")
def happy_path(sql_file):
    global client
    client = util.setup_instance('kylin_instance.yml')

    resp1 = client.build_segment(start_time="1325376000000",
                                end_time="1356998400000",
                                cube_name="kylin_sales_cube")

    resp2 = client.build_segment(start_time="1356998400000",
                                end_time="1388620800000",
                                cube_name="kylin_sales_cube")

    job_id1 = resp1['uuid']
    job_id2 = resp2['uuid']
    client.await_job(job_id1)
    client.await_job(job_id2)

    resp = client.merge_segment(cube_name="kylin_sales_cube",
                                start_time="1325376000000",
                                end_time="1388620800000")

    job_id = resp['uuid']
    client.await_job(job_id)

    resp = client.refresh_segment(cube_name="kylin_sales_cube",
                                  start_time="1325376000000",
                                  end_time="1388620800000")
    job_id = resp['uuid']
    client.await_job(job_id)

    with open(sql_file, 'r', encoding='utf8') as sql:
        sql = sql.read()

    equals.compare_sql_result(sql=sql, project='learn_kylin', kylin_client=client, expected_result=None)

    resp = client.disable_cube(cube_name="kylin_sales_cube")
    assert resp['status'] == 'DISABLED'

    time.sleep(10)

    client.purge_cube(cube_name="kylin_sales_cube")
    time.sleep(30)

    resp = client.get_cube_instance(cube_name="kylin_sales_cube")

    assert len(resp['segments']) == 0










