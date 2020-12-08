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

import os
import json

from getgauge.python import step

from kylin_utils import util
from kylin_utils import equals


@step("Query all SQL file in directory <sql_directory> in project <project_name>, compare <compare_level> with hive pushdown result and compare metrics info with sql_result json file in <sql_result_directory>")
def query_sql_file_and_compare(sql_directory, project_name, compare_level, sql_result_directory):
    sql_directory_list = os.listdir(sql_directory)
    for sql_file_name in sql_directory_list:
        if (sql_file_name.split('.')[len(sql_file_name.split('.'))-1]) == 'sql':
            with open(sql_directory + sql_file_name, 'r', encoding='utf8') as sql_file:
                sql = sql_file.read()

            client = util.setup_instance('kylin_instance.yml')
            expected_result_file_name = sql_result_directory + sql_file_name.split(".")[0] + '.json'
            expected_result = None
            if os.path.exists(expected_result_file_name):
                with open(expected_result_file_name, 'r', encoding='utf8') as expected_result_file:
                    expected_result = json.loads(expected_result_file.read())
            equals.compare_sql_result(sql=sql, project=project_name, kylin_client=client, expected_result=expected_result, compare_level=compare_level)



