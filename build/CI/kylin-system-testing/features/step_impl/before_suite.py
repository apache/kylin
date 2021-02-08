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

from getgauge.python import before_suite
import os
import json
import time

from kylin_utils import util


@before_suite()
def create_generic_model_and_cube():
    client = util.setup_instance('kylin_instance.yml')
    if client.version == '3.x':
        with open(os.path.join('meta_data/generic_desc_data', 'generic_desc_data_3x.json'), 'r') as f:
            data = json.load(f)
    elif client.version == '4.x':
        with open(os.path.join('meta_data/generic_desc_data', 'generic_desc_data_4x.json'), 'r') as f:
            data = json.load(f)

    project_name = client.generic_project
    if not util.if_project_exists(kylin_client=client, project=project_name):
        client.create_project(project_name)

    tables = data.get('load_table_list')
    resp = client.load_table(project_name=project_name, tables=tables)
    assert ",".join(resp["result.loaded"]) == tables

    model_desc_data = data.get('model_desc_data')
    model_name = model_desc_data.get('name')
    snowflake_left_incre_model = data.get('snowflake_left_incre_model')
    left_model_name = snowflake_left_incre_model.get('name')

    if not util.if_model_exists(kylin_client=client, model_name=model_name, project=project_name):
        resp = client.create_model(project_name=project_name, 
                                   model_name=model_name, 
                                   model_desc_data=model_desc_data)
        assert json.loads(resp['modelDescData'])['name'] == model_name

    if not util.if_model_exists(kylin_client=client, model_name=left_model_name, project=project_name):
        resp = client.create_model(project_name=project_name,
                                   model_name=left_model_name,
                                   model_desc_data=snowflake_left_incre_model)
        assert json.loads(resp['modelDescData'])['name'] == left_model_name

    cube_desc_data = data.get('cube_desc_data')
    cube_name = cube_desc_data.get('name')
    if not util.if_cube_exists(kylin_client=client, cube_name=cube_name, project=project_name):
        resp = client.create_cube(project_name=project_name,
                                  cube_name=cube_name,
                                  cube_desc_data=cube_desc_data)
        assert json.loads(resp['cubeDescData'])['name'] == cube_name
    if client.get_cube_instance(cube_name=cube_name).get('status') != 'READY' and len(client.list_jobs(project_name=project_name, job_search_mode='CUBING_ONLY')) == 0:
        client.full_build_cube(cube_name=cube_name)
    assert client.await_all_jobs(project_name=project_name, waiting_time=50)
    time.sleep(10)
