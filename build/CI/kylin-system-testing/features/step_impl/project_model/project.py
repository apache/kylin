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

global client
client = util.setup_instance('kylin_instance.yml')

@step("create a project <project_name> with description <project_description> and check that duplicate name is not allowed")
def create_duplicate_project(project_name, project_description):
    client.create_project(project_name, description=project_description)
    with pytest.raises(Exception, match=r'The project named .* already exists'):
        client.create_project(project_name, description=project_description)


@step("check that <project_name> is on the list")
def check_project(project_name):

    assert util.if_project_exists(kylin_client=client, project=project_name) == 1


@step("update the project <project_name> and edit the description to be <project_description>")
def update_project_description(project_name, project_description):
    update = client.update_project(project_name, description=project_description)
    resp = client.list_projects()
    update = False
    for i in range(len(resp)):
        update = update | ((resp[i]['name'] == project_name) & (resp[i]['description'] == project_description))
    assert update is True


@step("delete project <project_name> and check that it's not on the list")
def check_delete_project(project_name):
    delete = client.delete_project(project_name)
    resp = client.list_projects()
    exist = False
    for i in range(len(resp)):
        exist = exist | (resp[i]['name'] == project_name)
    assert exist is False






