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

from selenium import webdriver
from yaml import load, loader
import os
from kylin_utils import shell
from kylin_utils import kylin


def setup_instance(file_name):
    instances_file = os.path.join('kylin_instances/', file_name)
    stream = open(instances_file, 'r')
    for item in load(stream, Loader=loader.SafeLoader):
        host = item['host']
        port = item['port']
        version = item['version']
    return kylin.connect(host=host, port=port, version=version)


def kylin_url(file_name):
    instances_file = os.path.join('kylin_instances/', file_name)
    stream = open(instances_file, 'r')
    for item in load(stream, Loader=loader.SafeLoader):
        host = item['host']
        port = item['port']
    return "http://{host}:{port}/kylin".format(host=host, port=port)


def setup_browser(browser_type):
    if browser_type == "chrome":
        browser = webdriver.Chrome(executable_path="browser_driver/chromedriver")

    if browser_type == "firefox":
        browser = webdriver.Firefox(executable_path="browser_driver/geckodriver")

    if browser_type == "safari":
        browser = webdriver.Safari(executable_path="browser_driver/safaridriver")

    return browser


def if_project_exists(kylin_client, project):
    exists = 0
    resp = kylin_client.list_projects()
    for project_info in resp:
        if project_info.get('name') == project:
            exists = 1
    return exists


def if_cube_exists(kylin_client, cube_name, project=None):
    exists = 0
    resp = kylin_client.list_cubes(project=project)
    if resp is not None:
        for cube_info in resp:
            if cube_info.get('name') == cube_name:
                exists = 1
    return exists


def if_model_exists(kylin_client, model_name, project):
    exists = 0
    resp = kylin_client.list_model_desc(project_name=project, model_name=model_name)
    if len(resp) == 1:
        exists = 1
    return exists

def ssh_shell(config_file='kylin_host.yml'):
    instances_file = os.path.join('kylin_instances/', config_file)
    stream = open(instances_file, 'r')
    for item in load(stream, Loader=loader.SafeLoader):
        host = item['host']
        username = item['username']
        password = item['password']
    return shell.SSHShell(host=host, username=username, password=password)
