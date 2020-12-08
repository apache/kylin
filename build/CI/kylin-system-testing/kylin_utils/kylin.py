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

import json
import logging
import time
import random

import requests

from .basic import BasicHttpClient


class KylinHttpClient(BasicHttpClient):  # pylint: disable=too-many-public-methods
    _base_url = 'http://{host}:{port}/kylin/api'

    def __init__(self, host, port, version):
        super().__init__(host, port)

        self._headers = {
            'Content-Type': 'application/json;charset=utf-8'
        }

        self._base_url = self._base_url.format(host=self._host, port=self._port)
        self.generic_project = "generic_test_project"
        self.pushdown_project = "pushdown_test_project"
        self.version = version

    def login(self, username, password):
        self._inner_session.request('POST', self._base_url + '/user/authentication', auth=(username, password))
        return self._request('GET', '/user/authentication', inner_session=True)

    def check_login_state(self):
        return self._request('GET', '/user/authentication', inner_session=True)

    def get_session(self):
        return self._inner_session

    def logout(self):
        self._inner_session = requests.Session()

    def list_projects(self, limit=100, offset=0):
        params = {'limit': limit, 'offset': offset}
        resp = self._request('GET', '/projects', params=params)
        return resp

    def create_project(self, project_name, description=None, override_kylin_properties=None):
        data = {'name': project_name,
                'description': description,
                'override_kylin_properties': override_kylin_properties,
                }
        payload = {
            'projectDescData': json.dumps(data),
        }
        resp = self._request('POST', '/projects', json=payload)
        return resp

    def update_project(self, project_name, description=None, override_kylin_properties=None):
        """
        :param project_name: project name
        :param description: description of project
        :param override_kylin_properties: the kylin properties that needs to be override
        :return:
        """
        data = {'name': project_name,
                'description': description,
                'override_kylin_properties': override_kylin_properties,
                }
        payload = {
            'formerProjectName': project_name,
            'projectDescData': json.dumps(data),
        }
        resp = self._request('PUT', '/projects', json=payload)
        return resp

    def delete_project(self, project_name, force=False):
        """
        delete project API, before delete the project, make sure the project does not contain models and cubes.
        If you want to force delete the project, make force=True
        :param project_name: project name
        :param force: if force, delete cubes and models before delete project
        :return:
        """
        if force:
            cubes = self.list_cubes(project_name)
            logging.debug("Cubes to be deleted: %s", cubes)
            while cubes:
                for cube in cubes:
                    self.delete_cube(cube['name'])
                cubes = self.list_cubes(project_name)
            models = self.list_model_desc(project_name)
            logging.debug("Models to be deleted: %s", models)
            while models:
                for model in models:
                    self.delete_model(model['name'])
                models = self.list_model_desc(project_name)
        url = '/projects/{project}'.format(project=project_name)
        resp = self._request('DELETE', url)
        return resp

    def load_table(self, project_name, tables, calculate=False):
        """
        load or reload table api
        :param calculate: Default is True
        :param project_name: project name
        :param tables: table list, for instance, ['default.kylin_fact', 'default.kylin_sales']
        :return:
        """
        # workaround of #15337
        # time.sleep(random.randint(5, 10))
        url = '/tables/{tables}/{project}/'.format(tables=tables, project=project_name)
        payload = {'calculate': calculate
                   }
        resp = self._request('POST', url, json=payload)
        return resp

    def unload_table(self, project_name, tables):
        url = '/tables/{tables}/{project}'.format(tables=tables, project=project_name)
        resp = self._request('DELETE', url)
        return resp

    def list_hive_tables(self, project_name, extension=False, user_session=False):
        """
        :param project_name: project name
        :param extension: specify whether the table's extension information is returned
        :param user_session: boolean, true for using login session to execute
        :return:
        """
        url = '/tables'
        params = {'project': project_name, 'ext': extension}
        resp = self._request('GET', url, params=params, inner_session=user_session)
        return resp

    def get_table_info(self, project_name, table_name):
        """
        :param project_name: project name
        :param table_name: table name
        :return: hive table information
        """
        url = '/tables/{project}/{table}'.format(project=project_name, table=table_name)
        resp = self._request('GET', url)
        return resp

    def get_tables_info(self, project_name, ext='true'):
        url = '/tables'
        params = {'project': project_name, 'ext': ext}
        resp = self._request('GET', url, params=params)
        return resp

    def get_table_streaming_config(self, project_name, table_name, limit=100, offset=0):
        params = {'table': table_name, 'project': project_name, 'limit': limit, 'offset': offset}
        resp = self._request('GET', '/streaming/getConfig', params=params)
        return resp

    def load_kafka_table(self, project_name, kafka_config, streaming_config, table_data, message=None):
        url = '/streaming'
        payload = {'project': project_name,
                   'kafkaConfig': json.dumps(kafka_config),
                   'streamingConfig': json.dumps(streaming_config),
                   'tableData': json.dumps(table_data),
                   'message': message}
        resp = self._request('POST', url, json=payload)
        return resp

    def update_kafka_table(self, project_name, kafka_config, streaming_config, table_data, cluster_index=0):
        url = '/streaming'
        payload = {'project': project_name,
                   'kafkaConfig': kafka_config,
                   'streamingConfig': streaming_config,
                   'tableData': table_data,
                   'clusterIndex': cluster_index}
        resp = self._request('PUT', url, json=payload)
        return resp

    def list_model_desc(self, project_name=None, model_name=None, limit=100, offset=0):

        """
        :param offset:
        :param limit:
        :param project_name: project name
        :param model_name: model name
        :return: model desc list
        """
        params = {'limit': limit,
                  'offset': offset,
                  'modelName': model_name,
                  'projectName': project_name
                  }
        resp = self._request('GET', '/models', params=params)
        return resp

    def create_model(self, project_name, model_name, model_desc_data, user_session=False):
        url = '/models'
        payload = {
            'project': project_name,
            'model': model_name,
            'modelDescData': json.dumps(model_desc_data)
        }
        logging.debug("Current payload for creating model is %s", payload)
        resp = self._request('POST', url, json=payload, inner_session=user_session)
        return resp

    def update_model(self, project_name, model_name, model_desc_data, user_session=False):
        url = '/models'
        payload = {
            'project': project_name,
            'model': model_name,
            'modelDescData': json.dumps(model_desc_data)
        }
        resp = self._request('PUT', url, json=payload, inner_session=user_session)
        return resp

    def clone_model(self, project_name, model_name, new_model_name):
        url = '/models/{model}/clone'.format(model=model_name)
        payload = {'modelName': new_model_name, 'project': project_name}
        resp = self._request('PUT', url, json=payload)
        return resp

    def delete_model(self, model_name):
        url = '/models/{model}'.format(model=model_name)
        # return value is None here
        return self._request('DELETE', url)

    def get_cube_desc(self, cube_name):
        url = '/cube_desc/{cube}'.format(cube=cube_name)
        resp = self._request('GET', url)
        return resp

    def list_cubes(self, project=None, offset=0, limit=10000, cube_name=None, model_name=None, user_session=False):
        params = {'projectName': project, 'offset': offset, 'limit': limit,
                  'cubeName': cube_name, 'modelName': model_name}
        resp = self._request('GET', '/cubes/', params=params, inner_session=user_session)
        return resp

    def get_cube_instance(self, cube_name):
        url = '/cubes/{cube}'.format(cube=cube_name)
        resp = self._request('GET', url)
        return resp

    def create_cube(self, project_name, cube_name, cube_desc_data, user_session=False):
        # workaround of #15337
        time.sleep(random.randint(5, 10))
        url = '/cubes'
        payload = {
            'project': project_name,
            'cubeName': cube_name,
            'cubeDescData': json.dumps(cube_desc_data)
        }
        resp = self._request('POST', url, json=payload, inner_session=user_session)
        return resp

    def update_cube(self, project_name, cube_name, cube_desc_data, user_session=False):
        # workaround of #15337
        time.sleep(random.randint(5, 10))
        url = '/cubes'
        payload = {
            'project': project_name,
            'cubeName': cube_name,
            'cubeDescData': json.dumps(cube_desc_data)
        }
        resp = self._request('PUT', url, json=payload, inner_session=user_session)
        return resp

    def update_cube_engine(self, cube_name, engine_type):
        url = '/cubes/{cube}/engine/{engine}'.format(cube=cube_name, engine=engine_type)
        resp = self._request('PUT', url)
        return resp

    def build_segment(self, cube_name, start_time, end_time, force=False):
        """
        :param cube_name: the name of the cube to be built
        :param force: force submit mode
        :param start_time: long, start time, corresponding to the timestamp in GMT format,
        for instance, 1388534400000 corresponding to 2014-01-01 00:00:00
        :param end_time: long, end time, corresponding to the timestamp in GMT format
        :return:
        """
        url = '/cubes/{cube}/build'.format(cube=cube_name)
        payload = {
            'buildType': 'BUILD',
            'startTime': start_time,
            'endTime': end_time,
            'force': force
        }
        resp = self._request('PUT', url, json=payload)
        return resp

    def full_build_cube(self, cube_name, force=False):
        """
        :param cube_name: the name of the cube to be built
        :param force: force submit mode
        :return:
        """
        return self.build_segment(cube_name, force=force, start_time=0, end_time=31556995200000)

    def merge_segment(self, cube_name, start_time=0, end_time=31556995200000, force=True):
        """
        :param cube_name: the name of the cube to be built
        :param force: force submit mode
        :param start_time: long, start time, corresponding to the timestamp in GMT format,
        for instance, 1388534400000 corresponding to 2014-01-01 00:00:00
        :param end_time: long, end time, corresponding to the timestamp in GMT format
        :return:
        """
        url = '/cubes/{cube}/build'.format(cube=cube_name)
        payload = {
            'buildType': 'MERGE',
            'startTime': start_time,
            'endTime': end_time,
            'force': force
        }
        resp = self._request('PUT', url, json=payload)
        return resp

    def refresh_segment(self, cube_name, start_time, end_time, force=True):
        """
        :param cube_name: the name of the cube to be built
        :param force: force submit mode
        :param start_time: long, start time, corresponding to the timestamp in GMT format,
        for instance, 1388534400000 corresponding to 2014-01-01 00:00:00
        :param end_time: long, end time, corresponding to the timestamp in GMT format
        :return:
        """
        url = '/cubes/{cube}/build'.format(cube=cube_name)
        payload = {
            'buildType': 'REFRESH',
            'startTime': start_time,
            'endTime': end_time,
            'force': force
        }
        resp = self._request('PUT', url, json=payload)
        return resp

    def delete_segments(self, cube_name, segment_name):
        url = '/cubes/{cube}/segs/{segment}'.format(cube=cube_name, segment=segment_name)
        resp = self._request('DELETE', url)
        return resp

    def build_streaming_cube(self, project_name, cube_name, source_offset_start=0,
                             source_offset_end='9223372036854775807'):
        """
        :param cube_name: cube name
        :param source_offset_start: long, the start offset where build begins. Here 0 means it is from the last position
        :param source_offset_end: long, the end offset where build ends. 9223372036854775807 (Long.MAX_VALUE) means to
                                        the end position on Kafka topic.
        :param mp_values: string, multiple partition values of corresponding model
        :param force: boolean, force submit mode
        :return:
        """
        url = '/cubes/{cube}/segments/build_streaming'.format(cube=cube_name)
        payload = {
            'buildType': 'BUILD',
            'project': project_name,
            'sourceOffsetStart': source_offset_start,
            'sourceOffsetEnd': source_offset_end,
        }
        resp = self._request('PUT', url, json=payload)
        return resp

    def build_cube_customized(self, cube_name, source_offset_start, source_offset_end=None, mp_values=None,
                              force=False):
        """
        :param cube_name: cube name
        :param source_offset_start: long, the start offset where build begins
        :param source_offset_end: long, the end offset where build ends
        :param mp_values: string, multiple partition values of corresponding model
        :param force: boolean, force submit mode
        :return:
        """
        url = '/cubes/{cube}/segments/build_customized'.format(cube=cube_name)
        payload = {
            'buildType': 'BUILD',
            'sourceOffsetStart': source_offset_start,
            'sourceOffsetEnd': source_offset_end,
            'mpValues': mp_values,
            'force': force
        }
        resp = self._request('PUT', url, json=payload)
        return resp

    def clone_cube(self, project_name, cube_name, new_cube_name):
        """
        :param project_name: project name
        :param cube_name: cube name of being cloned
        :param new_cube_name: cube name to be cloned to
        :return:
        """
        url = '/cubes/{cube}/clone'.format(cube=cube_name)
        payload = {
            'cubeName': new_cube_name,
            'project': project_name
        }
        resp = self._request('PUT', url, json=payload)
        return resp

    def enable_cube(self, cube_name):
        url = '/cubes/{cube}/enable'.format(cube=cube_name)
        resp = self._request('PUT', url)
        return resp

    def disable_cube(self, cube_name):
        url = '/cubes/{cube}/disable'.format(cube=cube_name)
        resp = self._request('PUT', url)
        return resp

    def purge_cube(self, cube_name):
        url = '/cubes/{cube}/purge'.format(cube=cube_name)
        resp = self._request('PUT', url)
        return resp

    def delete_cube(self, cube_name):
        url = '/cubes/{cube}'.format(cube=cube_name)
        return self._request('DELETE', url)

    def list_holes(self, cube_name):
        """
        A healthy cube in production should not have holes in the meaning of inconsecutive segments.
        :param cube_name: cube name
        :return:
        """
        url = '/cubes/{cube}/holes'.format(cube=cube_name)
        resp = self._request('GET', url)
        return resp

    def fill_holes(self, cube_name):
        """
        For non-streaming data based Cube, Kyligence Enterprise will submit normal build cube job(s) with
        corresponding time partition value range(s); For streaming data based Cube, please make sure that
        corresponding data is not expired or deleted in source before filling holes, otherwise the build job will fail.
        :param cube_name: string, cube name
        :return:
        """
        url = '/cubes/{cube}/holes'.format(cube=cube_name)
        resp = self._request('PUT', url)
        return resp

    def export_cuboids(self, cube_name):
        url = '/cubes/{cube}/cuboids/export'.fomat(cube=cube_name)
        resp = self._request('PUT', url)
        return resp

    def refresh_lookup(self, cube_name, lookup_table):
        """
        Only lookup tables of SCD Type 1 are supported to refresh.
        :param cube_name: cube name
        :param lookup_table: the name of lookup table to be refreshed with the format DATABASE.TABLE
        :return:
        """
        url = '/cubes/{cube}/refresh_lookup'.format(cube=cube_name)
        payload = {
            'cubeName': cube_name,
            'lookupTableName': lookup_table
        }
        resp = self._request('PUT', url, json=payload)
        return resp

    def get_job_info(self, job_id):
        url = '/jobs/{job_id}'.format(job_id=job_id)
        resp = self._request('GET', url)
        return resp

    def get_job_status(self, job_id):
        return self.get_job_info(job_id)['job_status']

    def get_step_output(self, job_id, step_id):
        url = '/jobs/{jobId}/steps/{stepId}/output'.format(jobId=job_id, stepId=step_id)
        resp = self._request('GET', url)
        return resp

    def pause_job(self, job_id):
        url = '/jobs/{jobId}/pause'.format(jobId=job_id)
        resp = self._request('PUT', url)
        return resp

    def resume_job(self, job_id):
        url = '/jobs/{jobId}/resume'.format(jobId=job_id)
        resp = self._request('PUT', url)
        return resp

    def discard_job(self, job_id):
        url = '/jobs/{jobId}/cancel'.format(jobId=job_id)
        resp = self._request('PUT', url)
        return resp

    def delete_job(self, job_id):
        url = '/jobs/{jobId}/drop'.format(jobId=job_id)
        resp = self._request('DELETE', url)
        return resp

    def resubmit_job(self, job_id):
        url = '/jobs/{jobId}/resubmit'.format(jobId=job_id)
        resp = self._request('PUT', url)
        return resp

    def list_jobs(self, project_name, status=None, offset=0, limit=10000, time_filter=1, job_search_mode='ALL'):
        """
        list jobs in specific project
        :param job_search_mode: CUBING_ONLY, CHECKPOINT_ONLY, ALL
        :param project_name: project name
        :param status: int, 0 -> NEW, 1 -> PENDING, 2 -> RUNNING,
                            4 -> FINISHED, 8 -> ERROR, 16 -> DISCARDED, 32 -> STOPPED
        :param offset: offset of returned result
        :param limit: quantity of returned result per page
        :param time_filter: int, 0 -> last one day, 1 -> last one week,
                                 2 -> last one month, 3 -> last one year, 4 -> all
        :return:
        """
        url = '/jobs'
        params = {
            'projectName': project_name,
            'status': status,
            'offset': offset,
            'limit': limit,
            'timeFilter': time_filter,
            'jobSearchMode': job_search_mode
        }
        resp = self._request('GET', url, params=params)
        return resp

    def await_all_jobs(self, project_name, waiting_time=30):
        """
        await all jobs to be finished, default timeout is 30 minutes
        :param project_name: project name
        :param waiting_time: timeout, in minutes
        :return: boolean, timeout will return false
        """
        running_flag = ['PENDING', 'RUNNING']
        try_time = 0
        max_try_time = waiting_time * 2
        while try_time < max_try_time:
            jobs = self.list_jobs(project_name)
            all_finished = True
            for job in jobs:
                if job['job_status'] in running_flag:
                    all_finished = False
                    break
                if job['job_status'] == 'ERROR':
                    return False
            if all_finished:
                return True
            time.sleep(30)
            try_time += 1
        return False

    def await_job(self, job_id, waiting_time=20, interval=1, excepted_status=None):
        """
        Await specific job to be given status, default timeout is 20 minutes.
        :param job_id: job id
        :param waiting_time: timeout, in minutes.
        :param interval: check interval, default value is 1 second
        :param excepted_status: excepted job status list, default contains 'ERROR', 'FINISHED' and 'DISCARDED'
        :return: boolean, if the job is in finish status, return true
        """
        finish_flags = ['ERROR', 'FINISHED', 'DISCARDED']
        if excepted_status is None:
            excepted_status = finish_flags
        timeout = waiting_time * 60
        start = time.time()
        while time.time() - start < timeout:
            job_status = self.get_job_status(job_id)
            if job_status in excepted_status:
                return True
            if job_status in finish_flags:
                return False
            time.sleep(interval)
        return False

    def await_job_finished(self, job_id, waiting_time=20, interval=1):
        """
        Await specific job to be finished, default timeout is 20 minutes.
        :param job_id: job id
        :param waiting_time: timeout, in minutes.
        :param interval: check interval, default value is 1 second
        :return: boolean, if the job is in finish status, return true
        """
        return self.await_job(job_id, waiting_time, interval, excepted_status=['FINISHED'])

    def await_job_error(self, job_id, waiting_time=20, interval=1):
        """
        Await specific job to be error, default timeout is 20 minutes.
        :param job_id: job id
        :param waiting_time: timeout, in minutes.
        :param interval: check interval, default value is 1 second
        :return: boolean, if the job is in finish status, return true
        """
        return self.await_job(job_id, waiting_time, interval, excepted_status=['ERROR'])

    def await_job_discarded(self, job_id, waiting_time=20, interval=1):
        """
        Await specific job to be discarded, default timeout is 20 minutes.
        :param job_id: job id
        :param waiting_time: timeout, in minutes.
        :param interval: check interval, default value is 1 second
        :return: boolean, if the job is in finish status, return true
        """
        return self.await_job(job_id, waiting_time, interval, excepted_status=['DISCARDED'])

    def await_job_step(self, job_id, step, excepted_status=None, waiting_time=20, interval=1):
        """
        Await specific job step to be given status, default timeout is 20 minutes.
        :param job_id: job id
        :param step: job step
        :param waiting_time: timeout, in minutes.
        :param interval: check interval, default value is 1 second
        :param excepted_status: excepted job status list, default contains 'ERROR', 'FINISHED' and 'DISCARDED'
        :return: boolean, if the job is in finish status, return true
        """
        finish_flags = ['ERROR', 'FINISHED', 'DISCARDED']
        if excepted_status is None:
            excepted_status = finish_flags
        timeout = waiting_time * 60
        start = time.time()
        while time.time() - start < timeout:
            job_info = self.get_job_info(job_id)
            job_status = job_info['steps'][step]['step_status']
            if job_status in excepted_status:
                return True
            if job_status in finish_flags:
                return False
            time.sleep(interval)
        return False

    def execute_query(self, project_name, sql, cube_name=None, offset=None, limit=None, backdoortoggles=None,
                      user_session=False,
                      timeout=60):
        url = '/query'
        payload = {
            'project': project_name,
            'sql': sql,
            'offset': offset,
            'limit': limit
        }
        if cube_name:
            backdoortoggles = {"backdoorToggles": {"DEBUG_TOGGLE_HIT_CUBE": cube_name}}
        if backdoortoggles:
            payload.update(backdoortoggles)
        resp = self._request('POST', url, json=payload, inner_session=user_session, timeout=timeout)
        return resp

    def save_query(self, sql_name, project_name, sql, description=None):
        url = '/saved_queries'
        payload = {
            'name': sql_name,
            'project': project_name,
            'sql': sql,
            'description': description
        }
        self._request('POST', url, json=payload)

    def get_queries(self, project_name, user_session=False):
        url = '/saved_queries'
        params = {
            'project': project_name
        }
        response = self._request('GET', url, params=params, inner_session=user_session)
        return response

    def remove_query(self, sql_id):
        url = '/saved_queries/{id}'.format(id=sql_id)
        self._request('DELETE', url)

    def list_queryable_tables(self, project_name):
        url = '/tables_and_columns'
        params = {'project': project_name}
        resp = self._request('GET', url, params=params)
        return resp

    def get_all_system_prop(self, server=None):
        url = '/admin/config'
        if server is not None:
            url = '/admin/config?server={serverName}'.format(serverName=server)
        prop_resp = self._request('GET', url).get('config')
        property_values = {}
        if prop_resp is None:
            return property_values
        prop_lines = prop_resp.splitlines(False)
        for prop_line in prop_lines:
            splits = prop_line.split('=')
            property_values[splits[0]] = splits[1]
        return property_values

    def create_user(self, user_name, password, authorities, disabled=False, user_session=False):
        """
        create a user
        :param user_name: string, target user name
        :param password: string, target password
        :param authorities: array, user's authorities
        :param disabled: boolean, true for disabled user false for enable user
        :param user_session: boolean, true for using login session to execute
        :return:
        """
        url = '/user/{username}'.format(username=user_name)
        payload = {
            'username': user_name,
            'password': password,
            'authorities': authorities,
            'disabled': disabled,
        }
        resp = self._request('POST', url, json=payload, inner_session=user_session)
        return resp

    def delete_user(self, user_name, user_session=False):
        """
        delete user
        :param user_name: string
        :param user_session: boolean, true for using login session to execute
        :return:
        """
        url = '/user/{username}'.format(username=user_name)
        resp = self._request('DELETE', url, inner_session=user_session)
        return resp

    def update_user(self, user_name, authorities, password=None, disabled=False,
                    user_session=False, payload_user_name=None):
        """
        update user's info
        :param user_name: string, target user name
        :param password: string, target password
        :param authorities: array, user's authorities
        :param disabled: boolean, true for disabled user false for enable user
        :param user_session: boolean, true for using login session to execute
        :param payload_user_name: string, true for using login session to execute
        :return:
        """
        url = '/user/{username}'.format(username=user_name)
        username_in_payload = user_name if payload_user_name is None else payload_user_name
        payload = {
            'username': username_in_payload,
            'password': password,
            'authorities': authorities,
            'disabled': disabled,
        }
        resp = self._request('PUT', url, json=payload, inner_session=user_session)
        return resp

    def update_user_password(self, user_name, new_password, password=None, user_session=False):
        """
        update user's password
        :param user_name: string, target for username
        :param new_password: string, user's new password
        :param password: string, user's old password
        :param user_session: boolean, true for using login session to execute
        :return:
        """
        url = '/user/password'
        payload = {
            'username': user_name,
            'password': password,
            'newPassword': new_password
        }
        resp = self._request('PUT', url, json=payload, inner_session=user_session)
        return resp

    def list_users(self, project_name=None, group_name=None, is_fuzz_match=False, name=None, offset=0, limit=10000
                   , user_session=False):
        """
        list users
        :param group_name:string, group name
        :param project_name: string, project's name
        :param offset: offset of returned result
        :param limit: quantity of returned result per page
        :param is_fuzz_match: bool, true for param name fuzzy match
        :param name: string, user's name
        :param user_session: boolean, true for using login session to execute
        :return:
        """
        url = '/user/users'
        params = {
            'offset': offset,
            'limit': limit,
            'groupName': group_name,
            'project': project_name,
            'isFuzzMatch': is_fuzz_match,
            'name': name
        }
        resp = self._request('GET', url, params=params, inner_session=user_session)
        return resp

    def list_user_authorities(self, project_name, user_session=False):
        """
        list groups in a project
        :param project_name: string, target project name
        :param user_session: boolean, true for using login session to execute
        :return:
        """
        url = '/user_group/groups'
        params = {
            'project': project_name
        }
        resp = self._request('GET', url, params=params, inner_session=user_session)
        return resp

    def create_group(self, group_name, user_session=False):
        """
        create a group with group_name
        :param group_name:  string, target group name
        :param user_session: boolean, true for using login session to execute
        :return:
        """
        url = '/user_group/{group_name}'.format(group_name=group_name)
        resp = self._request('POST', url, inner_session=user_session)
        return resp

    def delete_group(self, group_name, user_session=False):
        """
        delete group by group_name
        :param group_name: string, target group name
        :param user_session: boolean, true for using login session to execute
        :return:
        """
        url = '/user_group/{group_name}'.format(group_name=group_name)
        resp = self._request('DELETE', url, inner_session=user_session)
        return resp

    def add_or_del_users(self, group_name, users):
        url = '/user_group/users/{group}'.format(group=group_name)
        payload = {'users': users}
        resp = self._request('POST', url, json=payload)
        return resp

    def _request(self, method, url, **kwargs):  # pylint: disable=arguments-differ
        return super()._request(method, self._base_url + url, **kwargs)


def connect(**conf):
    _host = conf.get('host')
    _port = conf.get('port')
    _version = conf.get('version')

    return KylinHttpClient(_host, _port, _version)
