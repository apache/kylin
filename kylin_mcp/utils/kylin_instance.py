# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import base64
import copy
import datetime
import functools
import logging
import os
import time
from venv import logger
import curlify
import requests
from requests import HTTPError

from requests.exceptions import HTTPError, ReadTimeout


class KylinHttpClient:
    _host = None
    _port = None

    _headers = {}

    _auth = ("ADMIN", "KYLIN")

    _inner_session = requests.Session()

    # pylint: disable=too-many-public-methods
    _base_url = "http://{host}:{port}/kylin/api"

    def __init__(self, host, port, username, password):
        if not host or not port:
            raise ValueError("init http client failed")

        self._host = host
        self._port = port
        if username is not None:
            self._auth = (username, self._auth[1])
        if password is not None:
            self._auth = (self._auth[0], password)

        self._headers = {
            "Accept": "application/vnd.apache.kylin-v4+json",
            "Accept-Language": "en",
            "Content-Type": "application/json;charset=utf-8",
        }
        self._public_headers = self._headers.copy()
        self._public_headers["Accept"] = "application/vnd.apache.kylin-v4-public+json"

        self._base_url = self._base_url.format(host=self._host, port=self._port)

        self.await_ke_running_health(300)

    def token(self, token):
        self._headers["Authorization"] = "Basic {token}".format(token=token)

    def auth(self, username, password):
        self._auth = (username, password)

    def header(self, name, value):
        self._headers[name] = value

    def headers(self, headers):
        self._headers = headers

    def _request(# pylint: disable=arguments-differ, method-hidden
        self,
        method,
        url,
        params=None,
        data=None,
        json=None,  # pylint: disable=too-many-arguments
        files=None,
        headers=None,
        stream=False,
        to_json=True,
        inner_session=False,
        timeout=120,
        origin_data=False,
    ):
        real_url = self._base_url + url
        # pylint: disable=arguments-differ, method-hidden
        if inner_session:
            return self._request_with_session(
                self._inner_session,
                method,
                real_url,
                params=params,
                data=data,
                json=json,
                files=files,
                headers=headers,
                stream=stream,
                to_json=to_json,
                timeout=timeout,
                origin_data=origin_data,
            )
        with requests.Session() as session:
            session.auth = self._auth
            return self._request_with_session(
                session,
                method,
                real_url,
                params=params,
                data=data,
                json=json,
                files=files,
                headers=headers,
                stream=stream,
                to_json=to_json,
                timeout=timeout,
                origin_data=origin_data,
            )

    def _request_with_session(
        self,
        session,
        method,
        url,
        params=None,
        data=None,  # pylint: disable=too-many-arguments
        json=None,
        files=None,
        headers=None,
        stream=False,
        to_json=True,
        timeout=120,
        origin_data=False,
    ):
        logger.info(f"Requesting {method} {self._base_url + url}")
        if headers is None:
            headers = self._headers

        try:
            resp = session.request(
                method,
                url,
                params=params,
                data=data,
                json=json,
                timeout=timeout,
                files=files,
                headers=headers,
                stream=stream,
            )
        except Exception as error:
            request = requests.Request(method, url, headers, data=data, json=json)
            logging.error(str(error))
            logging.error(curlify.to_curl(session.prepare_request(request)))
            raise error

        try:
            if stream:
                return resp.raw  # resp.raw: urllib3.response.HTTPResponse

            if not resp.content:
                return None

            if to_json:
                data = resp.json()
                resp.raise_for_status()
                if origin_data:
                    return data
                return data.get("data", data)

            return resp.text
        except requests.HTTPError as http_error:
            err_msg = (
                f"{str(http_error)} [return code: {data.get('code', '')}]-[{data.get('msg', '')}]\n"
                f"{data.get('stacktrace', '')}"
            )
            logging.error(err_msg)
            raise requests.HTTPError(
                err_msg,
                request=http_error.request,
                response=http_error.response,
            )
        except Exception as error:
            logging.error(str(error))
            raise error


    def raw_request(self, method, url, overwrite_header):
        """
        raw request
        :param method: http method
        :param url: http url
        :param overwrite_header: some custom header
        :return:
        """
        new_header = self._headers.copy()
        new_header.update(overwrite_header)
        resp = self._request(method, url, headers=new_header)
        return resp

    def login(self, username, password):
        login_resp = self._inner_session.request(
            "POST", self._base_url + "/user/authentication", auth=(username, password)
        )
        # return login_resp.json()["data"]
        time.sleep(2)
        login_resp_get = self._request(
            "GET", "/user/authentication", inner_session=True
        )
        return login_resp_get

    def logout(self):
        self._inner_session = requests.Session()
        return self._inner_session.request(
            "GET", self._base_url + "/j_spring_security_logout"
        )

    def authenticate(self, username, password):
        """
        login user with username and password
        :param username: string, user's name
        :param password: string, user's password
        :return:
        """
        resp = self._inner_session.request(
            "POST", self._base_url + "/user/authentication", auth=(username, password)
        )
        resp.raise_for_status()
        return resp.json()["data"]

    def check_ke_health(self):
        resp = {}
        import_headers = self._headers.copy()
        import_headers.pop("Accept")
        url = "/health"
        try:
            logger.info(f"Checking Kylin health at {self._base_url + url}")
            resp = self._request("GET", url=url, headers=import_headers)
        except HTTPError:
            resp["status"] = "DOWN"
        return resp

    def await_ke_running_health(self, timeout=90):
        start_time = time.time()
        while time.time() - start_time < timeout:
            resp = self.check_ke_health()
            try:
                assert resp["status"] == "UP"
            except AssertionError:
                time.sleep(2)
                continue
            return True
        return False

    def create_user(
        self,
        username,
        password,
        authorities,
        disabled=False,
        user_session=False,
        delete=True,
    ):
        if delete:
            users = self.list_users(username=username)
            exist_user = [
                user_item
                for user_item in users["value"]
                if user_item["username"] == username
            ]
            if exist_user:
                self.delete_user(exist_user[0]["uuid"])

        payload = {
            "username": username,
            "password": base64.b64encode(password.encode()).decode(),
            "authorities": authorities,
            "disabled": disabled,
        }
        resp = self._request("POST", "/user", json=payload, inner_session=user_session)
        return resp

    def delete_user(self, user_id):
        """
        delete user
        :param user_id: delete user id
        :return:
        """
        url = "/user/{username}".format(username=user_id)
        resp = self._request("DELETE", url)
        return resp

    def public_delete_user(self, username):
        """
        delete user
        :param username: delete user by username, case insensitive
        :return:
        """
        url = "/user/{username}".format(username=username)
        resp = self._request("DELETE", url, headers=self._public_headers)
        return resp

    def update_user_password(
        self, username, new_password, password, user_session=False
    ):
        """
        update user's password API
        :param username: string, target for username
        :param new_password: string, user's new password
        :param password: string, user's old password
        :param user_session: boolean, true for using login session to execute
        :return:
        """
        url = "/user/password"
        payload = {
            "username": username,
            "new_password": new_password,
            "password": password,
        }
        resp = self._request("PUT", url, json=payload, inner_session=user_session)
        return resp

    def update_user(
        self, username, uuid, authorities=None, disabled=False, user_session=False
    ):
        """
        update user's info
        :param username: string, user's name
        :param uuid: string, user's uuid
        :param authorities: list, user's authorities
        :param disabled: boolean, true for disable user
        :param user_session: boolean, true for using login session to execute
        :return:
        """
        payload = {
            "username": username,
            "uuid": uuid,
            "authorities": authorities,
            "disabled": disabled,
        }
        resp = self._request("PUT", "/user", json=payload, inner_session=user_session)
        return resp

    def list_users(
        self,
        username=None,
        project_name=None,
        is_case_sensitive=None,
        page_offset=0,
        page_size=10000,
        user_session=False,
    ):
        """
        user list
        :param username: string, filter username
        :param project_name: string, none for check session has ROLE_ADMIN,
        not none for check has ROLE_ADMIN or has project admin permission
        :param is_case_sensitive: boolean, true for name case sensitive, optional
        :param page_offset: int, page offset, optional
        :param page_size: int, page size, optional
        :param user_session: boolean, true for using login session toq execute
        :return: request session must has ROLE_ADMIN, if not, will raise access denied
        """
        params = {
            "name": username or "",
            "project": project_name,
            "is_case_sensitive": is_case_sensitive,
            "page_offset": page_offset,
            "page_size": page_size,
        }
        resp = self._request("GET", "/user", params=params, inner_session=user_session)
        return resp

    def add_user_group(self, group_name, user_session=False):
        """
        add user group
        :param group_name: string, group name
        :param user_session: boolean, true for using login session to execute
        :return:
        """
        url = "/user_group"
        payload = {"group_name": group_name}
        resp = self._request("POST", url, json=payload, inner_session=user_session)
        return resp

    def delete_user_group(self, uuid, user_session=False):
        """
        delete user group
        :param uuid: string, uuid of group
        :param user_session: boolean, true for using login session to execute
        :return:
        """
        url = "/user_group/{uuid}".format(uuid=uuid)
        resp = self._request("DELETE", url, inner_session=user_session)
        return resp

    def get_users_with_group(
        self, user_group_name=None, page_offset=0, page_size=10000, user_session=False
    ):
        """
        get users with group
        :param user_group_name: string, project name, optional
        :param page_offset: int,  offset of returned result, optional
        :param page_size: int , quantity of returned result per page, optional
        :param user_session: boolean, true for using login session to execute
        :return: list of all users with group
        """
        url = "/user_group/users_with_group"
        params = {
            "user_group_name": user_group_name or "",
            "page_offset": page_offset,
            "page_size": page_size,
            "user_session": user_session,
        }
        resp = self._request("GET", url, params=params, inner_session=user_session)
        return resp

    def get_group_members(self, uuid, name=None, page_offset=0, page_size=10000):
        """
        get all members of the group
        :param uuid: string, uuid of group
        :param name: string, user name
        :param page_offset: int, offset of returned result, optional
        :param page_size: int, quantity of returned result per page, optional
        :return:
        """
        url = "/user_group/group_members/{uuid}".format(uuid=uuid)
        params = {
            "name": name or "",
            "page_offset": page_offset,
            "page_size": page_size,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def get_groups(self, project_name):
        """
        get all groups
        :param project_name: string, project name
        :return:
        """
        url = "/user_group/groups"
        params = {"project": project_name}
        resp = self._request("GET", url, params=params)
        return resp

    def update_group_users(self, group_name, users):
        """
        kylin branch is 4.1.x or newten, update users in the group
        :param group_name: string, group name
        :param users: list, users name list
        :return:
        """
        url = "/user_group/users"
        payload = {"group_name": group_name, "users": users}
        resp = self._request("PUT", url, json=payload)
        return resp

    def get_users_and_groups(self):
        """
        get all users and group
        :return:
        """
        url = "/user_group/users_and_groups"
        resp = self._request("GET", url)
        return resp

    def get_project(
        self,
        project_name=None,
        page_offset=0,
        page_size=100000,
        exact=False,
        permission=None,
        user_session=False,
    ):
        """
        get project API
        :param project_name: project name, optional
        :param page_offset: offset of returned result
        :param page_size: quantity of returned result per page
        :param exact: exact match or not
        :param permission: string, ADMINISTRATION MANAGEMENT OPERATION READ
        :param user_session: boolean, true for using login session to execute
        :return: list of all projects if not specify project name, project info if specify the project name
        """
        url = "/projects"
        params = {
            "project": project_name or "",
            "page_offset": page_offset,
            "page_size": page_size,
            "exact": exact,
            "permission": permission or "ADMINISTRATION",
        }
        resp = self._request("GET", url, params=params, inner_session=user_session)
        return resp["value"]

    def create_project(
        self,
        project_name,
        description=None,
        override_kylin_properties=None,
        maintain_model_type="MANUAL_MAINTAIN",
    ):
        """
        create project API
        :param project_name: name of the project to be created
        :param description: project description
        :param override_kylin_properties: kylin properties to be override in the project
        :param maintain_model_type: MANUAL_MAINTAIN for expert model or AUTO_MAINTAIN for smart mode
        :return:
        """
        payload = {
            "name": project_name,
            "description": description or "",
            "override_kylin_properties": override_kylin_properties or {},
            "maintain_model_type": maintain_model_type,
        }
        resp = self._request("POST", "/projects", json=payload)
        return resp

    def create_smart_project(
        self, project_name, description=None, override_kylin_properties=None
    ):
        return self.create_project(
            project_name=project_name,
            description=description,
            override_kylin_properties=override_kylin_properties,
            maintain_model_type="AUTO_MAINTAIN",
        )

    def delete_project(self, project_name, timeout=60):
        """
        delete project API
        :param timeout:
        :param project_name: project name
        :return:
        """
        url = "/projects/{project}".format(project=project_name)
        resp = self._request("DELETE", url, timeout=timeout)
        return resp

    def set_source_type(self, project_name, source_type=9):
        """
        set source type
        :param project_name: string, project name
        :param source_type:, int, datasource_type is set to 9 by default, which means Hive
        :return:
        """
        url = "/projects/{project}/source_type".format(project=project_name)
        payload = {"source_type": source_type}
        resp = self._request("PUT", url, json=payload)
        return resp

    def set_default_database(self, project_name, default_db):
        url = "/projects/{project}/default_database".format(project=project_name)
        payload = {"default_database": default_db}
        resp = self._request("PUT", url, json=payload)
        return resp

    def general_setting(self, project_name, description=None, semi_automatic_mode=True):
        """
        project general info
        :param project_name: project name
        :param description: string
        :param semi_automatic_mode: boolean
        :return:
        """
        url = "/projects/{project}/project_general_info".format(project=project_name)
        payload = {
            "description": description or "",
            "semi_automatic_mode": semi_automatic_mode,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def delete_snapshot(self, project_name, table_snapshot_list):
        """
        :param project_name: project name
        :param table_snapshot_list: the table list
        """
        url = "/snapshots"
        params = {"project": project_name, "tables": table_snapshot_list}
        resp = self._request("DELETE", url, params=params)
        return resp

    def inefficient_storage(
        self, project_name, frequency_time_window, low_frequency_threshold
    ):
        """
        inefficient storage
        :param project_name: project name
        :param frequency_time_window: time,'DAY', 'WEEK', 'MONTH'
        :param low_frequency_threshold: long
        :return:boolean
        """
        url = "/projects/{project}/garbage_cleanup_config".format(project=project_name)
        payload = {
            "frequency_time_window": frequency_time_window,
            "low_frequency_threshold": low_frequency_threshold,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def segment_setting(
        self,
        project_name,
        time_type,
        auto_merge_enabled=False,
        retention_range_number=1,
        retention_range_type="YEAR",
        volatile_range_number=0,
        volatile_range_type="DAY",
        volatile_range_enabled=False,
        retention_range_enabled=False,
        create_empty_segment_enabled=False,
    ):
        """
        segment setting
        time_type: array contains at least one value
        The time unit of retention_range is equal to the maximum time unit in the auto_merge_time_ranges array,
        if "auto_merge_time_ranges":["DAY",MONTH"],then the time unit of retention_range is 'MONTH',
        if "auto_merge_time_ranges":["YEAR",WEEK"],then The time unit of retention_range is 'YEAR'
        """
        url = "/projects/{project}/segment_config".format(project=project_name)
        payload = {
            "auto_merge_enabled": auto_merge_enabled,
            "auto_merge_time_ranges": time_type,
            "retention_range": {
                "retention_range_number": retention_range_number,
                "retention_range_enabled": retention_range_enabled,
                "retention_range_type": retention_range_type,
            },
            "volatile_range": {
                "volatile_range_number": volatile_range_number,
                "volatile_range_enabled": volatile_range_enabled,
                "volatile_range_type": volatile_range_type,
            },
            "create_empty_segment_enabled": create_empty_segment_enabled,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def reset_project_setting(self, project_name, reset_item="job_notification_config"):
        """
        project reset setting
        :param reset_item: job_notification_config, segment_config
        :param project_name: project name
        :return:
        """
        url = "/projects/{project_name}/project_config".format(
            project_name=project_name
        )
        payload = {"reset_item": reset_item}
        resp = self._request("PUT", url, json=payload)
        return resp

    def update_multi_partition_config(self, project_name, multi_partition_enabled):
        """
        update multi partition config
        :param project_name: string, name of project
        :param multi_partition_enabled: bool, open or close model multi partition
        :return:
        """
        url = "/projects/{project_name}/multi_partition_config".format(
            project_name=project_name
        )
        payload = {
            "project": project_name,
            "multi_partition_enabled": multi_partition_enabled,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def get_project_setting(self, project_name):
        """
        get project setting
        :param project_name: project name
        :return:
        """
        url = "/projects/{project_name}/project_config".format(
            project_name=project_name
        )
        resp = self._request("GET", url)
        return resp

    def load_table(
        self,
        project_name,
        tables=None,
        need_sampling=False,
        databases=None,
        max_sample_count=20000000,
        datasource_type=9,
    ):
        """
        load table API
        :param project_name: string, project name
        :param tables: list, tables name, for instance, ['SSB_QUARD.lineorder', 'SSB_QUARD.part']
        :param need_sampling: boolean, enable table sampling or not
        :param databases: list, databases name, for instance ['SSB_QUARD', 'BI']
        :param max_sample_count: int, if need_sampling is set to true, default value is 20,000,000
        :param datasource_type: int, datasource_type is set to 9 by default, which means Hive
        :return:
        """
        # before load tables, we must set data source type
        # kymock.unique_key()

        self.set_source_type(project_name, datasource_type)
        url = "/tables"
        payload = {
            "project": project_name,
            "tables": tables or [],
            "databases": databases or [],
            "sampling_rows": max_sample_count,
            "need_sampling": need_sampling,
            "data_source_type": datasource_type,
        }
        resp = self._request("POST", url, json=payload, timeout=150)
        return resp

    def submit_sampling(
        self, project_name, qualified_table_name, max_sample_count=20000000
    ):
        """
        sampling job
        :param project_name: project name
        :param qualified_table_name: sampling table name
        :param max_sample_count: sampling max rows
        :return:
        """
        url = "/tables/sampling_jobs"
        payload = {
            "project": project_name,
            "qualified_table_name": qualified_table_name,
            "rows": max_sample_count,
        }
        time.sleep(60)
        resp = self._request("POST", url, json=payload, timeout=300)
        return resp

    def reload_table(
        self, project_name, table, need_sample=True, need_build=False, max_rows=20000000
    ):
        """
        reload table
        :param project_name: project name
        :param table: reload table_name
        :param need_build: boolean
        :param need_sample: boolean ,true for sample ,false for needn't sample
        :param max_rows: sampling max rows
        :return:
        """
        url = "/tables/reload"
        payload = {
            "project": project_name,
            "table": table,
            "need_build": need_build,
            "need_sample": need_sample,
            "max_rows": max_rows,
        }
        resp = self._request("POST", url, json=payload)
        return resp

    def unload_table(self, project_name, database, table, cascade=None):
        """
        unload table
        :param project_name: string, project name
        :param database: string, database name
        :param table: string, table name
        :param cascade: boolean, optional
        :return:
        """
        url = "/tables/{database}/{table}".format(
            database=database.upper(), table=table.upper()
        )
        params = {"project": project_name, "cascade": cascade}
        resp = self._request("DELETE", url, params=params)
        return resp

    def prepare_unload_table(self, project_name, database, table):
        """
        prepare unload table
        :param project_name: string, project name
        :param database: string, database name
        :param table: string, table name
        :return:
        """
        url = "/tables/{database}/{table}/prepare_unload".format(
            database=database, table=table
        )
        params = {"project": project_name}
        resp = self._request("GET", url, params=params)
        return resp

    def get_refresh_affected_date_range(
        self, project_name, table, start_time, end_time
    ):
        """
        get refresh affected date range
        :param project_name: string, project name
        :param table: string, table name
        :param start_time: string, start time
        :param end_time: string, end time
        :return:
        """
        url = "/tables/affected_data_range"
        params = {
            "project": project_name,
            "table": table,
            "start": start_time,
            "end": end_time,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def get_auto_merge_config(self, project_name, model_name=None, table_name=None):
        """
        get auto merge config
        :param project_name: string, project name
        :param model_name: string, model name, optional
        :param table_name: string, table name, optional
        :return:
        """
        url = "/tables/auto_merge_config"
        params = {"project": project_name, "model": model_name, "table": table_name}
        resp = self._request("GET", url, params=params)
        return resp

    def update_auto_merge_config(
        self,
        project_name,
        model_name,
        table_name,
        auto_merge_time_ranges,
        volatile_range_number,
        volatile_range_type,
        auto_merge_enabled=True,
        volatile_range_enabled=True,
    ):
        """
        update auto merge config
        :param project_name: string, project name
        :param model_name: string, model name
        :param table_name: string, table name
        :param auto_merge_time_ranges: list, auto merge time ranges
        :param volatile_range_number: int, volatile range number
        :param volatile_range_type: string, volatile range type
        :param auto_merge_enabled: boolean, auto merge enabled
        :param volatile_range_enabled: boolean, volatile range enabled
        :return:
        """
        url = "/tables/auto_merge_config"
        payload = {
            "project": project_name,
            "model": model_name,
            "table": table_name,
            "auto_merge_time_ranges": auto_merge_time_ranges,
            "volatile_range_number": volatile_range_number,
            "volatile_range_type": volatile_range_type,
            "volatile_range_enabled": volatile_range_enabled,
            "auto_merge_enabled": auto_merge_enabled,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def get_batch_load_tables(self, project_name):
        """
        get batch load tables
        :param project_name: string, project name
        :return:
        """
        url = "/tables/batch_load"
        params = {"project": project_name}
        resp = self._request("GET", url, params=params)
        return resp

    def batch_load_tables(self, project_name, table, start, end):
        """
        batch load tables
        :param project_name: string, project name
        :param table: string, table name
        :param start: string
        :param end: string
        :return:
        """
        url = "/tables/batch_load"
        payload = [
            {"end": end, "project": project_name, "start": start, "table": table}
        ]
        resp = self._request("POST", url, json=payload)
        return resp

    def refresh_segments(
        self,
        project_name,
        table_name,
        refresh_start,
        refresh_end,
        affected_start,
        affected_end,
    ):
        url = "/tables/data_range"
        payload = {
            "project": project_name,
            "table": table_name,
            "refresh_start": refresh_start,
            "refresh_end": refresh_end,
            "affected_start": affected_start,
            "affected_end": affected_end,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    # def get_latest_data(self, project_name, table_name):
    #     """
    #     get latest data
    #     :param project_name: string, project name
    #     :param table_name: string, table name
    #     :return:
    #     """
    #     url = '/tables/data_range/latest_data'
    #     params = {
    #         'project': project_name,
    #         'table': table_name
    #     }
    #     resp = self._request('GET', url, params=params)
    #     return resp

    def show_databases(self, project_name):
        """
        show databases
        :param project_name: string, project name
        :return:
        """
        url = "/tables/databases"
        params = {"project": project_name}
        resp = self._request("GET", url, params=params)
        return resp

    def get_loaded_databases(self, project_name):
        """
        get loaded databases
        :param project_name: string, project name
        :return:
        """
        url = "/tables/loaded_databases"
        params = {"project": project_name}
        resp = self._request("GET", url, params=params)
        return resp

    def show_tables(
        self,
        project_name,
        database_name,
        table_name=None,
        data_source_type=None,
        page_offset=0,
        page_size=10000,
    ):
        """
        show tables
        :param project_name: string
        :param database_name: string
        :param table_name: string, optional
        :param data_source_type: int, datasource_type is set to 9 by default, which means Hive, optional
        :param page_offset: int,offset of returned result
        :param page_size: int, quantity of returned result per page
        :param page_size: int, optional
        :return:
        """
        url = "/tables/names"
        params = {
            "project": project_name,
            "database": database_name,
            "table": table_name,
            "data_source_type": data_source_type,
            "page_size": page_size,
            "page_offset": page_offset,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def set_partition(self, project_name, model_id, partition=None, columns=None):
        """
        :param project_name: project name
        :param model_id:  model id
        :param partition: string, table column name eg:P_LINEORDER.LO_ORDERDATE
        :param columns: list, ["P_LINEORDER.LO_COMMITDATE"]
        :return:
        """
        url = "/models/{model_id}/partition".format(model_id=model_id)
        payload = {
            "project": project_name,
            "partition_desc": partition,
            "multi_partition_desc": columns and {"columns": columns},
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def set_multi_partition_values(self, project_name, model_id, values):
        """
        :param project_name: string, project name
        :param model_id: string, model id
        :param values: list, ex:[['1'], ['2']...]
        :return:
        """
        url = f"/models/{model_id}/multi_partition/sub_partition_values"
        payload = {
            "project": project_name,
            "model_id": model_id,
            "sub_partition_values": values,
        }
        resp = self._request("POST", url, json=payload)
        return resp

    def get_multi_partition_values(self, project_name, model_id):
        """
        :param project_name: string, project name
        :param model_id: string, model id
        :return:
        """
        url = f"/models/{model_id}/multi_partition/sub_partition_values"
        params = {"project": project_name, "model_id": model_id}
        resp = self._request("GET", url, params=params)
        return resp

    def delete_multi_partition_values(self, project_name, model_id, ids):
        """
        :param project_name: string, project name
        :param model_id: string, model id
        :param ids: list, [1,2,3...]
        :return:
        """
        url = f"/models/{model_id}/multi_partition/sub_partition_values"
        params = {"project": project_name, "model_id": model_id, "ids": ids}
        resp = self._request("DELETE", url, params=params)
        return resp

    def public_delete_segment_multi_partition_date(
        self, project_name, model_name, segment_id, sub_partition_values, to_json=True
    ):
        """
        :param project_name: string, project name
        :param model_name: string, model name
        :param segment_id: string, segment id
        :param sub_partition_values: string, eg:'1,2,3'
        :param to_json: bool
        :return:
        """
        url = "/models/segments/multi_partition"
        params = {
            "project": project_name,
            "model": model_name,
            "segment_id": segment_id,
            "sub_partition_values": sub_partition_values,
        }
        resp = self._request(
            "DELETE", url, headers=self._public_headers, params=params, to_json=to_json
        )
        return resp

    def get_partition_column_format(self, project_name, table_name, partition_column):
        """
        get partitioin column format
        :param project_name: string, project name
        :param table_name: string, table name
        :param partition_column: string, table column name
        :return:
        """
        url = "/tables/partition_column_format"
        params = {
            "project": project_name,
            "table": table_name,
            "partition_column": partition_column,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def prepare_reload_table(self, project_name, table_name, inner_session=False):
        """
        prepare reload table
        :param project_name: string, project name
        :param table_name: string, table name
        :param inner_session: bool
        :return:
        """
        url = "/tables/prepare_reload"
        params = {"project": project_name, "table": table_name}
        resp = self._request("GET", url, params=params, inner_session=inner_session)
        return resp

    def get_pushdown_model(self, project_name, table_name):
        """
        get pushdown model
        :param project_name: string, project name
        :param table_name: string, table name
        :return:
        """
        url = "/tables/pushdown_mode"
        params = {"project": project_name, "table": table_name}
        resp = self._request("GET", url, params=params)
        return resp

    def set_pushdown_model(self, project_name, table_name, pushdown_range_limited):
        """
        set pushdown model
        :param project_name: string, project name
        :param table_name: string, table name
        :param pushdown_range_limited: boolean, pushdown range limited
        :return:
        """
        url = "/tables/pushdown_mode"
        payload = {
            "project": project_name,
            "table": table_name,
            "pushdown_range_limited": pushdown_range_limited,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def get_tables_and_colomns(self, project_name, page_offset=0, page_size=10000):
        """
        get tables and colomns
        :param project_name: string, project name
        :param page_offset: int, page offset
        :param page_size: int, page size
        :return:
        """
        url = "/tables/simple_table"
        params = {
            "project": project_name,
            "page_offset": page_offset,
            "page_size": page_size,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def set_table_top(self, project_name, table_name, top):
        """
        set table top
        :param project_name: string, project name
        :param table_name: string, table name
        :param top: boolean
        :return:
        """
        url = "/tables/top"
        payload = {"project": project_name, "table": table_name, "top": top}
        resp = self._request("POST", url, json=payload)
        return resp

    def has_sampling_job(self, project_name, qualified_table_name):
        """
        check if the qualified table has a sampling task is running
        :param project_name: project name
        :param qualified_table_name: table name
        :return: boolean ,true for has sampling job is running
        """
        url = "/tables/sampling_check_result"
        params = {"project": project_name, "qualified_table_name": qualified_table_name}
        resp = self._request("GET", url, params=params)
        return resp

    def get_project_tables(
        self,
        project_name,
        table=None,
        ext=None,
        is_fuzzy=True,
        page_offset=0,
        page_size=10000,
        user_session=False,
    ):
        """
        get all tables info from project
        :param project_name:String , project name
        :param table:String ,table name
        :param ext: specify whether the table's extension information is returned
        :param is_fuzzy:boolean, fuzzy query table_name or not ,true for fuzzy query,false for not fuzzy query
        :param page_offset: integer,offset of returned result
        :param page_size:integer, quantity of returned result per page
        :param user_session: boolean, true for using login session to execute
        :return: all tables info from project
        """
        url = "/tables/project_tables"
        params = {
            "project": project_name,
            "page_offset": page_offset,
            "page_size": page_size,
            "table": table or "",
            "is_fuzzy": is_fuzzy,
            "ext": ext,
        }
        resp = self._request("GET", url, params=params, inner_session=user_session)
        return resp

    def set_partition_key(self, project_name, table, column, partition_column_format):
        """
        set partition key
        :param project_name: string, project name
        :param table: string, table name
        :param column: string, table column name
        :param partition_column_format: string, partition column format
        :return:
        """
        url = "/tables/partition_key"
        payload = {
            "project": project_name,
            "table": table,
            "column": column,
            "partition_column_format": partition_column_format,
        }
        resp = self._request("POST", url, json=payload)
        return resp

    # def set_data_range(self, project_name, table, start_time, end_time, auto_transform=False):
    #     """
    #     set data range of a table
    #     :param project_name: project name
    #     :param table: table name
    #     :param start_time: long, start time, if auto_transform is set to False,
    #     else string, like "2010-01-01 00:00:00" or "2010-01-01"
    #     :param end_time: long, end time, if auto_transform is set to False,
    #     else string, like "2010-01-02 00:00:00" or "2010-01-02"
    #     :param auto_transform: bool, if set to True, will convert start and end to long,
    #     carefully use this because the timezone is not under consideration when doing conversion
    #     :return:
    #     """
    #     url = '/tables/data_range'
    #     if auto_transform:
    #         start_time = string_to_ts(start_time)
    #         end_time = string_to_ts(end_time)
    #     payload = {
    #         'project': project_name,
    #         'table': table.upper(),
    #         'start': start_time,
    #         'end': end_time
    #     }
    #     resp = self._request('POST', url, json=payload)
    #     return resp

    def list_tables(
        self,
        project_name: str,
        database: str,
        table: str,
        is_fuzzy: bool = False,
        extension: bool = True,
        page_offset: int = 0,
        page_size: int = 10000,
        user_session: bool = False,
    ):
        """
        get table list desc
        :param project_name: string, project name
        :param database: string, database name, optional
        :param table: string, table name, optional
        :param is_fuzzy: boolean, fuzzy matching table name, optional
        :param extension: boolean, optional
        :param page_offset: int, offset of returned result, optional
        :param page_size: int, quantity of returned result per page, optional
        :return:
        """
        url = "/tables"
        params = {
            "project": project_name,
            "database": database,
            "table": table,
            "is_fuzzy": is_fuzzy,
            "ext": extension,
            "page_offset": page_offset,
            "page_size": page_size,
        }
        resp = self._request("GET", url, params=params, inner_session=user_session)
        return resp["tables"]

    def import_sqls(self, project_name, file_path):
        """
        import sqls to the white list
        :param project_name: project name
        :param file_path: file path
        :return: massaged query list
        """
        import_headers = self._headers.copy()
        import_headers.pop("Content-Type")
        url = "/query/favorite_queries/sql_files"
        params = {"project": project_name}

        file_path_list = file_path.split(";")
        file_list = [open(path, "rb") for path in file_path_list]
        files = [
            ("files", (os.path.basename(path), file))
            for path, file in zip(file_path_list, file_list)
        ]
        resp = self._request(
            "POST", url, headers=import_headers, params=params, files=files
        )

        for f in file_list:
            f.close()

        return resp["data"]

    def add_to_fqs(self, project_name, sqls):
        """
        add list of sqls to favorite query
        :param project_name: project name
        :param sqls: list of sqls
        :return:
        """
        url = "/query/favorite_queries"
        payload = {"project": project_name, "sqls": sqls}
        resp = self._request("POST", url, json=payload)
        return resp

    def get_acceleration_rules(self, project_name):
        """
        get acceleration rules
        :param project_name: string, project name
        :return:
        """
        url = "/projects/{project_name}/favorite_rules".format(
            project_name=project_name
        )
        resp = self._request("GET", url)
        return resp

    def update_favorite_rules(
        self,
        project_name,
        duration_enable=False,
        recommendation_enable=True,
        count_enable=True,
        recommendations_value=20,
        freq_value=0.1,
        submitter_enable=True,
        freq_enable=False,
        min_duration=0,
        count_value=10,
        max_duration=180,
        users=None,
        user_groups=None,
        effective_days=2,
        excluded_tables=None,
        excluded_tables_enable=False,
        min_hit_count=1,
        update_frequency=2,
    ):
        """
        update project favroite rules
        :param project_name: string, name of project
        :param duration_enable: bool
        :param recommendation_enable: bool
        :param count_enable:bool
        :param recommendations_value: int, recommendations number from query history to candidate
        :param freq_value: int
        :param submitter_enable: bool
        :param freq_enable:bool
        :param min_duration:int, min query duration
        :param count_value:int
        :param max_duration:int, max query duration
        :param users: list, user of query
        :param user_groups: list, group of query
        :param effective_days: int
        :param excluded_tables: list
        :param excluded_tables_enable: bool
        :param min_hit_count: int
        :param update_frequency: int
        :return:
        """
        url = "/projects/{project_name}/favorite_rules".format(
            project_name=project_name
        )
        payload = {
            "duration_enable": duration_enable,
            "recommendation_enable": recommendation_enable,
            "count_enable": count_enable,
            "recommendations_value": recommendations_value,
            "freq_value": freq_value,
            "submitter_enable": submitter_enable,
            "freq_enable": freq_enable,
            "min_duration": min_duration,
            "count_value": count_value,
            "max_duration": max_duration,
            "users": users or ["ADMIN"],
            "user_groups": user_groups or ["ROLE_ADMIN"],
            "project": project_name,
            "effective_days": effective_days,
            "excluded_tables": excluded_tables,
            "excluded_tables_enable": excluded_tables_enable,
            "min_hit_count": min_hit_count,
            "update_frequency": update_frequency,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def sql_validate(self, project_name, sql):
        """
        :param project_name: project name
        :param sql: string
        :return:
        """
        url = "/query/favorite_queries/sql_validation"
        payload = {"project": project_name, "sql": sql}
        resp = self._request("PUT", url, json=payload)
        return resp

    def format_query(self, sqls):
        """
        format query
        :param sqls: list, sql list
        :return:
        """
        url = "/query/format"
        payload = {"sqls": sqls}
        resp = self._request("PUT", url, json=payload)
        return resp

    def download_query_result(
        self, project_name, sql, limit=10000, format_type="csv", user_session=False
    ):
        """
        download query result to csv file
        :param project_name: string, project name
        :param sql: string, download the SQL for the query results
        :param limit: int, query result limit
        :param format_type: string, download query result format type
        :return:
        """
        url = "/query/format/{format}".format(format=format_type)
        import_headers = self._headers.copy()
        import_headers.pop("Content-Type")
        import_headers["enctype"] = "application/x-www-form-urlencoded"
        payload = {"project": project_name, "sql": sql, "limit": limit}
        resp = self._request(
            "POST",
            url,
            headers=import_headers,
            data=payload,
            stream=True,
            inner_session=user_session,
        )
        return resp

    def get_saved_queries(self, project_name, offset=0, limit=10000):
        """
        get saved queries
        :param project_name: string, project name
        :param offset: int, offset
        :param limit: int, limit
        :return:
        """
        url = "/query/saved_queries"
        params = {"project": project_name, "offset": offset, "limit": limit}
        resp = self._request("GET", url, params=params)
        return resp

    def save_query(self, project_name, name, sql, description=None):
        """
        save query
        :param project_name: string, project name
        :param name: string, save query sql name
        :param sql: string, query sql
        :param description: string, description
        :return:
        """
        url = "/query/saved_queries"
        payload = {
            "project": project_name,
            "name": name,
            "sql": sql,
            "description": description or "",
        }
        resp = self._request("POST", url, json=payload)
        return resp

    def remove_save_query(self, project_name, query_id):
        """
        remove save query
        :param project_name: string, project name
        :param query_id: string, save query sql id
        :return:
        """
        url = "/query/saved_queries/{id}".format(id=query_id)
        params = {"project": project_name}
        resp = self._request("DELETE", url, params=params)
        return resp

    def get_query_statistics(self, project_name, start_time, end_time):
        """
        get query statistics
        :param project_name: string, project name
        :param start_time: int, start time
        :param end_time: int, end time
        :return:
        """
        url = "/query/statistics"
        params = {
            "project": project_name,
            "start_time": start_time,
            "end_time": end_time,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def get_query_count(self, project_name, start_time, end_time, dimension):
        """
        get query count
        :param project_name: string, project name
        :param start_time: int, start time
        :param end_time: int, end time
        :param dimension: string, dimension
        :return:
        """
        url = "/query/statistics/count"
        params = {
            "project": project_name,
            "start_time": start_time,
            "end_time": end_time,
            "dimension": dimension,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def get_avg_duration(self, project_name, start_time, end_time, dimension):
        """
        get avg duration
        :param project_name: string, project name
        :param start_time: int, start time
        :param end_time: int, end time
        :param dimension: string, dimension
        :return:
        """
        url = "/query/statistics/duration"
        params = {
            "project": project_name,
            "start_time": start_time,
            "end_time": end_time,
            "dimension": dimension,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def get_metadata(self, project_name):
        """
        get metadata
        :param project_name: string, project name
        :return:
        """
        url = "/query/tables_and_columns"
        params = {"project": project_name}
        resp = self._request("GET", url, params=params)
        return resp

    def get_job_stats(self, project_name, start_time, end_time):
        """
        get job stats in dashboard
        :param project_name: string
        :return:
        """
        url = "/jobs/statistics"
        params = {
            "project": project_name,
            "start_time": start_time,
            "end_time": end_time,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def check_if_can_answered_by_existed_model(self, project_name, sqls=None):
        """
        :param sqls: array
        :param project_name: project name
        :return:
        """
        url = "/models/can_answered_by_existed_model"
        payload = {"project": project_name, "sqls": sqls or []}
        resp = self._request("POST", url, json=payload)
        return resp

    def get_computed_column_usage(self, project_name):
        """
        :param project_name: project name
        :return:
        """
        url = "/models/computed_columns/usage"
        params = {
            "project": project_name,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def get_model_config(
        self, project_name, model_name=None, page_offset=0, page_size=10000
    ):
        """
        :param project_name: string
        :param model_name: string
        :param page_offset: integer
        :param page_size: integer
        :return:
        """
        url = "/models/config"
        params = {
            "project": project_name,
            "model_name": model_name or "",
            "page_offset": page_offset,
            "page_size": page_size,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def disable_all_models(self, project_name):
        """
        :param project_name: project name
        :return:
        """
        url = "/models/disable_all_models"
        params = {"project": project_name}
        resp = self._request("PUT", url, params=params)
        return resp

    def enable_all_models(self, project_name):
        """
        :param project_name: project name
        :return:
        """
        url = "/models/enable_all_models"
        params = {"project": project_name}
        resp = self._request("PUT", url, params=params)
        return resp

    def suggest_model(
        self, project_name, reuse_existed_model=False, sqls=None, timeout=300
    ):
        """
        :param project_name: string
        :param reuse_existed_model: boolean
        :param sqls: string []
        :return:
        """
        url = "/models/suggest_model"
        payload = {
            "project": project_name,
            "reuse_existed_model": reuse_existed_model,
            "sqls": sqls or [],
        }
        resp = self._request("POST", url, json=payload, timeout=timeout)
        return resp

    def recommendations_acceleration(
        self, project_name, inner_session=False, timeout=300
    ):
        """
        accelerate query history and select topn
        :param project_name: string, name of project
        :param inner_session: bool, default False
        :return:
        """
        url = "/recommendations/acceleration"
        params = {"project": project_name}
        resp = self._request(
            "PUT", url, params=params, inner_session=inner_session, timeout=timeout
        )
        return resp

    def project_acceleration(self, project_name, timeout=300):
        """
        :param project_name: string, name of project
        :return:
        """
        time.sleep(15)
        url = "/projects/acceleration"
        params = {"project": project_name}
        resp = self._request("PUT", url, params=params, timeout=timeout)
        return resp

    def approve_suggest_model(self, project_name, reused_models=None, new_models=None):
        """
        :param project_name: string, name of project
        :param recommendations: list<string>, suggest model recommendations
        :param new_models: list<string>, suggest new models
        :return:
        """
        url = "/models/model_recommendation"
        payload = {
            "project": project_name,
            "reused_models": reused_models or [],
            "new_models": new_models or [],
        }
        resp = self._request("POST", url, json=payload)
        return resp

    def epoch(self, project_name_list, force):
        """
        :param project_name_list: list, for example [project1, project2.....]
        :param force: bool, if True update info
        :return:
        """
        url = "/epoch"
        payload = {"projects": project_name_list, "force": force}
        resp = self._request("POST", url, json=payload)
        return resp

    def get_agg_indices_list(
        self,
        project_name,
        model_id,
        content=None,
        index=None,
        is_case_sensitive=False,
        page_offset=0,
        page_size=100000,
        reverse=True,
        sort_by="last_modify_time",
    ):
        """
        :param project_name: string
        :param model_id: string
        :param content: string
        :param index: integer
        :param is_case_sensitive: boolean
        :param page_offset: integer
        :param page_size: integer
        :param reverse: boolean
        :param sort_by: string
        :return:
        """
        url = "/models/{model}/agg_indices".format(model=model_id)
        params = {
            "project": project_name,
            "content": content,
            "index": index,
            "is_case_sensitive": is_case_sensitive,
            "page_offset": page_offset,
            "page_size": page_size,
            "reverse": reverse,
            "sort_by": sort_by,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def list_models(
        self,
        project_name,
        exact=False,
        model_name=None,
        sortby="last_modify",
        reverse=True,
        owner=None,
        page_offset=0,
        page_size=10000,
        status=None,
        model_alias_or_owner=None,
    ):
        url = "/models"
        params = {
            "project": project_name,
            "exact": exact,
            "model_name": model_name or "",
            "sortBy": sortby,
            "reverse": reverse,
            "owner": owner or "",
            "page_offset": page_offset,
            "page_size": page_size,
            "status": status or "",
            "model_alias_or_owner": model_alias_or_owner or "",
        }
        resp = self._request("GET", url, params=params)
        return resp["value"]

    def export_model_metadata(
        self,
        project_name,
        ids,
        stream=False,
        export_over_props=None,
        export_recommendations=None,
    ):
        """
        export model metadata
        :param project_name: string
        :param ids: List<String>, list of model uuid
        :return:
        """
        url = "/metastore/backup/models?project={project_name}".format(
            project_name=project_name
        )

        payload = {
            "ids": ids,
            "exportOverProps": export_over_props,
            "exportRecommendations": export_recommendations,
        }

        export_headers = self._headers.copy()
        export_headers.pop("Content-Type")
        export_headers["enctype"] = "application/x-www-form-urlencoded"

        resp = self._request(
            "POST", url, headers=export_headers, data=payload, stream=stream
        )
        return resp

    def upload_and_check_model_metadata(self, project_name, file_path):
        """
        check imported zip file is validate
        :param project_name: string, project name
        :param file_path: string, full path of zip file
        :return:
        """
        url = "/metastore/validation/models?project={project_name}".format(
            project_name=project_name
        )
        upload_headers = self._headers.copy()
        upload_headers.pop("Content-Type")

        with open(file_path, "rb") as f:
            file = [("file", (os.path.basename(file_path), f))]
            resp = self._request("POST", url, headers=upload_headers, files=file)

        return resp

    def import_model_metadata(self, project_name, file_path, request_path):
        """
        import model metadata
        :param project_name: string, project name
        :param signature: string, request /metastore/validation/models api get signature
        :param file_path: string, full path of zip file
        :param ids: List<String>, list of model id
        :return:
        """
        url = "/metastore/models?project={project_name}".format(
            project_name=project_name
        )
        import_headers = self._headers.copy()
        import_headers.pop("Content-Type")

        with open(file_path, "rb") as f, open(request_path, "rb") as r_f:
            files = [
                ("file", (os.path.basename(file_path), f, "application/zip")),
                ("request", (os.path.basename(request_path), r_f, "application/json")),
            ]
            resp = self._request("POST", url, headers=import_headers, files=files)

        return resp

    def show_project_table_names(
        self,
        project_name,
        table_name=None,
        datasource_type=9,
        page_offset=0,
        page_size=10000,
    ):
        """
        search table or database
        :param project_name: project name
        :param table_name:  table name or database_name
        :param datasource_type: datasource_type is set to 9 by default, which means Hive
        :param page_offset: offset of returned result
        :param page_size: quantity of returned result per page
        :param timeout: timeout
        :return: searched tables
        """
        url = "/tables/project_table_names"
        params = {
            "project": project_name,
            "table": table_name or "",
            "data_source_type": datasource_type,
            "page_size": page_size,
            "page_offset": page_offset,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def reload_hive_table_name(self, project_name, force=True, timeout=300):
        """
        reload hive table_name
        :param project_name:
        :param force:boolean, true for reload hive table_name now
        :param timeout: timeout
        :return:
        """
        url = "/tables/reload_hive_table_name"
        params = {"force": force, "project": project_name}
        resp = self._request("GET", url, params=params, timeout=timeout)
        return resp

    def create_model(self, model_desc_data):
        url = "/models"
        payload = model_desc_data
        logging.debug("Current payload for creating model is %s", payload)
        resp = self._request("POST", url, json=payload)
        return resp

    def get_model_json(self, model_id, project_name):
        """
        get model json info
        :param model_id: str, model uuid
        :param project_name: str, project name
        :return:
        """
        url = "/models/{model}/json".format(model=model_id)
        params = {"project": project_name}
        resp = self._request("GET", url, params=params)
        return resp

    def clone_model(self, project_name, model_id, new_model_name):
        """
        clone model
        :param project_name: string, project name
        :param model_id: string, model id
        :param new_model_name: string, new model name
        :return:
        """
        url = "/models/{model}/clone".format(model=model_id)
        payload = {"new_model_name": new_model_name, "project": project_name}
        resp = self._request("POST", url, json=payload)
        return resp

    def delete_model(self, project_name, model_id):
        url = "/models/{model}".format(model=model_id)
        params = {"project": project_name}
        resp = self._request("DELETE", url, params=params)
        return resp

    def edit_model(self, edited_model_desc):
        url = "/models/semantic"
        payload = edited_model_desc
        resp = self._request("PUT", url, json=payload)
        return resp

    def calculate_agg_index_combination(
        self, project_name, model_id, agg_groups, load_data=None, global_dim_cap=None
    ):
        """
        Add aggregate index
        :param project_name: project name
        :param model_id: model id
        :param agg_groups: aggregate groups example:
        [{"includes":[0],"select_rule":{"mandatory_dims":[0],"hierarchy_dims":[],"joint_dims":[]}}]
        :param load_data: boolean, load data or not, if set to True, will trigger a build job
        :return:
        """
        url = "/index_plans/agg_index_count"
        payload = {
            "project": project_name,
            "model_id": model_id,
            "aggregation_groups": agg_groups,
            "load_data": load_data,
            "global_dim_cap": global_dim_cap,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def get_index(
        self,
        project_name,
        model_id,
        key=None,
        page_offset=0,
        page_size=10000,
        reverse=None,
        sort_by=None,
        sources=None,
        status=None,
    ):
        """
        :param project_name: project name
        :param model_id: string
        :param key: string
        :param page_offset: integer
        :param page_size: integer
        :param reverse: boolean
        :param sort_by: string
        :param sources: array[string]
        :return:
        """
        url = "/index_plans/index"
        params = {
            "project": project_name,
            "model": model_id,
            "key": key or "",
            "page_offset": page_offset,
            "page_size": page_size,
            "reverse": reverse or "",
            "sort_by": sort_by or "",
            "sources": sources or "",
            "status": status or "",
        }
        resp = self._request("GET", url, params=params)
        return resp

    def get_index_graph(self, project_name, model_id, order=100):
        """
        :param project_name: project name
        :param model_id: string
        :param order:  integer, Default value : 100
        :return:
        """
        url = "/index_plans/index_graph"
        params = {"project": project_name, "model": model_id, "order": order}
        resp = self._request("GET", url, params=params)
        return resp

    def delete_index(self, project_name, model_id, layout_id, index_range="EMPTY"):
        """
        :param project_name: project name
        :param model_id: string
        :param layout_id: integer
        :param index_range: string
        :return:
        """
        url = "/index_plans/index/{layout_id}".format(layout_id=layout_id)
        params = {
            "project": project_name,
            "model": model_id,
            "index_range": index_range,
        }
        resp = self._request("DELETE", url, params=params)
        return resp

    def get_rule_based_index(self, project_name, model_id):
        """
        :param project_name: string
        :param model_id: string
        :return:
        """
        url = "/index_plans/rule"
        params = {"project": project_name, "model": model_id}
        resp = self._request("GET", url, params=params)
        return resp

    def public_get_segments_detail(self, project_name, model_name):
        """
        :param project_name: string
        :param model_name: string
        :return:
        """
        url = f"/models/{model_name}/segments"
        params = {"project": project_name, "model_name": model_name}
        resp = self._request("GET", url, headers=self._public_headers, params=params)
        return resp

    def public_get_segments_multi_partition_detail(
        self, project_name, model_name, segment_id
    ):
        """
        :param project_name: string
        :param model_name: string
        :param segment_id: segment id
        :return:
        """
        url = f"/models/{model_name}/segments/multi_partition"
        params = {
            "project": project_name,
            "segment_id": segment_id,
            "model_name": model_name,
        }
        resp = self._request("GET", url, headers=self._public_headers, params=params)
        return resp

    def public_set_multi_partition_mapping(
        self,
        project_name,
        model_name,
        alias_columns,
        multi_partition_columns,
        value_mapping,
    ):
        """
        :param project_name: string, project name
        :param model_name: string, model name
        :param alias_columns: list, multi partition corresponding mapping column
        :param multi_partition_columns: list, multi partition corresponding mapping column
        :param value_mapping: array, [{"origin": ["1"], "target": ["1"]}, {"origin": ["2"],"target": ["2"]}],
                origin value is multi partition column value, target value is mapping column value
        :return:
        """
        url = f"/models/{model_name}/multi_partition/mapping"
        payload = {
            "project": project_name,
            "alias_columns": alias_columns,
            "multi_partition_columns": multi_partition_columns,
            "value_mapping": value_mapping,
        }
        resp = self._request("PUT", url, headers=self._public_headers, json=payload)
        return resp

    def public_get_query_history(
        self,
        project_name=None,
        start_time_from=None,
        start_time_to=None,
        page_offset=None,
        page_size=None,
    ):

        url = "/query/query_histories"
        params = {
            "project": project_name,
            "start_time_from": start_time_from,
            "start_time_to": start_time_to,
            "page_offset": page_offset,
            "page_size": page_size,
        }
        resp = self._request("GET", url, params=params)
        return resp["query_histories"]

    def add_aggregate_indices(
        self,
        project_name,
        model_id,
        agg_groups,
        load_data=False,
        global_dim_cap=None,
        restore_deleted_index=False,
    ):
        """
        Add aggregate index
        :param project_name: project name
        :param model_id: model id
        :param agg_groups: aggregate groups
        :param load_data: boolean, load data or not, if set to True, will trigger a build job
        :param global_dim_cap: default None
        :param restore_deleted_index: boolean, default False
        :return:
        """
        url = "/index_plans/rule"
        payload = {
            "project": project_name,
            "model_id": model_id,
            "aggregation_groups": agg_groups,
            "load_data": load_data,
            "global_dim_cap": global_dim_cap,
            "restore_deleted_index": restore_deleted_index,
            "scheduler_version": 1,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def add_table_indices(
        self,
        project_name,
        model_id,
        col_order,
        indices_id=None,  # pylint: disable=R0913
        sort_by_cols=None,
        shard_by_cols=None,
        load_data=True,
        storage_type=None,
        layout_override_indexes=None,
    ):
        """
        Add table index
        :param name: string
        :param indices_id:
        :param layout_override_indexes:
        :param storage_type:
        :param project_name: project name
        :param model_id: model id
        :param col_order: list of col order
        :param sort_by_cols: sort by columns
        :param shard_by_cols: shard_by_columns
        :param load_data: boolean, load data or not, if set to True, will trigger a build job
        :return:
        """
        url = "/index_plans/table_index"
        payload = {
            "col_order": col_order,
            "project": project_name,
            "id": indices_id or "",
            "model_id": model_id,
            "sort_by_columns": sort_by_cols or [],
            "shard_by_columns": shard_by_cols or [],
            "load_data": load_data,
            "storage_type": storage_type,
            "layout_override_indexes": layout_override_indexes,
        }
        resp = self._request("POST", url, json=payload)
        return resp

    def update_table_index(
        self,
        project_name,
        model_id,
        col_order,
        indices_id,
        sort_by_cols=None,
        shard_by_cols=None,
        load_data=True,
        storage_type=None,
        layout_override_indexes=None,
    ):
        """
        Add table index
        :param indices_id: id must > 20000000000
        :param layout_override_indexes:
        :param storage_type:
        :param project_name: project name
        :param model_id: model id
        :param col_order: list of col order
        :param sort_by_cols: sort by columns
        :param shard_by_cols: shard_by_columns
        :param load_data: boolean, load data or not, if set to True, will trigger a build job
        :return:
        """
        url = "/index_plans/table_index"
        payload = {
            "col_order": col_order,
            "project": project_name,
            "id": indices_id,
            "model_id": model_id,
            "sort_by_columns": sort_by_cols,
            "shard_by_columns": shard_by_cols,
            "load_data": load_data,
            "storage_type": storage_type,
            "layout_override_indexes": layout_override_indexes,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def get_table_indices(self, project_name, model_id):
        url = "/models/{model_id}/table_indices".format(model_id=model_id)
        params = {
            "project": project_name,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def get_table_index(self, project_name, model_id, page_offset=0, page_size=10):
        """
        get table index info
        :param project_name: str, project name
        :param model_id: str, model uuid
        :param page_offset: int, page offset number
        :param page_size: int, page size number
        :return:
        """
        url = "/index_plans/table_index"
        params = {
            "project": project_name,
            "model": model_id,
            "page_offset": page_offset,
            "page_size": page_size,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def build_segments(
        self,
        project_name,
        model_id,
        start_time=None,
        end_time=None,
        build_indexes=True,
        auto_transform=False,
    ):
        """
        build segment API, trigger full build by default. To trigger inc build, specify start_time and end_time properly
        :param build_indexes: bool
        :param project_name: project name
        :param model_id: model id
        :param start_time: start time
        :param end_time: end time
        :param auto_transform: bool, if set to True, will convert start and end to long,
        carefully use this because the timezone is not under consideration when doing conversion
        :return:
        """
        if auto_transform:
            start_time = string_to_ts(start_time)
            end_time = string_to_ts(end_time)
        url = "/models/{model}/segments".format(model=model_id)
        payload = {
            "build_all_indexes": build_indexes,
            "project": project_name,
            "model_id": model_id,
            "start": start_time,
            "end": end_time,
        }
        resp = self._request("POST", url, json=payload)
        return resp

    def public_build_segments(
        self,
        project_name,
        model_name,
        start_time=None,
        end_time=None,
        build_indexes=True,
        auto_transform=False,
        build_all_sub_partitions=False,
    ):
        """
        build segment public API, trigger full build by default. To trigger inc build, specify start_time and end_time properly
        :param build_indexes: bool
        :param project_name: project name
        :param model_name: model name
        :param start_time: start time
        :param end_time: end time
        :param auto_transform: bool, if set to True, will convert start and end to long,
        carefully use this because the timezone is not under consideration when doing conversion
        :param build_all_sub_partitions: boolean, if set to True, will build all sub_partition_values
        :return:
        """
        if auto_transform:
            start_time = string_to_ts(start_time)
            end_time = string_to_ts(end_time)
        url = "/models/{model_name}/segments".format(model_name=model_name)
        payload = {
            "build_all_indexes": build_indexes,
            "project": project_name,
            "start": start_time,
            "end": end_time,
            "build_all_sub_partitions": build_all_sub_partitions,
        }
        resp = self._request("POST", url, headers=self._public_headers, json=payload)
        return resp

    def public_multi_partition_build_segment(
        self,
        project_name,
        model_name,
        segment_id,
        partition_values,
        build_all_sub_partitions=False,
        to_json=True,
    ):
        """
        Build multi-partition subpartition segment API
        @param project_name: project name
        @param model_name:  model name
        @param segment_id:  segment id
        @param partition_values: partition value like [['1'],['2']]
        @param build_all_sub_partitions: boolean if build all sub partitions
        @param to_json: in order to catch exception msg
        @return:
        """
        url = "/models/{model_name}/segments/multi_partition".format(
            model_name=model_name
        )
        payload = {
            "project": project_name,
            "segment_id": segment_id,
            "sub_partition_values": partition_values,
            "build_all_sub_partitions": build_all_sub_partitions,
        }
        resp = self._request(
            "POST", url, json=payload, headers=self._public_headers, to_json=to_json
        )
        return resp

    def refresh_sub_partition(
        self,
        project_name,
        model_id,
        segment_id,
        partition_ids,
        partition_values=None,
        ignored_snapshot_tables=None,
    ):
        """
        Refresh sub partition
        @param project_name: project name
        @param model_id: model id
        @param segment_id: segment id
        @param partition_ids: partition_ids e.g. [0,1]
        @param partition_values: partition values like [['1'],['2']]
        @param ignored_snapshot_tables: ['TableA','TableB']
        @return:
        """
        url = "/models/{model_id}/model_segments/multi_partition".format(
            model_id=model_id
        )
        payload = {
            "project": project_name,
            "segment_id": segment_id,
            "partition_ids": partition_ids,
            "sub_partition_values": partition_values,
            "ignored_snapshot_tables": ignored_snapshot_tables,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def delete_sub_partition(self, project_name, model_id, segment_id, ids):
        """
        Delete sub partition
        @param project_name: project name
        @param model_id: model id
        @param segment_id: segment id
        @param ids: partition_ids e.g. [0,1]
        @return:
        """
        url = "/models/model_segments/multi_partition"
        params = {
            "project": project_name,
            "model": model_id,
            "segment": segment_id,
            "ids": ids,
        }
        resp = self._request("DELETE", url, params=params)
        return resp

    def validation_partition(
        self, project_name, model_id, start_time, end_time, auto_transform=False
    ):
        """
        :param auto_transform:
        :param project_name: project name
        :param model_id:  model id
        :param start_time: str, segments time range start. e.g. 694224000000
        :param end_time: str, segments time range end. e.g. 744249600000
        :return:
        """
        if auto_transform:
            start_time = string_to_ts(start_time)
            end_time = string_to_ts(end_time)
        url = "/models/{model}/segment/validation".format(model=model_id)
        payload = {"project": project_name, "start": start_time, "end": end_time}
        resp = self._request("POST", url, json=payload)
        return resp

    def build_model_segments(
        self,
        project_name,
        model_id,
        start_time,
        end_time,
        partition_date_column,
        partition_date_format,
        segment_holes,
        columns=None,
        partition_values=None,
        build_all_indexes=True,
        build_all_sub_partitions=False,
        auto_transform=False,
        to_json=True,
    ):
        """
        :param project_name: string, name of project
        :param model_id: string, uuid of model
        :param start_time: default int, example 1598889600000
        :param end_time: default int, example 1598889600000
        :param partition_date_column: string, table.column
        :param partition_date_format: string, "yyyy-MM-dd"
        :param segment_holes: list
        :param columns: string, table.column
        :param partition_values: list,
        :param build_all_indexes:bool, default False
        :param build_all_sub_partitions:bool, default False
        :param auto_transform:bool, default False
        :param to_json: in order to catch exception msg
        :return:
        """
        if auto_transform:
            start_time = string_to_ts(start_time)
            end_time = string_to_ts(end_time)
        url = f"/models/{model_id}/model_segments"
        payload = {
            "project": project_name,
            "start": start_time,
            "end": end_time,
            "partition_desc": {
                "partition_date_column": partition_date_column,
                "partition_date_format": partition_date_format,
            },
            "multi_partition_desc": columns and {"columns": columns},
            "sub_partition_values": partition_values,
            "segment_holes": segment_holes,
            "build_all_indexes": build_all_indexes,
            "build_all_sub_partitions": build_all_sub_partitions,
        }
        resp = self._request("PUT", url, json=payload, to_json=to_json)
        return resp

    def fill_holes(self, project_name, model_id, segment_holes):
        """
        :param project_name: project name
        :param model_id:  model id
        :param segment_holes: list eg: [{start: 744249600000, end: 946656000000}]
        :return:
        """
        url = "/models/{model}/segment_holes".format(model=model_id)
        payload = {"project": project_name, "segment_holes": segment_holes}
        resp = self._request("POST", url, json=payload)
        return resp

    def get_segments(
        self,
        project_name,
        model_id,
        status=None,
        start=1,
        end=9223372036854775807,
        sort_by="create_time_utc",
        reverse=True,
        page_offset=0,
        page_size=10,
    ):
        """
        get model segment info
        :param project_name: str, project name
        :param model_id: str, model uuid
        :param status: str, segments's status. e.g. 'READY'
        :param start: str, segments time range start. e.g. 694224000000
        :param end: str, segments time range end. e.g. 744249600000
        :param sort_by: str, default is last_modify
        :param reverse: boolean, is sort reverse. default is True
        :param page_offset: int, page offset number, default is 0
        :param page_size: int, page size number, default is 10
        :return:
        """
        url = "/models/{model}/segments".format(model=model_id)
        params = {
            "project": project_name,
            "status": status,
            "start": start,
            "end": end,
            "sort_by": sort_by,
            "reverse": reverse,
            "pageOffset": page_offset,
            "pageSize": page_size,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def get_multi_partition(
        self,
        project_name,
        model_id,
        segment_id,
        page_offset=0,
        page_size=2000,
        sort_by="last_modify",
        reverse=True,
        status=None,
    ):
        """
        Get multi partitions
        param project: project name
        param model_id: model is
        param segment_id: segment id
        param status: status e.g. ONLINE, REFRESHING
        param sort_by: sort_by
        param reverse: bool
        param page_offset: page offset
        param page_size: page size
        return:
        """
        url = "/models/{model}/model_segments/multi_partition".format(model=model_id)
        params = {
            "project": project_name,
            "page_offset": page_offset,
            "page_size": page_size,
            "sort_by": sort_by,
            "reverse": reverse,
            "segment_id": segment_id,
            "status": status,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def build_multi_partition(
        self,
        project_name,
        model_id,
        segment_id,
        partition_values,
        parallel_build_by_segment=False,
        build_all_sub_partitions=False,
    ):
        """
        build multi partition in segment
        :param project_name: string, name of project
        :param model_id: string, uuid of model
        :param segment_id: string, id of segment
        :param partition_values: list, e.g [['value'], ['value']]
        :param parallel_build_by_segment: bool, default is false
        :param build_all_sub_partitions: bool, default is false
        :return:
        """
        url = "/models/{model}/model_segments/multi_partition".format(model=model_id)
        payload = {
            "project": project_name,
            "segment_id": segment_id,
            "sub_partition_values": partition_values,
            "parallel_build_by_segment": parallel_build_by_segment,
            "build_all_sub_partitions": build_all_sub_partitions,
        }
        resp = self._request("POST", url, json=payload)
        return resp

    def public_refresh_multi_partition(
        self, project_name, model_name, segment_id, partition_values
    ):
        """
        public refresh multi partition
        :param project_name: string, name of project
        :param model_id: string, uuid of model
        :param segment_id: string, segment id of segment
        :param partition_values: [[string]], list of multi partition valuemiush
        :return:
        """
        url = "/models/{model_name}/segments/multi_partition".format(
            model_name=model_name
        )
        payload = {
            "project": project_name,
            "segment_id": segment_id,
            "sub_partition_values": partition_values,
        }
        resp = self._request("PUT", url, json=payload, headers=self._public_headers)
        return resp

    def delete_segments(self, project_name, model_id, ids, purge=False):
        """
        delete segments
        :param project_name: string, project name
        :param model_id: string, model id
        :param purge: boolean, purge
        :param ids: list, segments ids
        :return:
        """
        url = "/models/{model}/segments".format(model=model_id)
        params = {"project": project_name, "purge": purge, "ids": ids}
        resp = self._request("DELETE", url, params=params)
        return resp

    def delete_segment_contain_index(
        self, project_name, model_id, segment_ids, index_ids
    ):
        """
        :param project_name: string, name of project
        :param model_id: string, uuid of model
        :param segment_ids: list, except [segment_id1,segment_id2,...]
        :param index_ids: list, except [index_id1, index_id2,...]
        :return:
        """
        url = f"/models/{model_id}/model_segments/indexes/deletion"
        payload = {
            "project": project_name,
            "segment_ids": segment_ids,
            "index_ids": index_ids,
        }
        resp = self._request("POST", url, json=payload)
        return resp

    def completion_segment_all_index(
        self, project_name, model_id, segment_ids, parallel_build_by_segment=False
    ):
        """
        :param project_name: string, project name
        :param model_id: string, uuid of mode
        :param segment_ids:list, [segment_id1, segment_id2......]
        :param parallel_build_by_segment: bool
        :return:
        """
        url = f"/models/{model_id}/model_segments/all_indexes"
        payload = {
            "project": project_name,
            "segment_ids": segment_ids,
            "parallel_build_by_segment": parallel_build_by_segment,
        }
        resp = self._request("POST", url, json=payload)
        return resp

    def refresh_or_merge_segments_ids(self, project_name, model_id, ids, model_type):
        """
        refresh or merge segments by ids
        :param project_name: string, project name
        :param model_id: string, model id
        :param ids: list, ids list
        :param model_type: string, segment type, value: REFRESH, MERGE
        :return:
        """
        url = "/models/{model}/segments".format(model=model_id)
        payload = {"ids": ids, "project": project_name, "type": model_type}
        resp = self._request("PUT", url, json=payload)
        return resp

    def update_model_name(self, project_name, model_id, new_model_name, status=None):
        """
        update model name
        :param project_name: string, project name
        :param model_id: string, model id
        :param new_model_name: string, new model name
        :param status: string, model status, useless
        :return:
        """
        url = "/models/{model}/name".format(model=model_id)
        payload = {
            "project": project_name,
            "new_model_name": new_model_name,
            "status": status,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def update_model_status(self, project_name, model_id, status, new_model_name=None):
        """
        update model status
        :param project_name: string, project name
        :param model_id: string, model id
        :param new_model_name: string, new model name, useless
        :param status: string, model status
        :return:
        """
        url = "/models/{model}/status".format(model=model_id)
        payload = {
            "new_model_name": new_model_name,
            "project": project_name,
            "status": status,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def get_model_sql(self, project_name, model_id):
        """
        get model sql
        :param project_name: string, project name
        :param model_id: string, model id
        :return:
        """
        url = "/models/{model}/sql".format(model=model_id)
        params = {"project": project_name}
        resp = self._request("GET", url, params=params)
        return resp

    def get_layout_recommendation_content(
        self,
        project_name,
        model_id,
        item_id,
        content=None,
        page_offset=0,
        page_size=10000,
    ):
        """
        get layout recommendation content
        :param project_name: string, project name
        :param model_id: string, model id
        :param item_id: int, item id
        :param content: string, content, optional
        :param page_offset:offset of returned result, optional
        :param page_size: quantity of returned result per page, optional
        :return:
        """
        url = "/models/{model}/recommendations/index".format(model=model_id)
        params = {
            "project": project_name,
            "item_id": item_id,
            "content": content,
            "page_offset": page_offset,
            "page_size": page_size,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def remove_optimize_recommendations(
        self, project_name, model_id, recs_to_add_layout_id, inner_session=False
    ):
        """
        :param project_name: string, project_name
        :param model_id: string, model id
        :param recs_to_add_layout_id:  int, optimize recommendations layout id
        :param inner_session:  bool, default False
        :return:
        """
        url = f"/recommendations/{model_id}"
        params = {
            "project": project_name,
            "recs_to_add_layout": recs_to_add_layout_id,
        }
        resp = self._request("DELETE", url, params=params, inner_session=inner_session)
        return resp

    def remove_all_optimize_recommendations(self, project_name, model_id):
        """
        :param project_name: string, project_name
        :param model_id: string, model id
        """
        url = f"/recommendations/{model_id}/all"
        params = {
            "project": project_name,
        }
        resp = self._request("DELETE", url, params=params)
        return resp

    def apply_optimize_recommendations(
        self,
        project_name,
        model_id,
        measure=None,
        index=None,
        dimension=None,
        cc_recommendations=None,
    ):
        """
        apply optimize recommendations
        :param project_name: string, project name
        :param model_id: string, model id
        :param measure: list, measure recommendations, optional
        :param index: list, index recommendations, optional
        :param dimension: list, dimension recommendations, optional
        :param cc_recommendations: list, cc recommendations, optional
        :return:
        """
        url = "/models/{model}/recommendations".format(model=model_id)
        payload = {
            "project": project_name,
            "measure_recommendations": measure or [],
            "index_recommendation_item_ids": index or [],
            "dimension_recommendations": dimension or [],
            "cc_recommendations": cc_recommendations or [],
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def get_optimize_recommendations(
        self,
        project_name,
        model_id,
        page_offset=0,
        page_size=10,
        reverse=False,
        sort_by=None,
        type_=None,
        inner_session=False,
    ):
        """
        get apply optimize recommendations
        :param project_name: string, project name
        :param model_id: string, model id
        :param inner_session: bool, default False
        :return:
        """
        url = "/recommendations/{model}".format(model=model_id)
        params = {
            "project": project_name,
            "page_size": page_size,
            "page_offset": page_offset,
            "reverse": reverse,
            "sort_by": sort_by,
            "type": type_,
        }
        resp = self._request("GET", url, params=params, inner_session=inner_session)
        return resp

    def validate_optimize_recommendations(
        self,
        project_name,
        model_id,
        recs_to_add_layout=None,
        recs_to_remove_layout=None,
        names=None,
    ):
        """
        validate optimize recommendations
        :param project_name: string, name of model
        :param model_id: string, uuid of model
        :param recs_to_remove_layout: List<int>, layout id to remove
        :param recs_to_add_layout:  List<int>, layout id to add
        :param names: dict, layout id and name
        :return:
        """
        url = "/recommendations/{model_id}/validation".format(model_id=model_id)
        payload = {
            "project": project_name,
            "model_id": model_id,
            "recs_to_add_layout": recs_to_add_layout or [],
            "recs_to_remove_layout": recs_to_remove_layout or [],
            "names": names,
        }
        resp = self._request("POST", url, json=payload)
        return resp

    def approve_optimize_recommendations(
        self,
        project_name,
        model_id,
        names={},
        recs_to_add_layout=None,
        recs_to_remove_layout=None,
    ):
        """
        approve model optimize recommendations
        :param project_name: string, name of project
        :param model_id: string, uuid of model
        :param recs_to_add_layout: list, ids of layouts
        :param recs_to_remove_layout: list, ids of layouts
        :param names: dict, item_id: recommend item name which validation response
        :return:
        """
        url = "/recommendations/{model}".format(model=model_id)
        payload = {
            "project": project_name,
            "modelId": model_id,
            "recs_to_add_layout": recs_to_add_layout or [],
            "recs_to_remove_layout": recs_to_remove_layout or [],
            "names": names,
        }
        resp = self._request("POST", url, json=payload)
        return resp

    def get_optimize_recommendation_detail(
        self, project_name, model_id, item_id, is_add=True, inner_session=False
    ):
        """
        get model optimize detail
        :param project_name: string, name of project
        :param model_id: string, uuid of model
        :param item_id: int, recommendation item_id
        :param is_add: boolean, default is True
        :param inner_session: boolean, default is False
        :return:
        """
        url = "/recommendations/{model}/{item_id}".format(
            model=model_id, item_id=item_id
        )
        params = {"project": project_name, "is_add": is_add}
        resp = self._request("GET", url, params=params, inner_session=inner_session)
        return resp

    def get_purge_model_affected_response(self, project_name, model_id):
        """
        get purge model affected response
        :param project_name: string, project name
        :param model_id: string, model id
        :return:
        """
        url = "/models/{model}/purge_effect".format(model=model_id)
        params = {"project": project_name}
        resp = self._request("GET", url, params=params)
        return resp

    def unlink_model(self, project_name, model_id):
        """
        unlink model
        :param project_name: string, project name
        :param model_id: string, model id
        :return:
        """
        url = "/models/{model}/management_type".format(model=model_id)
        payload = {"project": project_name}
        resp = self._request("PUT", url, json=payload)
        return resp

    def build_indices_manually(self, project_name, model_id):
        """
        build indices manually
        :param project_name: string, project name
        :param model_id: string, model id
        :return:
        """
        url = "/models/{model}/indices".format(model=model_id)
        payload = {"project": project_name}
        resp = self._request("POST", url, json=payload)
        return resp

    # def get_model_latest_data(self, project_name, model_id):
    #     """
    #     get model latest data
    #     :param project_name: string, project name
    #     :param model_id: string, model id
    #     :return:
    #     """
    #     url = '/models/{model}/data_range/latest_data'.format(model=model_id)
    #     params = {
    #         'project': project_name
    #     }
    #     resp = self._request('GET', url, params=params)
    #     return resp

    def update_model_config(
        self,
        project_name,
        model_id,
        auto_merge_enabled,
        auto_merge_time_ranges,
        volatile_range,
        retention_range,
        override_props,
    ):
        """
        update model config
        :param project_name: string, project name
        :param model_id: string, model id
        :param auto_merge_enabled: boolean, auto merge enabled
        :param auto_merge_time_ranges: list, auto merge time ranges
        :param volatile_range: dict, volatile range
        :param retention_range: dict, retention range
        :param override_props: dict, override props
        :return:
        """
        url = "/models/{model}/config".format(model=model_id)
        payload = {
            "project": project_name,
            "auto_merge_enabled": auto_merge_enabled,
            "auto_merge_time_ranges": auto_merge_time_ranges,
            "volatile_range": volatile_range,
            "retention_range": retention_range,
            "override_props": override_props,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def update_agg_indices_shard_columns(
        self, project_name, model_id, shard_by_columns, load_data=False
    ):
        """
        update agg Indices shard columns
        :param project_name: string, project name
        :param model_id: string, model id
        :param shard_by_columns: list, shard by columns
        :param load_data: boolean, load data, optional
        :return:
        """
        url = "/models/{model}/agg_indices/shard_columns".format(model=model_id)
        payload = {
            "project": project_name,
            "shard_by_columns": shard_by_columns,
            "load_data": load_data,
        }
        resp = self._request("POST", url, json=payload)
        return resp

    def get_agg_indices_shard_columns(self, project_name, model_id):
        """
        get agg Indices shard columns
        :param project_name: string, project name
        :param model_id: string, model id
        :return:
        """
        url = "/models/{model}/agg_indices/shard_columns".format(model=model_id)
        payload = {"project": project_name}
        resp = self._request("POST", url, json=payload)
        return resp

    def list_jobs(
        self,
        project_name=None,
        status=None,
        page_offset=0,
        page_size=10000,  # pylint: disable=R0913
        time_filter=4,
        job_names=None,
        sort_by="create_time",
        reverse=True,
        subject=None,
        subject_alias=None,
    ):
        """
        list jobs in specific project
        :param project_name: project name
        :param status: string, DISCARDED, ERROR, FINISHED, NEW, PENDING, RUNNING, STOPPED
        :param page_offset: offset of returned result
        :param page_size: quantity of returned result per page
        :param time_filter: int, 0 -> last one day, 1 -> last one week,
                                 2 -> last one month, 3 -> last one year, 4 -> all
        :param job_names: list, job names
        :param sort_by: string, sort field, "last_modified", "create_time", "target_subject"
        :param reverse: boolean, whether sort reversely, "true" by default
        :param subject: string, model, segment
        :param subject_alias: string, alias of subject
        :return:
        """
        url = "/jobs"
        params = {
            "project": project_name,
            "statuses": status,
            "page_offset": page_offset,
            "page_size": page_size,
            "time_filter": time_filter,
            "job_names": job_names,
            "sort_by": sort_by,
            "reverse": reverse,
            "subject": subject,
            "subject_alias": subject_alias,
        }
        resp = self._request("GET", url, params=params)
        return resp["value"]

    def get_job(self, project_name, job_id):
        all_jobs = self.list_jobs(project_name)
        for job in all_jobs:
            if job["id"] == job_id:
                return job
        return None

    def update_job_status(self, project_name=None, jobs=None, action=None):
        """
        :param status: string, DISCARDED, ERROR, FINISHED, NEW, PENDING, RUNNING, STOPPED
        :param action: string, update status, DISCARD, ERROR, FINISHED, NEW, PENDING, RUNNING, STOPPED
        :param project_name: project name
        :param jobs: array[string]
        :return:
        """
        url = "/jobs/status"
        payload = {"job_ids": jobs, "project": project_name, "action": action}
        resp = self._request("PUT", url, json=payload)
        return resp

    def get_job_detail(self, project_name, job_id):
        """
        :param project_name: project name
        :param job_id: job id
        :return: job detail, which consists of two steps: "Detect Resource" and "Load Data to Index"
        """
        url = "/jobs/{job_id}/detail".format(job_id=job_id)
        params = {"project": project_name}
        resp = self._request("GET", url, params=params)
        return resp

    def get_job_step_output(self, project_name, job_id, step_id):
        """
        :param step_id: string
        :param project_name: project name
        :param job_id: string
        :return:
        """
        url = "/jobs/{job_id}/steps/{step_id}/output".format(
            job_id=job_id, step_id=step_id
        )
        params = {"project": project_name}
        resp = self._request("GET", url, params=params)
        return resp

    def drop_job(self, project_name, job_ids, status=None):
        """
        :param status: string, DISCARDED, ERROR, FINISHED, NEW, PENDING, RUNNING, STOPPED
        :param project_name: project name
        :param job_ids: list
        :return:
        """
        url = "/jobs"
        params = {"project": project_name, "job_ids": job_ids, "status": status}
        resp = self._request("DELETE", url, params=params)
        return resp

    def update_spark_job_time(
        self,
        project_name=None,
        job_id=None,
        yarn_job_run_time=None,
        task_id=None,
        yarn_job_wait_time=None,
    ):
        """
        :param yarn_job_wait_time: string
        :param job_id: string
        :param task_id: string
        :param project_name: project name
        :param yarn_job_run_time: string
        :return:
        """
        url = "/jobs/spark"
        payload = {
            "project": project_name,
            "job_id": job_id,
            "yarn_job_run_time": yarn_job_run_time,
            "yarn_job_wait_time": yarn_job_wait_time,
            "task_id": task_id,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def get_step_info(self, project_name, job_id, step_id):
        """
        get step information
        :param project_name: project name
        :param job_id: job id
        :param step_id: int, step id, 0 stands for "Detect Resource", 1 stands for "Load Data to Index"
        :return: step information
        """
        job_info = self.get_job_detail(project_name, job_id)
        return job_info[step_id]

    def waiting_jobs(self, project_name, model_id, offset=0, limit=10):
        """
        :param project_name: string, project name
        :param model_id: string, model uuid
        :param offset: int, page offset number
        :param limit: int, page size number
        :return:
        """
        url = "/jobs/waiting_jobs"
        params = {
            "project": project_name,
            "model": model_id,
            "offset": offset,
            "limit": limit,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def get_waiting_jobs_group_by_model(self, project_name):
        """
        :param project_name: string, project name
        :return:
        """
        url = "/jobs/waiting_jobs/models"
        params = {"project": project_name}
        resp = self._request("GET", url, params=params)
        return resp

    def await_all_jobs(self, project_name, waiting_time=50):
        """
        await all jobs to be finished, default timeout is 30 minutes
        :param project_name: project name
        :param waiting_time: timeout, in minutes
        :return: boolean, timeout will return false
        """
        running_flags = ["PENDING", "RUNNING"]
        try_time = 0
        max_try_time = waiting_time * 2
        # finish_flags = ['ERROR', 'FINISHED', 'DISCARDED']
        while try_time < max_try_time:
            jobs = self.list_jobs(project_name)
            all_finished = True
            if not jobs:
                return True
            for job in jobs:
                if job["job_status"] in running_flags:
                    all_finished = False
                    break
            if jobs and all_finished:
                return True
            time.sleep(30)
            try_time += 1
        return False

    def await_job(
        self, project_name, job_id, waiting_time=40, interval=1, excepted_status=None
    ):
        """
        Await specific job to be given status, default timeout is 20 minutes.
        :param project_name: project name of the job
        :param job_id: job id
        :param waiting_time: timeout, in minutes.
        :param interval: check interval, default value is 1 second
        :param excepted_status: excepted job status list, default contains 'ERROR', 'FINISHED' and 'DISCARDED'
        :return: boolean, if the job is in finish status, return true
        """
        finish_flags = ["ERROR", "DISCARDED"]
        if excepted_status is None:
            excepted_status = finish_flags
        timeout = waiting_time * 60
        start = time.time()
        job_info = None
        while time.time() - start < timeout:
            job_info = self.get_job(project_name, job_id)
            job_status = job_info["job_status"]
            if job_status in excepted_status:
                return True
            if job_status in finish_flags:
                logging.debug(
                    f"debug: await_job failed: job_id: {job_id}; job_info: {job_info}"
                )
                return False
            time.sleep(interval)

        logging.debug(
            f"debug: await_job failed: job_id: {job_id}; job_info: {job_info}"
        )
        return False

    def await_job_finished(self, project_name, job_id, waiting_time=40, interval=1):
        """
        Await specific job to be finished, default timeout is 20 minutes.
        :param project_name: project name of the job
        :param job_id: job id
        :param waiting_time: timeout, in minutes.
        :param interval: check interval, default value is 1 second
        :return: boolean, if the job is in finish status, return true
        """
        return self.await_job(
            project_name, job_id, waiting_time, interval, excepted_status=["FINISHED"]
        )

    def await_job_error(self, project_name, job_id, waiting_time=20, interval=1):
        """
        Await specific job to be error, default timeout is 20 minutes.
        :param project_name: project name of the job
        :param job_id: job id
        :param waiting_time: timeout, in minutes.
        :param interval: check interval, default value is 1 second
        :return: boolean, if the job is in finish status, return true
        """
        return self.await_job(
            project_name, job_id, waiting_time, interval, excepted_status=["ERROR"]
        )

    def await_job_discarded(self, project_name, job_id, waiting_time=20, interval=1):
        """
        Await specific job to be discarded, default timeout is 20 minutes.
        :param project_name: project name of the job
        :param job_id: job id
        :param waiting_time: timeout, in minutes.
        :param interval: check interval, default value is 1 second
        :return: boolean, if the job is in finish status, return true
        """
        return self.await_job(
            project_name, job_id, waiting_time, interval, excepted_status=["DISCARDED"]
        )

    def await_job_step(
        self,
        project_name,
        job_id,
        step_id,
        excepted_status=None,
        waiting_time=20,
        interval=1,
    ):
        """
        Await specific job step to be given status, default timeout is 20 minutes.
        :param project_name: project name
        :param job_id: job id
        :param step_id: step id, 0 means "Detect Resource", 1 means "Load Data to Index"
        :param waiting_time: timeout, in minutes.
        :param interval: check interval, default value is 1 second
        :param excepted_status: excepted job status list, default contains 'ERROR', 'FINISHED' and 'DISCARDED'
        :return: boolean, if the job is in finish status, return true
        """
        finish_flags = ["ERROR", "FINISHED", "DISCARDED"]
        if excepted_status is None:
            excepted_status = finish_flags
        timeout = waiting_time * 60
        start = time.time()
        while time.time() - start < timeout:
            step_info = self.get_step_info(project_name, job_id, step_id)
            step_status = step_info["step_status"]
            if step_status in excepted_status:
                return True
            if step_status in finish_flags:
                return False
            time.sleep(interval)
        return False

    def await_job_name_exist(
        self, project_name, job_names, waiting_time=10, interval=1
    ):
        """
        wait job exist in job list after submit job
        :param project_name: string, project name
        :param job_names: list, expected job names list
                         eg: ['INDEX_REFRESH', 'INDEX_BUILD', 'INDEX_BUILD', 'TABLE_SAMPLING', 'INC_BUILD','SUB_PARTITION_BUILD']
        :param waiting_time: int, minutes to wait job name exist in job list
        :param interval: int, interval time to get list jobs
        :return:
        """
        timeout = waiting_time * 60
        start = time.time()
        while time.time() - start < timeout:
            jobs = self.list_jobs(project_name=project_name)
            project_job_names = [
                job["job_name"]
                for job in jobs
                if str(job["project"]).lower() == str(project_name).lower()
            ]
            if (
                jobs
                and len(job_names) <= len(project_job_names)
                and set(job_names).issubset(set(project_job_names))
            ):
                return True
            time.sleep(interval)
        return False

    def get_job_id(
        self, project_name, job_name, model_id=None, waiting_time=2, interval=1
    ):
        """
        wait job exist in job list after submit job
        :param project_name: string, project name
        :param job_name: list, expected job names list
        :param model_id: string, model id, optional
        :param waiting_time: int, minutes to wait job name exist in job list
        :param interval: int, interval time to get list jobs
        :return:
        """
        timeout = waiting_time * 60
        start = time.time()
        while time.time() - start < timeout:
            jobs = self.list_jobs(project_name=project_name, subject=model_id)
            job_ids = [
                job_info["id"] for job_info in jobs if job_info["job_name"] == job_name
            ]
            if jobs and job_ids:
                return job_ids[0]
            time.sleep(interval)
        return None

    def execute_query(
        self,
        project_name: str,
        sql: str,
        offset: int = 0,
        limit: int = 0,
        accept_partial: bool = True,
        backdoor_toggles = None,
        user_session: bool = False,
        timeout: int = 120,
    ):
        url = "/query"
        payload = {
            "project": project_name,
            "sql": sql,
            "offset": offset,
            "limit": limit,
            "acceptPartial": accept_partial,
            "backdoorToggles": backdoor_toggles,
        }
        resp = self._request(
            "POST", url, json=payload, inner_session=user_session, timeout=timeout
        )
        return resp

    def list_query_histories(
        self,
        project_name,
        start_time_from=None,
        start_time_to=None,  # pylint: disable=R0913
        latency_from=None,
        latency_to=None,
        sql=None,
        realization=None,
        server=None,
        offset=0,
        limit=10000,
        query_status=None,
    ):
        """
        list query histories
        :param project_name: string, project name
        :param start_time_from: string, start time from, optional
        :param start_time_to: string, end time from, optional
        :param latency_from: string, latency from
        :param latency_to: string, latency to
        :param sql: string, sql, optional
        :param realization: list, realization, optional
        :param server: string, server, optional
        :param offset: int, offset
        :param limit: int, limit
        :param query_status: list, query status, optional
        """
        url = "/query/history_queries"
        params = {
            "project": project_name,
            "start_time_from": start_time_from,
            "start_time_to": start_time_to,
            "latency_from": latency_from,
            "latency_to": latency_to,
            "sql": sql,
            "realization": realization,
            "server": server,
            "offset": offset,
            "limit": limit,
            "query_status": query_status,
        }
        resp = self._request("GET", url, params=params)
        return resp["query_histories"]

    def list_query_histories_table_names(self, project_names=None):
        """
        list query histories table names
        :param project_names: list, project name, optional
        :return:
        """
        url = "/query/history_queries/table_names"
        params = {"projects": project_names}
        resp = self._request("GET", url, params=params)
        return resp

    def list_query_servers(self):
        url = "/query/servers"
        resp = self._request("GET", url)
        return resp

    def get_access_entities(
        self,
        entity_type,
        uuid,
        name=None,
        is_case_sensitive=False,
        page_offset=0,
        page_size=10,
    ):
        """
        get access entry list of a domain object
        :param entity_type: string, eg: ProjectInstance
        :param uuid: string, union id for different param entity_type
        :param name: string, principal name
        :param is_case_sensitive: boolean, true for param name case sensitive
        :param page_offset: int, offset of returned result
        :param page_size: int, quantity of returned result per page
        :return:
        """
        url = "/access/{entity_type}/{uuid}".format(entity_type=entity_type, uuid=uuid)
        params = {
            "name": name,
            "is_case_sensitive": is_case_sensitive,
            "page_offset": page_offset,
            "page_size": page_size,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def grant_access(
        self, entity_type, uuid, sid, access_entry_id, principal, permission
    ):
        """
        :param entity_type: string, eg: ProjectInstance
        :param uuid: string, union id for different param entity_type
        :param sid: string
        :param access_entry_id: int
        :param principal: boolean
        :param permission: string
        :return:
        """
        url = "/access/{entity_type}/{uuid}".format(entity_type=entity_type, uuid=uuid)
        payload = {
            "access_entry_id": access_entry_id,
            "permission": permission,
            "principal": principal,
            "sid": sid,
        }
        resp = self._request("POST", url, json=payload)
        return resp

    def batch_grant(
        self, entity_type, uuid, sid, access_entry_id, principal, permission
    ):
        """
        :param entity_type: string, eg: ProjectInstance
        :param uuid: string, union id for different param entity_type
        :param sid: string
        :param access_entry_id: int
        :param principal: boolean
        :param permission: string
        :return:
        """
        url = "/access/{type}/{uuid}".format(type=entity_type, uuid=uuid)
        payload = {
            "access_entry_id": access_entry_id,
            "permission": permission,
            "principal": principal,
            "sid": sid,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def revoke_acl(self, entity_type, uuid, sid, access_entry_id, principal):
        """
        :param entity_type: string, eg: ProjectInstance
        :param uuid: string, union id for different param entity_type
        :param sid: string
        :param access_entry_id: int
        :param principal: boolean
        :return:
        """
        url = "/access/{entity_type}/{uuid}".format(entity_type=entity_type, uuid=uuid)
        params = {
            "access_entry_id": access_entry_id,
            "sid": sid,
            "principal": principal,
        }
        resp = self._request("DELETE", url, params=params)
        return resp

    def batch_grant_access(self, entity_type, uuid, batch_access_list):
        """
        batch grant new access on a domain object to a user
        :param entity_type: string, ProjectInstance
        :param uuid: string, union id for different param entity_type, If the project is authorized, it is the project id
        :param batch_access_list: list[access_dict], list contains dict, multi access to grant
        :batch_access_list access_dict param permission: string, ADMINISTRATION MANAGEMENT OPERATION READ
        :batch_access_list access_dict param sids: list[string], user name or group name list, eg: ['user','admin']
        :batch_access_list access_dict param principal: boolean, true for user, false for group
        :batch_access_list access_dict param accessEntryId: int, access entry's id. not required
        access_dict example:
            {
                'permission': 'ADMINISTRATION',
                'sids': ['username'],
                'principal': True,
                'accessEntryId': None
            }
        :return:
        """
        url = "/access/batch/{entity_type}/{uuid}".format(
            entity_type=entity_type, uuid=uuid
        )
        resp = self._request("POST", url, json=batch_access_list)
        return resp

    def get_available_sids(
        self,
        sid_type,
        uuid,
        name=None,
        is_case_sensitive=False,
        page_offset=0,
        page_size=10,
    ):
        """
        :param sid_type: string, eg: ProjectInstance
        :param uuid: string, union id for different param entity_type
        :param name: project name
        :param is_case_sensitive: boolean, true for param name case sensitive
        :param page_offset: int, offset of returned result
        :param page_size: int, quantity of returned result per page
        :return:
        """
        url = "/access/available/{sid_type}/{uuid}".format(sid_type=sid_type, uuid=uuid)
        params = {
            "name": name,
            "is_case_sensitive": is_case_sensitive,
            "page_offset": page_offset,
            "page_size": page_size,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def get_user_permission_in_project(self, project_name):
        url = "/access/permission/project_permission"
        params = {"project": project_name}
        resp = self._request("GET", url, params=params)
        return resp

    def get_project_sid_tcr(self, project_name, sid, sid_type, authorized_only=False):
        """
        :param project_name:
        :param sid_type: string, User or Group
        :param sid: string, user name or user group name
        :param authorized_only: boolean
        :return:
        """
        url = "/acl/sid/{sid_type}/{sid}".format(sid_type=sid_type, sid=sid)
        params = {"project": project_name, "authorized_only": authorized_only}
        resp = self._request("GET", url, params=params)
        return resp

    def get_project_sid_tcr_v2(
        self, project_name, sid, sid_type, authorized_only=False
    ):
        """
        :param project_name:
        :param sid_type: string, User or Group
        :param sid: string, user name or user group name
        :param authorized_only: boolean
        :return:
        """
        url = f"/acl/{sid_type}/{sid}"
        params = {"project": project_name, "authorized_only": authorized_only}
        resp = self._request("GET", url, params=params, headers=self._public_headers)
        return resp

    def update_project_sid_tcr(self, project_name, sid, tcr_list, sid_type):
        """
        :param tcr_list:type list
        sample:
        [{"database_name":"string","tables":[{"authorized":true,"authorized_column_num":0,
        "columns":[{"dbname":"string","size":0,"tables":[{}]}],
        "rows":[{"indexes":[{"dbname":"string","size":0,"tables":[{}]}],"total_size":0}],
        "table_name":"string","total_column_num":0}]}]
        :param project_name:
        :param sid_type: string
        :param sid: string
        :return:
        """
        url = "/acl/sid/{sid_type}/{sid}".format(sid_type=sid_type, sid=sid)
        params = {
            "project": project_name,
        }
        payload = tcr_list
        resp = self._request("PUT", url, params=params, json=payload)
        return resp

    def update_project_sid_tcr_v2(self, project_name, sid, tcr_list, sid_type):
        """
        :param tcr_list:type list
        sample:
        [{"database_name":"string","tables":[{"authorized":true,"authorized_column_num":0,
        "columns":[{"dbname":"string","size":0,"tables":[{}]}],
        "row_filter":,
        "table_name":"string","total_column_num":0}]}]
        :param project_name:
        :param sid_type: string
        :param sid: string
        :return:
        """
        url = f"/acl/{sid_type}/{sid}"
        params = {
            "project": project_name,
        }
        payload = tcr_list
        resp = self._request(
            "PUT", url, params=params, json=payload, headers=self._public_headers
        )
        return resp

    def get_instance_config(self):
        url = "/admin/instance_info"
        resp = self._request("GET", url)
        return resp

    def get_public_config(self):
        url = "/admin/public_config"
        resp = self._request("GET", url)
        return resp

    def is_cloud(self):
        url = "/config/is_cloud"
        resp = self._request("GET", url)
        return resp

    def backup_project(self, project_name):
        """
        backup project
        :param project_name: string, project name
        :return:
        """
        url = "/projects/{project}/backup".format(project=project_name)
        resp = self._request("POST", url)
        return resp

    def update_storage_quota_config(
        self, project_name, storage_quota_size, storage_quota_tb_size
    ):
        """
        update storage quota config
        :param project_name: string, project name
        :param storage_quota_size: int, byte of storage quota, optional values: positive integer and 0
        :return:
        """
        url = "/projects/{project}/storage_quota".format(project=project_name)
        payload = {
            "storage_quota_size": storage_quota_size,
            "storage_quota_tb_size": storage_quota_tb_size,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def update_push_down_config(self, project_name, push_down_enabled):
        """
        update push down config
        :param project_name: string, project name
        :param push_down_enabled: boolean，Enable query and pushdown, true for enable, false for close
        :return:
        """
        url = "/projects/{project}/push_down_config".format(project=project_name)
        payload = {"push_down_enabled": push_down_enabled}
        resp = self._request("PUT", url, json=payload)
        return resp

    def get_query_accelerate_threshold_config(self, project_name):
        """
        get query accelerate threshold config
        :param project_name: string，project name
        :return:
        """
        url = "/projects/{project}/query_accelerate_threshold".format(
            project=project_name
        )
        resp = self._request("GET", url)
        return resp

    def update_query_accelerate_threshold_config(
        self, project_name, tips_enabled, threshold
    ):
        """
        update query accelerate threshold config
        :param project_name: string，project name
        :param tips_enabled: boolean，whether to turn on the acceleration prompt, true for turn on, false for turn off
        :param threshold: int, start number of acceleration prompt, optional value: positive integer or 0
        :return:
        """
        url = "/projects/{project}/query_accelerate_threshold".format(
            project=project_name
        )
        payload = {
            "threshold": threshold,
            "tips_enabled": tips_enabled,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def update_job_notification_config(
        self,
        project_name,
        data_load_empty_notification_enabled,
        job_error_notification_enabled,
        job_notification_emails,
    ):
        """
        update job notification config
        :param project_name:
        :param data_load_empty_notification_enabled: boolean，whether to enable empty data reminder? true means to enable
        :param job_error_notification_enabled: boolean，whether to start the failed task reminder. True means to start
        :param job_notification_emails: list，email address of task reminder, email format: xx@xx.xx
        :return:
        """
        url = "/projects/{project}/job_notification_config".format(project=project_name)
        payload = {
            "data_load_empty_notification_enabled": data_load_empty_notification_enabled,
            "job_error_notification_enabled": job_error_notification_enabled,
            "job_notification_emails": job_notification_emails,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def update_shard_num_config(self, project_name, col_to_num):
        """
        update shard num config
        :param project_name: string, project name
        :param col_to_num: dict, column num, ootional
        :return:
        """
        url = "/projects/{project}/shard_num_config".format(project=project_name)
        payload = {"col_to_num": col_to_num}
        resp = self._request("PUT", url, json=payload)
        return resp

    def cleanup_project_storage(self, project_name):
        """
        cleanup project storage
        :param project_name: string, project name
        :return:
        """
        url = "/projects/{project}/storage".format(project=project_name)
        resp = self._request("PUT", url)
        return resp

    def get_storage_volume_info(self, project_name):
        """
        get storage volume info
        :param project_name: string, project name
        :return:
        """
        url = "/projects/{project}/storage_volume_info".format(project=project_name)
        resp = self._request("GET", url)
        return resp

    def update_yarn_queue(self, project_name, queue_name, user_session=False):
        """
        update yarn queue
        :param user_session: boolean
        :param project_name: string, project name
        :param queue_name: string, queue name
        :return:
        """
        url = "/projects/{project}/yarn_queue".format(project=project_name)
        payload = {"queue_name": queue_name}
        resp = self._request("PUT", url, json=payload, inner_session=user_session)
        return resp

    def get_memory_metrics(self):
        """
        get memory metrics
        :return:
        """
        url = "/monitor/memory_info"
        resp = self._request("GET", url)
        return resp

    def get_thread_info_metrics(self):
        """
        get thread info metrics
        :return:
        """
        url = "/monitor/thread_info"
        resp = self._request("GET", url)
        return resp

    def list_license(self):
        """
        get license info
        :return:
        """
        url = "/system/license"
        resp = self._request("GET", url)
        return resp

    def upload_license_content(self, content):
        """
        upload license with string content
        :param content: string, license content
        :return:
        """
        url = "/system/license/content"
        resp = self._request("POST", url, data=content)
        return resp

    def trial_license(
        self, email, company, username, lang="en", product_type="kap", category="4.x"
    ):
        """
        trial license with email company username
        :param email: string, email format
        :param company: string, company name
        :param username: string, user's name
        :param lang: string, default is en
        :param product_type: string, default is kap
        :param category: string
        :return:
        """
        payload = {
            "email": email,
            "company": company,
            "username": username,
            "lang": lang,
            "product_type": product_type,
            "category": category,
        }
        url = "/system/license/trial"
        resp = self._request("POST", url, json=payload)
        return resp

    def upload_license_file(self, file_path, timeout=120):
        """
        upload license with file
        :param file_path: string, upload license file full path
        :param timeout: timeout for request
        :return:
        """
        headers = copy.deepcopy(self._headers)
        headers.pop("Content-Type")

        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f)}
            url = "/system/license/file"
            resp = self._request(
                "POST", url, headers=headers, files=files, timeout=timeout
            )

        return resp

    def request_license(self, stream=False, to_json=False):
        """
        get license info
        :param stream: when is False, return raw response to get headers
        :param to_json: when is False, return text response to get license info
        :return:
        """
        url = "/system/license/info"
        resp = self._request("GET", url, stream=stream, to_json=to_json)
        return resp

    def batch_save_models(self, project_name, model_json_list):
        """
        batch save models
        :param project_name: string, project name
        :param model_json_list: list, create models json list
        :return:
        """
        url = "/models/batch_save_models"
        params = {"project": project_name}
        payload = model_json_list
        resp = self._request("POST", url, params=params, json=payload)
        return resp

    def get_model_affected(self, project_name, table, action):
        """
        get model affected
        :param project_name: string, project name
        :param table: string, table name
        :param action: string, TOGGLE_PARTITION or DROP_TABLE or RELOAD_ROOT_FACT
        :return:
        """
        url = "/models/affected_models"
        params = {"project": project_name, "table": table, "action": action}
        resp = self._request("GET", url, params=params)
        return resp

    def public_list_tables(
        self,
        project_name,
        database,
        table,
        is_fuzzy=False,
        extension=True,
        page_offset=0,
        page_size=10000,
        user_session=False,
    ):
        """
        public get table list desc
        :param project_name: string, project name
        :param database: string, database name, optional
        :param table: string, table name, optional
        :param is_fuzzy: boolean, fuzzy matching table name, optional
        :param extension: boolean, optional
        :param page_offset: int, offset of returned result, optional
        :param page_size: int, quantity of returned result per page, optional
        :return:
        """
        url = "/tables"
        params = {
            "project": project_name,
            "database": database,
            "table": table,
            "is_fuzzy": is_fuzzy,
            "ext": extension,
            "page_offset": page_offset,
            "page_size": page_size,
        }
        resp = self._request("GET", url, params=params, inner_session=user_session)
        return resp

    def public_get_recommendations(
        self, project_name, model_name, rec_action_type=None
    ):
        """
        public api get recommendations
        :param project_name: string, name of project
        :param model_name: string, name of model
        :param rec_action_type: string, not required, default is all
        :return:
        """
        url = "/models/{model_name}/recommendations".format(model_name=model_name)
        params = {"project": project_name, "recActionType": rec_action_type}
        resp = self._request("GET", url, headers=self._public_headers, params=params)
        return resp

    def public_batch_apply_recommendations(
        self,
        project_name,
        model_names=None,
        filter_by_models=False,
        del_param_keys=None,
        user_session=False,
    ):
        """
        batch apply recommendations, public api
        :param project_name: string
        :param model_names: array[string]
        :param filter_by_models: boolean, whether to pass recommendations in batches by model
        :return:
        """
        url = "/models/recommendations/batch"
        payload = {
            "project": project_name,
            "model_names": model_names,
            "filter_by_models": filter_by_models,
        }

        for k in del_param_keys or []:
            payload.pop(k)

        resp = self._request(
            "PUT",
            url,
            json=payload,
            headers=self._public_headers,
            inner_session=user_session,
        )
        return resp

    def get_model_desc(self, project_name, model_name):
        """
        get model description information, public api
        :param project_name: string, project name
        :param model_name: string, model name
        :return: model description information
        """
        url = "/models/{project}/{model_name}/model_desc".format(
            project=project_name, model_name=model_name
        )
        resp = self._request("GET", url)
        return resp

    def set_partition_column(
        self, project_name, model_name, partition_desc=None, start=None, end=None
    ):
        """
        set partition column, public api
        :param project_name: string, project name
        :param model_name: string, model name
        :param partition_desc: string, defines the column name and data type of the partition column
        :param start: string, segment start time
        :param end: string, segment end time
        :return:
        """
        url = "/models/{project}/{model_name}/partition_desc".format(
            project=project_name, model_name=model_name
        )
        payload = {"partition_desc": partition_desc, "start": start, "end": end}
        resp = self._request("PUT", url, json=payload)
        return resp

    def model_validation(self, project_name, sqls, timeout=120):
        """
        model validation, public api
        :param project_name: string, project name
        :param sqls: list, validation sqls list
        :return:
        """
        url = "/models/model_validation"
        payload = {"project": project_name, "sqls": sqls}
        resp = self._request(
            "POST", url, headers=self._public_headers, json=payload, timeout=timeout
        )
        return resp

    def introduction_sql_recommended_model(self, project_name, sqls, timeout=300):
        """
        introduction sql recommended model, public api
        :param project_name: string, project name
        :param sqls: list, query sqls list
        :return:
        """
        url = "/models/model_suggestion"
        payload = {"project": project_name, "sqls": sqls}
        resp = self._request("POST", url, json=payload, timeout=timeout)
        return resp

    def model_optimization_advice(
        self, project_name, sqls, accept_recommendation=False
    ):
        """
        optimize the existing model based on the incoming SQL and generate optimization recommendations, public api
        :param project_name: string, project name
        :param sqls: list, optimization model query sqls
        :return:
        """
        url = "/models/model_optimization"
        payload = {
            "project": project_name,
            "accept_recommendation": accept_recommendation,
            "sqls": sqls,
        }
        resp = self._request("POST", url, json=payload, headers=self._public_headers)
        return resp

    def build_index_segment(self, project_name, model_name):
        """
        build index segment, public api
        :param project_name: string, project name
        :param model_name: string, model name
        :return:
        """
        url = "/models/{model_name}/indexes".format(model_name=model_name)
        payload = {"project": project_name}
        resp = self._request("POST", url, json=payload)
        return resp

    def add_indexes_to_segments(
        self,
        project_name,
        model_id,
        segment_ids,
        index_ids,
        parallel_build_by_segment=False,
    ):
        """
        :param project_name: string, project name
        :param model_id: string, example 42f55796-7e0e-4d3b-b291-dae4326307fe
        :param segment_ids: list, segment_ids
        :param index_ids: list, index_ids
        :return:
        """
        url = f"/models/{model_id}/model_segments/indexes"
        payload = {
            "project": project_name,
            "segment_ids": segment_ids,
            "index_ids": index_ids,
            "parallel_build_by_segment": parallel_build_by_segment,
        }
        resp = self._request("POST", url, json=payload)
        return resp

    def public_check_segment_overlap(
        self, project_name, model_name, start_time, end_time, auto_transform=False
    ):
        """
        :param project_name: string, project name
        :param model_name: string, model name
        :param start_time: string, start time of segment
        :param end_time: string, end time of segment
        :param auto_transform: bool, default True
        :return:
        """
        url = f"/models/{model_name}/segments/check"
        if auto_transform:
            start_time = string_to_ts(start_time)
            end_time = string_to_ts(end_time)
        payload = {
            "project": project_name,
            "start": start_time,
            "end": end_time,
        }
        resp = self._request("POST", url, headers=self._public_headers, json=payload)
        return resp

    def public_delete_model(self, project_name, model_name):
        """
        :param project_name: string, project name
        :param model_name: string, model name
        :return:
        """
        url = f"/models/{model_name}?project={project_name}"
        resp = self._request("DELETE", url, headers=self._public_headers)
        return resp

    def public_completion_segment(
        self, project_name, model_name, segment_ids, parallel=False
    ):
        """
        :param project_name: string, project name
        :param model_name: string, model name
        :param parallel: boolean, is parallel to complete segement
        :param segment_ids: array, ids of segment
        :return:
        """
        segment_id_str = ",".join(segment_ids)
        url = (
            f"/models/{model_name}/segments/completion?project={project_name}"
            f"&ids={segment_id_str}&parallel={parallel}"
        )
        resp = self._request("POST", url, headers=self._public_headers)
        return resp

    def update_snapshot_config(self, project_name, snapshot_manual_enabled=True):
        """
        update snapshot management config
        :param project_name: string, name of project
        :param snapshot_manual_management_enabled: bool, open or close snapshot manual management
        :return:
        """
        url = "/projects/{project_name}/snapshot_config".format(
            project_name=project_name
        )
        payload = {
            "project": project_name,
            "snapshot_manual_management_enabled": snapshot_manual_enabled,
        }
        resp = self._request("PUT", url, json=payload)
        return resp

    def refresh_snapshot(
        self,
        project_name,
        table_names,
        partition_table=None,
        partition_col=None,
        incremental_build=False,
        partitions_to_build=None,
    ):
        """
        refresh snapshot
        :param project_name: string, name of project
        :param table_names: list, tables to build.
        :param partition_table: string, partition_table name
        :param partition_col: string, partition_col name
        :param incremental_build: bool
        :param partitions_to_build: list, partitions to refresh
        :return:
        """
        url = "/snapshots"
        payload = {"project": project_name, "tables": table_names, "databases": []}
        if partition_table:
            payload["options"] = {
                partition_table: {
                    "partition_col": partition_col,
                    "incremental_build": incremental_build,
                    "partitions_to_build": partitions_to_build,
                }
            }
        resp = self._request("PUT", url, json=payload)
        return resp

    def list_snapshot_tables(
        self,
        project_name,
        status,
        table,
        page_offset=0,
        page_size=10000,
        sort_by="last_modified_time",
        user_session=False,
    ):
        """
        public get table list desc
        :param project_name: string, project name
        :param table: string, table name
        :param page_offset: int, offset of returned result, optional
        :param page_size: int, quantity of returned result per page, optional
        :return:
        """
        url = "/snapshots"
        params = {
            "project": project_name,
            "status": status,
            "table": table,
            "page_offset": page_offset,
            "page_size": page_size,
            "sort_by": sort_by,
        }
        resp = self._request("GET", url, params=params, inner_session=user_session)
        return resp["value"]

    def list_snapshot_sources(
        self, project_name, page_offset=0, page_size=10000, user_session=False
    ):
        """
        public get table list desc
        :param project_name: string, project name
        :param page_offset: int, offset of returned result, optional
        :param page_size: int, quantity of returned result per page, optional
        :return:
        """
        url = "/snapshots/tables"
        params = {
            "project": project_name,
            "page_offset": page_offset,
            "page_size": page_size,
        }
        resp = self._request("GET", url, params=params, inner_session=user_session)
        return resp

    def set_snapshot_partition(self, project_name, database, table, partition_col):
        url = "/snapshots/partitions"
        payload = {
            "project": project_name,
            "table_cols": {f"{database}.{table}": partition_col},
        }
        resp = self._request("POST", url, json=payload)
        return resp

    def v2_cubes_list(
        self, project_name, model_name=None, page_offset=0, page_size=10000
    ):
        """
        get cubes, application/vnd.apache.kylin-v2+json api
        :param project_name: string, project name
        :param model_name: string, model name
        :param page_offset: offset of returned result
        :param page_size: quantity of returned result per page
        :return:
        """
        url = "/cubes"
        params = {
            "projectName": project_name,
            "modelName": model_name,
            "pageOffset": page_offset,
            "pageSize": page_size,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def v2_get_cube(self, cube_name, project_name):
        """
        get appoint cube, application/vnd.apache.kylin-v2+json api
        :param cube_name: string, model name
        :return:
        """
        url = "/cubes/{cube_name}".format(cube_name=cube_name)
        params = {"project": project_name}
        resp = self._request("GET", url, params=params)
        return resp

    def v2_get_cube_holes(self, cube_name, project_name=None):
        """
        get cube holes, application/vnd.apache.kylin-v2+json api
        :param cube_name: string, model name
        :param project_name: string, optional
        :return:
        """
        url = "/cubes/{cube_name}/holes".format(cube_name=cube_name)
        params = {"project": project_name}
        resp = self._request("GET", url, params=params)
        return resp

    def v2_get_cube_sql(self, cube_name):
        """
        get cube sql, application/vnd.apache.kylin-v2+json api
        :param cube_name: string, model name
        :return:
        """
        url = "/cubes/{cube_name}/sql".format(cube_name=cube_name)
        resp = self._request("GET", url)
        return resp

    def v2_get_project(
        self, project_name=None, page_offset=0, page_size=100000, exact=False
    ):
        """
        get project API, application/vnd.apache.kylin-v2+json api
        :param project_name: string, project name, optional
        :param page_offset: offset of returned result
        :param page_size: quantity of returned result per page
        :param exact: boolean, exact match or not
        :param user_session: boolean, true for using login session to execute
        """
        url = "/projects"
        params = {
            "projectName": project_name,
            "pageOffset": page_offset,
            "pageSize": page_size,
            "exact": exact,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def v2_list_models(
        self,
        project_name,
        exact=True,
        model_name=None,
        reverse=True,
        owner=None,
        page_offset=0,
        page_size=10000,
        status=None,
    ):
        """
        get model lists, application/vnd.apache.kylin-v2+json api
        :param project_name: string, project name
        :param exact: boolean, exact match or not
        :param model_name: string, model name, optional
        :param reverse: boolean, whether sort reversely, "true" by default, optional
        :param owner: string, model owner, optional
        :param page_offset: offset of returned result
        :param page_size: quantity of returned result per page
        :param status: string, model status, optional
        :return:
        """
        url = "/models"
        params = {
            "projectName": project_name,
            "exactMatch": exact,
            "modelName": model_name,
            "reverse": reverse,
            "owner": owner,
            "pageOffset": page_offset,
            "pageSize": page_size,
            "status": status,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def v2_list_jobs(
        self,
        project_name=None,
        status=None,
        page_offset=0,
        page_size=10000,
        time_filter=4,
        sort_by="last_modified",
        reverse=True,
    ):
        """
        list jobs in specific project, application/vnd.apache.kylin-v2+json api
        :param project_name: project name
        :param status: int, 0 -> NEW, 1 -> PENDING, 2 -> RUNNING, 4 -> FINISHED, 8 -> ERROR, 16 -> DISCARDED,
         32 -> STOPPED, 64 -> SUICIDAL
        :param page_offset: offset of returned result
        :param page_size: quantity of returned result per page
        :param time_filter: int, 0 -> last one day, 1 -> last one week,
                                 2 -> last one month, 3 -> last one year, 4 -> all
        :param job_names: list, job names
        :param sort_by: string, sort field, "last_modified", "create_time", "target_subject"
        :param reverse: boolean, whether sort reversely, "true" by default
        :param subject: string, model, segment
        :param subject_alias: string, alias of subject
        :return:
        """
        url = "/jobs"
        params = {
            "projectName": project_name,
            "status": status,
            "pageOffset": page_offset,
            "pageSize": page_size,
            "timeFilter": time_filter,
            "sortBy": sort_by,
            "reverse": reverse,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def v2_get_users_list(
        self,
        project_name=None,
        username=None,
        is_case_sensitive=False,
        page_offset=0,
        page_size=10000,
    ):
        """
        get users list, application/vnd.apache.kylin-v2+json api
        :param project_name: string, project name, optional
        :param username: string, user name, optional
        :param is_case_sensitive: boolean, fuzzy matching of user names is case-sensitive, not case-sensitive by default
        :param page_offset: offset of returned result
        :param page_size: quantity of returned result per page
        :return:
        """
        url = "/kap/user/users"
        params = {
            "name": username,
            "project": project_name,
            "isCaseSensitive": is_case_sensitive,
            "pageOffset": page_offset,
            "pageSize": page_size,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def v2_get_user_project_and_table_permissions(self, username):
        """
        returns the project and table permissions owned by the user, application/vnd.apache.kylin-v2+json api
        :param username: string, user name
        :return:
        """
        url = "/access/{username}".format(username=username)
        resp = self._request("GET", url)
        return resp

    def v2_get_users_with_group(self, page_offset=0, page_size=10000):
        """
        get users with group, application/vnd.apache.kylin-v2+json api
        :param user_group_name: string, project name, optional
        :param page_offset: int,  offset of returned result, optional
        :param page_size: int , quantity of returned result per page, optional
        :param user_session: boolean, true for using login session to execute
        :return: list of all users with group
        """
        url = "/user_group/usersWithGroup"
        params = {
            "pageOffset": page_offset,
            "pageSize": page_size,
        }
        resp = self._request("GET", url, params=params)
        return resp

    def v2_restore_job_task(self, uuid):
        """
        restore job task, application/vnd.apache.kylin-v2+json api
        :param uuid: string, job task id
        :return:
        """
        url = "/jobs/{uuid}/resume".format(uuid=uuid)
        resp = self._request("PUT", url)
        return resp

    def v2_refresh_or_merge_segments(
        self, cube_name, build_type, segments, project_name=None
    ):
        """
        refresh or merge segments, application/vnd.apache.kylin-v2+json api
        :param cube_name: string, model name
        :param build_type:  string, MERGE or REFRESH
        :param segments: list, segment name
        :param project_name: string, optional
        :return:
        """
        url = "/cubes/{cube_name}/segments".format(cube_name=cube_name)
        params = {"project": project_name}
        payload = {"buildType": build_type, "segments": segments}
        resp = self._request("PUT", url, params=params, json=payload)
        return resp

    def v2_rebuild_segment(
        self, cube_name, start_time, end_time, build_type, project_name=None
    ):
        """
        build segment, application/vnd.apache.kylin-v2+json api
        :param cube_name: string, model name
        :param start_time: int, build start time
        :param end_time: int, build end time
        :param build_type: string, REFRESH or BUILD
        :param project_name: string, optional
        :return:
        """
        url = "/cubes/{cube_name}/rebuild".format(cube_name=cube_name)
        params = {"project": project_name}
        payload = {
            "startTime": start_time,
            "endTime": end_time,
            "buildType": build_type,
        }
        resp = self._request("PUT", url, params=params, json=payload)
        return resp

    def v2_get_cube_desc(self, project_name, cube_name):
        """
        v2 get cube desc, application/vnd.apache.kylin-v2+json api
        :param project_name: string, project name
        :param cube_name: string, model name
        :return:
        """
        url = "/cube_desc/{project_name}/{cube_name}".format(
            project_name=project_name, cube_name=cube_name
        )
        resp = self._request("GET", url)
        return resp

    def list_snapshots(self, project_name, page_offset=0, page_size=100000, table=""):
        """
        list snapshot
        :param table: string, table name, optional
        :param project_name: string, project name
        :param page_offset: int, page offset, optional
        :param page_size: int, page size, optional
        :return:
        """
        url = f"/snapshots?project={project_name}&page_offset={page_offset}&page_size={page_size}&table={table}"
        resp = self._request("GET", url)
        return resp

    def build_snapshot(
        self,
        project_name,
        tables,
        databases=None,
        partition_table=None,
        partition_col=None,
        incremental_build=False,
        partitions_to_build=None,
    ):
        """
        build snapshot
        :param project_name: string, project name
        :param tables: list, table name
        :param databases: list, database name
        :param partition_table: string, partition_table name
        :param partition_col: string, partition_col name
        :param incremental_build: bool
        :param partitions_to_build: list, partition_value
        :return:
        """
        url = "/snapshots"
        payload = {
            "project": project_name,
            "tables": tables,
            "databases": databases or [],
        }
        if partition_table:
            payload["options"] = {
                partition_table: {
                    "partition_col": partition_col,
                    "incremental_build": incremental_build,
                    "partitions_to_build": partitions_to_build,
                }
            }
        if partitions_to_build:
            payload["options"][partition_table][
                "partitions_to_build"
            ] = partitions_to_build
        resp = self._request("POST", url, json=payload)
        return resp

    def check_before_delete_snapshot(self, project_name, tables):
        """
        check before delete snapshot
        :param project_name: string, project name
        :param tables: list, table name
        :return:
        """
        url = "/snapshots/check_before_delete"
        payload = {"project": project_name, "tables": tables}
        resp = self._request("POST", url, json=payload)
        return resp

    def public_access_acls(self, acl_type, user_or_group_name, project_name=None):
        """
        get user or group's acl permissions

        :param acl_type: user or group, case insensitive
        :param user_or_group_name: user or group's name, case insensitive
        :param project_name: project's name, optional, case insensitive
        :return:
        """
        params = {"type": acl_type, "name": user_or_group_name, "project": project_name}
        url = "/access/acls"
        resp = self._request("GET", url, headers=self._public_headers, params=params)
        return resp

    def public_access_project(
        self,
        project_name,
        name=None,
        is_case_sensitive=False,
        page_offset=0,
        page_size=1000,
    ):
        """
        get project's permissions

        :param project_name: project's name, case insensitive
        :param name: user or group's name, optional
        :param is_case_sensitive: bool, whether name is case sensitive, optional
        :param page_offset: int, page offset, optional
        :param page_size: int, page size, optional
        :return:
        """
        params = {
            "name": name,
            "project": project_name,
            "is_case_sensitive": is_case_sensitive,
            "page_offset": page_offset,
            "page_size": page_size,
        }
        url = "/access/project"
        resp = self._request("GET", url, headers=self._public_headers, params=params)
        return resp

    def async_query(
        self,
        project,
        sql,
        separator=",",
        limit=0,
        offset=0,
        format="csv",
        encode="utf-8",
        file_name="result",
    ):
        """
        execute async query
        :param project: str, name of project
        :param sql: str
        :param separator: str, default is ','
        :param limit: int, default is 0
        :param offset: int, default is 0
        :param format: str, hdfs file type, default is csv
        :param encode: str, hdfs file encoding, default is utf-8
        :param file_name: str, hdfs file name, default is result
        :return:
        """
        payload = {
            "project": project,
            "sql": sql,
            "separator": separator,
            "offset": offset,
            "limit": limit,
            "format": format,
            "encode": encode,
            "file_name": file_name,
        }
        url = "/async_query"
        resp = self._request("POST", url, json=payload)
        return resp

    def async_query_status(self, project, query_id):
        """
        async query status
        :param project: str, name of project
        :param query_id: str, query id
        :return:
        """
        params = {"project": project}
        url = "/async_query/{query_id}/status".format(query_id=query_id)
        resp = self._request("GET", url, params=params)
        return resp

    def async_query_path(self, project, query_id):
        """
        async query path
        :param project: str, name of project
        :param query_id: str, query id
        :return:
        """
        params = {"project": project}
        url = "/async_query/{query_id}/result_path".format(query_id=query_id)
        resp = self._request("GET", url, params=params)
        return resp

    def delete_async_query(self, project=None, older_than=None):
        """
        delete async query
        :param project: str, name of project
        :param older_than: str, fmt is yyyy-MM-dd HH:mm:ss, hdfs older than will be delete
        :return:
        """
        params = {"project": project, "older_than": older_than}
        url = "/async_query/"
        resp = self._request("DELETE", url, params=params)
        return resp

    def delete_async_query_by_query_id(self, project, query_id):
        """
        delete async query by query id
        :param project: str, name of project
        :param query_id: str, query id
        :return:
        """
        params = {"project": project}
        url = "/async_query/{query_id}".format(query_id=query_id)
        resp = self._request("DELETE", url, params=params)
        return resp

    def async_result_download(
        self, project, query_id, include_header=False, stream=True
    ):
        """
        async result download
        :param project: str, name of project
        :param query_id: str, id of query
        :param include_header: bool, is include table header, default is False
        :param stream: bool, request type
        :return:
        """
        params = {"project": project, "include_header": include_header}
        url = "/async_query/{query_id}/result_download".format(query_id=query_id)
        resp = self._request("GET", url, params=params, stream=stream)
        return resp

    def async_query_metadata(self, project, query_id):
        """
        async query metadata
        :param project: str, name of project
        :param query_id: str, id of async query
        :return:
        """
        params = {"project": project}
        url = "/async_query/{query_id}/metadata".format(query_id=query_id)
        resp = self._request("GET", url, params=params)
        return resp

    def async_query_file_status(self, project, query_id):
        """
        async query file status
        :param project: str, name of project
        :param query_id: str, id of async query
        :return:
        """
        params = {"project": project}
        url = "/async_query/{query_id}/file_status".format(query_id=query_id)
        resp = self._request("GET", url, params=params)
        return resp

    def update_project_config(self, project, key, value):
        """
        :param project: str, name of project
        :param key: config key
        :param value: config value
        :return:
        """
        payload = {key: value}
        url = "/projects/{project}/config".format(project=project)
        resp = self._request("PUT", url, json=payload)
        return resp

    def download_query_diag(self, project, server, query_id):
        """
        download diagnostic package
        :param project: str, name of project
        :param server: str, download diagnostic package at service
        :param query_id: str, id of query
        :return:
        """
        url = "/system/diag/query?host=http://{server}".format(server=server)
        payload = {"project": project, "query_id": query_id}
        resp = self._request("POST", url, json=payload)
        return resp

    def decorate_request(self):
        self._request = self.sleep_after_request(self._request)
        logging.debug(f"[kylin]Decorate 'self._request' function")

    def project_kerberos_info(self, project, principal_name, keytab_file):
        """
        param principal_name: str, kerberos keytab principal name
        param keytab_file: binary, kerberos keytab file
        """
        headers = copy.deepcopy(self._headers)
        headers.pop("Content-Type")
        url = f"/projects/{project}/project_kerberos_info"
        payload = {
            "principal": principal_name,
        }

        resp = self._request(
            "PUT", data=payload, url=url, headers=headers, files=keytab_file
        )
        return resp

    @staticmethod
    def sleep_after_request(func):
        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if args[0] != "GET":
                time.sleep(2)

            return result

        return _wrapper


def string_to_ts(st):
    # convert date/timestamp pattern string to long, supports "YYYY-mm-dd" and "YYYY-mm-dd HH:MM:SS"
    try:
        if ":" in st:
            ts = (
                datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")
                .replace(tzinfo=datetime.timezone(datetime.timedelta(hours=8)))
                .timestamp()
            )
        else:
            ts = (
                datetime.datetime.strptime(st, "%Y-%m-%d")
                .replace(tzinfo=datetime.timezone(datetime.timedelta(hours=8)))
                .timestamp()
            )
        return int(ts) * 1000
    except:  # pylint: disable=W0702
        print("Failed to convert current string %s" % st)
        return 0


def connect(**conf):
    _host = conf.get("host")
    _port = conf.get("port")
    _username = conf.get("username")
    _password = conf.get("password")
    logging.info(f"Connect to Kylin Http Server {_host}, {_port}, user {_username}")
    return KylinHttpClient(_host, _port, _username, _password)
