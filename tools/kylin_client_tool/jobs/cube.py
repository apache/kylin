# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'

from models.request import CubeRequest, JobListRequest
from models.cube import CubeInstance
from models.job import JobInstance
from rest.apis import KylinRestApi


class CubeJob:
    RUNNING_JOB_STATUS = [0, 1, 2]
    FINISHED_JOB_STATUS = [4]
    ERROR_JOB_STATUS = [8]
    DISCARDED_JOB_STATUS = [16]

    @staticmethod
    def create_cube(cube_desc, model_desc, project=None):
        cube_request_result = None

        try:
            # set last modified time to 0
            cube_desc.last_modified = 0
            cube_request = CubeRequest.get_cube_request_from_cube_desc(cube_desc, model_desc, project)

            kylin_rest_api = KylinRestApi()
            response = kylin_rest_api.http_post('cubes', '', payload=cube_request.to_json())

            if KylinRestApi.is_response_ok(response):
                cube_request_result = CubeRequest.from_json(response.json())
                # set result to null if the operation is not successful
                if not cube_request_result.successful:
                    cube_request_result = None
                    print response.json()
            else:
                print response.json()
                # cube_request = None
        except Exception, ex:
            pass

        return cube_request_result

    @staticmethod
    def update_cube(cube_desc, model_desc, project=None):
        cube_request_result = None

        try:
            cube_request = CubeRequest.get_cube_request_from_cube_desc(cube_desc, model_desc, project)

            kylin_rest_api = KylinRestApi()
            response = kylin_rest_api.http_put('cubes', '', payload=cube_request.to_json())

            if KylinRestApi.is_response_ok(response):
                cube_request_result = CubeRequest.from_json(response.json())
                # set result to null if the operation is not successful
                if not cube_request_result.successful:
                    cube_request_result = None
                    print response.json()
            else:
                print response.json()
                # cube_request = None
        except Exception, ex:
            pass

        return cube_request_result

    @staticmethod
    def delete_cube(cube_name):
        status = None

        try:
            kylin_rest_api = KylinRestApi()
            response = kylin_rest_api.http_delete('cubes/' + cube_name, '')
            # print response.json()

            if KylinRestApi.is_response_ok(response):
                status = 0
            elif response is not None and response.json() and "not found" in str(response.json()):
                status = 0
            else:
                print response.json()
                # cube_request = None
        except Exception, ex:
            pass

        return status

    @staticmethod
    def disable_cube(cube_name):
        status = None

        try:
            kylin_rest_api = KylinRestApi()
            response = kylin_rest_api.http_put('cubes/' + cube_name + '/disable', '')

            if KylinRestApi.is_response_ok(response):
                status = 0
            elif response is not None and response.json() and "is DISABLED" in str(response.json()):
                status = 0
            else:
                print response.json()
                # cube_request = None
        except Exception, ex:
            pass

        return status

    @staticmethod
    def enable_cube(cube_name):
        status = None

        try:
            kylin_rest_api = KylinRestApi()
            response = kylin_rest_api.http_put('cubes/' + cube_name + '/enable', '')

            if KylinRestApi.is_response_ok(response):
                status = 0
            elif response is not None and response.json() and "is READY" in str(response.json()):
                status = 0
            else:
                print response.json()
                # cube_request = None
        except Exception, ex:
            pass

        return status

    @staticmethod
    def get_cube_job(cube_name, status_list=None):
        job_list_req = JobListRequest()
        job_list_req.cubeName = cube_name
        job_list_req.limit = 10000
        job_list_req.offset = 0
        job_list_req.status = status_list

        job_instance_list = []

        try:
            kylin_rest_api = KylinRestApi()
            response = kylin_rest_api.http_get('jobs/', job_list_req.to_query_string())

            if KylinRestApi.is_response_ok(response):
                job_instance_list = [JobInstance.from_json(json_dict) for json_dict in response.json()]
            else:
                print response.json()
        except Exception, ex:
            pass

        return job_instance_list

    @staticmethod
    def list_cubes(cube_name=None, project_name=None):
        offset, limit = 0, 10000
        query_string = '' + ('cubeName=' + cube_name + '&' if cube_name else '') + \
                       ('projectName=' + project_name + '&' if project_name else '') + \
                       ('offset=' + str(offset) + '&') + ('limit=' + str(limit) + '&')
        cube_instance_list = []

        try:
            kylin_rest_api = KylinRestApi()

            response = kylin_rest_api.http_get('cubes/', query_string)

            if KylinRestApi.is_response_ok(response):
                cube_instance_list = [CubeInstance.from_json(json_dict) for json_dict in response.json()]
            else:
                print response.json()
        except Exception, ex:
            pass

        return cube_instance_list

    @staticmethod
    def update_cube_cost(cube_name, cost):
        cube_instance = None

        try:
            kylin_rest_api = KylinRestApi()
            response = kylin_rest_api.http_put('cubes/' + cube_name + '/cost', 'cost=' + str(cost))

            if KylinRestApi.is_response_ok(response):
                cube_instance = CubeInstance.from_json(response.json())
            else:
                print response.json()
        except Exception, ex:
            pass

        return cube_instance
