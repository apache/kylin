# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'

from rest.apis import KylinRestApi
from models.job import JobInstance


class CubeBuildJob:
    @staticmethod
    def rebuild_cube(cube_name, job_build_request):
        job_instance = None

        try:
            Kylin_rest_api = KylinRestApi()
            response = Kylin_rest_api.http_put('cubes/' + cube_name + '/rebuild', '',
                                               payload=job_build_request.to_json())

            if KylinRestApi.is_response_ok(response):
                job_instance = JobInstance.from_json(response.json())
            else:
                print response.json()
        except Exception, ex:
            print ex

        return job_instance

    @staticmethod
    def get_job(job_uuid):
        job_instance = None

        try:
            kylin_rest_api = KylinRestApi()
            response = kylin_rest_api.http_get('jobs/' + job_uuid, '')

            if KylinRestApi.is_response_ok(response):
                job_instance = JobInstance.from_json(response.json())
            else:
                print response.json()
        except Exception, ex:
            pass

        return job_instance

    @staticmethod
    def cancel_job(job_uuid):
        job_instance = None

        try:
            kylin_rest_api = KylinRestApi()
            response = kylin_rest_api.http_put('jobs/' + job_uuid + '/cancel', '')

            if KylinRestApi.is_response_ok(response):
                job_instance = JobInstance.from_json(response.json())
            else:
                print response.json()
        except Exception, ex:
            pass

        return job_instance

    @staticmethod
    def resume_job(job_uuid):
        job_instance = None

        try:
            kylin_rest_api = KylinRestApi()
            response = kylin_rest_api.http_put('jobs/' + job_uuid + '/resume', '')

            if KylinRestApi.is_response_ok(response):
                job_instance = JobInstance.from_json(response.json())
            else:
                print response.json()
        except Exception, ex:
            pass

        return job_instance
