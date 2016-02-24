# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'

from rest.apis import KylinRestApi


class AdminJob:
    @staticmethod
    def get_env():
        status = None

        try:
            kylin_rest_api = KylinRestApi()
            response = kylin_rest_api.http_get('admin/env', '')

            if KylinRestApi.is_response_ok(response):
                status = 0
            # elif response is not None and response.json() and "does not exist" in str(response.json()):
            #     status = 0
            else:
                print response.json()
                # cube_request = None
        except Exception, ex:
            pass

        return response

    @staticmethod
    def get_conf():
        status = None

        try:
            kylin_rest_api = KylinRestApi()
            response = kylin_rest_api.http_get('admin/config', '')

            if KylinRestApi.is_response_ok(response):
                status = 0
            # elif response is not None and response.json() and "does not exist" in str(response.json()):
            #     status = 0
            else:
                print response.json()
                # cube_request = None
        except Exception, ex:
            pass

        return response

    @staticmethod
    def cleanup_storage():
        status = None

        try:
            kylin_rest_api = KylinRestApi()
            response = kylin_rest_api.http_delete('admin/storage', '')

            if KylinRestApi.is_response_ok(response):
                status = 0
            # elif response is not None and response.json() and "does not exist" in str(response.json()):
            #     status = 0
            else:
                print response.json()
                # cube_request = None
        except Exception, ex:
            pass

        return status
