# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'

import json
import sys
import requests
from requests.auth import HTTPBasicAuth
from settings import settings


class KylinRestApi:
    cooikes = None

    def __init__(self):
        self.host = settings.KYLIN_REST_HOST
        self.port = settings.KYLIN_REST_PORT
        self.user = settings.KYLIN_USER
        self.password = settings.KYLIN_PASSWORD
        self.rest_path_prefix = settings.KYLIN_REST_PATH_PREFIX

        if not KylinRestApi.cooikes:
            KylinRestApi.cooikes = KylinRestApi.login(self)

        if not KylinRestApi.cooikes:
            print "can't set cookies, exiting..."
            sys.exit(1)

    @staticmethod
    def login(kylin_rest_api):
        if kylin_rest_api.user and kylin_rest_api.password:
            # auth and get back cookies
            headers = {}
            headers['content-type'] = 'application/json'
            req_response = requests.post(kylin_rest_api.get_api_url('user/authentication', ''), \
                                         auth=HTTPBasicAuth(kylin_rest_api.user, kylin_rest_api.password))
            return req_response.cookies

        return None

    @staticmethod
    def is_response_ok(response):
        return str(response.status_code) == '200'

    def get_api_url(self, uri, query_string):
        return self.host + ':' + str(self.port) + self.rest_path_prefix \
               + '/' + uri + '?' + query_string

    def http_get(self, uri, query_string, headers=None):
        api_url = self.get_api_url(uri, query_string)

        headers = headers if headers and type(headers) == dict else {}
        headers['content-type'] = 'application/json'

        req_response = requests.get(api_url, headers=headers, cookies=KylinRestApi.cooikes)

        return req_response

    def http_post(self, uri, query_string, headers=None, payload=None):
        api_url = self.get_api_url(uri, query_string)

        headers = headers if headers and type(headers) == dict else {}
        headers['content-type'] = 'application/json'

        if payload:
            data = payload if type(payload) == str else json.dumps(payload)
            req_response = requests.post(api_url, data=data, headers=headers, cookies=KylinRestApi.cooikes)
        else:
            req_response = requests.post(api_url, headers=headers, cookies=KylinRestApi.cooikes)

        return req_response

    def http_put(self, uri, query_string, headers=None, payload=None):
        api_url = self.get_api_url(uri, query_string)

        headers = headers if headers and type(headers) == dict else {}
        headers['content-type'] = 'application/json'

        if payload:
            data = payload if type(payload) == str else json.dumps(payload)
            req_response = requests.put(api_url, data=data, headers=headers, cookies=KylinRestApi.cooikes)
        else:
            req_response = requests.put(api_url, headers=headers, cookies=KylinRestApi.cooikes)

        return req_response

    def http_delete(self, uri, query_string, headers=None, payload=None):
        api_url = self.get_api_url(uri, query_string)

        headers = headers if headers and type(headers) == dict else {}
        headers['content-type'] = 'application/json'

        # print payload

        if payload:
            data = payload if type(payload) == str else json.dumps(payload)
            req_response = requests.delete(api_url, data=data, headers=headers, cookies=KylinRestApi.cooikes)
        else:
            req_response = requests.delete(api_url, headers=headers, cookies=KylinRestApi.cooikes)

        # print str(req_response.json())

        return req_response
