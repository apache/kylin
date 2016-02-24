# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'

import os

WORKING_DIR = os.path.dirname(os.path.dirname(__file__))
PID_PATH = os.path.join(WORKING_DIR, 'kylin_client_tool.pid')

KYLIN_USER = 'ADMIN'
KYLIN_PASSWORD = 'KYLIN'
KYLIN_REST_HOST = 'http://123.103.21.35'
KYLIN_REST_PORT = 7070
KYLIN_REST_PATH_PREFIX = '/kylin/api'
KYLIN_JOB_MAX_COCURRENT = 3
KYLIN_JOB_MAX_RETRY = 1
