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
#

import os

CUR_DIR = os.path.dirname(__file__)
BACKUP_PATH = os.path.join(CUR_DIR, '..', 'backup')
JARS_PATH = os.path.join(BACKUP_PATH, 'jars')
DEMOS_PATH = os.path.join(BACKUP_PATH, 'demos')
SCRIPTS_PATH = os.path.join(BACKUP_PATH, 'scripts')
TARS_PATH = os.path.join(BACKUP_PATH, 'tars')
KYLIN_PROPERTIES_DIR = os.path.join(BACKUP_PATH, 'properties')
KYLIN_PROPERTIES_TEMPLATES_DIR = os.path.join(KYLIN_PROPERTIES_DIR, 'mode_templates')

KYLIN_PROPERTIES_TEMPLATE_DIR = os.path.join(KYLIN_PROPERTIES_DIR, '{cluster_num}')
PROPERTIES_TEMPLATE_DIR = os.path.join(KYLIN_PROPERTIES_DIR, 'templates')

TEMPLATE_OF_KYLIN_PROPERTIES = 'kylin.properties.template'
RENDERED_FILE = '.rendered'

ROOT_DIR = os.path.join(CUR_DIR, '..')
TEMPLATES_PATH = os.path.join(ROOT_DIR, 'cloudformation_templates')
