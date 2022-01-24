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

import logging

from instances.kylin_instance import KylinInstance

logger = logging.getLogger(__name__)


class KylinUtils:

    @staticmethod
    def is_kylin_accessible(kylin_address: str, port: str = '7070') -> bool:
        kylin = KylinInstance(host=kylin_address, port=port)
        try:
            assert kylin.client.await_kylin_start(
                check_action=kylin.client.check_login_state,
                timeout=1800,
                check_times=3
            )
        except AssertionError:
            logger.error('Check kylin status failed, please check the public ip whether is opened.')
            return False
        return True
