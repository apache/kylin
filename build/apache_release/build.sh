#!/bin/bash

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

dir=$(dirname ${0})

cd ${dir}/../..

echo 'Build back-end'
echo "-----SKIP------"
## mvn clean install -DskipTests -Dcheckstyle.skip $@ || { exit 1; }

#package webapp
echo 'Build front-end'
if [ "${WITH_FRONT}" = "1" ]; then
    cd kystudio
    echo 'Install front-end dependencies'
    npm install  || { exit 1; }
    echo 'Install front-end end'
    npm run build		 || { exit 1; }
    echo 'build front-end dist end'
fi

