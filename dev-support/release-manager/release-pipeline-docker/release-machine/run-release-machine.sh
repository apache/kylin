#!/bin/bash

#
# /*
#  * Licensed to the Apache Software Foundation (ASF) under one
#  * or more contributor license agreements.  See the NOTICE file
#  * distributed with this work for additional information
#  * regarding copyright ownership.  The ASF licenses this file
#  * to you under the Apache License, Version 2.0 (the
#  * "License"); you may not use this file except in compliance
#  * with the License.  You may obtain a copy of the License at
#  *
#  *     http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  */
#

docker stop release-machine
docker container rm release-machine
bash create-release-machine.sh


RM_WORKDIR=~/release-manager/working-dir
SOURCE_DIR=~/release-manager/kylin-folder
source ~/.apache_secret.sh
mkdir -p $RM_WORKDIR

docker run -d \
    --name release-machine \
    --hostname release-machine \
    --interactive \
    --volume $RM_WORKDIR:/root/release-manager \
    --volume $SOURCE_DIR:/root/release-manager/kylin-folder \
    --env GIT_USERNAME='XiaoxiangYu' \
    --env GPG_KEY='$GPG_KEY' \
    --env GPG_PASSPHRASE='$GPG_PASSPHRASE' \
    --env ASF_USERNAME='$ASF_USERNAME' \
    --env ASF_PASSWORD='$ASF_PASSWORD' \
    --env GIT_BRANCH='kylin5' \
    --env GITHUB_UID='hit-lacus' \
    --env RELEASE_VERSION='5.0.0' \
    --env NEXT_RELEASE_VERSION='5.0.1' \
    --env RC_NUMBER=0 \
    --env GIT_EMAIL='xxyu@apache.org' \
    -p 4040:4040 \
    --tty \
    release-machine:latest \
    bash
docker logs --follow release-machine

docker exec -it release-machine bash