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
docker rm release-machine
docker image rm release-machine:latest

# if you do not want to build from "apachekylin/release-machine:5.0-base",
# please remove comments of following code
#build_status='1'
#while [ "$build_status" != "0" ]
#do
#  echo "Build release-machine from $(date)"
#  docker build -f Dockerfile_1 -t release-machine:5.0-base .
#  build_status="$?"
#done

docker build -f Dockerfile_2 -t release-machine:latest .
docker image tag release-machine:latest apachekylin/release-machine:latest
#docker push apachekylin/release-machine:latest

#docker login -u xiaoxiangyu
#docker push apachekylin/release-machine:latest

# docker run --name kylin-rm --hostname kylin-rm -p 7070:7070 \
#   -i -t apachekylin/release-machine:latest  bash
