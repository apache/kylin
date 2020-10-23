#!/usr/bin/env bash

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

### Thank you for https://github.com/apache/spark/tree/master/dev/create-release .

# docker build -f Dockerfile -t apachekylin/release-machine:jdk8-slim .
# docker run --name machine apachekylin/release-machine:jdk8-slim

cat > $ENVFILE <<EOF
DRY_RUN=$DRY_RUN
SKIP_TAG=$SKIP_TAG
RUNNING_IN_DOCKER=1
GIT_BRANCH=$GIT_BRANCH
NEXT_VERSION=$NEXT_VERSION
RELEASE_VERSION=$RELEASE_VERSION
RELEASE_TAG=$RELEASE_TAG
GIT_REF=$GIT_REF
ASF_USERNAME=$ASF_USERNAME
GIT_NAME=$GIT_NAME
GIT_EMAIL=$GIT_EMAIL
GPG_KEY=$GPG_KEY
ASF_PASSWORD=$ASF_PASSWORD
GPG_PASSPHRASE=$GPG_PASSPHRASE
RELEASE_STEP=$RELEASE_STEP
USER=$USER
EOF


docker run -ti \
  --env-file "$ENVFILE" \
  apachekylin/release-machine:jdk8-slim

docker cp machine:/root/kylin/dist/apache-kylin-*-SNAPSHOT-bin.tar.gz .