/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.init;

import org.apache.kylin.common.util.RestServerRegister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegistryDiscoveryInitialTask extends InitialTask {

    private static final Logger LOG = LoggerFactory.getLogger(RegistryDiscoveryInitialTask.class);

    @Override
    public void execute() {

        LOG.info(" rest server based on zookeeper is starting");
        RestServerRegister register = RestServerRegister.getInstance();

        try {
            register.register();
        } catch (Exception e) {
            LOG.error("registry client based on zookeeper register to zookeeper occur error ", e);
            throw new RuntimeException(e);
        }
    }
}
