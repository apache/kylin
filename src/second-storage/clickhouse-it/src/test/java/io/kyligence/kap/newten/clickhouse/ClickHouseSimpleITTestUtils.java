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
package io.kyligence.kap.newten.clickhouse;

import java.util.List;
import java.util.Optional;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.junit.jupiter.api.Assertions;

import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;

public class ClickHouseSimpleITTestUtils {

    public static void checkLockOperateResult(EnvelopeResponse response, List<String> expectedLocks, String project) {
        Assertions.assertEquals("000", response.getCode());
        Optional<Manager<NodeGroup>> nodeGroupManager = SecondStorageUtil
                .nodeGroupManager(KylinConfig.getInstanceFromEnv(), project);
        List<NodeGroup> nodeGroups = nodeGroupManager.get().listAll();
        nodeGroups.stream().forEach(x -> {
            Assertions.assertIterableEquals(x.getLockTypes(), expectedLocks);
        });
    }
}
