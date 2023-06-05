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

package org.apache.kylin.tool.setup;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
class KapGetClusterInfoTest {

    @Test
    void testGetYarnMetrics() {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.job.yarn-app-rest-check-status-url",
                "http://127.0.0.1:8080`echo`");
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            KapGetClusterInfo getClusterInfo = new KapGetClusterInfo();
            getClusterInfo.getYarnMetrics();
        });
    }
}
