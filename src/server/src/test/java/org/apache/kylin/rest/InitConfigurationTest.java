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
package org.apache.kylin.rest;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.DefaultHostInfoFetcher;
import org.apache.kylin.common.util.HostInfoFetcher;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

@MetadataInfo(onlyProps = true)
class InitConfigurationTest {
    private InitConfiguration configuration = new InitConfiguration();

    @Test
    void testInit() {
        HostInfoFetcher hostInfoFetcher = Mockito.spy(new DefaultHostInfoFetcher());
        ReflectionTestUtils.setField(configuration, "hostInfoFetcher", hostInfoFetcher);
        Mockito.when(hostInfoFetcher.getHostname()).thenReturn("ke_host");
        KylinConfig.getInstanceFromEnv().setProperty("kylin.env.hostname-check-enabled", "false");
        try {
            configuration.init();
        } catch (KylinException e) {
            Assert.fail();
        }
        KylinConfig.getInstanceFromEnv().setProperty("kylin.env.hostname-check-enabled", "true");
        Mockito.when(hostInfoFetcher.getHostname()).thenReturn("ke-host");
        try {
            configuration.init();
        } catch (KylinException e) {
            Assert.fail();
        }
        Mockito.when(hostInfoFetcher.getHostname()).thenReturn("ke_host");
        Assert.assertThrows(KylinRuntimeException.class, () -> configuration.init());
    }
}
