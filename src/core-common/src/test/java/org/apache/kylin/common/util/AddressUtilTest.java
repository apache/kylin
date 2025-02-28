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
package org.apache.kylin.common.util;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;

import lombok.val;

@MetadataInfo(onlyProps = true)
class AddressUtilTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final DefaultHostInfoFetcher hostInfoFetcher = new DefaultHostInfoFetcher();

    @Test
    void testGetLocalInstance() {
        val localAddress = AddressUtil.getLocalInstance();
        Assertions.assertTrue(localAddress.endsWith(getTestConfig().getServerPort()));
    }

    @Test
    void testGetZkLocalInstance() {
        val localHost = AddressUtil.getZkLocalInstance();
        Assertions.assertTrue(localHost.endsWith(getTestConfig().getServerPort()));
    }

    @Test
    void testConvertHost() {
        val host = AddressUtil.convertHost("localhost:7070");
        Assertions.assertEquals("127.0.0.1:7070", host);
        Assertions.assertEquals("127.0.0.1:7070", AddressUtil.convertHost("unknown:7070"));
    }

    @Test
    void testGetMockPortAddress() {
        val mockAddr = AddressUtil.getMockPortAddress();
        Assertions.assertTrue(mockAddr.endsWith(AddressUtil.MAINTAIN_MODE_MOCK_PORT));

    }

    @Test
    void testGetLocalServerInfo() {
        val servInfo = AddressUtil.getLocalServerInfo();
        Assertions.assertTrue(servInfo.startsWith(hostInfoFetcher.getHostname().replaceAll("[^(_a-zA-Z0-9)]", "")));
    }

    @Test
    void testGetLocalHostExactAddress() {
        val old = getTestConfig().getServerIpAddress();
        val mockIp = "192.168.1.101";
        getTestConfig().setProperty("kylin.env.ip-address", mockIp);
        AddressUtil.clearLocalIpAddressCache();
        val servIp = AddressUtil.getLocalHostExactAddress();
        Assertions.assertEquals(mockIp, servIp);
        if (!StringUtils.isEmpty(old)) {
            getTestConfig().setProperty("kylin.env.ip-address", old);
        }
    }

    @Test
    void testCheckHost() {
        AddressUtil.validateHost("127.0.0.1:7070");
        AddressUtil.validateHost("127.0.0.1");
        AddressUtil.validateHost("707");
        AddressUtil.validateHost("");
        testCheckHostFail("127.0.0.1:");
        testCheckHostFail(":7070");
        testCheckHostFail("127.0.0.1:7070>");
    }

    void testCheckHostFail(String host) {
        try {
            AddressUtil.validateHost(host);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertInstanceOf(IllegalArgumentException.class, e);
            Assertions.assertTrue(e.getMessage().contains("Url contains disallowed chars, host: "));
        }
    }

    @Test
    void testIsSameHost() {
        Assertions.assertTrue(AddressUtil.isSameHost(hostInfoFetcher.getHostname()));
        Assertions.assertFalse(AddressUtil.isSameHost("unknown"));
    }
}
