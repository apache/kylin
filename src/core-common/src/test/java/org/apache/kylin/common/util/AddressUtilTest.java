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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;

import lombok.val;

@MetadataInfo(onlyProps = true)
public class AddressUtilTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private DefaultHostInfoFetcher hostInfoFetcher = new DefaultHostInfoFetcher();

    @Test
    public void testGetLocalInstance() {
        val localAddress = AddressUtil.getLocalInstance();
        Assert.assertTrue(localAddress.endsWith(getTestConfig().getServerPort()));
    }

    @Test
    public void testGetZkLocalInstance() {
        val localHost = AddressUtil.getZkLocalInstance();
        Assert.assertTrue(localHost.endsWith(getTestConfig().getServerPort()));
    }

    @Test
    public void testConvertHost() {
        val host = AddressUtil.convertHost("localhost:7070");
        Assert.assertEquals("127.0.0.1:7070", host);
        Assert.assertEquals("127.0.0.1:7070", AddressUtil.convertHost("unknown:7070"));
    }

    @Test
    public void testGetMockPortAddress() {
        val mockAddr = AddressUtil.getMockPortAddress();
        Assert.assertTrue(mockAddr.endsWith(AddressUtil.MAINTAIN_MODE_MOCK_PORT));

    }

    @Test
    public void testGetLocalServerInfo() {
        val servInfo = AddressUtil.getLocalServerInfo();
        Assert.assertTrue(servInfo.startsWith(hostInfoFetcher.getHostname().replaceAll("[^(_a-zA-Z0-9)]", "")));
    }

    @Test
    public void testGetLocalHostExactAddress() {
        val old = getTestConfig().getServerIpAddress();
        val mockIp = "192.168.1.101";
        getTestConfig().setProperty("kylin.env.ip-address", mockIp);
        val servIp = AddressUtil.getLocalHostExactAddress();
        Assert.assertEquals(servIp, mockIp);
        if (!StringUtils.isEmpty(old)) {
            getTestConfig().setProperty("kylin.env.ip-address", old);
        }
    }

    @Test
    public void testIsSameHost() {
        Assert.assertTrue(AddressUtil.isSameHost(hostInfoFetcher.getHostname()));
        Assert.assertFalse(AddressUtil.isSameHost("unknown"));
    }
}
