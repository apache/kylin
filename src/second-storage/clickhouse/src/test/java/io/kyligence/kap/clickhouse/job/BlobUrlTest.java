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

package io.kyligence.kap.clickhouse.job;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

import lombok.val;
import org.mockito.Mockito;

public class BlobUrlTest {

    @After
    public void teardown() {
        Mockito.clearAllCaches();
    }

    @Test
    public void fromHttpUrl() {
        // case 1
        String accountKey = "testkey";
        val config = Maps.<String, String>newHashMap();
        config.put("fs.azure.account.key.account.blob.core.chinacloudapi.cn", accountKey);
        HadoopMockUtil.mockGetConfiguration(config);
        BlobUrl blobUrl = BlobUrl.fromHttpUrl("https://account.blob.core.chinacloudapi.cn/container/blob.parquet");
        Assert.assertEquals("account", blobUrl.getAccountName());
        Assert.assertEquals("blob.core.chinacloudapi.cn", blobUrl.getHostSuffix());
        Assert.assertEquals("container", blobUrl.getContainer());
        Assert.assertEquals("https", blobUrl.getHttpSchema());
        Assert.assertEquals("wasbs", blobUrl.getBlobSchema());
        Assert.assertEquals("/blob.parquet", blobUrl.getPath());
        Assert.assertEquals(accountKey, blobUrl.getAccountKey());

        // case2
        BlobUrl blobUrl2 = BlobUrl.fromHttpUrl("http://account.blob.core.chinacloudapi.cn/container/blob.parquet");
        Assert.assertEquals("wasb", blobUrl2.getBlobSchema());
        Assert.assertEquals("http://account.blob.core.chinacloudapi.cn", blobUrl2.getHttpEndpoint());
    }

    @Test
    public void fromBlobUrl() {
        // case1
        String accountKey = "testkey";
        val config = Maps.<String, String>newHashMap();
        config.put("fs.azure.account.key.account.blob.core.chinacloudapi.cn", accountKey);
        HadoopMockUtil.mockGetConfiguration(config);
        BlobUrl blobUrl = BlobUrl.fromBlobUrl("wasbs://container@account.blob.core.chinacloudapi.cn/blob.parquet");
        Assert.assertEquals("account", blobUrl.getAccountName());
        Assert.assertEquals("blob.core.chinacloudapi.cn", blobUrl.getHostSuffix());
        Assert.assertEquals("container", blobUrl.getContainer());
        Assert.assertEquals("https", blobUrl.getHttpSchema());
        Assert.assertEquals("wasbs", blobUrl.getBlobSchema());
        Assert.assertEquals("/blob.parquet", blobUrl.getPath());
        Assert.assertEquals(accountKey, blobUrl.getAccountKey());

        // case2
        BlobUrl blobUrl2 = BlobUrl.fromBlobUrl("wasb://container@account.blob.core.chinacloudapi.cn/blob.parquet");
        Assert.assertEquals("http", blobUrl2.getHttpSchema());
        Assert.assertEquals("DefaultEndpointsProtocol=http;AccountName=account;AccountKey=testkey;EndpointSuffix=core.chinacloudapi.cn", blobUrl2.getConnectionString());
    }
}