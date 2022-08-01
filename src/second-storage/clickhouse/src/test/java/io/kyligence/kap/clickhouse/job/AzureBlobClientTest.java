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

import java.net.URISyntaxException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

import lombok.val;

public class AzureBlobClientTest {

    @After
    public void teardown() {
        Mockito.clearAllCaches();
    }

    @Test
    public void getBlob() throws URISyntaxException, StorageException {
        val container = Mockito.mock(CloudBlobContainer.class);
        val blob = Mockito.mock(CloudBlockBlob.class);
        val client = new AzureBlobClient(null, null);
        Mockito.when(container.getBlockBlobReference(Mockito.anyString())).thenReturn(blob);
        // support hadoop-azure 2.10.1
        Mockito.when(container.getBlobReferenceFromServer(Mockito.anyString())).thenReturn(blob);
        val result = client.getBlob(container, "/test/a.txt");
        Assert.assertEquals(blob, result);
    }
}