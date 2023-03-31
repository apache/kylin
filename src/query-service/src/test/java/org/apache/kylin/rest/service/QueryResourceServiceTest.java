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

package org.apache.kylin.rest.service;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.spark.ExecutorAllocationClient;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import lombok.val;
import lombok.var;
import scala.collection.JavaConverters;

public class QueryResourceServiceTest extends NLocalFileMetadataTestCase {

    private SparkSession ss;
    @InjectMocks
    private final QueryResourceService queryResourceService = Mockito.spy(new QueryResourceService());

    @Mock
    private ExecutorAllocationClient client;

    @Before
    public void setUp() throws Exception {
        System.setProperty("SPARK_LOCAL_IP", "localhost");
        MockitoAnnotations.initMocks(this);
        createTestMetadata();
        ss = SparkSession.builder().appName("local").master("local[1]").getOrCreate();
        SparderEnv.setSparkSession(ss);
        SparderEnv.setExecutorAllocationClient(client);
        Mockito.doReturn(true).when(client).isExecutorActive(Mockito.anyString());
        ss.range(1, 10).createOrReplaceTempView("queryResourceServiceTest");
        val data = ss.sql("SELECT id,count(0) FROM queryResourceServiceTest group by id");
        data.persist();
        data.show();
        data.unpersist();
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        ss.stop();
    }

    @Test
    public void testAdjustQueryResource() {
        Assert.assertTrue(queryResourceService.isAvailable());
        QueryResourceService.QueryResource queryResource = new QueryResourceService.QueryResource();

        queryResource.setInstance(1);
        var resource = queryResourceService.adjustQueryResource(queryResource);
        Assert.assertEquals(0, resource.getInstance());
        Mockito.doReturn(true).when(client).requestExecutors(Mockito.anyInt());
        resource = queryResourceService.adjustQueryResource(queryResource);
        Assert.assertEquals(1, resource.getInstance());

        queryResource.setInstance(-1);
        val seqs = JavaConverters.asScalaBuffer(Lists.newArrayList("1")).toSeq();
        Mockito.doReturn(seqs).when(client).getExecutorIds();
        Assert.assertEquals(1, queryResourceService.getExecutorSize());
        Mockito.doReturn(seqs).when(client).killExecutors(Mockito.any(), Mockito.anyBoolean(), Mockito.anyBoolean(),
                Mockito.anyBoolean());
        resource = queryResourceService.adjustQueryResource(queryResource);
        Assert.assertEquals(1, resource.getInstance());

        queryResource.setInstance(0);
        queryResource.setForce(true);
        resource = queryResourceService.adjustQueryResource(queryResource);
        Assert.assertEquals(0, resource.getInstance());
    }
}
