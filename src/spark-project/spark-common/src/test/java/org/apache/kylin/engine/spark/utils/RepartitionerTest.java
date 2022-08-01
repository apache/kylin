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

package org.apache.kylin.engine.spark.utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.hadoop.fs.ContentSummary;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class RepartitionerTest {

    @Test
    public void testNeedRepartitionForFileSize() {
        ContentSummary sc = mock(ContentSummary.class);
        when(sc.getFileCount()).thenReturn(2L);
        when(sc.getLength()).thenReturn(1024 * 1024L);
        Repartitioner helper = new Repartitioner(2, 1, 1000L, 1000L, sc, null, null, true);
        Assert.assertTrue(helper.needRepartitionForFileSize());
        Assert.assertFalse(helper.needRepartitionForShardByColumns());
        Assert.assertTrue(helper.needRepartition());
    }

    @Test
    public void testNeedRepartitionForFileSize_only1File() {
        ContentSummary sc = mock(ContentSummary.class);
        when(sc.getFileCount()).thenReturn(1L);
        when(sc.getLength()).thenReturn(512 * 1024L);
        Repartitioner helper = new Repartitioner(2, 1, 1000L, 1000L, sc, null, null, true);
        Assert.assertFalse(helper.needRepartitionForFileSize());
        Assert.assertFalse(helper.needRepartitionForShardByColumns());
        Assert.assertFalse(helper.needRepartition());
    }

    @Test
    public void testNeedRepartitionForShardByColumns() {
        List<Integer> shardByColumns = Lists.newArrayList(1);
        ContentSummary sc = mock(ContentSummary.class);
        when(sc.getFileCount()).thenReturn(2L);
        when(sc.getLength()).thenReturn(3 * 1024 * 1024L);
        Repartitioner helper = new Repartitioner(2, 1, 1000L, 1000L, sc, shardByColumns, null, true);
        Assert.assertTrue(helper.needRepartitionForShardByColumns());
        Assert.assertFalse(helper.needRepartitionForFileSize());
        Assert.assertTrue(helper.needRepartition());
    }

    @Test
    public void testGetRepartitionNum() {
        ContentSummary sc = mock(ContentSummary.class);
        when(sc.getFileCount()).thenReturn(6L);
        when(sc.getLength()).thenReturn(4 * 1024 * 1024L);
        Repartitioner helper = new Repartitioner(2, 1, 1000L, 1000L, sc, null, null, true);
        Assert.assertEquals(2, helper.getRepartitionNumByStorage());
    }

    @Test
    public void testRowCountNum() {
        ContentSummary sc = mock(ContentSummary.class);
        when(sc.getFileCount()).thenReturn(6L);
        when(sc.getLength()).thenReturn(4 * 1024 * 1024L);
        Repartitioner helper = new Repartitioner(2, 1, 2000L, 500L, sc, null, null, true);
        // size = (4M/2M - 2000/500)/2 + min(4M/2M)
        Assert.assertEquals(3, helper.getRepartitionNumByStorage());
    }

    @Test
    public void testRepartitionNumEqualsCurrentPartitionNum() {
        ContentSummary sc = mock(ContentSummary.class);
        when(sc.getFileCount()).thenReturn(3L);
        when(sc.getLength()).thenReturn(4 * 1024 * 1024L);
        Repartitioner helper = new Repartitioner(2, 2, 1500L, 500L, sc, null, null, true);
        // size = 2000/500
        Assert.assertTrue(!helper.needRepartition());
    }
}
