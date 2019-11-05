/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.utils;

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
        Repartitioner helper = new Repartitioner(2, 1, 1000L, 1000L, sc, null);
        Assert.assertTrue(helper.needRepartitionForFileSize());
        Assert.assertFalse(helper.needRepartitionForShardByColumns());
        Assert.assertTrue(helper.needRepartition());
    }

    @Test
    public void testNeedRepartitionForFileSize_only1File() {
        ContentSummary sc = mock(ContentSummary.class);
        when(sc.getFileCount()).thenReturn(1L);
        when(sc.getLength()).thenReturn(512 * 1024L);
        Repartitioner helper = new Repartitioner(2, 1, 1000L, 1000L, sc, null);
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
        Repartitioner helper = new Repartitioner(2, 1, 1000L, 1000L, sc, shardByColumns);
        Assert.assertTrue(helper.needRepartitionForShardByColumns());
        Assert.assertFalse(helper.needRepartitionForFileSize());
        Assert.assertTrue(helper.needRepartition());
    }

    @Test
    public void testGetRepartitionNum() {
        ContentSummary sc = mock(ContentSummary.class);
        when(sc.getFileCount()).thenReturn(6L);
        when(sc.getLength()).thenReturn(4 * 1024 * 1024L);
        Repartitioner helper = new Repartitioner(2, 1, 1000L, 1000L, sc, null);
        Assert.assertEquals(2, helper.getRepartitionNumByStorage());
    }

    @Test
    public void testRowCountNum() {
        ContentSummary sc = mock(ContentSummary.class);
        when(sc.getFileCount()).thenReturn(6L);
        when(sc.getLength()).thenReturn(4 * 1024 * 1024L);
        Repartitioner helper = new Repartitioner(2, 1, 2000L, 500L, sc, null);
        // size = (4M/2M - 2000/500)/2 + min(4M/2M)
        Assert.assertEquals(3, helper.getRepartitionNumByStorage());
    }

    @Test
    public void testRepartitionNumEqualsCurrentPartitionNum() {
        ContentSummary sc = mock(ContentSummary.class);
        when(sc.getFileCount()).thenReturn(3L);
        when(sc.getLength()).thenReturn(4 * 1024 * 1024L);
        Repartitioner helper = new Repartitioner(2, 2, 1500L, 500L, sc, null);
        // size = 2000/500
        Assert.assertTrue(!helper.needRepartition());
    }
}
