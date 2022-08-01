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
package io.kyligence.kap.secondstorage;

import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.kylin.metadata.cube.model.NDataflow;


@RunWith(PowerMockRunner.class)
public class NameUtilTest {
    private static final String PROJECT = "test_table_index";
    private static final long LAYOUT_ID = 2000000001;
    private final NDataflow dataflow = Mockito.mock(NDataflow.class);
    private final KylinConfigExt config = Mockito.mock(KylinConfigExt.class);

    @Test
    public void testNameUtilInUT() {
        final String uuid = RandomUtil.randomUUIDStr();
        Mockito.when(dataflow.getConfig()).thenReturn(config);
        Mockito.when(dataflow.getProject()).thenReturn(PROJECT);
        Mockito.when(dataflow.getUuid()).thenReturn(uuid);
        Mockito.when(config.isUTEnv()).thenReturn(true);
        final String tablePrefix = NameUtil.tablePrefix(uuid);

        Assert.assertEquals("UT_" + PROJECT, NameUtil.getDatabase(dataflow));
        Assert.assertTrue(NameUtil.getTable(dataflow, LAYOUT_ID).startsWith(tablePrefix));
        Assert.assertTrue(NameUtil.getTable(dataflow, LAYOUT_ID).endsWith(String.valueOf(LAYOUT_ID)));

        Assert.assertEquals(PROJECT, NameUtil.recoverProject(NameUtil.getDatabase(dataflow), config));
        Assert.assertEquals(Pair.newPair(uuid, LAYOUT_ID),
                NameUtil.recoverLayout(NameUtil.getTable(dataflow, LAYOUT_ID)));
    }

    @Test
    public void testNameUtil() {
        final String uuid = RandomUtil.randomUUIDStr();
        final String metaUrl = "ke_metadata";
        Mockito.when(dataflow.getConfig()).thenReturn(config);
        Mockito.when(dataflow.getProject()).thenReturn(PROJECT);
        Mockito.when(dataflow.getUuid()).thenReturn(uuid);
        Mockito.when(config.isUTEnv()).thenReturn(false);
        Mockito.when(config.getMetadataUrlPrefix()).thenReturn(metaUrl);

        final String tablePrefix = NameUtil.tablePrefix(uuid);

        Assert.assertEquals(metaUrl + "_" + PROJECT, NameUtil.getDatabase(dataflow));
        Assert.assertTrue(NameUtil.getTable(dataflow, LAYOUT_ID).startsWith(tablePrefix));
        Assert.assertTrue(NameUtil.getTable(dataflow, LAYOUT_ID).endsWith(String.valueOf(LAYOUT_ID)));

        Assert.assertEquals(PROJECT, NameUtil.recoverProject(NameUtil.getDatabase(dataflow), config));
        Assert.assertEquals(Pair.newPair(uuid, LAYOUT_ID),
                NameUtil.recoverLayout(NameUtil.getTable(dataflow, LAYOUT_ID)));
    }
}
