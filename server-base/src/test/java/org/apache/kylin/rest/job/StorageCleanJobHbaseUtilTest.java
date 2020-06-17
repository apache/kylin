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

package org.apache.kylin.rest.job;

import static org.apache.kylin.common.util.LocalFileMetadataTestCase.cleanAfterClass;
import static org.apache.kylin.common.util.LocalFileMetadataTestCase.staticCreateTestMetadata;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.kylin.common.util.LocalFileMetadataTestCase.OverlayMetaHook;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class StorageCleanJobHbaseUtilTest {
    @Before
    public void setup() {
        staticCreateTestMetadata(true, new OverlayMetaHook("src/test/resources/ut_meta/hbase_storage_ut/"));
    }

    @After
    public void after() {
        cleanAfterClass();
    }

    @Test
    public void test() throws IOException {
        HBaseAdmin hBaseAdmin = mock(HBaseAdmin.class);
        HTableDescriptor[] hds = new HTableDescriptor[2];
        HTableDescriptor d1 = mock(HTableDescriptor.class);
        HTableDescriptor d2 = mock(HTableDescriptor.class);
        hds[0] = d1;
        hds[1] = d2;
        when(d1.getValue("KYLIN_HOST")).thenReturn("../examples/test_metadata/");
        when(d2.getValue("KYLIN_HOST")).thenReturn("../examples/test_metadata/");
        when(d1.getTableName()).thenReturn(TableName.valueOf("KYLIN_J9TE08D9IA"));
        String toBeDel = "to-be-del";
        when(d2.getTableName()).thenReturn(TableName.valueOf(toBeDel));
        when(hBaseAdmin.listTables("KYLIN_.*")).thenReturn(hds);

        when(hBaseAdmin.tableExists(toBeDel)).thenReturn(true);
        when(hBaseAdmin.isTableEnabled(toBeDel)).thenReturn(false);
        StorageCleanJobHbaseUtil.cleanUnusedHBaseTables(hBaseAdmin, true, 100000, 1);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(hBaseAdmin).deleteTable(captor.capture());
        assertEquals(Lists.newArrayList(toBeDel), captor.getAllValues());
    }
}
