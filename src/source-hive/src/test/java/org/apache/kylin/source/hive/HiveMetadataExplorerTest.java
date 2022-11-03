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

package org.apache.kylin.source.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class HiveMetadataExplorerTest extends NLocalFileMetadataTestCase {

    @Test
    public void testCheckIsRangePartitionTable() throws Exception {
        List<HiveTableMeta.HiveTableColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("A", "varchar", ""));
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("B", "varchar", ""));
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("C", "varchar", ""));
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("D", "varchar", ""));
        Assert.assertEquals(Boolean.FALSE, columnMetas.stream().collect(Collectors.groupingBy(p -> p.name)).values()
                .stream().anyMatch(p -> p.size() > 1));
        HiveMetadataExplorer hiveMetadataExplorer = Mockito.mock(HiveMetadataExplorer.class);
        Mockito.when(hiveMetadataExplorer.checkIsRangePartitionTable(Mockito.anyList())).thenCallRealMethod();
        Assert.assertEquals(Boolean.FALSE, hiveMetadataExplorer.checkIsRangePartitionTable(columnMetas));
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("A", "varchar", ""));
        Assert.assertEquals(Boolean.TRUE, columnMetas.stream().collect(Collectors.groupingBy(p -> p.name)).values()
                .stream().anyMatch(p -> p.size() > 1));
        Assert.assertEquals(Boolean.TRUE, hiveMetadataExplorer.checkIsRangePartitionTable(columnMetas));
    }

    @Test
    public void testGetColumnDescs() {
        createTestMetadata();
        HiveMetadataExplorer hiveMetadataExplorer = Mockito.mock(HiveMetadataExplorer.class);
        Mockito.when(hiveMetadataExplorer.getColumnDescs(Mockito.anyList())).thenCallRealMethod();
        List<HiveTableMeta.HiveTableColumnMeta> columnMetas = new ArrayList<>();
        List<ColumnDesc> columnDescs = hiveMetadataExplorer.getColumnDescs(columnMetas);
        Assert.assertNotNull(columnDescs);
        Assert.assertEquals(0, columnDescs.size());
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("A", "varchar", ""));
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("B", "varchar", ""));
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("C", "varchar", ""));
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("D", "varchar", ""));
        columnDescs = hiveMetadataExplorer.getColumnDescs(columnMetas);
        Assert.assertEquals(4, columnDescs.size());
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("A", "varchar", ""));
        columnDescs = hiveMetadataExplorer.getColumnDescs(columnMetas);
        Assert.assertEquals(4, columnDescs.size());
        columnMetas.add(new HiveTableMeta.HiveTableColumnMeta("E", "varchar", ""));
        columnDescs = hiveMetadataExplorer.getColumnDescs(columnMetas);
        Assert.assertEquals(5, columnDescs.size());
    }
}
