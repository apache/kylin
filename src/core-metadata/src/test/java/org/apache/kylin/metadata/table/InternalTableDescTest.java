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

package org.apache.kylin.metadata.table;

import java.util.HashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo
class InternalTableDescTest {

    @Test
    void testNullProperties() {
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "default");
        TableDesc originTable = tableMetadataManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        InternalTableDesc table = new InternalTableDesc(originTable);

        table.setStorageType(null);
        Assertions.assertEquals(InternalTableDesc.StorageType.GLUTEN, table.getStorageType());
        table.setStorageType(InternalTableDesc.StorageType.PARQUET.name());
        Assertions.assertEquals(InternalTableDesc.StorageType.PARQUET, table.getStorageType());

        HashMap<String, String> tblProperties = new HashMap<>();
        table.setTblProperties(tblProperties);

        Assertions.assertNull(table.getBucketColumn());
        Assertions.assertEquals(0, table.getBucketNumber());
        Assertions.assertNull(table.getPartitionColumns());
        Assertions.assertNull(table.getDatePartitionFormat());

        tblProperties.put("bucketCol", "trans_id");
        tblProperties.put("bucketNum", "3 ");
        tblProperties.put("nullValue", null);
        tblProperties.put("nonexistence_col", "trans_id_xxx");
        table.setTblProperties(tblProperties);
        table.optimizeTblProperties();

        String[] partitionCols = new String[] { "CAL_DT" };
        String dateFormat = "yyyy-MM-dd";
        table.setTablePartition(new InternalTablePartition(partitionCols, dateFormat));

        Assertions.assertEquals("trans_id", table.getBucketColumn());
        Assertions.assertEquals(3, table.getBucketNumber());
        Assertions.assertEquals(partitionCols, table.getPartitionColumns());
        Assertions.assertEquals(dateFormat, table.getDatePartitionFormat());
        Assertions.assertEquals(3, table.getTblProperties().size());
    }
}
