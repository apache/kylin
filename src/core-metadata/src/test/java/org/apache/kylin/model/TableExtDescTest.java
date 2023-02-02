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

package org.apache.kylin.model;

import static org.apache.kylin.metadata.model.NTableMetadataManager.getInstance;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TableExtDescTest extends NLocalFileMetadataTestCase {

    private final String project = "default";
    private NTableMetadataManager tableMetadataManager;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        tableMetadataManager = getInstance(getTestConfig(), project);
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testBasic() {
        final String tableName = "DEFAULT.TEST_KYLIN_FACT";
        final TableDesc tableDesc = tableMetadataManager.getTableDesc(tableName);
        TableExtDesc tableExtDesc = tableMetadataManager.getOrCreateTableExt(tableName);
        tableExtDesc = tableMetadataManager.copyForWrite(tableExtDesc);

        final String colName = "col_1";
        final List<TableExtDesc.ColumnStats> columnStatsList = new ArrayList<>(tableDesc.getColumnCount());
        TableExtDesc.ColumnStats colStats = new TableExtDesc.ColumnStats();
        colStats.setColumnName(colName);
        columnStatsList.add(updateColStats(colStats, 10, 1000d, -1000d, 4, 2, "9999", "99"));

        tableExtDesc.setColumnStats(columnStatsList);
        tableMetadataManager.saveTableExt(tableExtDesc);

        columnStatsList.clear();
        tableExtDesc = tableMetadataManager.getOrCreateTableExt(tableName);
        tableExtDesc = tableMetadataManager.copyForWrite(tableExtDesc);
        colStats = tableExtDesc.getColumnStatsByName(colName);
        Assert.assertEquals(colName, colStats.getColumnName());
        Assert.assertEquals(10, colStats.getNullCount());

        columnStatsList.add(updateColStats(colStats, 11, 9999d, -9999d, 5, 1, "99999", "9"));

        tableExtDesc.setColumnStats(columnStatsList);
        tableMetadataManager.saveTableExt(tableExtDesc);

        tableExtDesc = tableMetadataManager.getOrCreateTableExt(tableName);
        colStats = tableExtDesc.getColumnStatsByName(colName);
        Assert.assertEquals(colName, colStats.getColumnName());
        Assert.assertEquals(21, colStats.getNullCount());
        Assert.assertEquals(9999d, colStats.getMaxNumeral(), 0.0001);
        Assert.assertEquals(-9999d, colStats.getMinNumeral(), 0.0001);
        Assert.assertEquals(5, colStats.getMaxLength().intValue());
        Assert.assertEquals(1, colStats.getMinLength().intValue());
        Assert.assertEquals("99999", colStats.getMaxLengthValue());
        Assert.assertEquals("9", colStats.getMinLengthValue());

    }

    @Test
    public void testGetS3RoleAndLocation() {
        final String tableName = "DEFAULT.TEST_KYLIN_FACT";
        TableExtDesc tableExtDesc = tableMetadataManager.getOrCreateTableExt(tableName);
        tableExtDesc.addDataSourceProp(TableExtDesc.LOCATION_PROPERTY_KEY, "");
        tableExtDesc.addDataSourceProp(TableExtDesc.S3_ENDPOINT_KEY, "testEndpoint");
        assert tableExtDesc.getS3RoleCredentialInfo() == null;
        tableExtDesc.addDataSourceProp(TableExtDesc.LOCATION_PROPERTY_KEY, "::aaa/bbb");
        assert tableExtDesc.getS3RoleCredentialInfo() == null;
        tableExtDesc.addDataSourceProp(TableExtDesc.LOCATION_PROPERTY_KEY, "s3://aaa/bbb");
        assert tableExtDesc.getS3RoleCredentialInfo().getEndpoint().equals("testEndpoint");
        tableExtDesc.addDataSourceProp(TableExtDesc.S3_ROLE_PROPERTY_KEY, "test");
        assert tableExtDesc.getS3RoleCredentialInfo().getBucket().equals("aaa");
        assert tableExtDesc.getS3RoleCredentialInfo().getRole().equals("test");
        assert tableExtDesc.getS3RoleCredentialInfo().getEndpoint().equals("testEndpoint");

    }

    private TableExtDesc.ColumnStats updateColStats(TableExtDesc.ColumnStats colStats, long nullCount, double maxValue,
            double minValue, int maxLength, int minLength, String maxLengthValue, String minLengthValue) {

        colStats.addNullCount(nullCount);
        colStats.updateBasicStats(maxValue, minValue, maxLength, minLength, maxLengthValue, minLengthValue);

        return colStats;
    }
}
