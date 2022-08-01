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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.NTableMetadataManager;
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
        final SegmentRange segRange_1 = new SegmentRange.TimePartitionedSegmentRange(0L, 10L);
        TableExtDesc.ColumnStats colStats = new TableExtDesc.ColumnStats();
        colStats.setColumnName(colName);
        HLLCounter col_hllc = mockHLLCounter(2, 5);
        columnStatsList.add(updateColStats(colStats, 10, segRange_1, col_hllc, 1000d, -1000d, 4, 2, "9999", "99"));

        tableExtDesc.setColumnStats(columnStatsList);
        tableMetadataManager.saveTableExt(tableExtDesc);

        columnStatsList.clear();
        tableExtDesc = tableMetadataManager.getOrCreateTableExt(tableName);
        tableExtDesc = tableMetadataManager.copyForWrite(tableExtDesc);
        colStats = tableExtDesc.getColumnStatsByName(colName);
        Assert.assertEquals(colName, colStats.getColumnName());
        Assert.assertEquals(10, colStats.getNullCount());

        final SegmentRange segRange_2 = new SegmentRange.TimePartitionedSegmentRange(10L, 20L);
        col_hllc = mockHLLCounter(6, 10);
        columnStatsList.add(updateColStats(colStats, 11, segRange_2, col_hllc, 9999d, -9999d, 5, 1, "99999", "9"));

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

    private TableExtDesc.ColumnStats updateColStats(TableExtDesc.ColumnStats colStats, long nullCount,
            SegmentRange segRange, HLLCounter hllc, double maxValue, double minValue, int maxLength, int minLength,
            String maxLengthValue, String minLengthValue) {

        colStats.addNullCount(nullCount);

        colStats.addRangeHLLC(segRange, hllc);

        colStats.updateBasicStats(maxValue, minValue, maxLength, minLength, maxLengthValue, minLengthValue);

        return colStats;
    }

    private HLLCounter mockHLLCounter(int min, int max) {
        final HLLCounter hllCounter = new HLLCounter(14);
        for (int i = min; i <= max; i++) {
            hllCounter.add(RandomStringUtils.randomAlphanumeric(i));
        }

        return hllCounter;
    }
}
