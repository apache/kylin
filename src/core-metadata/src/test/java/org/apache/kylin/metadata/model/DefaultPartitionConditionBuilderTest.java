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

package org.apache.kylin.metadata.model;

import static org.junit.Assert.assertThrows;

import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.common.util.DateFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DefaultPartitionConditionBuilderTest {

    private PartitionDesc.DefaultPartitionConditionBuilder partitionConditionBuilder;
    private CleanMetadataHelper cleanMetadataHelper = null;
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
        partitionConditionBuilder = new PartitionDesc.DefaultPartitionConditionBuilder();
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void testDatePartitionCheckFormat() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "string");
        partitionDesc.setPartitionDateColumnRef(col);
        Assert.assertTrue(partitionDesc.checkIntTypeDateFormat());
        col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "bigint");
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        partitionDesc.setPartitionDateFormat("yyyyMMdd");
        partitionDesc.setPartitionDateColumnRef(col);
        Assert.assertTrue(partitionDesc.checkIntTypeDateFormat());
    }

    @Test
    public void testDatePartition() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "string");
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(
                DateFormat.stringToMillis("2016-02-22"), DateFormat.stringToMillis("2016-02-23"));
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.DATE_COLUMN >= '2016-02-22' AND UNKNOWN_ALIAS.DATE_COLUMN < '2016-02-23'",
                condition);

        range = new SegmentRange.TimePartitionedSegmentRange(0L, 0L);
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("1=1", condition);
    }

    @Test
    public void testTimePartition() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 2, "HOUR_COLUMN", "string");
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        partitionDesc.setPartitionDateFormat("HH");
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(
                DateFormat.stringToMillis("2016-02-22 00:00:00"), DateFormat.stringToMillis("2016-02-23 01:00:00"));
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.HOUR_COLUMN >= '00' AND UNKNOWN_ALIAS.HOUR_COLUMN < '01'", condition);
    }

    public void testDatePartitionHelper(String dataType, String dataFormat, String startTime, String endTime,
            String expect) {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", dataType);
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        partitionDesc.setPartitionDateFormat(dataFormat);
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(
                DateFormat.stringToMillis(startTime), DateFormat.stringToMillis(endTime));
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals(expect, condition);
    }

    @Test
    public void testDatePartition_BigInt_yyyy_MM_timestamp_disable() {
        assertThrows(RuntimeException.class, () -> testDatePartitionHelper("bigint", "yyyy-MM", "2016-02-22",
                "2016-03-23", "UNKNOWN_ALIAS.DATE_COLUMN >= 201602 AND UNKNOWN_ALIAS.DATE_COLUMN < 201603"));
    }

    @Test
    public void testDatePartition_BigInt_yyyy_timestamp_disable() {
        assertThrows(RuntimeException.class, () -> testDatePartitionHelper("bigint", "yyyy", "2016-02-22", "2016-03-23",
                "UNKNOWN_ALIAS.DATE_COLUMN >= 201602 AND UNKNOWN_ALIAS.DATE_COLUMN < 201603"));
    }

    @Test
    public void testDatePartition_BigInt_yyyy_MM_timestamp_enable() {
        partitionConditionBuilder.setUseBigintAsTimestamp(true);
        testDatePartitionHelper("bigint", "yyyy-MM", "2016-02-22", "2016-02-23",
                "UNKNOWN_ALIAS.DATE_COLUMN >= 1456070400000 AND UNKNOWN_ALIAS.DATE_COLUMN < 1456156800000");
        partitionConditionBuilder.setUseBigintAsTimestamp(false);
    }

    @Test
    public void testDatePartition_BigInt_yyyy_MM_dd_timestamp_enable() {
        partitionConditionBuilder.setUseBigintAsTimestamp(true);
        testDatePartitionHelper("bigint", "yyyy-MM-dd", "2016-02-22", "2016-02-23",
                "UNKNOWN_ALIAS.DATE_COLUMN >= 20160222 AND UNKNOWN_ALIAS.DATE_COLUMN < 20160223");
        partitionConditionBuilder.setUseBigintAsTimestamp(false);
    }

    @Test
    public void testDatePartition_BigInt() {
        assertThrows(RuntimeException.class, () -> testDatePartitionHelper("bigint", "yyyy-MM-dd", "2016-02-22",
                "2016-02-23", "UNKNOWN_ALIAS.DATE_COLUMN >= 20160222 AND UNKNOWN_ALIAS.DATE_COLUMN < 20160223"));
    }

    @Test
    public void testDatePartition_Date() {
        testDatePartitionHelper("date", "yyyy/MM/dd", "2016-02-22", "2016-02-23",
                "UNKNOWN_ALIAS.DATE_COLUMN >= to_date('2016/02/22', 'yyyy/MM/dd') AND "
                        + "UNKNOWN_ALIAS.DATE_COLUMN < to_date('2016/02/23', 'yyyy/MM/dd')");

    }

    @Test
    public void testDatePartition_Long() {
        testDatePartitionHelper("long", "yyyyMMdd", "2016-02-22", "2016-02-23",
                "UNKNOWN_ALIAS.DATE_COLUMN >= 20160222 AND UNKNOWN_ALIAS.DATE_COLUMN < 20160223");
    }

    @Test
    public void testDatePartition_yyyymm_Long() {
        testDatePartitionHelper("long", "yyyyMM", "2016-02-22", "2016-04-23",
                "UNKNOWN_ALIAS.DATE_COLUMN >= 201602 AND UNKNOWN_ALIAS.DATE_COLUMN < 201604");
    }

    @Test
    public void testDatePartition_Int() {
        assertThrows(RuntimeException.class, () -> testDatePartitionHelper("int", "yyyy-MM-dd", "2016-02-22",
                "2016-02-23", "UNKNOWN_ALIAS.DATE_COLUMN >= 20160222 AND UNKNOWN_ALIAS.DATE_COLUMN < 20160223"));
    }

    @Test
    public void testDatePartition_Timestamp() {
        testDatePartitionHelper("timestamp", "yyyy-MM-dd HH:mm:ss", "2016-02-22", "2016-02-23",
                "UNKNOWN_ALIAS.DATE_COLUMN >= '2016-02-22 00:00:00' AND UNKNOWN_ALIAS.DATE_COLUMN < '2016-02-23 00:00:00'");
    }

    @Test
    public void testCCPartition() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "CC_COLUMN", "string",
                "CAST(DATE_COLUMN AS DATE)");
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(
                DateFormat.stringToMillis("2016-02-22"), DateFormat.stringToMillis("2016-02-23"));
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("CAST(DATE_COLUMN AS DATE) >= '2016-02-22' AND CAST(DATE_COLUMN AS DATE) < '2016-02-23'",
                condition);
    }

    @Test
    public void testDatePartition_TimestampType_Long() {
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "long");
        PartitionDesc partitionDesc = new PartitionDesc();
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        long startLong = System.currentTimeMillis();
        long endLong = startLong + 24 * 3600 * 1000L;
        for (PartitionDesc.TimestampType timestampType : PartitionDesc.TimestampType.values()) {
            partitionDesc.setPartitionDateFormat(timestampType.name);
            SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(startLong,
                    endLong);
            String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
            Assert.assertEquals(
                    "UNKNOWN_ALIAS.DATE_COLUMN >= " + startLong / timestampType.millisecondRatio
                            + " AND UNKNOWN_ALIAS.DATE_COLUMN < " + endLong / timestampType.millisecondRatio,
                    condition);
        }
    }

    @Test
    public void testDatePartition_customizeFormat() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "string");
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd'@:008'HH:mm:ss");
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(
                DateFormat.stringToMillis("2016-02-22"), DateFormat.stringToMillis("2016-02-23"));
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals(
                "UNKNOWN_ALIAS.DATE_COLUMN >= '2016-02-22@:00800:00:00' AND UNKNOWN_ALIAS.DATE_COLUMN < '2016-02-23@:00800:00:00'",
                condition);
    }
}
