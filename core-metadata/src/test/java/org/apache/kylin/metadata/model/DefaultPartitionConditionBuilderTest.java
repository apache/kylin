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

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultPartitionConditionBuilderTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    private PartitionDesc.DefaultPartitionConditionBuilder partitionConditionBuilder;

    @Before
    public void setUp() {
        partitionConditionBuilder = new PartitionDesc.DefaultPartitionConditionBuilder();
    }

    @Test
    public void testDatePartition() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "string");
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        TSRange range = new TSRange(DateFormat.stringToMillis("2016-02-22"), DateFormat.stringToMillis("2016-02-23 21:32:29"));
        String condition;

        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.DATE_COLUMN >= '2016-02-22' AND UNKNOWN_ALIAS.DATE_COLUMN < '2016-02-23'", condition);

        partitionDesc.setPartitionDateFormat("yyyy-MM-dd HH:mm");
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.DATE_COLUMN >= '2016-02-22 00:00' AND UNKNOWN_ALIAS.DATE_COLUMN < '2016-02-23 21:32'", condition);

        partitionDesc.setPartitionDateFormat("yyyyMMddHH");
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.DATE_COLUMN >= '2016022200' AND UNKNOWN_ALIAS.DATE_COLUMN < '2016022321'", condition);

        partitionDesc.setPartitionDateFormat("yyyyMMddHHmm");
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.DATE_COLUMN >= '201602220000' AND UNKNOWN_ALIAS.DATE_COLUMN < '201602232132'", condition);

        range = new TSRange(0L, 0L);
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("1=0", condition);
    }

    @Test
    public void testDatePartitionYmdInt() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "integer");
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        TSRange range = new TSRange(DateFormat.stringToMillis("2016-02-22"), DateFormat.stringToMillis("2016-02-23 20:32:29"));
        String condition;

        partitionDesc.setPartitionDateFormat("yyyyMMdd");
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.DATE_COLUMN >= 20160222 AND UNKNOWN_ALIAS.DATE_COLUMN < 20160223", condition);

        partitionDesc.setPartitionDateFormat("yyyyMMddHHmmss");
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.DATE_COLUMN >= 20160222000000 AND UNKNOWN_ALIAS.DATE_COLUMN < 20160223203229", condition);
    }

    @Test
    public void testDatePartitionTimeMillis() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "bigint");
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        TSRange range = new TSRange(DateFormat.stringToMillis("2016-02-22"), DateFormat.stringToMillis("2016-02-23 20:32:29"));
        String condition;

        partitionDesc.setPartitionDateFormat("yyyy-MM-dd HH:mm:ss");
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.DATE_COLUMN >= 1456099200000 AND UNKNOWN_ALIAS.DATE_COLUMN < 1456259549000", condition);
    }

    @Test
    public void testTimePartition() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 2, "HOUR_COLUMN", "string");
        partitionDesc.setPartitionTimeColumnRef(col);
        partitionDesc.setPartitionTimeColumn(col.getCanonicalName());
        TSRange range = new TSRange(DateFormat.stringToMillis("2016-02-22 00:12:00"), DateFormat.stringToMillis("2016-02-23 01:00:00"));
        String condition;

        partitionDesc.setPartitionTimeFormat("HH");
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.HOUR_COLUMN >= '00' AND UNKNOWN_ALIAS.HOUR_COLUMN < '01'", condition);

        partitionDesc.setPartitionTimeFormat("HH:mm");
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.HOUR_COLUMN >= '00:12' AND UNKNOWN_ALIAS.HOUR_COLUMN < '01:00'", condition);
    }

    @Test
    public void testTimePartitionIsInt() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 2, "HOUR_COLUMN", "integer");
        partitionDesc.setPartitionTimeColumnRef(col);
        partitionDesc.setPartitionTimeColumn(col.getCanonicalName());
        TSRange range = new TSRange(DateFormat.stringToMillis("2016-02-22 19:12:00"), DateFormat.stringToMillis("2016-02-23 20:00:00"));
        String condition;

        partitionDesc.setPartitionTimeFormat("HH");
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.HOUR_COLUMN >= 19 AND UNKNOWN_ALIAS.HOUR_COLUMN < 20", condition);
    }

    @Test
    public void testDateAndTimePartitionString() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col1 = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "string");
        partitionDesc.setPartitionDateColumnRef(col1);
        partitionDesc.setPartitionDateColumn(col1.getCanonicalName());
        TblColRef col2 = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 2, "HOUR_COLUMN", "string");
        partitionDesc.setPartitionTimeColumnRef(col2);
        partitionDesc.setPartitionTimeColumn(col2.getCanonicalName());
        TSRange range = new TSRange(DateFormat.stringToMillis("2016-02-22 00:00:00"), DateFormat.stringToMillis("2016-02-23 12:23:00"));
        String condition;

        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        partitionDesc.setPartitionTimeFormat("H");
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals(
                "((UNKNOWN_ALIAS.DATE_COLUMN = '2016-02-22' AND UNKNOWN_ALIAS.HOUR_COLUMN >= '0') OR (UNKNOWN_ALIAS.DATE_COLUMN > '2016-02-22')) AND ((UNKNOWN_ALIAS.DATE_COLUMN = '2016-02-23' AND UNKNOWN_ALIAS.HOUR_COLUMN < '12') OR (UNKNOWN_ALIAS.DATE_COLUMN < '2016-02-23'))",
                condition);

        partitionDesc.setPartitionDateFormat("yyyyMMdd");
        partitionDesc.setPartitionTimeFormat("HH:mm");
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals(
                "((UNKNOWN_ALIAS.DATE_COLUMN = '20160222' AND UNKNOWN_ALIAS.HOUR_COLUMN >= '00:00') OR (UNKNOWN_ALIAS.DATE_COLUMN > '20160222')) AND ((UNKNOWN_ALIAS.DATE_COLUMN = '20160223' AND UNKNOWN_ALIAS.HOUR_COLUMN < '12:23') OR (UNKNOWN_ALIAS.DATE_COLUMN < '20160223'))",
                condition);
    }

    @Test
    public void testDateAndTimePartitionInt() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col1 = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "integer");
        partitionDesc.setPartitionDateColumnRef(col1);
        partitionDesc.setPartitionDateColumn(col1.getCanonicalName());
        TblColRef col2 = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 2, "HOUR_COLUMN", "bigint");
        partitionDesc.setPartitionTimeColumnRef(col2);
        partitionDesc.setPartitionTimeColumn(col2.getCanonicalName());
        TSRange range = new TSRange(DateFormat.stringToMillis("2016-02-22 00:00:00"), DateFormat.stringToMillis("2016-02-23 12:23:00"));
        String condition;

        partitionDesc.setPartitionDateFormat("yyyyMMdd");
        partitionDesc.setPartitionTimeFormat("HH");
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals(
                "((UNKNOWN_ALIAS.DATE_COLUMN = 20160222 AND UNKNOWN_ALIAS.HOUR_COLUMN >= 00) OR (UNKNOWN_ALIAS.DATE_COLUMN > 20160222)) AND ((UNKNOWN_ALIAS.DATE_COLUMN = 20160223 AND UNKNOWN_ALIAS.HOUR_COLUMN < 12) OR (UNKNOWN_ALIAS.DATE_COLUMN < 20160223))",
                condition);

    }
}
