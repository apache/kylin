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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultPartitionConditionBuilderTest {
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
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, DateFormat.stringToMillis("2016-02-22"), DateFormat.stringToMillis("2016-02-23"));
        Assert.assertEquals("UNKNOWN_ALIAS.DATE_COLUMN >= '2016-02-22' AND UNKNOWN_ALIAS.DATE_COLUMN < '2016-02-23'", condition);
    }

    @Test
    public void testTimePartition() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 2, "HOUR_COLUMN", "string");
        partitionDesc.setPartitionTimeColumnRef(col);
        partitionDesc.setPartitionTimeColumn(col.getCanonicalName());
        partitionDesc.setPartitionTimeFormat("HH");
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, DateFormat.stringToMillis("2016-02-22 00:00:00"), DateFormat.stringToMillis("2016-02-23 01:00:00"));
        Assert.assertEquals("UNKNOWN_ALIAS.HOUR_COLUMN >= '00' AND UNKNOWN_ALIAS.HOUR_COLUMN < '01'", condition);
    }

    @Test
    public void testDateAndTimePartition() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col1 = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "string");
        partitionDesc.setPartitionDateColumnRef(col1);
        partitionDesc.setPartitionDateColumn(col1.getCanonicalName());
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        TblColRef col2 = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 2, "HOUR_COLUMN", "string");
        partitionDesc.setPartitionTimeColumnRef(col2);
        partitionDesc.setPartitionTimeColumn(col2.getCanonicalName());
        partitionDesc.setPartitionTimeFormat("H");
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, DateFormat.stringToMillis("2016-02-22 00:00:00"), DateFormat.stringToMillis("2016-02-23 01:00:00"));
        Assert.assertEquals("((UNKNOWN_ALIAS.DATE_COLUMN = '2016-02-22' AND UNKNOWN_ALIAS.HOUR_COLUMN >= '0') OR (UNKNOWN_ALIAS.DATE_COLUMN > '2016-02-22')) AND ((UNKNOWN_ALIAS.DATE_COLUMN = '2016-02-23' AND UNKNOWN_ALIAS.HOUR_COLUMN < '1') OR (UNKNOWN_ALIAS.DATE_COLUMN < '2016-02-23'))", condition);
    }

}