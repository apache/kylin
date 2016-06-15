/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.metadata.model;

import java.util.HashMap;

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
        partitionDesc.setPartitionDateColumn("DEFAULT.TABLE_NAME.DATE_COLUMN");
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, DateFormat.stringToMillis("2016-02-22"), DateFormat.stringToMillis("2016-02-23"), new HashMap<String, String>());
        Assert.assertEquals("DEFAULT.TABLE_NAME.DATE_COLUMN >= '2016-02-22' AND DEFAULT.TABLE_NAME.DATE_COLUMN < '2016-02-23'", condition);
    }

    @Test
    public void testTimePartition() {
        PartitionDesc partitionDesc = new PartitionDesc();
        partitionDesc.setPartitionTimeColumn("DEFAULT.TABLE_NAME.HOUR_COLUMN");
        partitionDesc.setPartitionTimeFormat("HH");
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, DateFormat.stringToMillis("2016-02-22 00:00:00"), DateFormat.stringToMillis("2016-02-23 01:00:00"), new HashMap<String, String>());
        Assert.assertEquals("DEFAULT.TABLE_NAME.HOUR_COLUMN >= '00' AND DEFAULT.TABLE_NAME.HOUR_COLUMN < '01'", condition);
    }

    @Test
    public void testDateAndTimePartition() {
        PartitionDesc partitionDesc = new PartitionDesc();
        partitionDesc.setPartitionDateColumn("DEFAULT.TABLE_NAME.DATE_COLUMN");
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        partitionDesc.setPartitionTimeColumn("DEFAULT.TABLE_NAME.HOUR_COLUMN");
        partitionDesc.setPartitionTimeFormat("H");
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, DateFormat.stringToMillis("2016-02-22 00:00:00"), DateFormat.stringToMillis("2016-02-23 01:00:00"), new HashMap<String, String>());
        Assert.assertEquals("((DEFAULT.TABLE_NAME.DATE_COLUMN = '2016-02-22' AND DEFAULT.TABLE_NAME.HOUR_COLUMN >= '0') OR (DEFAULT.TABLE_NAME.DATE_COLUMN > '2016-02-22')) AND ((DEFAULT.TABLE_NAME.DATE_COLUMN = '2016-02-23' AND DEFAULT.TABLE_NAME.HOUR_COLUMN < '1') OR (DEFAULT.TABLE_NAME.DATE_COLUMN < '2016-02-23'))", condition);
    }

    @Test
    public void testDateAndTimePartitionWithAlias() {
        PartitionDesc partitionDesc = new PartitionDesc();
        partitionDesc.setPartitionDateColumn("TABLE_ALIAS.DATE_COLUMN");
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        partitionDesc.setPartitionTimeColumn("TABLE_ALIAS.HOUR_COLUMN");
        partitionDesc.setPartitionTimeFormat("H");
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, DateFormat.stringToMillis("2016-02-22 00:00:00"), DateFormat.stringToMillis("2016-02-23 01:00:00"), new HashMap<String, String>() {
            {
                put("TABLE_ALIAS", "DEFAULT.TABLE_NAME");
            }
        });
        Assert.assertEquals("((DEFAULT.TABLE_NAME.DATE_COLUMN = '2016-02-22' AND DEFAULT.TABLE_NAME.HOUR_COLUMN >= '0') OR (DEFAULT.TABLE_NAME.DATE_COLUMN > '2016-02-22')) AND ((DEFAULT.TABLE_NAME.DATE_COLUMN = '2016-02-23' AND DEFAULT.TABLE_NAME.HOUR_COLUMN < '1') OR (DEFAULT.TABLE_NAME.DATE_COLUMN < '2016-02-23'))", condition);
    }
}