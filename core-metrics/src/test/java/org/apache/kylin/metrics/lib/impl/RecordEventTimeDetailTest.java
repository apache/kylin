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

package org.apache.kylin.metrics.lib.impl;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.StringUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RecordEventTimeDetailTest extends LocalFileMetadataTestCase {

    private static String eventTimeZone;

    @BeforeClass
    public static void beforeClass() throws Exception {
        staticCreateTestMetadata();
        eventTimeZone = System.getProperty("kylin.metrics.event-time-zone");
        System.setProperty("kylin.metrics.event-time-zone", "GMT");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        staticCleanupTestMetadata();
        if (!StringUtil.isEmpty(eventTimeZone)) {
            System.setProperty("kylin.metrics.event-time-zone", eventTimeZone);
        }
    }

    @Test
    public void testFormatted() {
        RecordEventTimeDetail timeDetail = new RecordEventTimeDetail(200000000000L);

        Assert.assertEquals("1976-01-01", timeDetail.year_begin_date);
        Assert.assertEquals("1976-05-01", timeDetail.month_begin_date);
        Assert.assertEquals("1976-05-02", timeDetail.week_begin_date);
        Assert.assertEquals("1976-05-03", timeDetail.date);
        Assert.assertEquals("19:33:20", timeDetail.time);
        Assert.assertEquals(19, timeDetail.hour);
        Assert.assertEquals(33, timeDetail.minute);
        Assert.assertEquals(20, timeDetail.second);
    }
}
