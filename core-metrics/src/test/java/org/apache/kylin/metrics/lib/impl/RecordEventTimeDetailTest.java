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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class RecordEventTimeDetailTest extends LocalFileMetadataTestCase {

    private static String eventTimeZone;

    @BeforeAll
    static void beforeClass() throws Exception {
        staticCreateTestMetadata();
        eventTimeZone = System.getProperty("kylin.metrics.event-time-zone");
        System.setProperty("kylin.metrics.event-time-zone", "GMT");
    }

    @AfterAll
    static void afterClass() throws Exception {
        staticCleanupTestMetadata();
        if (!StringUtil.isEmpty(eventTimeZone)) {
            System.setProperty("kylin.metrics.event-time-zone", eventTimeZone);
        }
    }

    @Test
    void testFormatted() {
        RecordEventTimeDetail timeDetail = new RecordEventTimeDetail(200000000000L);

        Assertions.assertEquals("1976-01-01", timeDetail.year_begin_date);
        Assertions.assertEquals("1976-05-01", timeDetail.month_begin_date);
        Assertions.assertEquals("1976-05-02", timeDetail.week_begin_date);
        Assertions.assertEquals("1976-05-03", timeDetail.date);
        Assertions.assertEquals("19:33:20", timeDetail.time);
        Assertions.assertEquals(19, timeDetail.hour);
        Assertions.assertEquals(33, timeDetail.minute);
        Assertions.assertEquals(20, timeDetail.second);
    }
}
