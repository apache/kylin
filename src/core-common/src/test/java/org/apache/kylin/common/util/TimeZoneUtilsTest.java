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
package org.apache.kylin.common.util;

import java.util.Arrays;
import java.util.TimeZone;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.MultiTimezoneTest;
import org.junit.Assert;

@MetadataInfo(onlyProps = true)
public class TimeZoneUtilsTest {

    @MultiTimezoneTest(timezones = { "UTC", "GMT+8", "GMT+15" })
    public void testSetDefaultTimeZone() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        TimeZoneUtils.setDefaultTimeZone(kylinConfig);
        Assert.assertEquals(TimeZone.getDefault().getID(), TimeZone.getDefault().toZoneId().getId());

        for (String timeZone : Arrays.asList("GMT+8", "CST", "PST", "UTC")) {
            kylinConfig.setProperty("kylin.web.timezone", timeZone);
            TimeZoneUtils.setDefaultTimeZone(kylinConfig);
            Assert.assertEquals(TimeZone.getDefault().getID(), TimeZone.getDefault().toZoneId().getId());
        }
    }

}
