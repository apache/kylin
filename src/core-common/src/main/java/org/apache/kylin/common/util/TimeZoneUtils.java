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

import java.time.ZoneId;
import java.util.TimeZone;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TimeZoneUtils {
    private static final Logger log = LoggerFactory.getLogger(TimeZoneUtils.class);

    private TimeZoneUtils() {
    }

    /**
     * short time zone like [CST, PST], may raise some problem (issue#13185).
     * TimeZone.ID & TimeZone.ZoneId.ID set same data, like [Asia/Shanghai, America/New_York].
     *
     * @param kylinConfig
     */
    public static void setDefaultTimeZone(KylinConfig kylinConfig) {
        ZoneId zoneId = TimeZone.getTimeZone(kylinConfig.getTimeZone()).toZoneId();
        TimeZone.setDefault(TimeZone.getTimeZone(zoneId));
        log.info("System timezone set to {}, TimeZoneId: {}.", kylinConfig.getTimeZone(),
                TimeZone.getDefault().getID());
    }
}
