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

package org.apache.kylin.stream.core.storage.columnar;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.dimension.TimeDerivedColumnType;

public class TimeDerivedColumnEncoding {
    private String columnName;
    private TimeDerivedColumnType timeColumnType;
    private long baseTime;
    private long normalizedBaseTime;

    public TimeDerivedColumnEncoding(String columnName, long baseTime) {
        this.columnName = columnName;
        this.baseTime = baseTime;
        this.timeColumnType = TimeDerivedColumnType.getTimeDerivedColumnType(columnName);
        this.normalizedBaseTime = timeColumnType.normalize(baseTime);
    }

    public void encode(String value, byte[] output, int outputOffset) {
        long timeVal = DateFormat.stringToMillis(value);
        timeColumnType.calculateTimeUnitRange(timeVal);

    }
}
