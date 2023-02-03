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

package org.apache.kylin.query.routing;

import org.apache.kylin.metadata.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class RealizationPrunerTest {

    @Test
    void testCheckAndReformatDateType() {
        long segmentTs = 1675396800000L;
        {
            String formattedValue = ReflectionTestUtils.invokeMethod(RealizationPruner.class,
                    "checkAndReformatDateType", "2023-02-03", segmentTs, new DataType("date", 0, 0));
            Assertions.assertEquals("2023-02-03", formattedValue);
        }

        {
            String formattedValue = ReflectionTestUtils.invokeMethod(RealizationPruner.class,
                    "checkAndReformatDateType", "2023-02-03 12:00:00", segmentTs, new DataType("date", 0, 0));
            Assertions.assertEquals("2023-02-03", formattedValue);
        }

        {
            String formattedValue = ReflectionTestUtils.invokeMethod(RealizationPruner.class,
                    "checkAndReformatDateType", "2023-02-03", segmentTs, new DataType("timestamp", 0, 0));
            Assertions.assertEquals("2023-02-03 12:00:00", formattedValue);
        }

        {
            String formattedValue = ReflectionTestUtils.invokeMethod(RealizationPruner.class,
                    "checkAndReformatDateType", "2023-02-03 12:00:00", segmentTs, new DataType("timestamp", 0, 0));
            Assertions.assertEquals("2023-02-03 12:00:00", formattedValue);
        }

        {
            String formattedValue = ReflectionTestUtils.invokeMethod(RealizationPruner.class,
                    "checkAndReformatDateType", "2023-02-03 12:00:00", segmentTs, new DataType("varchar", 0, 0));
            Assertions.assertEquals("2023-02-03 12:00:00", formattedValue);
        }

        {
            String formattedValue = ReflectionTestUtils.invokeMethod(RealizationPruner.class,
                    "checkAndReformatDateType", "2023-02-03 12:00:00", segmentTs, new DataType("string", 0, 0));
            Assertions.assertEquals("2023-02-03 12:00:00", formattedValue);
        }

        {
            String formattedValue = ReflectionTestUtils.invokeMethod(RealizationPruner.class,
                    "checkAndReformatDateType", "2023-02-03 12:00:00", segmentTs, new DataType("integer", 0, 0));
            Assertions.assertEquals("2023-02-03 12:00:00", formattedValue);
        }

        {
            String formattedValue = ReflectionTestUtils.invokeMethod(RealizationPruner.class,
                    "checkAndReformatDateType", "2023-02-03 12:00:00", segmentTs, new DataType("bigint", 0, 0));
            Assertions.assertEquals("2023-02-03 12:00:00", formattedValue);
        }

        {
            DataType errorType = new DataType("error_type", 0, 0);
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> ReflectionTestUtils.invokeMethod(RealizationPruner.class, "checkAndReformatDateType",
                            "2023-02-03 12:00:00", segmentTs, errorType));
        }
    }
}
