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
package io.kyligence.kap.secondstorage.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class SecondStorageDateUtilsTest {
    @Test
    public void splitByDayStrTest() {
        List<String> expected = Arrays.asList("1992-02-01", "1992-02-02");
        List<String> result = SecondStorageDateUtils.splitByDayStr(696873600000L, 697046400000L);
        Assertions.assertIterableEquals(result, expected);

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class, () -> SecondStorageDateUtils.splitByDayStr(696873600000L, Long.MAX_VALUE));
        Assertions.assertEquals("segmentRange end is invalid.", exception.getMessage());
    }

    @Test
    public void testIsInfinite() {
        Assertions.assertTrue(SecondStorageDateUtils.isInfinite(0, Long.MAX_VALUE));
    }
}
