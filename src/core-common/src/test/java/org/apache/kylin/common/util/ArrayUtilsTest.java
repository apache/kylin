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

import java.util.List;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ArrayUtilsTest {
    @Test
    public void testTo2DArray() {
        // normal case
        List<List<String>> input = Lists.newArrayList();
        input.add(Lists.newArrayList("1", "2"));
        input.add(Lists.newArrayList("3"));
        input.add(Lists.newArrayList("4", "5", "6"));
        input.add(null);
        input.add(Lists.newArrayList());

        String[][] expected = new String[5][];
        expected[0] = new String[] { "1", "2" };
        expected[1] = new String[] { "3" };
        expected[2] = new String[] { "4", "5", "6" };
        expected[3] = null;
        expected[4] = new String[0];

        Assertions.assertArrayEquals(expected, ArrayUtils.to2DArray(input));

        // empty case
        Assertions.assertArrayEquals(new String[0][], ArrayUtils.to2DArray(Lists.newArrayList()));

        // null case
        Assertions.assertNull(ArrayUtils.to2DArray(null));
    }
}
