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

import org.junit.Test;

import java.util.BitSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BitSetsTest {

    @Test
    public void basicTest() {
        BitSet a = BitSets.valueOf(new int[] { 1, 3, 10 });
        assertEquals(3, a.cardinality());
        assertTrue(10 < a.size());
        assertTrue(a.get(3));
    }

  @Test
  public void testValueOfWithNull() {
      BitSet bitSet = BitSets.valueOf((int[]) null);

      assertEquals("{}", bitSet.toString());
      assertEquals(0, bitSet.cardinality());

      assertEquals(0, bitSet.length());
      assertTrue(bitSet.isEmpty());

      assertEquals(64, bitSet.size());
  }

}
