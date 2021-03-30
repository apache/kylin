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

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for class {@link SumHelper}.
 *
 * @see SumHelper
 *
 */
public class SumHelperTest{

  @Test
  public void testSumDouble() {
      List<Double> linkedList = new LinkedList<>();
      Double doubleValue = new Double(2832);
      linkedList.add(doubleValue);
      Double result = SumHelper.sumDouble(linkedList);

      assertTrue(linkedList.contains(result));
      assertEquals(result, doubleValue, 0.01);

      assertEquals(2832.0, result, 0.01);
      assertEquals(1, linkedList.size());
  }

  @Test
  public void testSumIntegerReturningLongWhereShortValueIsPositive() {
      List<Integer> linkedList = new LinkedList<>();
      Integer integer = Integer.valueOf(4584);
      linkedList.add(integer);
      Long result = SumHelper.sumInteger(linkedList);

      assertTrue(linkedList.contains(integer));
      assertNotNull(result);

      assertEquals(4584L, (long) result);
      assertEquals(1, linkedList.size());
  }

  @Test
  public void testSumLong() {
      List<Integer> linkedList = new LinkedList<>();
      Long resultOne = SumHelper.sumInteger(linkedList);

      assertEquals(0L, (long) resultOne);
      assertEquals(0, linkedList.size());
      
      List<Long> linkedListTwo = new LinkedList<>();
      linkedListTwo.add(resultOne);
      Long resultTwo = SumHelper.sumLong(linkedListTwo);

      assertEquals(0L, (long) resultTwo);
      assertEquals(1, linkedListTwo.size());
  }

}