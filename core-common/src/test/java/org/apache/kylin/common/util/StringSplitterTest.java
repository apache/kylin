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

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for class {@link StringSplitter}.
 *
 * @see StringSplitter
 *
 */
public class StringSplitterTest{

  @Test
  public void testSplitReturningNonEmptyArray() {
      String[] stringArray = StringSplitter.split("Fc8!v~f?aQL", "Fc8!v~f?aQL");

      assertEquals(2, stringArray.length);
      assertEquals("", stringArray[0]);
      assertEquals("", stringArray[1]);
  }

  @Test
  public void testSplitWithNonEmptyString() {
      String[] stringArray = StringSplitter.split("]sZ}gR\"cws,8p#|m", "Fc8!v~f?aQL");

      assertEquals(1, stringArray.length);
      assertEquals("]sZ}gR\"cws,8p#|m", stringArray[0]);
  }

}