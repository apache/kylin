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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TailBuilderTest {

    @Test
    public void testConstructor() {
        assertThrows(IllegalArgumentException.class, () -> new TailBuilder(-1));
    }

    @Test
    public void testAppendCharSequence() {
        TailBuilder tailBuilder = new TailBuilder(10);

        // empty string
        tailBuilder.append("");
        assertEquals(0, tailBuilder.length());

        // str size < maxCapacity
        tailBuilder.append("abc");
        assertEquals(3, tailBuilder.length());

        // total size > maxCapacity
        tailBuilder.append("12345678");
        assertEquals("bc12345678", tailBuilder.toString());
        assertEquals(10, tailBuilder.length());

        // str size = maxCapacity
        tailBuilder = new TailBuilder(10);
        tailBuilder.append("1234567890");
        assertEquals("1234567890", tailBuilder.toString());
        assertEquals(10, tailBuilder.length());

        // str size > maxCapacity
        tailBuilder = new TailBuilder(6);
        tailBuilder.append("12345678901");
        assertEquals("678901", tailBuilder.toString());
        assertEquals(6, tailBuilder.length());
    }

    @Test
    public void testAppendChar() {
        TailBuilder tailBuilder = new TailBuilder(10);
        tailBuilder.append('a');
        assertEquals(1, tailBuilder.length());
    }

    @Test
    public void testEnsureWriteEntry() {
        TailBuilder tailBuilder = new TailBuilder(10);

        for (int i = 0; i < 10; i++) {
            tailBuilder.append('a');
        }
        tailBuilder.append('b');
        assertEquals(10, tailBuilder.length());
        assertEquals("aaaaaaaaab", tailBuilder.toString());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testLargeStr(boolean appendHead) {
        StringBuilder stringBuilder = new StringBuilder();
        TailBuilder tailBuilder = new TailBuilder(TailBuilder.ENTRY_SIZE / 2);
        if (appendHead) {
            stringBuilder.append("test head");
            tailBuilder.append("test head");
        }
        char[] chars = new char[TailBuilder.ENTRY_SIZE + 100];
        for (int i = 0; i < TailBuilder.ENTRY_SIZE + 100; i++) {
            chars[i] = (char) ('a' + (i % 26));
        }
        String testStr = new String(chars);
        stringBuilder.append(testStr);
        tailBuilder.append(testStr);
        assertEquals(TailBuilder.ENTRY_SIZE / 2, tailBuilder.length());
        assertEquals(stringBuilder.substring(stringBuilder.length() - TailBuilder.ENTRY_SIZE / 2),
                tailBuilder.toString());
    }

}
