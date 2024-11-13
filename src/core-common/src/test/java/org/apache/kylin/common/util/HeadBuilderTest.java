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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class HeadBuilderTest {

    private static boolean isFull(HeadBuilder stringWriter) {
        return stringWriter.length() == stringWriter.capacity();
    }

    @Test
    void testWriteLimited() throws IOException {
        HeadBuilder stringWriter = new HeadBuilder(10);
        stringWriter.append('1');
        assertEquals("1", stringWriter.toString());
        assertEquals(10, stringWriter.capacity());
        assertFalse(isFull(stringWriter));
        stringWriter.append('2');
        assertEquals("12", stringWriter.toString());
        assertEquals(10, stringWriter.capacity());
        assertEquals(2, stringWriter.length());
        assertFalse(isFull(stringWriter));
        stringWriter.append("345");
        assertEquals("12345", stringWriter.toString());
        assertFalse(isFull(stringWriter));
        stringWriter.append("678910");
        assertWhenFull(stringWriter);
        stringWriter.append('a');
        assertWhenFull(stringWriter);
        stringWriter.append("aa", 1, 2);
        assertWhenFull(stringWriter);
        testFull(20, 12345);
        testFull(16, 12345);
        testFull(1204, 10024);
        testFull(1, 52431);
    }

    @Test
    void testConstructor() {
        // legal capacity
        new HeadBuilder(0);
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HeadBuilder(-1));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new HeadBuilder(-1, 0));
    }

    private static void testFull(int initCapacity, int maxCapacity) {
        HeadBuilder stringWriter;
        stringWriter = new HeadBuilder(initCapacity, maxCapacity);
        final String APPEND_STR = new String(new char[123]);
        final char APPEND_CHAR = 'a';
        final int SUB_APPEND_LENGTH = 89;
        while (!isFull(stringWriter)) {
            append(stringWriter, APPEND_STR, APPEND_CHAR, SUB_APPEND_LENGTH);
        }
        // append when full to test capacity not increase
        append(stringWriter, APPEND_STR, APPEND_CHAR, SUB_APPEND_LENGTH);
        assertEquals(maxCapacity, stringWriter.capacity());
        assertEquals(maxCapacity, stringWriter.length());
    }

    private static void append(HeadBuilder stringWriter, String appendStr, char appendChar, int subAppendLength) {
        int length;
        length = stringWriter.length();
        stringWriter.append(appendStr);
        if (!isFull(stringWriter)) {
            assertEquals(123, stringWriter.length() - length);
        }
        length = stringWriter.length();
        stringWriter.append(appendChar);
        if (!isFull(stringWriter)) {
            assertEquals(1, stringWriter.length() - length);
        }
        length = stringWriter.length();
        stringWriter.append(appendStr, 0, subAppendLength);
        if (!isFull(stringWriter)) {
            assertEquals(subAppendLength, stringWriter.length() - length);
        }
    }

    private static void assertWhenFull(HeadBuilder stringWriter) {
        assertEquals("1234567891", stringWriter.toString());
        assertEquals(10, stringWriter.capacity());
        assertEquals(10, stringWriter.length());
        assertTrue(isFull(stringWriter));
    }

}
