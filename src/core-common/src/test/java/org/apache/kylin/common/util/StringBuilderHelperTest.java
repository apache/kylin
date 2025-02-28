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

import java.util.Random;

import org.junit.jupiter.api.Test;

public class StringBuilderHelperTest {

    @Test
    public void testHead() {
        StringBuilderHelper helper = StringBuilderHelper.head(4);
        helper.append("abc");
        assertEquals("abc", helper.toString());
        helper.append('d');
        assertEquals("abcd", helper.toString());

        helper = StringBuilderHelper.head(4);
        helper.append("abcd");
        assertEquals("abcd", helper.toString());
        // append after full
        helper.append('e');
        assertEquals("abcd", helper.toString());
        helper.append("fg");
        assertEquals("abcd", helper.toString());
    }

    @Test
    public void testTail() {
        StringBuilderHelper helper = StringBuilderHelper.tail(4);
        helper.append("abc");
        assertEquals("abc", helper.toString());
        helper.append('d');
        assertEquals("abcd", helper.toString());
        // append after full
        helper.append('e');
        assertEquals("bcde", helper.toString());
        helper.append("fg");
        assertEquals("defg", helper.toString());
    }

    @Test
    public void testHeadAndTail() {
        StringBuilderHelper helper1 = StringBuilderHelper.headTail(3, 2);
        helper1.append("abc");
        assertEquals("abc", helper1.toString());
        helper1.append('d');
        assertEquals("abcd", helper1.toString());
        helper1.append('e');
        assertEquals("abcde", helper1.toString());
        helper1.append("fg");
        assertEquals("abcfg", helper1.toString());
        helper1.append('h');
        assertEquals("abcgh", helper1.toString());
    }

    @Test
    public void testHeadAndTailWithLargeSize() {
        // expected truncate
        testWithLength(3000, 3000, 10000, 0, 1, "TEST HEAD", "TEST TAIL");
        testWithLength(3000, 3000, 10000, 10, 100, "TEST HEAD", "TEST TAIL");
        testWithLength(3000, 3000, 10000, 10, TailBuilder.ENTRY_SIZE + 10, "TEST HEAD", "TEST TAIL");
        testWithLength(3000, 3000, 10000, TailBuilder.ENTRY_SIZE, TailBuilder.ENTRY_SIZE + 10, "TEST HEAD",
                "TEST TAIL");

        // expected not truncate
        testWithLength(3000, 3000, 1500, 0, 1, "TEST HEAD", "TEST TAIL");
        testWithLength(3000, 3000, 1500, 10, 100, "TEST HEAD", "TEST TAIL");
        testWithLength(3000, 3000, 1500, 10, TailBuilder.ENTRY_SIZE + 10, "TEST HEAD", "TEST TAIL");
        testWithLength(3000, 3000, 1500, TailBuilder.ENTRY_SIZE, TailBuilder.ENTRY_SIZE + 10, "TEST HEAD", "TEST TAIL");

    }

    private void testWithLength(int headSize, int tailSize, int appendTotal, int strMinLengthExclusive,
            int strMaxLength, String head, String tail) {
        StringBuilder stringBuilder = new StringBuilder();
        StringBuilderHelper helper1 = StringBuilderHelper.headTail(headSize, tailSize);
        helper1.append(head);
        stringBuilder.append(head);
        assertEquals(head, helper1.toString());
        int totalSize = headSize + tailSize;
        Random random = new Random();
        int appended = 0;
        while (appended < appendTotal) {
            int length = Math.min(random.nextInt(strMaxLength - strMinLengthExclusive + 1) + strMinLengthExclusive,
                    appendTotal - appended);
            String str = random.ints('a', 'z' + 1).limit(length)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
            if (str.length() == 1) {
                // test append char
                helper1.append(str.charAt(0));
            } else {
                helper1.append(str);
            }
            stringBuilder.append(str);
            appended += str.length();
        }
        helper1.append(tail);
        stringBuilder.append(tail);
        String string = stringBuilder.toString();
        if (string.length() > totalSize) {
            string = string.substring(0, headSize) + string.substring(string.length() - tailSize);
        }
        assertEquals(string, helper1.toString());
    }

}
