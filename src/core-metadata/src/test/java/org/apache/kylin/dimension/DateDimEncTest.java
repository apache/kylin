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

package org.apache.kylin.dimension;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.DateFormat;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Note the test must be consistent with DateStrDictionaryTest,
 * to ensure DateDimEnc is backward compatible with DateStrDictionary.
 */
@Ignore("Unused now")
public class DateDimEncTest {

    DateDimEnc enc;
    byte[] buf;

    @Before
    public void setup() {
        enc = new DateDimEnc(null);
        buf = new byte[enc.getLengthOfEncoding()];
    }

    private long encode(String value) {
        enc.encode(value, buf, 0);
        return BytesUtil.readLong(buf, 0, buf.length);
    }

    private String decode(long code) {
        BytesUtil.writeLong(code, buf, 0, buf.length);
        return enc.decode(buf, 0, buf.length);
    }

    @Test
    public void testMinMaxId() {
        assertEquals(0, encode("0000-01-01"));
        assertEquals(DateDimEnc.ID_9999_12_31, encode("9999-12-31"));

        try {
            encode("10000-1-1");
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            // good
        }
    }

    @Test
    public void testNull() {
        long nullId = encode(null);
        assertNull(decode(nullId));
        assertEquals(0xffffff, nullId & 0xffffff);
    }

    @Test
    public void test() {
        checkPair("0001-01-01");
        checkPair("1970-01-02");
        checkPair("1975-06-24");
        checkPair("2024-10-04");
        checkPair("9999-12-31");
    }

    private void checkPair(String dateStr) {
        long id = encode(dateStr);
        String millisStr = decode(id);
        String dateStrBack = DateFormat.formatToDateStr(Long.parseLong(millisStr));
        assertEquals(dateStr, dateStrBack);
    }

    @Test
    public void testIllegalArgument() {
        try {
            encode("abcd");
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            // good
        }
    }

}
