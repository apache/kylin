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

package org.apache.kylin.dict;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.apache.kylin.common.util.DateFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.testing.EqualsTester;

/**
 */
public class TimeStrDictionaryTest {
    TimeStrDictionary dict;

    @Before
    public void setup() {
        dict = new TimeStrDictionary();
    }

    @Test
    public void basicTest() {
        int a = dict.getIdFromValue("1999-01-01");
        int b = dict.getIdFromValue("1999-01-01 00:00:00");
        int c = dict.getIdFromValue("1999-01-01 00:00:00.000");
        int d = dict.getIdFromValue("1999-01-01 00:00:00.022");

        assertEquals(a, b);
        assertEquals(a, c);
        assertEquals(a, d);
    }

    @Test
    public void testGetValueFromIdImpl() {
        String value = dict.getValueFromIdImpl(0xffffffff);
        assertEquals(null, value);
    }

    @Test
    public void testEncodeDecode() {
        encodeDecode("1999-01-12");
        encodeDecode("2038-01-09");
        encodeDecode("2038-01-08");
        encodeDecode("1970-01-01");
        encodeDecode("1970-01-02");

        encodeDecode("1999-01-12 11:00:01");
        encodeDecode("2038-01-09 01:01:02");
        encodeDecode("2038-01-19 03:14:06");
        encodeDecode("1970-01-01 23:22:11");
        encodeDecode("1970-01-02 23:22:11");
    }

    @Test
    public void testIllegal() {
        try {
            dict.getIdFromValue("2038-01-19 03:14:07");
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            //correct
        }
    }

    public void encodeDecode(String origin) {
        int a = dict.getIdFromValue(origin);
        String back = dict.getValueFromId(a);

        String originChoppingMilis = DateFormat.formatToTimeWithoutMilliStr(DateFormat.stringToMillis(origin));
        Assert.assertEquals(originChoppingMilis, back);
    }

    @Test
    public void testGetMinId() {
        assertEquals(0, dict.getMinId());
    }

    @Test
    public void testGetMaxId() {
        assertEquals(Integer.MAX_VALUE - 1, dict.getMaxId());
    }

    @Test
    public void testGetSizeOfId() {
        assertEquals(4, dict.getSizeOfId());
    }

    @Test
    public void testGetSizeOfValue() {
        assertEquals(19, dict.getSizeOfValue());
    }

    @Test
    public void testEqualsAndHashCode() {
        new EqualsTester().addEqualityGroup(dict, new TimeStrDictionary()).addEqualityGroup(new String("invalid"))
                .testEquals();
    }

    @Test
    public void testContains() {
        assertTrue(dict.contains(new TimeStrDictionary()));
    }

    @Test
    public void testDump() throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(bout, false, StandardCharsets.UTF_8.name());
        dict.dump(ps);

        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        BufferedReader reader = new BufferedReader(new InputStreamReader(bin, StandardCharsets.UTF_8));
        String output = reader.readLine();

        bout.close();
        bin.close();
        ps.close();
        reader.close();

        assertEquals(
                "TimeStrDictionary supporting from 1970-01-01 00:00:00 to 2038/01/19 03:14:07 (does not support millisecond)",
                output);
    }
}
