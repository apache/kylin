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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.dimension.DateDimEnc;
import org.junit.Before;
import org.junit.Test;

import com.google.common.testing.EqualsTester;

public class DateStrDictionaryTest {

    DateStrDictionary dict;
    DateStrDictionary dictWithBaseId;
    int baseId = 1;
    static String datePattern = "yyyy-MM-dd";

    @Before
    public void setup() {
        dict = new DateStrDictionary();
        dictWithBaseId = new DateStrDictionary(datePattern, baseId);
    }

    @Test
    public void testGetMinId() {
        assertEquals(0, dict.getMinId());
        assertEquals(baseId, dictWithBaseId.getMinId());
    }

    @Test
    public void testGetMaxId() {
        assertEquals(DateDimEnc.ID_9999_12_31, dict.getMaxId());
        assertEquals(baseId + DateDimEnc.ID_9999_12_31, dictWithBaseId.getMaxId());
    }

    @Test
    public void testGetSizeOfValue() {
        assertEquals(DateFormat.DEFAULT_DATE_PATTERN.length(), dict.getSizeOfValue());
        assertEquals(datePattern.length(), dictWithBaseId.getSizeOfValue());
    }

    @Test
    public void testWrite() throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dataout = new DataOutputStream(bout);
        dict.write(dataout);
        dataout.close();
        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        DataInputStream datain = new DataInputStream(bin);

        String datePattern = datain.readUTF();
        int baseId = datain.readInt();
        datain.close();

        assertEquals(DateFormat.DEFAULT_DATE_PATTERN, datePattern);
        assertEquals(baseId, 0);
    }

    @Test
    public void testReadFields() throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dataout = new DataOutputStream(bout);
        dataout.writeUTF(DateFormat.DEFAULT_DATE_PATTERN);
        dataout.writeInt(0);
        dataout.close();

        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        DataInputStream datain = new DataInputStream(bin);
        dict.readFields(datain);
        datain.close();

        assertEquals(DateFormat.DEFAULT_DATE_PATTERN.length(), dict.getSizeOfValue());
        assertEquals(0, dict.getMinId());
        test();
    }

    @Test
    public void testEqualsAndHashCode() {
        new EqualsTester().addEqualityGroup(dict, new DateStrDictionary(DateFormat.DEFAULT_DATE_PATTERN, 0))
                .addEqualityGroup(dictWithBaseId).testEquals();
    }

    @Test
    public void testContains() {
        assertTrue(dict.contains(new DateStrDictionary()));
        assertFalse(dict.contains(dictWithBaseId));
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

        assertEquals("DateStrDictionary [pattern=yyyy-MM-dd, baseId=0]", output);
    }

    @Test
    public void testMinMaxId() {
        assertEquals(0, dict.getIdFromValue("0000-01-01"));
        assertEquals(DateDimEnc.ID_9999_12_31, dict.getIdFromValue("9999-12-31"));

        try {
            dict.getValueFromId(-2); // -1 is id for NULL
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            // good
        }

        try {
            dict.getValueFromId(DateDimEnc.ID_9999_12_31 + 1);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            // good
        }

        try {
            dict.getIdFromValue("10000-1-1");
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            // good
        }
    }

    @Test
    public void testNull() {
        int nullId = dict.getIdFromValue(null);
        assertNull(dict.getValueFromId(nullId));
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
        int id = dict.getIdFromValue(dateStr);
        String dateStrBack = dict.getValueFromId(id);
        assertEquals(dateStr, dateStrBack);
    }

    @Test
    public void testIllegalArgument() {
        try {
            dict.getIdFromValue("abcd");
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            // good
        }

        try {
            dict.getValueFromId(-2);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            // good
        }
    }

}
