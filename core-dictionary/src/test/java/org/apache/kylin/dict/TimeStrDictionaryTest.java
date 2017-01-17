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

import static org.junit.Assert.fail;

import org.apache.kylin.common.util.DateFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

        Assert.assertEquals(a, b);
        Assert.assertEquals(a, c);
        Assert.assertEquals(a, d);
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

}
