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

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kylin.metadata.model.DataType;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * @author yangli9
 * 
 */
public class NumberDictionaryTest {

    NumberDictionary.NumberBytesCodec codec = new NumberDictionary.NumberBytesCodec();
    Random rand = new Random();


    @Test
    public void testEmptyInput() {
        String[] ints = new String[] { "", "0", "5", "100", "13" };
        Collection<byte[]> intBytes = new ArrayList<byte[]>();
        for (String s : ints) {
            intBytes.add((s == null) ? null : Bytes.toBytes(s));
        }
        
        // check "" is treated as NULL, not a code of dictionary
        Dictionary<?> dict = DictionaryGenerator.buildDictionaryFromValueList(DataType.getInstance("integer"), intBytes);
        assertEquals(5, dict.getSize());

        final int id = ((NumberDictionary<String>) dict).getIdFromValue("");
        assertEquals(id, dict.nullId());
    }

    
    @Test
    public void testNumberEncode() {
        checkCodec("12345", "00000000000012345");
        checkCodec("12345.123", "00000000000012345.123");
        checkCodec("-12345", "-9999999999987654;");
        checkCodec("-12345.123", "-9999999999987654.876;");
        checkCodec("0", "00000000000000000");
        checkCodec("0.0", "00000000000000000.0");
    }

    private void checkCodec(String number, String code) {
        assertEquals(code, encodeNumber(number));
        assertEquals(number, decodeNumber(code));
    }

    private String decodeNumber(String code) {
        byte[] buf = Bytes.toBytes(code);
        System.arraycopy(buf, 0, codec.buf, 0, buf.length);
        codec.bufOffset = 0;
        codec.bufLen = buf.length;
        int len = codec.decodeNumber(buf, 0);
        return Bytes.toString(buf, 0, len);
    }

    private String encodeNumber(String number) {
        byte[] num1 = Bytes.toBytes(number);
        codec.encodeNumber(num1, 0, num1.length);
        return Bytes.toString(codec.buf, codec.bufOffset, codec.bufLen);
    }

    @Test
    public void testDictionary() {
        int n = 100;

        Set<BigDecimal> set = Sets.newHashSet();
        NumberDictionaryBuilder<String> builder = new NumberDictionaryBuilder<String>(new StringBytesConverter());
        for (int i = 0; i < n; i++) {
            String num = randNumber();
            if (set.add(new BigDecimal(num))) {
                builder.addValue(num);
            }
        }

        List<BigDecimal> sorted = Lists.newArrayList();
        sorted.addAll(set);
        Collections.sort(sorted);

        // test exact match
        NumberDictionary<String> dict = builder.build(0);
        for (int i = 0; i < sorted.size(); i++) {
            String dictNum = dict.getValueFromId(i);
            System.out.println(sorted.get(i) + "\t" + dictNum);
        }
        
        for (int i = 0; i < sorted.size(); i++) {
            String dictNum = dict.getValueFromId(i);
            assertEquals(sorted.get(i), new BigDecimal(dictNum));
        }

        // test rounding
        for (int i = 0; i < n * 50; i++) {
            String randStr = randNumber();
            BigDecimal rand = new BigDecimal(randStr);
            int binarySearch = Collections.binarySearch(sorted, rand);
            if (binarySearch >= 0)
                continue;
            int insertion = -(binarySearch + 1);
            int expectedLowerId = insertion - 1;
            int expectedHigherId = insertion;
            // System.out.println("-- " + randStr + ", " + expectedLowerId +
            // ", " + expectedHigherId);

            if (expectedLowerId < 0) {
                try {
                    dict.getIdFromValue(randStr, -1);
                    fail();
                } catch (IllegalArgumentException ex) {
                    // expect
                }
            } else {
                assertEquals(expectedLowerId, dict.getIdFromValue(randStr, -1));
            }

            if (expectedHigherId >= sorted.size()) {
                try {
                    dict.getIdFromValue(randStr, 1);
                    fail();
                } catch (IllegalArgumentException ex) {
                    // expect
                }
            } else {
                assertEquals(expectedHigherId, dict.getIdFromValue(randStr, 1));
            }
        }
    }

    private String randNumber() {
        int digits1 = rand.nextInt(10);
        int digits2 = rand.nextInt(3);
        int sign = rand.nextInt(2);
        if (digits1 == 0 && digits2 == 0) {
            return randNumber();
        }
        StringBuilder buf = new StringBuilder();
        if (sign == 1)
            buf.append("-");
        for (int i = 0; i < digits1; i++)
            buf.append("" + rand.nextInt(10));
        if (digits2 > 0) {
            buf.append(".");
            for (int i = 0; i < digits2; i++)
                buf.append("" + rand.nextInt(9) + 1); // BigDecimal thinks 4.5
                                                      // != 4.50, my god!
        }
        return buf.toString();
    }

}
