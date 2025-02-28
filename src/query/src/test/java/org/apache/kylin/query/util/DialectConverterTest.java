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

package org.apache.kylin.query.util;

import java.util.List;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DialectConverterTest extends NLocalFileMetadataTestCase {

    DialectConverter dialectConverter = new DialectConverter();

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testConvertSuccess() {
        List<Pair<String, String>> convertList = Lists.newArrayList(
                Pair.newPair("select PRICE from KYLIN_SALES \n limit 1",
                        "SELECT \"PRICE\"\nFROM \"KYLIN_SALES\"\nLIMIT 1"),
                Pair.newPair("select PRICE from KYLIN_SALES \n FETCH FIRST 1 ROWS ONLY ",
                        "SELECT \"PRICE\"\nFROM \"KYLIN_SALES\"\nLIMIT 1"),
                Pair.newPair("select PRICE from KYLIN_SALES \n FETCH FIRST 1 ROW ONLY ",
                        "SELECT \"PRICE\"\nFROM \"KYLIN_SALES\"\nLIMIT 1"),
                Pair.newPair("select PRICE from KYLIN_SALES \n OFFSET 0 ROWS\n FETCH NEXT 1 ROWS ONLY ",
                        "SELECT \"PRICE\"\nFROM \"KYLIN_SALES\"\nLIMIT 1\nOFFSET 0"),
                Pair.newPair("select PRICE from KYLIN_SALES \n OFFSET 0 ROWS",
                        "SELECT \"PRICE\"\nFROM \"KYLIN_SALES\"\nOFFSET 0"),
                Pair.newPair("select PRICE from KYLIN_SALES \n OFFSET 0 ROW",
                        "SELECT \"PRICE\"\nFROM \"KYLIN_SALES\"\nOFFSET 0"),
                Pair.newPair("select PRICE from KYLIN_SALES \n FETCH NEXT 1 ROWS ONLY",
                        "SELECT \"PRICE\"\nFROM \"KYLIN_SALES\"\nLIMIT 1"));

        for (Pair<String, String> p : convertList) {
            Assert.assertEquals(dialectConverter.convert(p.getFirst(), null, null), p.getSecond());
        }
    }

    @Test
    public void testConvertFailure() {
        List<String> convertList = Lists
                .newArrayList("select PRICE from KYLIN_SALES \n FETCH FIRST 1 ROWS  ONLY limit 1");

        for (String s : convertList) {
            Assert.assertEquals(dialectConverter.convert(s, null, null), s);
        }
    }
}
