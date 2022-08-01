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

package org.apache.kylin.query.udf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.kylin.query.udf.stringUdf.ConcatUDF;
import org.apache.kylin.query.udf.stringUdf.InStrUDF;
import org.apache.kylin.query.udf.stringUdf.InitCapbUDF;
import org.apache.kylin.query.udf.stringUdf.LengthUDF;
import org.apache.kylin.query.udf.stringUdf.SplitPartUDF;
import org.apache.kylin.query.udf.stringUdf.StrPosUDF;
import org.apache.kylin.query.udf.stringUdf.SubStrUDF;
import org.junit.Test;

public class StringUDFTest {

    @Test
    public void testConcatUDF() throws Exception {
        ConcatUDF cu = new ConcatUDF();
        String str1 = cu.CONCAT("Apache ", "Kylin");
        assertEquals("Apache Kylin", str1);

        String str2 = cu.CONCAT("", "Kylin");
        assertEquals("Kylin", str2);

        String str3 = cu.CONCAT("Apache", "");
        assertEquals("Apache", str3);
    }

    @Test
    public void testInitCapbUDF() throws Exception {
        InitCapbUDF icu = new InitCapbUDF();
        String str1 = icu.INITCAPB("abc DEF 123aVC 124Btd,lAsT");
        assertEquals("Abc Def 123avc 124btd,Last", str1);

        String str2 = icu.INITCAPB("");
        assertEquals("", str2);
    }

    @Test
    public void testInStrUDF() throws Exception {
        InStrUDF isd = new InStrUDF();
        int s1 = isd.INSTR("abcdebcf", "bc");
        assertEquals(2, s1);

        int s2 = isd.INSTR("", "bc");
        assertEquals(0, s2);

        int s3 = isd.INSTR("a", "bc");
        assertEquals(0, s3);

        int s4 = isd.INSTR("abcdebcf", "");
        assertEquals(1, s4);

        int s5 = isd.INSTR("abcdebcf", "bc", 4);
        assertEquals(6, s5);
    }

    @Test
    public void testLeftUDF() throws Exception {
        //TODO
    }

    @Test
    public void testLengthUDF() throws Exception {
        LengthUDF lu = new LengthUDF();
        int len1 = lu.LENGTH("apache kylin");
        assertEquals(12, len1);

        int len2 = lu.LENGTH("");
        assertEquals(0, len2);
    }

    @Test
    public void testStrPosUDF() throws Exception {
        StrPosUDF spu = new StrPosUDF();
        int s1 = spu.STRPOS("abcdebcf", "bc");
        assertEquals(2, s1);

        int s2 = spu.STRPOS("", "bc");
        assertEquals(0, s2);

        int s3 = spu.STRPOS("a", "bc");
        assertEquals(0, s3);

        int s4 = spu.STRPOS("abcdebcf", "");
        assertEquals(1, s4);
    }

    @Test
    public void testSubStrUDF() throws Exception {
        SubStrUDF ssu = new SubStrUDF();

        String s1 = ssu.SUBSTR("apache kylin", 2);
        assertEquals("pache kylin", s1);

        String s2 = ssu.SUBSTR("apache kylin", 2, 5);
        assertEquals("pache", s2);

        String s3 = ssu.SUBSTR("", 2);
        assertNull(s3);

        String s4 = ssu.SUBSTR("", 2, 5);
        assertNull(s4);

        String s5 = ssu.SUBSTR("", 0, 5);
        assertNull(s5);

        String s6 = ssu.SUBSTR("a", 1, 5);
        assertEquals("a", s6);
    }

    @Test
    public void testSplitPartUDF() throws Exception {
        SplitPartUDF splitPartUDF = new SplitPartUDF();
        assertEquals("one", splitPartUDF.SPLIT_PART("oneAtwoBthreeC", "[ABC]", 1));
        assertEquals("two", splitPartUDF.SPLIT_PART("oneAtwoBthreeC", "[ABC]", 2));
        assertEquals("three", splitPartUDF.SPLIT_PART("oneAtwoBthreeC", "[ABC]", 3));
        assertNull(splitPartUDF.SPLIT_PART("oneAtwoBthreeC", "[ABC]", 10));
    }
}
