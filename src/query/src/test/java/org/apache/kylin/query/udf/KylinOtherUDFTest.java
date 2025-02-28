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
import static org.junit.Assert.assertThrows;

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.junit.annotation.MultiTimezoneTest;
import org.junit.Test;

public class KylinOtherUDFTest {
    private final KylinOtherUDF kylinOtherUDF = new KylinOtherUDF();

    @Test
    public void testVersionUDF() {
        String currentVer = KylinVersion.getCurrentVersion().toString();
        String udfVer = new KylinOtherUDF().VERSION();
        assertEquals(currentVer, udfVer);
    }

    @MultiTimezoneTest(timezones = { "GMT+8", "GMT+12", "GMT+0" })
    public void testYmdintBetween() {
        assertEquals("120906", kylinOtherUDF._ymdint_between(Date.valueOf("1990-04-30"), Date.valueOf("2003-02-05")));
        assertThrows(IllegalArgumentException.class,
                () -> kylinOtherUDF._ymdint_between("1990-04-30 00:00:00", "2003-02-05 00:00:00"));
    }

    @Test
    public void testToCharUDF() {

        assertEquals("2019", kylinOtherUDF.TO_CHAR(Timestamp.valueOf("2019-05-09 11:49:45"), "year"));
        assertEquals("05", kylinOtherUDF.TO_CHAR(Timestamp.valueOf("2019-05-09 11:49:45"), "month"));
        assertEquals("09", kylinOtherUDF.TO_CHAR(Timestamp.valueOf("2019-05-09 11:49:45"), "day"));
        assertEquals("11", kylinOtherUDF.TO_CHAR(Timestamp.valueOf("2019-05-09 11:49:45"), "hour"));
        assertEquals("49", kylinOtherUDF.TO_CHAR(Timestamp.valueOf("2019-05-09 11:49:45"), "minute"));
        assertEquals("45", kylinOtherUDF.TO_CHAR(Timestamp.valueOf("2019-05-09 11:49:45"), "seconds"));
        assertEquals("2019", kylinOtherUDF.TO_CHAR(Date.valueOf("2019-05-09"), "year"));
        assertEquals("05", kylinOtherUDF.TO_CHAR(Date.valueOf("2019-05-09"), "month"));
        assertEquals("09", kylinOtherUDF.TO_CHAR(Date.valueOf("2019-05-09"), "day"));
    }

    @Test
    public void testSplitPartUDF() {
        assertEquals("one", kylinOtherUDF.SPLIT_PART("oneAtwoBthreeC", "[ABC]", 1));
        assertEquals("two", kylinOtherUDF.SPLIT_PART("oneAtwoBthreeC", "[ABC]", 2));
        assertEquals("three", kylinOtherUDF.SPLIT_PART("oneAtwoBthreeC", "[ABC]", 3));
        assertNull(kylinOtherUDF.SPLIT_PART("oneAtwoBthreeC", "[ABC]", 10));
    }

    @Test
    public void testInStrUDF() {
        int s1 = kylinOtherUDF.INSTR("abcdebcf", "bc");
        assertEquals(2, s1);

        int s2 = kylinOtherUDF.INSTR("", "bc");
        assertEquals(0, s2);

        int s3 = kylinOtherUDF.INSTR("a", "bc");
        assertEquals(0, s3);

        int s4 = kylinOtherUDF.INSTR("abcdebcf", "");
        assertEquals(1, s4);

        int s5 = kylinOtherUDF.INSTR("abcdebcf", "bc", 4);
        assertEquals(6, s5);
    }

    @Test
    public void testStrPosUDF() {
        int s1 = kylinOtherUDF.STRPOS("abcdebcf", "bc");
        assertEquals(2, s1);

        int s2 = kylinOtherUDF.STRPOS("", "bc");
        assertEquals(0, s2);

        int s3 = kylinOtherUDF.STRPOS("a", "bc");
        assertEquals(0, s3);

        int s4 = kylinOtherUDF.STRPOS("abcdebcf", "");
        assertEquals(1, s4);
    }
}
