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

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.kylin.query.udf.formatUdf.DateFormatUDF;
import org.apache.kylin.query.udf.formatUdf.ToCharUDF;
import org.apache.kylin.query.udf.formatUdf.ToDateUDF;
import org.apache.kylin.query.udf.formatUdf.ToTimestampUDF;
import org.junit.Test;

public class FormatUDFTest {

    @Test
    public void testToTimestampUDF() throws Exception {
        ToTimestampUDF toTimestampUDF = new ToTimestampUDF();
        assertEquals(toTimestampUDF.TO_TIMESTAMP("2019-08-05 10:54:23"), Timestamp.valueOf("2019-08-05 10:54:23"));
        assertEquals(toTimestampUDF.TO_TIMESTAMP("2019-08-05 10:54:23", "yyyy-MM-dd hh:mm"),
                Timestamp.valueOf("2019-08-05 10:54:00"));
        assertEquals(toTimestampUDF.TO_TIMESTAMP("2019-08-05 10:54:23", "yyyy-MM-dd hh"),
                Timestamp.valueOf("2019-08-05 10:00:00"));
        assertEquals(toTimestampUDF.TO_TIMESTAMP("2019-08-05 10:54:23", "yyyy-MM-dd"),
                Timestamp.valueOf("2019-08-05 00:00:00"));
        assertEquals(toTimestampUDF.TO_TIMESTAMP("2019-08-05 10:54:23", "yyyy-MM"),
                Timestamp.valueOf("2019-08-01 00:00:00"));
        assertEquals(toTimestampUDF.TO_TIMESTAMP("2019-08-05 10:54:23", "yyyy"),
                Timestamp.valueOf("2019-01-01 00:00:00"));
    }

    @Test
    public void testToDateUDF() throws Exception {
        ToDateUDF toDateUDF = new ToDateUDF();
        assertEquals(toDateUDF.TO_DATE("2019-08-05"), Date.valueOf("2019-08-05"));
        assertEquals(toDateUDF.TO_DATE("2019-08-05", "yyyy-MM-dd"), Date.valueOf("2019-08-05"));
        assertEquals(toDateUDF.TO_DATE("2019-08-05", "yyyy-MM"), Date.valueOf("2019-08-01"));
        assertEquals(toDateUDF.TO_DATE("2019-08-05", "yyyy"), Date.valueOf("2019-01-01"));
    }

    @Test
    public void testToCharUDF() throws Exception {
        ToCharUDF toCharUDF = new ToCharUDF();
        assertEquals("2019", toCharUDF.TO_CHAR(Timestamp.valueOf("2019-05-09 11:49:45"), "year"));
        assertEquals("05", toCharUDF.TO_CHAR(Timestamp.valueOf("2019-05-09 11:49:45"), "month"));
        assertEquals("09", toCharUDF.TO_CHAR(Timestamp.valueOf("2019-05-09 11:49:45"), "day"));
        assertEquals("11", toCharUDF.TO_CHAR(Timestamp.valueOf("2019-05-09 11:49:45"), "hour"));
        assertEquals("49", toCharUDF.TO_CHAR(Timestamp.valueOf("2019-05-09 11:49:45"), "minute"));
        assertEquals("45", toCharUDF.TO_CHAR(Timestamp.valueOf("2019-05-09 11:49:45"), "seconds"));
        assertEquals("2019", toCharUDF.TO_CHAR(Date.valueOf("2019-05-09"), "year"));
        assertEquals("05", toCharUDF.TO_CHAR(Date.valueOf("2019-05-09"), "month"));
        assertEquals("09", toCharUDF.TO_CHAR(Date.valueOf("2019-05-09"), "day"));
    }

    @Test
    public void testDateFormatUDF() throws Exception {
        DateFormatUDF dateFormatUDF = new DateFormatUDF();
        assertEquals("2019", dateFormatUDF.DATE_FORMAT(Timestamp.valueOf("2019-05-09 11:49:45"), "year"));
        assertEquals("05", dateFormatUDF.DATE_FORMAT(Timestamp.valueOf("2019-05-09 11:49:45"), "month"));
        assertEquals("09", dateFormatUDF.DATE_FORMAT(Timestamp.valueOf("2019-05-09 11:49:45"), "day"));
        assertEquals("11", dateFormatUDF.DATE_FORMAT(Timestamp.valueOf("2019-05-09 11:49:45"), "hour"));
        assertEquals("49", dateFormatUDF.DATE_FORMAT(Timestamp.valueOf("2019-05-09 11:49:45"), "minute"));
        assertEquals("45", dateFormatUDF.DATE_FORMAT(Timestamp.valueOf("2019-05-09 11:49:45"), "seconds"));
        assertEquals("2019", dateFormatUDF.DATE_FORMAT(Date.valueOf("2019-05-09"), "year"));
        assertEquals("05", dateFormatUDF.DATE_FORMAT(Date.valueOf("2019-05-09"), "month"));
        assertEquals("09", dateFormatUDF.DATE_FORMAT(Date.valueOf("2019-05-09"), "day"));
    }
}
