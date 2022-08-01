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

import org.apache.kylin.query.udf.dateUdf.DateDiffUDF;
import org.apache.kylin.query.udf.dateUdf.DatePartUDF;
import org.apache.kylin.junit.annotation.MultiTimezoneTest;

public class DateUDFTest {

    @MultiTimezoneTest(timezones = { "GMT+8", "GMT+12", "GMT+0" })
    public void testDateDiffUDF() throws Exception {
        DateDiffUDF dateDiffUDF = new DateDiffUDF();
        assertEquals(-35, (long) dateDiffUDF.DATEDIFF(Date.valueOf("2019-06-28"), Date.valueOf("2019-08-02")));
    }

    @MultiTimezoneTest(timezones = { "GMT+8", "GMT+12", "GMT+0" })
    public void testDatePartUDF() throws Exception {
        DatePartUDF datePartUDF = new DatePartUDF();
        assertEquals(2019, (long) datePartUDF.DATE_PART("year", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(5, (long) datePartUDF.DATE_PART("month", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(9, (long) datePartUDF.DATE_PART("day", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(11, (long) datePartUDF.DATE_PART("hour", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(49, (long) datePartUDF.DATE_PART("minute", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(45, (long) datePartUDF.DATE_PART("seconds", Timestamp.valueOf("2019-05-09 11:49:45")));
        assertEquals(2019, (long) datePartUDF.DATE_PART("year", Date.valueOf("2019-05-09")));
        assertEquals(5, (long) datePartUDF.DATE_PART("month", Date.valueOf("2019-05-09")));
        assertEquals(9, (long) datePartUDF.DATE_PART("day", Date.valueOf("2019-05-09")));
    }
}
