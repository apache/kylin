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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.kylin.query.udf.nullHandling.IfNullUDF;
import org.apache.kylin.query.udf.nullHandling.IsNullUDF;
import org.junit.Test;

public class NullHandlingUDFTest {

    @Test
    public void testIfNullUDF() throws Exception {
        IfNullUDF ifNullUDF = new IfNullUDF();

        String str1 = ifNullUDF.IFNULL("Apache", "Kylin");
        assertEquals("Apache", str1);
        String str2 = ifNullUDF.IFNULL(null, "Kylin");
        assertEquals("Kylin", str2);

        double d1 = ifNullUDF.IFNULL(2.3, 2.4);
        assertEquals(d1, 2.3, 0);
        double d2 = ifNullUDF.IFNULL(null, 2.4);
        assertEquals(d2, 2.4, 0);

        int num1 = ifNullUDF.IFNULL(23, 24);
        assertEquals(num1, 23);
        int num2 = ifNullUDF.IFNULL(null, 24);
        assertEquals(num2, 24);

        Date date1 = new Date(System.currentTimeMillis());
        Date date2 = new Date(System.currentTimeMillis());
        Date date3 = ifNullUDF.IFNULL(date1, date2);
        assertEquals(date3, date1);
        Date date4 = ifNullUDF.IFNULL(null, date2);
        assertEquals(date4, date2);

        Timestamp timestamp1 = new Timestamp(System.currentTimeMillis());
        Timestamp timestamp2 = new Timestamp(System.currentTimeMillis());
        Timestamp timestamp3 = ifNullUDF.IFNULL(timestamp1, timestamp2);
        assertEquals(timestamp3, timestamp1);
        Timestamp timestamp4 = ifNullUDF.IFNULL(null, timestamp2);
        assertEquals(timestamp4, timestamp2);

        assertTrue(ifNullUDF.IFNULL(true, false));
        assertFalse(ifNullUDF.IFNULL(null, false));
    }

    @Test
    public void testIsNullUDF() throws Exception {
        IsNullUDF isNullUDF = new IsNullUDF();

        assertFalse(isNullUDF.ISNULL("Apache"));
        String str = null;
        assertTrue(isNullUDF.ISNULL(str));

        assertFalse(isNullUDF.ISNULL(2.3));
        Double d = null;
        assertTrue(isNullUDF.ISNULL(d));

        assertFalse(isNullUDF.ISNULL(2));
        Integer integer = null;
        assertTrue(isNullUDF.ISNULL(integer));

        Date date1 = new Date(System.currentTimeMillis());
        assertFalse(isNullUDF.ISNULL(date1));
        Date date2 = null;
        assertTrue(isNullUDF.ISNULL(date2));

        Timestamp timestamp1 = new Timestamp(System.currentTimeMillis());
        assertFalse(isNullUDF.ISNULL(timestamp1));
        Timestamp timestamp2 = null;
        assertTrue(isNullUDF.ISNULL(timestamp2));

        assertFalse(isNullUDF.ISNULL(true));
        Boolean b = null;
        assertTrue(isNullUDF.ISNULL(b));
    }
}
