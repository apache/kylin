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
package org.apache.kylin.hive.serde2.lazy.lazy;

import static org.apache.kylin.hive.serde2.lazy.LazyQuoteAwareSerDe.PrimitiveParser.parseDate;
import static org.apache.kylin.hive.serde2.lazy.LazyQuoteAwareSerDe.PrimitiveParser.parseTimestamp;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.hive.serde2.lazy.LazyQuoteAwareSerDe;
import org.junit.Assert;
import org.junit.Test;

public class LazyQuoteAwareSerDeTest {

    @Test
    public void testParseCsvLines() {
        List<String[]> content = LazyQuoteAwareSerDe
                .parseCsvLines(ImmutableList.of("Bella1,$5%,2020-07-01,2022-04-14 12:00:00",
                        "Emily1,\"@6,000%\",2021-08-01,2022-04-14 12:00:00", "Coco,10,2012-03-01,2022-04-13 12:00:00"));
        String[] expectCol2 = new String[] { "$5%", "@6,000%", "10" };
        for (int i = 0; i < content.size(); i++) {
            String[] strs = content.get(i);
            System.out.println(Arrays.toString(strs));
            Assert.assertEquals(expectCol2[i], strs[1]);
        }
    }

    @Test
    public void testMissingOrExtraFields() throws Exception {
        LazyQuoteAwareSerDe serde = new LazyQuoteAwareSerDe();
        Properties tbl = new Properties();
        tbl.put("escape.delim", "\\");
        tbl.put("field.delim", ",");
        tbl.put("columns", "name,age,birth_day,update_time");
        tbl.put("columns.types", "string:bigint:date:timestamp");
        serde.initialize(new Configuration(), tbl);

        StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();

        {
            Text text = new Text("\"Coco\",3,2020-11-01,2022-04-13 12:00:00");
            LazyStruct struct = (LazyStruct) serde.deserialize(text);
            Assert.assertEquals("Coco", str(struct, 0, soi));
        }
        {
            Text text = new Text("Bella1,5,2020-07-01,2022-04-14 12:00:00,EXTRA_FIELD");
            LazyStruct struct = (LazyStruct) serde.deserialize(text);
            Assert.assertEquals("Bella1", str(struct, 0, soi));
            Assert.assertTrue(serde.hasExtraFieldWarn());
            Assert.assertFalse(serde.hasMissingFieldWarn());
        }
        {
            Text text = new Text("\"Emily1\",6,2021-08-01");
            LazyStruct struct = (LazyStruct) serde.deserialize(text);
            Assert.assertEquals("Emily1", str(struct, 0, soi));
            Assert.assertEquals("6", str(struct, 1, soi));
            Assert.assertEquals("2021-08-01", str(struct, 2, soi));
            Assert.assertNull(str(struct, 3, soi));
            Assert.assertTrue(serde.hasExtraFieldWarn());
            Assert.assertTrue(serde.hasMissingFieldWarn());
        }
    }

    @Test
    public void testQuoteEnabled() throws Exception {
        LazyQuoteAwareSerDe serde = new LazyQuoteAwareSerDe();
        Properties tbl = new Properties();
        tbl.put("escape.delim", "\\");
        tbl.put("field.delim", ",");
        tbl.put("quote.delim", "\"");
        tbl.put("columns", "name,age,birth_day,update_time,active");
        tbl.put("columns.types", "string:bigint:date:timestamp:boolean");
        serde.initialize(new Configuration(), tbl);

        StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();

        {
            Text text = new Text("\"Coco\",\"3\",\"2020-11-01\",\"2022-04-13 12:00:00\",0");
            LazyStruct struct = (LazyStruct) serde.deserialize(text);
            Assert.assertEquals("Coco", str(struct, 0, soi));
            Assert.assertEquals("3", str(struct, 1, soi));
            Assert.assertEquals("2020-11-01", str(struct, 2, soi));
            Assert.assertEquals("2022-04-13 12:00:00", str(struct, 3, soi));
            Assert.assertEquals("false", str(struct, 4, soi));
        }
        {
            Text text = new Text("Bel\"\"la1,$5%,2020/07/01,2022-04-14T12:00:00,true");
            LazyStruct struct = (LazyStruct) serde.deserialize(text);
            Assert.assertEquals("Bel\"\"la1", str(struct, 0, soi));
            Assert.assertEquals("5", str(struct, 1, soi));
            Assert.assertEquals("2020-07-01", str(struct, 2, soi));
            Assert.assertEquals("2022-04-14 12:00:00", str(struct, 3, soi));
            Assert.assertEquals("true", str(struct, 4, soi));
        }
        {
            Text text = new Text("\"Emi\"\"ly1\",\"￥6,000\",8/1/2021,2022-04-14T12:00:00.123Z,F");
            LazyStruct struct = (LazyStruct) serde.deserialize(text);
            Assert.assertEquals("Emi\"ly1", str(struct, 0, soi));
            Assert.assertEquals("6000", str(struct, 1, soi));
            Assert.assertEquals("2021-08-01", str(struct, 2, soi));
            Assert.assertEquals("2022-04-14 12:00:00.123", str(struct, 3, soi));
            Assert.assertEquals("false", str(struct, 4, soi));
        }
    }

    @Test
    public void testBadData() throws Exception {
        LazyQuoteAwareSerDe serde = new LazyQuoteAwareSerDe();
        Properties tbl = new Properties();
        tbl.put("escape.delim", "\\");
        tbl.put("field.delim", ",");
        tbl.put("quote.delim", "\"");
        tbl.put("columns", "name,age,birth_day,update_time");
        tbl.put("columns.types", "string:bigint:date:timestamp");
        serde.initialize(new Configuration(), tbl);

        StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();

        {
            Text text = new Text("\"Coco\",\"BAD_NUM\",\"BAD_DATE\",\"BAD_TIME\"");
            LazyStruct struct = (LazyStruct) serde.deserialize(text);
            Assert.assertEquals("Coco", str(struct, 0, soi));
            Assert.assertNull(str(struct, 1, soi));
            Assert.assertNull(str(struct, 2, soi));
            Assert.assertNull(str(struct, 3, soi));
        }
        {
            Text text = new Text(",,,");
            LazyStruct struct = (LazyStruct) serde.deserialize(text);
            Assert.assertEquals("", str(struct, 0, soi));
            Assert.assertNull(str(struct, 1, soi));
            Assert.assertNull(str(struct, 2, soi));
            Assert.assertNull(str(struct, 3, soi));
        }
    }

    @Test
    public void testDateTimeParser() {
        Assert.assertEquals("2022-01-01", parseDate("2022-1-1").toString());
        Assert.assertEquals("2022-01-01", parseDate("2022/1/1").toString());
        Assert.assertEquals("2022-01-01", parseDate("1/1/2022").toString());
        assertException(IllegalArgumentException.class, () -> parseDate(""));

        Assert.assertEquals("2022-01-01 00:00:00.0", parseTimestamp("2022-01-01 00:00:00").toString());
        Assert.assertEquals("2022-01-01 00:00:00.1", parseTimestamp("2022-01-01 00:00:00.1").toString());
        Assert.assertEquals("2022-01-01 00:00:00.456", parseTimestamp("2022-01-01 00:00:00,456").toString());
        Assert.assertEquals("2022-01-01 00:00:00.0", parseTimestamp("2022-01-01T00:00:00").toString());
        Assert.assertEquals("2022-01-01 00:00:00.12", parseTimestamp("2022-01-01T00:00:00.12").toString());
        Assert.assertEquals("2022-01-01 00:00:00.0", parseTimestamp("2022-01-01T00:00:00Z").toString());
        Assert.assertEquals("2022-01-01 00:00:00.123", parseTimestamp("2022-01-01T00:00:00.123Z").toString());
        assertException(IllegalArgumentException.class, () -> parseTimestamp(""));

        Assert.assertEquals("2022-01-01 13:24:00.0", parseTimestamp("2022-01-01 13:24").toString());
    }

    @Test
    public void testNumberForceReadIsFalse() throws Exception {
        LazyQuoteAwareSerDe serde = new LazyQuoteAwareSerDe();
        Properties tbl = new Properties();
        tbl.put("escape.delim", "\\");
        tbl.put("field.delim", ",");
        tbl.put("quote.delim", "\"");
        tbl.put("columns", "int_1,long_2,float_3,double_4");
        tbl.put("columns.types", "int:bigint:float:double");
        serde.initialize(new Configuration(), tbl);

        StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();

        {
            Text text = new Text("\"¥1\",\"$ 22\",\"1.1%\",\"22.2$$\"");
            LazyStruct struct = (LazyStruct) serde.deserialize(text);
            Assert.assertEquals("1", str(struct, 0, soi));
            Assert.assertEquals("22", str(struct, 1, soi));
            Assert.assertEquals("1.1", str(struct, 2, soi));
            Assert.assertEquals("22.2", str(struct, 3, soi));
        }
        {
            Text text = new Text("\"Q1\",\"$$$22\",\"1.1M\",\"22.2%%%\"");
            LazyStruct struct = (LazyStruct) serde.deserialize(text);
            Assert.assertNull(str(struct, 0, soi));
            Assert.assertNull(str(struct, 1, soi));
            Assert.assertNull(str(struct, 2, soi));
            Assert.assertNull(str(struct, 3, soi));
        }
    }

    @Test
    public void testNumberForceReadIsTrue() throws Exception {
        LazyQuoteAwareSerDe serde = new LazyQuoteAwareSerDe();
        Properties tbl = new Properties();
        tbl.put("escape.delim", "\\");
        tbl.put("field.delim", ",");
        tbl.put("quote.delim", "\"");
        tbl.put("columns", "int_1,long_2,float_3,double_4");
        tbl.put("columns.types", "int:bigint:float:double");
        tbl.put("number.force", "true");
        serde.initialize(new Configuration(), tbl);

        StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();

        {
            Text text = new Text("\"¥1\",\"$ 22\",\"1.1%\",\"22.2$$\"");
            LazyStruct struct = (LazyStruct) serde.deserialize(text);
            Assert.assertEquals("1", str(struct, 0, soi));
            Assert.assertEquals("22", str(struct, 1, soi));
            Assert.assertEquals("1.1", str(struct, 2, soi));
            Assert.assertEquals("22.2", str(struct, 3, soi));
        }
        {
            Text text = new Text("\"Q1\",\"$$$22\",\"1.1M\",\"22.2%%%\"");
            LazyStruct struct = (LazyStruct) serde.deserialize(text);
            Assert.assertEquals("1", str(struct, 0, soi));
            Assert.assertEquals("22", str(struct, 1, soi));
            Assert.assertEquals("1.1", str(struct, 2, soi));
            Assert.assertEquals("22.2", str(struct, 3, soi));
        }
    }

    @Test
    public void testDates() throws Exception {
        LazyQuoteAwareSerDe serde = new LazyQuoteAwareSerDe();
        Properties tbl = new Properties();
        tbl.put("escape.delim", "\\");
        tbl.put("field.delim", ",");
        tbl.put("columns", "d1,d2,d3,d4");
        tbl.put("columns.types", "date,date,date,date");
        serde.initialize(new Configuration(), tbl);

        StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();

        {
            Text text = new Text("20231001,2023/11,2023-01,202301");
            LazyStruct struct = (LazyStruct) serde.deserialize(text);
            Assert.assertEquals("2023-10-01", str(struct, 0, soi));
            Assert.assertEquals("2023-11-01", str(struct, 1, soi));
            Assert.assertEquals("2023-01-01", str(struct, 2, soi));
            Assert.assertEquals("2023-01-01", str(struct, 3, soi));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void assertException(Class<?> exClass, Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception ex) {
            if (exClass != ex.getClass())
                throw new IllegalStateException("Expect exception type " + exClass + ", but it is: ", ex);
            return; // good ending
        }
        throw new IllegalStateException("Expect exception type " + exClass);
    }

    private String str(LazyStruct struct, int fieldId, StructObjectInspector soi) {
        StructField field = soi.getAllStructFieldRefs().get(fieldId);
        //noinspection unchecked
        LazyPrimitive<ObjectInspector, Writable> val = (LazyPrimitive<ObjectInspector, Writable>) soi
                .getStructFieldData(struct, field);
        return val == null ? null : val.getWritableObject().toString();
    }
}
