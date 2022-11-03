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
package org.apache.kylin.streaming.jobs;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.ShortType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

import org.apache.kylin.streaming.PartitionRowIterator;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;
import scala.collection.AbstractIterator;

public class PartitionRowIteratorTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testNextEmpty() {
        val schema = new StructType().add("value", StringType);

        val partitionRowIter = new PartitionRowIterator(new AbstractIterator<Row>() {
            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Row next() {
                return RowFactory.create("");
            }
        }, schema, "value");
        Assert.assertTrue(partitionRowIter.hasNext());
        val row = partitionRowIter.next();
        Assert.assertEquals(0, row.length());
    }

    @Test
    public void testNextParseException() {
        val schema = new StructType().add("value", IntegerType);

        val partitionRowIter = new PartitionRowIterator(new AbstractIterator<Row>() {
            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Row next() {
                return RowFactory.create("{\"value\":\"ab\"}");
            }
        }, schema, "value");
        Assert.assertTrue(partitionRowIter.hasNext());
        val row = partitionRowIter.next();
        Assert.assertEquals(0, row.length());
    }

    @Test
    public void testConvertEmptyJsonValue2Row() {
        val schemas = Arrays.asList(ShortType, IntegerType, LongType, DoubleType, FloatType, BooleanType, TimestampType,
                DateType, DecimalType.apply(5, 2));
        schemas.stream().forEach(dataType -> {
            val schema = new StructType().add("value", dataType);
            val partitionRowIter = new PartitionRowIterator(null, schema, "value");
            {
                val row = partitionRowIter.convertJson2Row("{}");
                Assert.assertNull(row.get(0));
            }
            {
                val row = partitionRowIter.convertJson2Row("{value:\"\"}");
                Assert.assertNull(row.get(0));
            }
            {
                val row = partitionRowIter.convertJson2Row("{value2:\"\"}");
                Assert.assertNull(row.get(0));
            }
        });
    }

    @Test
    public void testConvertJson2Row() {
        {
            val schema = new StructType().add("value1", ShortType).add("value2", IntegerType).add("value3", LongType)
                    .add("value4", DoubleType).add("value5", FloatType).add("value6", BooleanType);
            val partitionRowIter = new PartitionRowIterator(null, schema, "value2");
            val row = partitionRowIter.convertJson2Row("{\"value1\":121,"
                    + "\"value2\":122,\"value3\":123,\"value4\":124, \"value5\":125,\"value6\":true}");
            Assert.assertEquals((short) 121, row.get(0));
            Assert.assertEquals(122, row.get(1));
            Assert.assertEquals(123L, row.get(2));
            Assert.assertEquals(124D, row.get(3));
            Assert.assertEquals(125F, row.get(4));
            Assert.assertEquals(true, row.get(5));
        }
        {
            val schema = new StructType().add("value1", DecimalType.apply(38, 18));
            val partitionRowIter = new PartitionRowIterator(null, schema, null);
            val row = partitionRowIter.convertJson2Row("{\"value1\":4.567}");
            Assert.assertEquals(scala.math.BigDecimal.valueOf(4.567), row.get(0));
        }
        {
            val schema = new StructType().add("value1", StringType);
            val partitionRowIter = new PartitionRowIterator(null, schema, null);
            val row = partitionRowIter.convertJson2Row("{\"value1\":\"\"}");
            Assert.assertEquals("", row.get(0));
        }
    }

    @Test
    public void testTimestampType() {
        val schema = new StructType().add("value", TimestampType);
        val partitionRowIterator = new PartitionRowIterator(null, schema, "value");
        {
            Assert.assertThrows("invalid value 1970-01-01 00:00:00", IllegalArgumentException.class,
                    () -> partitionRowIterator.convertJson2Row("{value: \"1970-01-01 00:00:00\"}"));
        }
        {
            val row = partitionRowIterator.convertJson2Row("{value: \"1970-01-01 08:00:00\"}");
            Assert.assertEquals(0L, ((Timestamp) row.get(0)).getTime());
        }
        {
            val row = partitionRowIterator.convertJson2Row("{value: \"2022-01-01 08:00:00\"}");
            Assert.assertEquals(1640995200000L, ((Timestamp) row.get(0)).getTime());
        }
    }

    @Test
    public void testTimestampTypeOfNonePartitionColumn() {
        val schema = new StructType().add("value", TimestampType);
        val partitionRowIterator = new PartitionRowIterator(null, schema, null);
        {
            val row = partitionRowIterator.convertJson2Row("{value: \"1956-01-01 00:00:00\"}");
            Assert.assertEquals(-441878400000L, ((Timestamp) row.get(0)).getTime());
        }
        {
            val row = partitionRowIterator.convertJson2Row("{value: \"1970-01-01 08:00:00\"}");
            Assert.assertEquals(0L, ((Timestamp) row.get(0)).getTime());
        }
        {
            val row = partitionRowIterator.convertJson2Row("{value: \"2022-01-01 08:00:00\"}");
            Assert.assertEquals(1640995200000L, ((Timestamp) row.get(0)).getTime());
        }
    }

    @Test
    public void testDateType() {
        val schema = new StructType().add("value", DateType);
        val partitionRowIterator = new PartitionRowIterator(null, schema, null);
        {
            val row = partitionRowIterator.convertJson2Row("{value: \"1956-01-02\"}");
            Assert.assertEquals(-441792000000L, ((Date) row.get(0)).getTime());
        }
        {
            val row = partitionRowIterator.convertJson2Row("{value: \"1970-01-01\"}");
            Assert.assertEquals(-28800000L, ((Date) row.get(0)).getTime());
        }
        {
            val row = partitionRowIterator.convertJson2Row("{value: \"1970-01-02\"}");
            Assert.assertEquals(57600000L, ((Date) row.get(0)).getTime());
        }
        {
            val row = partitionRowIterator.convertJson2Row("{value: \"2022-01-01\"}");
            Assert.assertEquals(1640966400000L, ((Date) row.get(0)).getTime());
        }
    }
}
