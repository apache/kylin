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

import java.util.Arrays;

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

import com.google.gson.JsonParser;

import lombok.val;
import scala.collection.AbstractIterator;

public class PartitionRowIteratorTest extends StreamingTestCase {

    private static String PROJECT = "streaming_test";
    private static String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";
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
        }, schema);
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
        }, schema);
        Assert.assertTrue(partitionRowIter.hasNext());
        val row = partitionRowIter.next();
        Assert.assertEquals(0, row.length());
    }

    @Test
    public void testConvertJson2Row() {
        val parser = new JsonParser();
        val schemas = Arrays.asList(ShortType, IntegerType, LongType, DoubleType, FloatType, BooleanType, TimestampType,
                DateType, DecimalType.apply(5, 2));
        schemas.stream().forEach(dataType -> {
            val schema = new StructType().add("value", dataType);
            val partitionRowIter = new PartitionRowIterator(null, schema);

            val row = partitionRowIter.convertJson2Row("{}", parser);
            Assert.assertNull(row.get(0));
            val row1 = partitionRowIter.convertJson2Row("{value:\"\"}", parser);
            Assert.assertNull(row1.get(0));
            val row2 = partitionRowIter.convertJson2Row("{value2:\"\"}", parser);
            Assert.assertNull(row2.get(0));
        });

    }
}
