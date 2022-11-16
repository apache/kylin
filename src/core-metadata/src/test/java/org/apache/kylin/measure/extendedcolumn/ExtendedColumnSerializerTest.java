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

package org.apache.kylin.measure.extendedcolumn;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExtendedColumnSerializerTest {
    private static MeasureType<ByteArray> measureType;

    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
        measureType = (MeasureType<ByteArray>) MeasureTypeFactory.create("EXTENDED_COLUMN", "extendedcolumn(20)");
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void testSerDesNull() {
        ExtendedColumnSerializer serializer = new ExtendedColumnSerializer(DataType.getType("extendedcolumn(20)"));
        MeasureIngester<ByteArray> ingester = measureType.newIngester();
        ByteArray array = ingester.valueOf(new String[] { null, null }, null, null);
        Assert.assertTrue(new ByteArray().equals(array));

        ByteBuffer buffer = ByteBuffer.allocate(serializer.maxLength());
        serializer.serialize(array, buffer);
        buffer.flip();
        ByteArray des = serializer.deserialize(buffer);
        Assert.assertTrue(new ByteArray().equals(des));
    }

    @Test
    public void testNormal() {
        String text = StringUtils.repeat("h", 20);

        ExtendedColumnSerializer serializer = new ExtendedColumnSerializer(DataType.getType("extendedcolumn(20)"));
        MeasureIngester<ByteArray> ingester = measureType.newIngester();
        ByteArray array = ingester.valueOf(new String[] { null, text }, null, null);

        ByteBuffer buffer = ByteBuffer.allocate(serializer.maxLength());
        serializer.serialize(array, buffer);
        buffer.flip();
        ByteArray des = serializer.deserialize(buffer);
        Assert.assertEquals(new ByteArray(text.getBytes(Charset.defaultCharset())), des);
    }

    @Test
    public void testOverflow() {
        String text = StringUtils.repeat("h", 21);
        ExtendedColumnSerializer serializer = new ExtendedColumnSerializer(DataType.getType("extendedcolumn(20)"));
        MeasureIngester<ByteArray> ingester = measureType.newIngester();
        ByteArray array = ingester.valueOf(new String[] { null, text }, null, null);

        ByteBuffer buffer = ByteBuffer.allocate(serializer.maxLength());
        serializer.serialize(array, buffer);
        buffer.flip();
        ByteArray des = serializer.deserialize(buffer);
        Assert.assertEquals(new ByteArray(StringUtils.repeat("h", 20).getBytes(Charset.defaultCharset())), des);
    }
}
