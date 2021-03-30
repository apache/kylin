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

package org.apache.kylin.storage.hbase.steps;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RowValueDecoderTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testDecode() throws Exception {
        CubeDesc cubeDesc = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_with_slr_ready").getDescriptor();
        HBaseColumnDesc hbaseCol = cubeDesc.getHbaseMapping().getColumnFamily()[0].getColumns()[0];

        BufferedMeasureCodec codec = new BufferedMeasureCodec(hbaseCol.getMeasures());
        BigDecimal sum = new BigDecimal("333.1234567");
        BigDecimal min = new BigDecimal("333.1111111");
        BigDecimal max = new BigDecimal("333.1999999");
        Long count = new Long(2);
        Long item_count = new Long(100);
        ByteBuffer buf = codec.encode(new Object[] { sum, min, max, count, item_count });

        buf.flip();
        byte[] valueBytes = new byte[buf.limit()];
        System.arraycopy(buf.array(), 0, valueBytes, 0, buf.limit());

        RowValueDecoder rowValueDecoder = new RowValueDecoder(hbaseCol);
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            FunctionDesc aggrFunc = measure.getFunction();
            int index = hbaseCol.findMeasure(aggrFunc);
            rowValueDecoder.setProjectIndex(index);
        }

        rowValueDecoder.decodeAndConvertJavaObj(valueBytes);
        Object[] measureValues = rowValueDecoder.getValues();
        //BigDecimal.ROUND_HALF_EVEN in BigDecimalSerializer
        assertEquals("[333.1235, 333.1111, 333.2000, 2, 100]", Arrays.toString(measureValues));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testError() throws Exception {
        CubeDesc cubeDesc = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_with_slr_ready").getDescriptor();
        HBaseColumnDesc hbaseCol = cubeDesc.getHbaseMapping().getColumnFamily()[0].getColumns()[0];

        BufferedMeasureCodec codec = new BufferedMeasureCodec(hbaseCol.getMeasures());
        BigDecimal sum = new BigDecimal("11111111111111111111333.1234567");
        BigDecimal min = new BigDecimal("333.1111111");
        BigDecimal max = new BigDecimal("333.1999999");
        LongWritable count = new LongWritable(2);
        Long item_count = new Long(100);
        codec.encode(new Object[] { sum, min, max, count, item_count });

    }
}
