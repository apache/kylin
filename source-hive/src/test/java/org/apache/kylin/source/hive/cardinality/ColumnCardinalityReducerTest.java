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

package org.apache.kylin.source.hive.cardinality;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.measure.BufferedMeasureEncoder;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounter;
import org.junit.Before;
import org.junit.Test;

/**
 * @author ysong1
 * 
 */
public class ColumnCardinalityReducerTest {

    public final static String strArr = "abc,tests,test,test,as,sts,test,tss,sets";

    ReduceDriver<IntWritable, BytesWritable, IntWritable, LongWritable> reduceDriver;
    String localTempDir = System.getProperty("java.io.tmpdir") + File.separator;

    @Before
    public void setUp() {
        ColumnCardinalityReducer reducer = new ColumnCardinalityReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    private byte[] getBytes(String str) throws IOException {
        HyperLogLogPlusCounter hllc = new HyperLogLogPlusCounter();
        StringTokenizer tokenizer = new StringTokenizer(str, ColumnCardinalityMapper.DEFAULT_DELIM);
        int i = 0;
        while (tokenizer.hasMoreTokens()) {
            String temp = i + "_" + tokenizer.nextToken();
            i++;
            hllc.add(Bytes.toBytes(temp));
        }
        ByteBuffer buf = ByteBuffer.allocate(BufferedMeasureEncoder.DEFAULT_BUFFER_SIZE);
        buf.clear();
        hllc.writeRegisters(buf);
        buf.flip();
        return buf.array();
    }

    @Test
    public void testReducer() throws IOException {
        IntWritable key1 = new IntWritable(1);
        List<BytesWritable> values1 = new ArrayList<BytesWritable>();
        values1.add(new BytesWritable(getBytes(strArr)));

        IntWritable key2 = new IntWritable(2);
        List<BytesWritable> values2 = new ArrayList<BytesWritable>();
        values2.add(new BytesWritable(getBytes(strArr + " x")));

        IntWritable key3 = new IntWritable(3);
        List<BytesWritable> values3 = new ArrayList<BytesWritable>();
        values3.add(new BytesWritable(getBytes(strArr + " xx")));

        IntWritable key4 = new IntWritable(4);
        List<BytesWritable> values4 = new ArrayList<BytesWritable>();
        values4.add(new BytesWritable(getBytes(strArr + " xxx")));

        IntWritable key5 = new IntWritable(5);
        List<BytesWritable> values5 = new ArrayList<BytesWritable>();
        values5.add(new BytesWritable(getBytes(strArr + " xxxx")));

        reduceDriver.withInput(key1, values1);
        reduceDriver.withInput(key2, values2);
        reduceDriver.withInput(key3, values3);
        reduceDriver.withInput(key4, values4);
        reduceDriver.withInput(key5, values5);

        List<Pair<IntWritable, LongWritable>> result = reduceDriver.run();

        assertEquals(5, result.size());

        int outputKey1 = result.get(0).getFirst().get();
        LongWritable value1 = result.get(0).getSecond();
        assertTrue(outputKey1 == 1);
        assertTrue((10 == value1.get()) || (9 == value1.get()));

    }
}
