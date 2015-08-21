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

package org.apache.kylin.job.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.job.hadoop.cardinality.ColumnCardinalityMapper;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author ysong1
 * 
 */
@Ignore("This test is invalid now as the mapper uses HCatalog to fetch the data which need a hive env")
public class ColumnCardinalityMapperTest {

    @SuppressWarnings("rawtypes")
    MapDriver mapDriver;
    String localTempDir = System.getProperty("java.io.tmpdir") + File.separator;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Before
    public void setUp() {
        ColumnCardinalityMapper mapper = new ColumnCardinalityMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    public final static String strArr = "abc,tests,test,test,as,sts,test,tss,sets";

    @SuppressWarnings({ "unchecked" })
    @Test
    @Ignore
    public void testMapperOn177() throws IOException {
        mapDriver.clearInput();
        File file = new File("src/test/resources/data/test_cal_dt/part-r-00000");
        FileReader reader = new FileReader(file);
        BufferedReader breader = new BufferedReader(reader);
        String s = breader.readLine();
        int i = 0;
        while (s != null) {
            LongWritable inputKey = new LongWritable(i++);
            mapDriver.addInput(inputKey, new Text(s));
            s = breader.readLine();
        }
        // breader.close();
        List<Pair<IntWritable, BytesWritable>> result = mapDriver.run();
        breader.close();
        assertEquals(9, result.size());

        int key1 = result.get(0).getFirst().get();
        BytesWritable value1 = result.get(0).getSecond();
        byte[] bytes = value1.getBytes();
        HyperLogLogPlusCounter hllc = new HyperLogLogPlusCounter();
        hllc.readRegisters(ByteBuffer.wrap(bytes));
        assertTrue(key1 > 0);
        assertEquals(8, hllc.getCountEstimate());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMapperOnComma() throws IOException {
        mapDriver.clearInput();
        LongWritable inputKey1 = new LongWritable(1);
        LongWritable inputKey2 = new LongWritable(2);
        LongWritable inputKey3 = new LongWritable(3);
        LongWritable inputKey4 = new LongWritable(4);
        LongWritable inputKey5 = new LongWritable(5);
        LongWritable inputKey6 = new LongWritable(6);
        LongWritable inputKey7 = new LongWritable(7);

        mapDriver.addInput(inputKey1, new Text());
        mapDriver.addInput(inputKey2, new Text(strArr));
        mapDriver.addInput(inputKey3, new Text(strArr));
        mapDriver.addInput(inputKey4, new Text(strArr));
        mapDriver.addInput(inputKey5, new Text(strArr));
        mapDriver.addInput(inputKey6, new Text(strArr));
        mapDriver.addInput(inputKey7, new Text(strArr));

        List<Pair<IntWritable, BytesWritable>> result = mapDriver.run();

        assertEquals(9, result.size());

        int key1 = result.get(0).getFirst().get();
        BytesWritable value1 = result.get(0).getSecond();
        byte[] bytes = value1.getBytes();
        HyperLogLogPlusCounter hllc = new HyperLogLogPlusCounter();
        hllc.readRegisters(ByteBuffer.wrap(bytes));
        System.out.println("ab\177ab".length());
        assertTrue(key1 > 0);
        assertEquals(1, hllc.getCountEstimate());
    }

}
