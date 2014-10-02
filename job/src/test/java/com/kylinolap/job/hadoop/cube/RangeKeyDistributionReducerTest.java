/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job.hadoop.cube;

import com.kylinolap.job.constant.BatchConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author ysong1
 */
public class RangeKeyDistributionReducerTest {

    ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
    String localTempDir = System.getProperty("java.io.tmpdir") + File.separator;

    @Before
    public void setUp() {
        RangeKeyDistributionReducer reducer = new RangeKeyDistributionReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        Text key1 = new Text(new byte[]{1});
        List<LongWritable> values1 = new ArrayList<LongWritable>();
        values1.add(new LongWritable(RangeKeyDistributionReducer.TEN_GIGA_BYTES));
        values1.add(new LongWritable(1));

        Text key2 = new Text(new byte[]{2});
        List<LongWritable> values2 = new ArrayList<LongWritable>();
        values2.add(new LongWritable(123));

        Text key3 = new Text(new byte[]{3});
        List<LongWritable> values3 = new ArrayList<LongWritable>();
        values3.add(new LongWritable(RangeKeyDistributionReducer.TEN_GIGA_BYTES));

        Text key4 = new Text(new byte[]{4});
        List<LongWritable> values4 = new ArrayList<LongWritable>();
        values4.add(new LongWritable(RangeKeyDistributionReducer.TEN_GIGA_BYTES));

        Text key5 = new Text(new byte[]{5});
        List<LongWritable> values5 = new ArrayList<LongWritable>();
        values5.add(new LongWritable(1));

        reduceDriver.withInput(key1, values1);
        reduceDriver.withInput(key2, values2);
        reduceDriver.withInput(key3, values3);
        reduceDriver.withInput(key4, values4);
        reduceDriver.withInput(key5, values5);

        reduceDriver.getConfiguration().set(BatchConstants.CUBE_CAPACITY, "MEDIUM");

        List<Pair<Text, LongWritable>> result = reduceDriver.run();

        assertEquals(4, result.size());

        byte[] outputKey1 = result.get(0).getFirst().getBytes();
        LongWritable value1 = result.get(0).getSecond();
        assertArrayEquals(new byte[]{1}, outputKey1);
        assertEquals(10737418241L, value1.get());

        byte[] outputKey2 = result.get(1).getFirst().getBytes();
        LongWritable value2 = result.get(1).getSecond();
        assertArrayEquals(new byte[]{3}, outputKey2);
        assertEquals(10737418363L, value2.get());

        byte[] outputKey3 = result.get(2).getFirst().getBytes();
        LongWritable value3 = result.get(2).getSecond();
        assertArrayEquals(new byte[]{4}, outputKey3);
        assertEquals(10737418240L, value3.get());

        byte[] outputKey4 = result.get(3).getFirst().getBytes();
        LongWritable value4 = result.get(3).getSecond();
        assertArrayEquals(new byte[]{5}, outputKey4);
        assertEquals(1L, value4.get());
    }
}
