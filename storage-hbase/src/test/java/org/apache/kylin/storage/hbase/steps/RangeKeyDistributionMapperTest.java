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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

/**
 * @author ysong1
 * 
 */
public class RangeKeyDistributionMapperTest {

    @SuppressWarnings("rawtypes")
    MapDriver mapDriver;
    String localTempDir = System.getProperty("java.io.tmpdir") + File.separator;

    @Before
    public void setUp() {
        RangeKeyDistributionMapper mapper = new RangeKeyDistributionMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMapperWithoutHeader() throws IOException {

        Text inputKey1 = new Text(new byte[] { 0, 0, 0, 0, 0, 0, 0, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7 });
        Text inputKey2 = new Text(new byte[] { 0, 0, 0, 0, 0, 0, 0, 127, 11, 122, 1, 0, 22, 98, 1, 0, 121, 7 });
        Text inputKey3 = new Text(new byte[] { 2, 2, 2, 2, 2, 2, 2, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7 });
        Text inputKey4 = new Text(new byte[] { 3, 3, 3, 3, 3, 3, 3, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7 });
        Text inputKey5 = new Text(new byte[] { 4, 4, 4, 4, 4, 4, 4, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7 });
        Text inputKey6 = new Text(new byte[] { 5, 5, 5, 5, 5, 5, 5, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7 });
        Text inputKey7 = new Text(new byte[] { 6, 6, 6, 6, 6, 6, 6, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7 });

        mapDriver.addInput(inputKey1, new Text("abc"));
        mapDriver.addInput(inputKey2, new Text("abc"));
        mapDriver.addInput(inputKey3, new Text("abc"));
        mapDriver.addInput(inputKey4, new Text("abc"));
        mapDriver.addInput(inputKey5, new Text("abc"));
        mapDriver.addInput(inputKey6, new Text("abc"));
        mapDriver.addInput(inputKey7, new Text("abc"));

        List<Pair<Text, LongWritable>> result = mapDriver.run();

        assertEquals(1, result.size());

        byte[] key1 = result.get(0).getFirst().getBytes();
        LongWritable value1 = result.get(0).getSecond();
        assertArrayEquals(new byte[] { 6, 6, 6, 6, 6, 6, 6, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7 }, key1);
        assertEquals(147, value1.get());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMapperWithHeader() throws IOException {

        Text inputKey1 = new Text(new byte[] { 0, 0, 0, 0, 0, 0, 0, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7, 0, 0, 0, 0, 0, 0, 0, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7 });
        Text inputKey2 = new Text(new byte[] { 0, 0, 0, 0, 0, 0, 0, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7, 0, 0, 0, 0, 0, 0, 0, 127, 11, 122, 1, 0, 22, 98, 1, 0, 121, 7 });
        Text inputKey3 = new Text(new byte[] { 0, 0, 0, 0, 0, 0, 0, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7, 2, 2, 2, 2, 2, 2, 2, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7 });
        Text inputKey4 = new Text(new byte[] { 0, 0, 0, 0, 0, 0, 0, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7, 3, 3, 3, 3, 3, 3, 3, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7 });
        Text inputKey5 = new Text(new byte[] { 0, 0, 0, 0, 0, 0, 0, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7, 4, 4, 4, 4, 4, 4, 4, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7 });
        Text inputKey6 = new Text(new byte[] { 0, 0, 0, 0, 0, 0, 0, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7, 5, 5, 5, 5, 5, 5, 5, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7 });
        Text inputKey7 = new Text(new byte[] { 0, 0, 0, 0, 0, 0, 0, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7, 6, 6, 6, 6, 6, 6, 6, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7 });

        mapDriver.addInput(inputKey1, new Text("abc"));
        mapDriver.addInput(inputKey2, new Text("abc"));
        mapDriver.addInput(inputKey3, new Text("abc"));
        mapDriver.addInput(inputKey4, new Text("abc"));
        mapDriver.addInput(inputKey5, new Text("abc"));
        mapDriver.addInput(inputKey6, new Text("abc"));
        mapDriver.addInput(inputKey7, new Text("abc"));

        List<Pair<Text, LongWritable>> result = mapDriver.run();

        assertEquals(1, result.size());

        byte[] key1 = result.get(0).getFirst().getBytes();
        LongWritable value1 = result.get(0).getSecond();
        assertArrayEquals(new byte[] { 0, 0, 0, 0, 0, 0, 0, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7, 6, 6, 6, 6, 6, 6, 6, 127, 11, 56, -23, 0, 22, 98, 1, 0, 121, 7 }, key1);
        assertEquals(273, value1.get());

    }

}
