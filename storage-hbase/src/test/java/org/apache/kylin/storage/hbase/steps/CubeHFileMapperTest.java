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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author George Song (ysong1)
 * 
 */
public class CubeHFileMapperTest {

    MapDriver<Text, Text, ImmutableBytesWritable, KeyValue> mapDriver;

    private String cube_name = "FLAT_ITEM_CUBE";

    @Before
    public void setUp() {
        CubeHFileMapper mapper = new CubeHFileMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @SuppressWarnings("deprecation")
    @Test
    @Ignore("not maintaining")
    public void testMapper2() throws IOException {
        mapDriver.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cube_name);

        mapDriver.addInput(new Text("52010tech"), new Text("35.432"));

        List<Pair<ImmutableBytesWritable, KeyValue>> result = mapDriver.run();

        assertEquals(2, result.size());

        byte[] bytes = { 0, 0, 0, 0, 0, 0, 0, 119, 33, 0, 22, 1, 0, 121, 7 };
        ImmutableBytesWritable key = new ImmutableBytesWritable(bytes);

        Pair<ImmutableBytesWritable, KeyValue> p1 = result.get(0);
        Pair<ImmutableBytesWritable, KeyValue> p2 = result.get(1);

        assertEquals(key, p1.getFirst());
        assertEquals("cf1", new String(p1.getSecond().getFamily()));
        assertEquals("usd_amt", new String(p1.getSecond().getQualifier()));
        assertEquals("35.43", new String(p1.getSecond().getValue()));

        assertEquals(key, p2.getFirst());
        assertEquals("cf1", new String(p2.getSecond().getFamily()));
        assertEquals("item_count", new String(p2.getSecond().getQualifier()));
        assertEquals("2", new String(p2.getSecond().getValue()));
    }
}
