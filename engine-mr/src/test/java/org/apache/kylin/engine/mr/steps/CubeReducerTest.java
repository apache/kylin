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

package org.apache.kylin.engine.mr.steps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.BufferedMeasureEncoder;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.datatype.LongMutable;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 */
@SuppressWarnings("rawtypes")
public class CubeReducerTest extends LocalFileMetadataTestCase {

    ReduceDriver<Text, Text, Text, Text> reduceDriver;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();

        // hack for distributed cache
        FileUtils.deleteDirectory(new File("../job/meta"));
        FileUtils.copyDirectory(new File(getTestConfig().getMetadataUrl()), new File("../job/meta"));

        CuboidReducer reducer = new CuboidReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        FileUtils.deleteDirectory(new File("../job/meta"));
    }

    @Test
    public void testReducer() throws Exception {

        reduceDriver.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, "test_kylin_cube_with_slr_ready");

        CubeDesc cubeDesc = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_with_slr_ready").getDescriptor();
        BufferedMeasureEncoder codec = new BufferedMeasureEncoder(cubeDesc.getMeasures());

        Text key1 = new Text("72010ustech");
        List<Text> values1 = new ArrayList<Text>();
        values1.add(newValueText(codec, "15.09", "15.09", "15.09", 1, 100));
        values1.add(newValueText(codec, "20.34", "20.34", "20.34", 1, 200));
        values1.add(newValueText(codec, "10", "10", "10", 1, 300));

        Text key2 = new Text("1tech");
        List<Text> values2 = new ArrayList<Text>();
        values2.add(newValueText(codec, "15.09", "15.09", "15.09", 1, 500));
        values2.add(newValueText(codec, "20.34", "20.34", "20.34", 1, 1000));

        Text key3 = new Text("0");
        List<Text> values3 = new ArrayList<Text>();
        values3.add(newValueText(codec, "146.52", "146.52", "146.52", 0, 0));

        reduceDriver.withInput(key1, values1);
        reduceDriver.withInput(key2, values2);
        reduceDriver.withInput(key3, values3);

        List<Pair<Text, Text>> result = reduceDriver.run();

        Pair<Text, Text> p1 = new Pair<Text, Text>(new Text("72010ustech"), newValueText(codec, "45.43", "10", "20.34", 3, 600));
        Pair<Text, Text> p2 = new Pair<Text, Text>(new Text("1tech"), newValueText(codec, "35.43", "15.09", "20.34", 2, 1500));
        Pair<Text, Text> p3 = new Pair<Text, Text>(new Text("0"), newValueText(codec, "146.52", "146.52", "146.52", 0, 0));

        assertEquals(3, result.size());

        assertTrue(result.contains(p1));
        assertTrue(result.contains(p2));
        assertTrue(result.contains(p3));
    }

    @Test
    public void testReducerOnlyAggrInBaseCuboid() throws Exception {
        reduceDriver.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, "test_kylin_cube_with_slr_ready");
        reduceDriver.getConfiguration().setInt(BatchConstants.CFG_CUBE_CUBOID_LEVEL, 1);

        CubeDesc cubeDesc = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_with_slr_ready").getDescriptor();
        MeasureDesc measureDesc = cubeDesc.getMeasures().get(0);
        FunctionDesc functionDesc = measureDesc.getFunction();
        Field field = FunctionDesc.class.getDeclaredField("measureType");
        field.setAccessible(true);
        MeasureType origMeasureType = functionDesc.getMeasureType();
        field.set(functionDesc, new MockUpMeasureType(origMeasureType));

        BufferedMeasureEncoder codec = new BufferedMeasureEncoder(cubeDesc.getMeasures());

        Text key1 = new Text("72010ustech");
        List<Text> values1 = new ArrayList<Text>();
        values1.add(newValueText(codec, "15.09", "15.09", "15.09", 1, 100));
        values1.add(newValueText(codec, "20.34", "20.34", "20.34", 1, 200));
        values1.add(newValueText(codec, "10", "10", "10", 1, 300));

        Text key2 = new Text("1tech");
        List<Text> values2 = new ArrayList<Text>();
        values2.add(newValueText(codec, "15.09", "15.09", "15.09", 1, 500));
        values2.add(newValueText(codec, "20.34", "20.34", "20.34", 1, 1000));

        Text key3 = new Text("0");
        List<Text> values3 = new ArrayList<Text>();
        values3.add(newValueText(codec, "146.52", "146.52", "146.52", 0, 0));

        reduceDriver.withInput(key1, values1);
        reduceDriver.withInput(key2, values2);
        reduceDriver.withInput(key3, values3);

        List<Pair<Text, Text>> result = reduceDriver.run();

        Pair<Text, Text> p1 = new Pair<Text, Text>(new Text("72010ustech"), newValueText(codec, "0", "10", "20.34", 3, 600));
        Pair<Text, Text> p2 = new Pair<Text, Text>(new Text("1tech"), newValueText(codec, "0", "15.09", "20.34", 2, 1500));
        Pair<Text, Text> p3 = new Pair<Text, Text>(new Text("0"), newValueText(codec, "0", "146.52", "146.52", 0, 0));

        assertEquals(3, result.size());

        assertTrue(result.contains(p1));
        assertTrue(result.contains(p2));
        assertTrue(result.contains(p3));
    }

    private Text newValueText(BufferedMeasureEncoder codec, String sum, String min, String max, int count, int item_count) {
        Object[] values = new Object[] { new BigDecimal(sum), new BigDecimal(min), new BigDecimal(max), new LongMutable(count), new LongMutable(item_count) };

        ByteBuffer buf = codec.encode(values);

        Text t = new Text();
        t.set(buf.array(), 0, buf.position());
        return t;
    }

    class MockUpMeasureType extends MeasureType {
        MeasureType origMeasureType;

        public MockUpMeasureType(MeasureType origMeasureType) {
            this.origMeasureType = origMeasureType;
        }

        @Override
        public boolean onlyAggrInBaseCuboid() {
            return true;
        }

        @Override
        public MeasureIngester newIngester() {
            return origMeasureType.newIngester();
        }

        @Override
        public MeasureAggregator newAggregator() {
            return origMeasureType.newAggregator();
        }

        @Override
        public boolean needRewrite() {
            return origMeasureType.needRewrite();
        }

        @Override
        public Class<?> getRewriteCalciteAggrFunctionClass() {
            return origMeasureType.getRewriteCalciteAggrFunctionClass();
        }
    }
}
