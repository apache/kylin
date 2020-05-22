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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidModeEnum;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;

public class CalculateStatsFromBaseCuboidMapperTest extends LocalFileMetadataTestCase {
    private String cubeName;
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private MapDriver<Text, Text, Text, Text> mapDriver;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        FileUtils.deleteDirectory(new File("./meta"));
        FileUtils.copyDirectory(new File(getTestConfig().getMetadataUrl().toString()), new File("./meta"));

        cubeName = "test_kylin_cube_with_slr_1_new_segment";
        cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        CalculateStatsFromBaseCuboidMapper calStatsFromBasicCuboidMapper = new CalculateStatsFromBaseCuboidMapper();
        mapDriver = MapDriver.newMapDriver(calStatsFromBasicCuboidMapper);
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testMapper() throws Exception {
        mapperTest();
    }

    @Test
    public void testMapperWithOldHLL() throws Exception {
        cubeDesc.setVersion("1.5.2");
        mapperTest();
    }

    private void mapperTest() throws Exception {
        setConfiguration();
        Text key1 = new Text();
        byte[] shard = new byte[] { 0, 0 };
        byte[] cuboidId = Cuboid.getBaseCuboid(cubeDesc).getBytes();
        byte[] col1 = new byte[] { 0, 0, 0, 1 };
        byte[] col2 = new byte[] { 0, 6, 0 };
        byte[] col3 = new byte[] { 1 };
        byte[] col4 = new byte[] { 1 };
        byte[] col5 = new byte[] { 1 };
        byte[] col6 = new byte[] { 1 };
        byte[] col7 = new byte[] { 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0 };
        byte[] col8 = new byte[] { 1, 0 };
        byte[] col9 = new byte[] { 1 };
        key1.set(Bytes.concat(shard, cuboidId, col1, col2, col3, col4, col5, col6, col7, col8, col9));
        Text val1 = new Text();

        mapDriver.setInput(key1, val1);
        List<Pair<Text, Text>> result = mapDriver.run();
        Set<Long> cuboidIdSet = cube.getCuboidsByMode(CuboidModeEnum.CURRENT);
        assertEquals(cuboidIdSet.size(), result.size());
        Long[] cuboids = cuboidIdSet.toArray(new Long[cuboidIdSet.size()]);
        Arrays.sort(cuboids);

        List<Long> resultCuboidList = Lists.transform(result, new Function<Pair<Text, Text>, Long>() {
            @Nullable
            @Override
            public Long apply(@Nullable Pair<Text, Text> input) {
                byte[] bytes = input.getFirst().getBytes();
                return org.apache.kylin.common.util.Bytes.toLong(bytes);
            }
        });
        Long[] resultCuboids = resultCuboidList.toArray(new Long[resultCuboidList.size()]);
        Arrays.sort(resultCuboids);
        assertArrayEquals(cuboids, resultCuboids);
    }

    private void setConfiguration() throws Exception {
        Configuration configuration = mapDriver.getConfiguration();
        configuration.set(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT, "100");
        configuration.set(BatchConstants.CFG_CUBE_NAME, cubeName);
        configuration.set(BatchConstants.CFG_CUBE_SEGMENT_ID, "198va32a-a33e-4b69-83dd-0bb8b1f8c53b");
    }

}
