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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FactDistinctColumnsMapperTest extends LocalFileMetadataTestCase {
    private String cubeName;
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private MapDriver<LongWritable, Object, SelfDefineSortableKey, Text> mapDriver;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        FileUtils.deleteDirectory(new File("./meta"));
        FileUtils.copyDirectory(new File(getTestConfig().getMetadataUrl().toString()), new File("./meta"));

        cubeName = "test_kylin_cube_with_slr_1_new_segment";
        cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        FactDistinctColumnsMapper<LongWritable> factDistinctColumnsMapper = new FactDistinctColumnsMapper<>();
        mapDriver = MapDriver.newMapDriver(factDistinctColumnsMapper);
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testMapper() throws IOException {
        Configuration configuration = mapDriver.getConfiguration();
        configuration.set(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT, "100");
        configuration.set(BatchConstants.CFG_CUBE_NAME, "test_kylin_cube_with_slr_1_new_segment");
        configuration.set(BatchConstants.CFG_CUBE_SEGMENT_ID, "198va32a-a33e-4b69-83dd-0bb8b1f8c53b");
        HCatRecord value1 = new DefaultHCatRecord(11);
        value1.set(0, "2012-08-16");
        value1.set(1, "48027");
        value1.set(2, "0");
        value1.set(3, "Home & Garden");
        value1.set(4, "Cheese & Crackers");
        value1.set(5, "Cheese & Crackers");
        value1.set(6, "48027");
        value1.set(7, "16");
        value1.set(8, "10000010");
        value1.set(9, "204.28");
        value1.set(10, "5");
        mapDriver.addInput(new LongWritable(0), value1);

        List<Pair<SelfDefineSortableKey, Text>> result = mapDriver.run();
        int colsNeedDictSize = cubeDesc.getAllColumnsNeedDictionaryBuilt().size();
        int cuboidsCnt = cubeDesc.getAllCuboids().size();

        assertEquals(
                colsNeedDictSize + (cubeDesc.getRowkey().getRowKeyColumns().length - colsNeedDictSize) * 2 + cuboidsCnt,
                result.size());
    }

}
