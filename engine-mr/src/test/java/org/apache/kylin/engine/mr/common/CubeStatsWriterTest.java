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

package org.apache.kylin.engine.mr.common;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class CubeStatsWriterTest extends LocalFileMetadataTestCase {
    private CubeInstance cube;
    private CubeSegment cubeSegment;

    private final String segmentId = "198va32a-a33e-4b69-83dd-0bb8b1f8c53b";

    @Before
    public void setup() throws Exception {
        File tmpFolder = getTempFolder();
        FileUtils.deleteDirectory(tmpFolder);
        tmpFolder.mkdirs();
        createTestMetadata();
        cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getCube("test_kylin_cube_with_slr_1_new_segment");
        cubeSegment = cube.getSegmentById(segmentId);
    }

    @After
    public void after() throws Exception {
        File tmpFolder = getTempFolder();
        FileUtils.deleteDirectory(tmpFolder);
        cleanupTestMetadata();
    }

    @Test
    public void testWrite() throws IOException {
        Configuration conf = HadoopUtil.getCurrentConfiguration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.set("mapreduce.application.framework.path", "");
        conf.set("fs.file.impl.disable.cache", "true");

        final Path outputPath = new Path(getTmpFolderPath(), segmentId);

        System.out.println(outputPath);
        Map<Long, HLLCounter> cuboidHLLMap = Maps.newHashMap();

        Set<Long> allCuboids = cube.getDescriptor().getAllCuboids();
        for (Long cuboid : allCuboids) {
            cuboidHLLMap.put(cuboid, createMockHLLCounter());
        }
        CubeStatsWriter.writeCuboidStatistics(conf, outputPath, cuboidHLLMap, 100);
        assertTrue(new File(outputPath.toString(), BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME).exists());
    }

    private HLLCounter createMockHLLCounter() {
        HLLCounter one = new HLLCounter(14);
        for (int i = 0; i < 100; i++) {
            one.clear();
            one.add(i);
        }
        return one;
    }

    private File getTempFolder() {
        return new File(getTmpFolderPath());
    }

    private String getTmpFolderPath() {
        return "_tmp_cube_statistics";
    }
}
