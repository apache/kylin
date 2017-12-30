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

package org.apache.kylin.tool.metrics.systemcube;

import static org.junit.Assert.assertEquals;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metrics.lib.SinkTool;
import org.apache.kylin.tool.metrics.systemcube.util.HiveSinkTool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class SCCreatorTest extends LocalFileMetadataTestCase {

    private File tempMetadataDir;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();

        File tempDir = File.createTempFile(getClass().getName(), "system");
        FileUtils.forceDelete(tempDir);
        tempMetadataDir = new File(tempDir, "meta");
    }

    @After
    public void after() throws Exception {
        if (tempMetadataDir != null && tempMetadataDir.exists()) {
            FileUtils.forceDelete(tempMetadataDir.getParentFile());
        }
        staticCleanupTestMetadata();
    }

    @Test
    public void testExecute() throws Exception {
        String metadataPath = tempMetadataDir.getPath();

        String inputPath = "src/main/resources/SCSinkTools.json";

        SCCreator cli = new SCCreator();
        cli.execute("ADMIN", metadataPath, inputPath);
        Assert.assertTrue(tempMetadataDir.isDirectory());

        KylinConfig local = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
        local.setMetadataUrl(metadataPath);

        CubeManager cubeManager = CubeManager.getInstance(local);
        List<CubeInstance> cubeList = cubeManager.listAllCubes();
        System.out.println("System cubes: " + cubeList);
        assertEquals(cubeList.size(), 5);

        for (CubeInstance cube : cubeList) {
            Assert.assertTrue(cube.getStatus() != RealizationStatusEnum.DESCBROKEN);
        }
    }

    @Test
    public void testWriteSinkToolsJson() throws Exception {
        Map<String, String> cubeDescOverrideProperties = Maps.newHashMap();
        cubeDescOverrideProperties.put("kylin.cube.algorithm", "INMEM");

        HiveSinkTool hiveSinkTool = new HiveSinkTool();
        hiveSinkTool.setCubeDescOverrideProperties(cubeDescOverrideProperties);

        String outputPath = "src/test/resources/SCSinkTools.json";
        try (BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(outputPath))) {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enableDefaultTyping();
            mapper.writeValue(os, Sets.newHashSet(hiveSinkTool));
        }

        Set<SinkTool> sinkToolSet = readSinkToolsJson(outputPath);
        for (SinkTool entry : sinkToolSet) {
            Map<String, String> props = entry.getCubeDescOverrideProperties();
            for (String key : cubeDescOverrideProperties.keySet()) {
                assertEquals(props.get(key), cubeDescOverrideProperties.get(key));
            }
        }
    }

    @Test
    public void testReadSinkToolsJson() throws Exception {
        Set<SinkTool> sinkToolSet = readSinkToolsJson("src/main/resources/SCSinkTools.json");
        for (SinkTool entry : sinkToolSet) {
            Map<String, String> props = entry.getCubeDescOverrideProperties();
            assertEquals(props.get("kylin.cube.algorithm"), "INMEM");
        }
    }

    private Set<SinkTool> readSinkToolsJson(String jsonPath) throws Exception {
        try (BufferedInputStream is = new BufferedInputStream(new FileInputStream(jsonPath))) {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enableDefaultTyping();
            return mapper.readValue(is, HashSet.class);
        }
    }
}