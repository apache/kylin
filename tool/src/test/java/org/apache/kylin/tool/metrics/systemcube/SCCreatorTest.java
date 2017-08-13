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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.tool.metrics.systemcube.util.HiveSinkTool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class SCCreatorTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testExecute() throws Exception {
        String outputPath = "../examples/system";
        String inputPath = "src/main/resources/SCSinkTools.json";

        SCCreator cli = new SCCreator();
        cli.execute("ADMIN", outputPath, inputPath);
    }

    @Test
    public void testWriteSinkToolsJson() throws Exception {
        Map<String, String> cubeDescOverrideProperties = Maps.newHashMap();
        cubeDescOverrideProperties.put("kylin.cube.algorithm", "INMEM");

        HiveSinkTool hiveSinkTool = new HiveSinkTool();
        hiveSinkTool.setCubeDescOverrideProperties(cubeDescOverrideProperties);

        try (BufferedOutputStream os = new BufferedOutputStream(
                new FileOutputStream("src/test/resources/SCSinkTools.json"))) {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enableDefaultTyping();
            mapper.writeValue(os, Sets.newHashSet(hiveSinkTool));
        }
    }

    @Test
    public void testReadSinkToolsJson() throws Exception {
        try (BufferedInputStream is = new BufferedInputStream(
                new FileInputStream("src/main/resources/SCSinkTools.json"))) {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enableDefaultTyping();
            Set<HiveSinkTool> sinkToolSet = mapper.readValue(is, HashSet.class);
            for (HiveSinkTool entry : sinkToolSet) {
                System.out.println(entry.getCubeDescOverrideProperties());
            }
        }
    }

}