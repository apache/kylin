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

package org.apache.kylin.rec;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kylin.common.util.TestUtils;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.val;
import lombok.var;

@MetadataInfo(project = "ssb")
public class SmartSSBTest {

    @Test
    public void testSSB() throws IOException {
        final String project = "ssb";
        NProjectManager projectManager;

        final String sqlsPath = "./src/test/resources/nsmart/ssb/sql";
        File fileFolder = new File(sqlsPath);

        List<String> sqls = Lists.newArrayList();

        for (final File sqlFile : Objects.requireNonNull(fileFolder.listFiles())) {
            sqls.add(new String(Files.readAllBytes(Paths.get(sqlFile.getAbsolutePath())), StandardCharsets.UTF_8));
        }
        AccelerationUtil.runWithSmartContext(TestUtils.getTestConfig(), project, sqls.toArray(new String[0]), true);

        TestUtils.getTestConfig().clearManagers();

        val dataflowManager = NDataflowManager.getInstance(TestUtils.getTestConfig(), project);
        projectManager = NProjectManager.getInstance(TestUtils.getTestConfig());

        Assertions.assertFalse(projectManager.listAllRealizations(project).isEmpty());
        Assertions.assertFalse(dataflowManager.listUnderliningDataModels().isEmpty());
    }

    @Test
    public void testTwice_DifferentIds() throws IOException {
        testSSB();
        val cubeManager = NIndexPlanManager.getInstance(TestUtils.getTestConfig(), "ssb");
        var cube = cubeManager.listAllIndexPlans().get(0);
        val maxAggId1 = cube.getNextAggregationIndexId();
        val maxTableId1 = cube.getNextTableIndexId();
        val aggSize = cube.getAllIndexes().stream().filter(c -> !c.isTableIndex()).count();
        val tableSize = cube.getAllIndexes().stream().filter(IndexEntity::isTableIndex).count();
        cube = cubeManager.updateIndexPlan(cube.getUuid(),
                copyForWrite -> copyForWrite.removeLayouts(
                        copyForWrite.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet()),
                        true, false));

        testSSB();
        cube = cubeManager.getIndexPlan(cube.getUuid());
        Assertions.assertEquals(maxAggId1 + IndexEntity.INDEX_ID_STEP * aggSize, cube.getNextAggregationIndexId());
        Assertions.assertEquals(maxTableId1 + IndexEntity.INDEX_ID_STEP * tableSize, cube.getNextTableIndexId());
    }
}
