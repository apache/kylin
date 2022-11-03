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

package org.apache.kylin.tool.snapshot;

import static org.apache.kylin.common.constant.Constants.MARK;
import static org.apache.kylin.common.constant.Constants.SNAPSHOT_AUTO_REFRESH;
import static org.apache.kylin.common.constant.Constants.SOURCE_TABLE_STATS;
import static org.apache.kylin.common.constant.Constants.VIEW_MAPPING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Files;

import io.kyligence.kap.engine.spark.job.NSparkCubingStep;
import io.kyligence.kap.guava20.shaded.common.collect.ImmutableMap;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import lombok.val;

@MetadataInfo
class SnapshotSourceTableStatsToolTest {
    private static final String PROJECT = "default";

    @Test
    void extractSourceTableStatsSnapshotJob() {
        val tables = Maps.<String, Set<String>> newHashMap();
        tables.put("default.test_snapshot_table", Sets.newHashSet("default.test_snapshot_table"));
        val config = KylinConfig.getInstanceFromEnv();
        val tempDir = Files.createTempDir();
        val job = new DefaultExecutable();

        job.setJobType(JobTypeEnum.SNAPSHOT_BUILD);
        AtomicReference<Boolean> result = new AtomicReference<>(
                SnapshotSourceTableStatsTool.extractSourceTableStats(config, tempDir, PROJECT, job));
        assertFalse(result.get());

        job.setParam(NBatchConstants.P_TABLE_NAME, "TEST_SNAPSHOT_TABLE");
        result.set(SnapshotSourceTableStatsTool.extractSourceTableStats(config, tempDir, PROJECT, job));
        assertFalse(result.get());

        job.setParam(NBatchConstants.P_TABLE_NAME, "DEFAULT.TEST_SNAPSHOT_TABLE");
        result.set(SnapshotSourceTableStatsTool.extractSourceTableStats(config, tempDir, PROJECT, job));
        assertTrue(result.get());
    }

    @Test
    void extractSourceTableStatsNSparkCubingStep() throws Exception {
        val tables = Maps.<String, Set<String>> newHashMap();
        tables.put("default.test_snapshot_table", Sets.newHashSet("default.test_snapshot_table"));
        val config = KylinConfig.getInstanceFromEnv();
        val tempDir = Files.createTempDir();
        val job = new NSparkCubingStep();
        job.setProject(PROJECT);
        job.setJobType(JobTypeEnum.INC_BUILD);
        val result = SnapshotSourceTableStatsTool.extractSourceTableStats(config, tempDir, PROJECT, job);
        assertTrue(result);
    }

    @Test
    void extractSourceTableStatsFailed() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val tempDir = Files.createTempDir();
        val job = new NSparkCubingStep();
        val sourceTableStats = new File(tempDir, SOURCE_TABLE_STATS);
        Files.write(new byte[] {}, sourceTableStats);
        val result = SnapshotSourceTableStatsTool.extractSourceTableStats(config, tempDir, PROJECT, job);
        assertFalse(result);
    }

    @Test
    void extractSnapshotSourceTableStats() throws Exception {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        val config = KylinConfig.getInstanceFromEnv();
        val sourceTableStatsDir = new Path(config.getSnapshotAutoRefreshDir(PROJECT), SOURCE_TABLE_STATS);
        fs.mkdirs(sourceTableStatsDir);
        val sourceTableStats = new Path(sourceTableStatsDir, "default.table");

        val expected = new ImmutableMap.Builder().put("test", "test").build();
        try (val out = fs.create(sourceTableStats, true)) {
            out.write(JsonUtil.writeValueAsBytes(expected));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        val tempDir = Files.createTempDir();
        val resultDir = new File(tempDir, "result");
        val snapshotSourceTableStats = Lists.newArrayList("default.table");
        SnapshotSourceTableStatsTool.extractSnapshotSourceTableStats(PROJECT, config, resultDir, fs,
                snapshotSourceTableStats);
        val resultTableStatsDir = new File(resultDir, SOURCE_TABLE_STATS);
        val result = new File(resultTableStatsDir, "default.table");
        val actual = JsonUtil.readValue(result, new TypeReference<Map<String, String>>() {
        });
        assertEquals(expected.size(), actual.size());
        actual.forEach((k, v) -> assertEquals(expected.get(k), v));
    }

    @Test
    void getSourceTables() throws Exception {
        val project = "default";
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val pathStr = KylinConfig.readSystemKylinConfig().getSnapshotAutoRefreshDir(project) + VIEW_MAPPING;
        val snapshotTablesPath = new Path(pathStr);
        val viewMapping = Maps.<String, Set<String>> newHashMap();
        viewMapping.put("DEFAULT.TEST", Sets.newHashSet("default.test1", "default.test2", "default.test3"));
        try (val out = fileSystem.create(snapshotTablesPath, true)) {
            out.write(JsonUtil.writeValueAsBytes(viewMapping));
        }

        val table1 = Mockito.mock(TableDesc.class);
        Mockito.when(table1.isView()).thenReturn(true);
        Mockito.when(table1.getIdentity()).thenReturn("DEFAULT.TEST");
        val sourceTables1 = SnapshotSourceTableStatsTool.getSourceTables(KylinConfig.readSystemKylinConfig(), PROJECT,
                fileSystem, table1);
        assertEquals(3, sourceTables1.size());
        assertTrue(sourceTables1.containsAll(Lists.newArrayList("default.test1", "default.test2", "default.test3")));

        val table2 = Mockito.mock(TableDesc.class);
        Mockito.when(table2.isView()).thenReturn(true);
        Mockito.when(table2.getIdentity()).thenReturn("DEFAULT.TEST22");
        val sourceTables2 = SnapshotSourceTableStatsTool.getSourceTables(KylinConfig.readSystemKylinConfig(), PROJECT,
                fileSystem, table2);
        assertTrue(CollectionUtils.isEmpty(sourceTables2));

        val table3 = Mockito.mock(TableDesc.class);
        Mockito.when(table3.getIdentity()).thenReturn("DEFAULT.TEST33");
        val sourceTables3 = SnapshotSourceTableStatsTool.getSourceTables(KylinConfig.readSystemKylinConfig(), PROJECT,
                fileSystem, table3);
        assertEquals(1, sourceTables3.size());
        assertTrue(sourceTables3.containsAll(Lists.newArrayList("default.test33")));
    }

    @Test
    void testExtractSourceTableStatsAllProject() throws Exception {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        val config = KylinConfig.getInstanceFromEnv();
        val markFile = new Path(config.getSnapshotAutoRefreshDir(PROJECT), MARK);
        try (val out = fs.create(markFile, true)) {
            out.write(new byte[] {});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        val sourceTableStatsDir = new Path(config.getSnapshotAutoRefreshDir(PROJECT), SOURCE_TABLE_STATS);
        fs.mkdirs(sourceTableStatsDir);
        val sourceTableStats = new Path(sourceTableStatsDir, "default.table");

        val expected = new ImmutableMap.Builder().put("test", "test").build();
        try (val out = fs.create(sourceTableStats, true)) {
            out.write(JsonUtil.writeValueAsBytes(expected));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        val tempDir = Files.createTempDir();
        val resultDir = new File(tempDir, "result");
        val extractResult = SnapshotSourceTableStatsTool.extractSnapshotAutoUpdate(resultDir);
        assertTrue(extractResult);
        val resultPath = resultDir.getAbsolutePath() + "/" + SNAPSHOT_AUTO_REFRESH + "/" + PROJECT + "/"
                + SNAPSHOT_AUTO_REFRESH + "/" + SOURCE_TABLE_STATS + "/" + "default.table";
        val result = new File(resultPath);
        val actual = JsonUtil.readValue(result, new TypeReference<Map<String, String>>() {
        });
        assertEquals(expected.size(), actual.size());
        actual.forEach((k, v) -> assertEquals(expected.get(k), v));

        val resultMarkPath = resultDir.getAbsolutePath() + "/" + SNAPSHOT_AUTO_REFRESH + "/" + PROJECT + "/"
                + SNAPSHOT_AUTO_REFRESH + "/" + MARK;
        assertTrue(new File(resultMarkPath).exists());
    }
}
