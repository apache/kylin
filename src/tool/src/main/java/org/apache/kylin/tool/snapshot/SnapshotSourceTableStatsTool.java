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

import static org.apache.kylin.common.constant.Constants.BACKSLASH;
import static org.apache.kylin.common.constant.Constants.SNAPSHOT_AUTO_REFRESH;
import static org.apache.kylin.common.constant.Constants.SOURCE_TABLE_STATS;
import static org.apache.kylin.common.constant.Constants.VIEW_MAPPING;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class SnapshotSourceTableStatsTool {

    private SnapshotSourceTableStatsTool() {
        throw new IllegalStateException("Utility class");
    }

    public static Boolean extractSourceTableStats(KylinConfig config, File exportDir, String project,
            AbstractExecutable job) {
        try {
            log.info("extractSourceTableStats start ...");
            File sourceTableStatsDir = new File(exportDir, SNAPSHOT_AUTO_REFRESH);
            FileUtils.forceMkdir(sourceTableStatsDir);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (Sets.newHashSet(JobTypeEnum.SNAPSHOT_BUILD, JobTypeEnum.SNAPSHOT_REFRESH).contains(job.getJobType())) {
                log.info("extract snapshot job[{}] snapshot hive table", job.getDisplayName());
                val tableIdentity = job.getParam(NBatchConstants.P_TABLE_NAME);
                if (StringUtils.isBlank(tableIdentity)) {
                    log.info("Snapshot job param P_TABLE_NAME[{}] has error", tableIdentity);
                    return false;
                }
                val tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(tableIdentity);

                val sourceTables = getSourceTables(config, project, fs, tableDesc);
                extractSnapshotSourceTableStats(project, config, sourceTableStatsDir, fs, sourceTables);
                return true;
            }
            log.info("extract build job[{}] snapshot hive table", job.getDisplayName());
            val targetModelId = job.getTargetModelId();
            val sourceTables = getSourceTablesByModelId(project, targetModelId, config, fs);
            extractSnapshotSourceTableStats(project, config, sourceTableStatsDir, fs, sourceTables);
            return true;
        } catch (Exception e) {
            log.error("Failed to SourceTableStats. ", e);
        }
        return false;
    }

    public static List<String> getSourceTables(KylinConfig config, String project, FileSystem fs, TableDesc tableDesc) {
        val sourceTables = Lists.<String> newArrayList();
        if (tableDesc.isView()) {
            try {
                val viewMapping = new Path(config.getSnapshotAutoRefreshDir(project) + VIEW_MAPPING);
                if (fs.exists(viewMapping)) {
                    try (FSDataInputStream inputStream = fs.open(viewMapping)) {
                        val result = JsonUtil.readValue(inputStream, new TypeReference<Map<String, List<String>>>() {
                        });
                        val sources = result.getOrDefault(tableDesc.getIdentity(), Lists.newArrayList());
                        sourceTables.addAll(sources);
                    }
                }
            } catch (IOException e) {
                log.error("read viewMapping has error", e);
            }
        } else {
            sourceTables.add(tableDesc.getIdentity().toLowerCase(Locale.ROOT));
        }
        return sourceTables;
    }

    public static List<String> getSourceTablesByModelId(String project, String modelId, KylinConfig config,
            FileSystem fs) {
        val modelManager = NDataModelManager.getInstance(config, project);
        val model = modelManager.getDataModelDesc(modelId);
        if (null != model) {
            val lookupTables = model.getLookupTables();
            val lookTableSourceTables = lookupTables.stream().map(TableRef::getTableDesc)
                    .map(tableDesc -> getSourceTables(config, project, fs, tableDesc)).flatMap(Collection::stream)
                    .collect(Collectors.toList());
            log.info("Model{} lookup tables source tables size: {}", modelId, lookTableSourceTables.size());
            return lookTableSourceTables;
        }
        return Lists.newArrayList();
    }

    public static void extractSnapshotSourceTableStats(String project, KylinConfig config, File sourceTableStatsDir,
            FileSystem fs, List<String> tables) {
        for (String table : tables) {
            val fullTablePathString = config.getSnapshotAutoRefreshDir(project) + SOURCE_TABLE_STATS + BACKSLASH + table;
            try {
                File sourceTableStatsSnapshotDir = new File(sourceTableStatsDir, SOURCE_TABLE_STATS);
                if (!sourceTableStatsSnapshotDir.exists()) {
                    FileUtils.forceMkdir(sourceTableStatsSnapshotDir);
                }
                val path = new Path(fullTablePathString);
                if (fs.exists(path) && fs.isFile(path)) {
                    log.info("extract SourceTableStats [{}]", fullTablePathString);
                    fs.copyToLocalFile(false, path, new Path(sourceTableStatsSnapshotDir.getAbsolutePath()), true);
                }
            } catch (IOException e) {
                log.error("extract SourceTableStats [{}] has error", fullTablePathString);
            }
        }
    }

    public static Boolean extractSnapshotAutoUpdate(File exportDir) {
        try {
            val fs = HadoopUtil.getWorkingFileSystem();
            val config = KylinConfig.readSystemKylinConfig();
            val projectManager = NProjectManager.getInstance(config);
            val allProject = projectManager.listAllProjects();
            File snapshotAutoUpdate = new File(exportDir, SNAPSHOT_AUTO_REFRESH);
            if (!snapshotAutoUpdate.exists()) {
                FileUtils.forceMkdir(snapshotAutoUpdate);
            }
            for (ProjectInstance project : allProject) {
                val fullTablePathString = config.getSnapshotAutoRefreshDir(project.getName());
                val path = new Path(fullTablePathString);
                if (fs.exists(path)) {
                    File dst = new File(snapshotAutoUpdate, project.getName());
                    if (!dst.exists()) {
                        FileUtils.forceMkdir(dst);
                    }
                    log.info("extract SourceTableStats [{}] to [{}]", path.toString(), dst.getAbsolutePath());
                    fs.copyToLocalFile(false, path, new Path(dst.getAbsolutePath()), true);
                }
            }
            return true;
        } catch (Exception e) {
            log.error("extract SourceTableStats has error", e);
            return false;
        }
    }
}
