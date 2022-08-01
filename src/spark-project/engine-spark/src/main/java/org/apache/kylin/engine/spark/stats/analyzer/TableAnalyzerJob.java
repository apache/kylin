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
package org.apache.kylin.engine.spark.stats.analyzer;

import static org.apache.kylin.engine.spark.job.StageType.TABLE_SAMPLING;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.job.TableAnalysisJob;
import org.apache.kylin.engine.spark.job.exec.TableAnalyzerExec;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.apache.spark.utils.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import lombok.val;
import lombok.var;

public class TableAnalyzerJob extends SparkApplication implements Serializable {
    public static final ImmutableList<String> TABLE_STATS_METRICS = ImmutableList.<String> builder()
            .add("COUNT", "COUNT_DISTINCT", "MAX", "MIN").build();
    private static final Logger logger = LoggerFactory.getLogger(TableAnalyzerJob.class);

    public static void main(String[] args) {
        TableAnalyzerJob job = new TableAnalyzerJob();
        job.execute(args);
    }

    @Override
    protected void doExecute() {
        val jobStepId = StringUtils.replace(infos.getJobStepId(), JOB_NAME_PREFIX, "");
        val exec = new TableAnalyzerExec(jobStepId);
        TABLE_SAMPLING.createStage(this, null, null, exec);
        exec.analyzerTable();
    }

    public void analyzerTable() {
        String tableName = getParam(NBatchConstants.P_TABLE_NAME);
        long rowCount = Long.parseLong(getParam(NBatchConstants.P_SAMPLING_ROWS));
        String prjName = getParam(NBatchConstants.P_PROJECT_NAME);
        TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(tableName);
        analyzeTable(tableDesc, prjName, (int) rowCount, ss);
    }

    void analyzeTable(TableDesc tableDesc, String project, int rowCount, SparkSession ss) {

        long start = System.currentTimeMillis();
        Row[] row = new TableAnalysisJob(tableDesc, project, rowCount, ss, jobId).analyzeTable();
        logger.info("sampling rows from table {} takes {}s", tableDesc.getIdentity(),
                (System.currentTimeMillis() - start) / 1000);

        val tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        var tableExtDesc = tableMetadataManager.getOrCreateTableExt(tableDesc);
        final long count_star = Long.parseLong(row[0].get(0).toString());
        final List<TableExtDesc.ColumnStats> columnStatsList = new ArrayList<>(tableDesc.getColumnCount());
        for (int colIdx = 0; colIdx < tableDesc.getColumnCount(); colIdx++) {
            final ColumnDesc columnDesc = tableDesc.getColumns()[colIdx];
            if (columnDesc.isComputedColumn()) {
                continue;
            }
            TableExtDesc.ColumnStats colStats = tableExtDesc.getColumnStatsByName(columnDesc.getName());
            if (colStats == null) {
                colStats = new TableExtDesc.ColumnStats();
                colStats.setColumnName(columnDesc.getName());
            }
            int metricLen = TABLE_STATS_METRICS.size();
            for (int i = 0; i < metricLen; i++) {
                String value = row[0].get(i + 1 + metricLen * colIdx) == null ? null
                        : row[0].get(i + 1 + metricLen * colIdx).toString();

                switch (TABLE_STATS_METRICS.get(i)) {
                case "COUNT":
                    colStats.setNullCount(count_star - Long.parseLong(value));
                    break;
                case "MAX":
                    colStats.setMaxValue(value);
                    break;
                case "MIN":
                    colStats.setMinValue(value);
                    break;
                case "COUNT_DISTINCT":
                    colStats.setCardinality(Long.parseLong(value));
                    break;
                default:
                    throw new IllegalArgumentException(
                            "not support this metric" + TABLE_STATS_METRICS.get(i) + "in table Sampling");
                }
            }
            columnStatsList.add(colStats);
        }

        List<String[]> sampleData = Lists.newArrayList();
        IntStream.range(1, row.length).forEach(i -> {
            String[] data = new String[row[i].length()];
            IntStream.range(0, row[i].length()).forEach(j -> {
                final Object obj = row[i].get(j);
                if (obj == null) {
                    data[j] = null;
                } else if (obj instanceof Timestamp) {
                    data[j] = DateFormat.castTimestampToString(((Timestamp) obj).getTime());
                } else {
                    data[j] = obj.toString();
                }
            });
            sampleData.add(data);
        });

        UnitOfWork.doInTransactionWithRetry(() -> {
            val tableMetadataManagerForUpdate = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    project);
            var tableExt = tableMetadataManagerForUpdate.getOrCreateTableExt(tableDesc);
            tableExt = tableMetadataManagerForUpdate.copyForWrite(tableExt);
            tableExt.setTotalRows(count_star);
            tableExt.setColumnStats(columnStatsList);
            tableExt.setSampleRows(sampleData);
            tableExt.setJodID(jobId);
            tableMetadataManagerForUpdate.saveTableExt(tableExt);

            return null;
        }, project);
        logger.info("Table {} analysis finished, update table ext desc done.", tableDesc.getName());
    }

    @Override
    protected String calculateRequiredCores() throws Exception {
        String tableName = getParam(NBatchConstants.P_TABLE_NAME);
        long rowCount = Long.parseLong(getParam(NBatchConstants.P_SAMPLING_ROWS));
        Path shareDir = config.getJobTmpShareDir(project, jobId);
        val child = tableName + "_" + ResourceDetectUtils.samplingDetectItemFileSuffix();
        val detectItems = ResourceDetectUtils.readDetectItems(new Path(shareDir, child));
        return ResourceUtils.caculateRequiredCores(detectItems);
    }
}
