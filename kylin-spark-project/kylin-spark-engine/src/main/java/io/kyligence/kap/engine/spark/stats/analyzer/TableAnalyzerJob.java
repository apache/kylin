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
package io.kyligence.kap.engine.spark.stats.analyzer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.utils.ResourceUtils;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.job.SparkJobConstants;
import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.engine.spark.job.TableAnalysisJob;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.val;
import lombok.var;

public class TableAnalyzerJob extends SparkApplication implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(TableAnalyzerJob.class);

    public static final ImmutableList<String> TABLE_STATS_METRICS = ImmutableList.<String> builder()
            .add("COUNT", "COUNT_DISTINCT", "MAX", "MIN").build();

    @Override
    protected void doExecute() {
        String tableName = getParam(NBatchConstants.P_TABLE_NAME);
        Long rowCount = Long.valueOf(getParam(NBatchConstants.P_SAMPLING_ROWS));
        String prjName = getParam(NBatchConstants.P_PROJECT_NAME);
        TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(tableName);
        logger.info("Start analyse table {} ", tableName);
        analyzeTable(tableDesc, prjName, rowCount.intValue(), config, ss);
    }

    void analyzeTable(TableDesc tableDesc, String project, int rowCount, KylinConfig config, SparkSession ss) {

        long start = System.currentTimeMillis();
        Row[] row = new TableAnalysisJob(tableDesc, project, rowCount, ss).analyzeTable();
        logger.info("sampling rows from table {} takes {}s", tableDesc.getIdentity(),
                (System.currentTimeMillis() - start) / 1000);

        val tableMetadataManager = NTableMetadataManager.getInstance(config, project);
        var tableExt = tableMetadataManager.getOrCreateTableExt(tableDesc);
        tableExt = tableMetadataManager.copyForWrite(tableExt);

        final long count_star = Long.parseLong(row[0].get(0).toString());
        tableExt.setTotalRows(count_star);
        final List<TableExtDesc.ColumnStats> columnStatsList = new ArrayList<>(tableDesc.getColumnCount());
        for (int colIdx = 0; colIdx < tableDesc.getColumnCount(); colIdx++) {
            final ColumnDesc columnDesc = tableDesc.getColumns()[colIdx];
            if (columnDesc.isComputedColumn()) {
                continue;
            }
            TableExtDesc.ColumnStats colStats = tableExt.getColumnStatsByName(columnDesc.getName());
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
                    colStats.setCardinality(Long.valueOf(value));
                    break;
                default:
                    throw new IllegalArgumentException(
                            "not support this metric" + TABLE_STATS_METRICS.get(i) + "in table Sampling");
                }
            }
            columnStatsList.add(colStats);
        }
        tableExt.setColumnStats(columnStatsList);

        List<String[]> sampleData = Lists.newArrayList();
        for (int i = 1; i < row.length; i++) {
            String[] data = new String[row[i].length()];
            for (int j = 0; j < row[i].length(); j++) {
                data[j] = row[i].get(j) == null ? null : row[i].get(j).toString();
            }
            sampleData.add(data);
        }
        tableExt.setSampleRows(sampleData);

        tableMetadataManager.saveTableExt(tableExt);
        logger.info("Table {} analysis finished, update table ext desc done.", tableDesc.getName());
    }

    @Override
    protected String calculateRequiredCores() throws Exception {
        String tableName = getParam(NBatchConstants.P_TABLE_NAME);
        Long rowCount = Long.valueOf(getParam(NBatchConstants.P_SAMPLING_ROWS));
        if (config.getSparkEngineDataImpactInstanceEnabled()) {
            Path shareDir = config.getJobTmpShareDir(project, jobId);
            val  child = tableName + "_" + ResourceDetectUtils.samplingDetectItemFileSuffix();
            val  detectItems = ResourceDetectUtils.readDetectItems(new Path(shareDir,  child));
            return ResourceUtils.caculateRequiredCores(config.getSparkEngineSampleSplitThreshold(), detectItems, rowCount);
        } else {
            return SparkJobConstants.DEFAULT_REQUIRED_CORES;
        }
    }


    public static void main(String[] args) {
        TableAnalyzerJob job = new TableAnalyzerJob();
        job.execute(args);
    }
}
