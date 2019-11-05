/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
