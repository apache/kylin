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

package org.apache.kylin.engine.spark.job;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.apache.spark.utils.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class TableAnalyzerJob extends SparkApplication implements Serializable {

    protected static final Logger logger = LoggerFactory.getLogger(TableAnalyzerJob.class);

    public static final ImmutableList<String> TABLE_STATS_METRICS = ImmutableList.<String> builder()
            .add("COUNT", "COUNT_DISTINCT", "MAX", "MIN").build();

    public static final String P_TABLE_NAME = "table";

    public static final String P_SAMPLING_ROWS = "maxSampleCount";

    @Override
    protected void doExecute() throws Exception {
        String tableName = getParam(P_TABLE_NAME);
        long rowCount = Long.parseLong(getParam(P_SAMPLING_ROWS));
        TableDesc tableDesc = TableMetadataManager.getInstance(config).getTableDesc(tableName, project);
        analyzeTable(tableDesc, project, (int) rowCount, config, ss);
    }

    void analyzeTable(TableDesc tableDesc, String project, int rowCount, KylinConfig config, SparkSession ss) {

        long start = System.currentTimeMillis();
        TableAnalysisJob tableAnalysisJob = new TableAnalysisJob(tableDesc, project, rowCount, ss, jobId);
        Dataset<Row> sampleTableDataSet = tableAnalysisJob.getSampleTableDataSet();
        Row[] row = tableAnalysisJob.analyzeTable(sampleTableDataSet);
        Double sparkSampleTableHignFrequency = config.getSparkSampleTableHignFrequency();
        logger.info("sampling rows from table {} takes {}s", tableDesc.getIdentity(),
                (System.currentTimeMillis() - start) / 1000);

        TableMetadataManager tableMetadataManager = TableMetadataManager.getInstance(config);
        TableExtDesc tableExt = tableMetadataManager.getTableExt(tableDesc);

        final long count_star = Long.parseLong(row[0].get(0).toString());
        tableExt.setTotalRows(count_star);
        final List<TableExtDesc.ColumnStats> columnStatsList = new ArrayList<>(tableDesc.getColumnCount());

        //save Characteristics of the data
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
            long count = 0L;
            for (int i = 0; i < metricLen; i++) {
                String value = row[0].get(i + 1 + metricLen * colIdx) == null ? null
                        : row[0].get(i + 1 + metricLen * colIdx).toString();

                switch (TABLE_STATS_METRICS.get(i)) {
                case "COUNT":
                    colStats.setNullCount(count_star - Long.parseLong(value));
                    count = Long.parseLong(value);
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
            /*Map<String, Long> dataSkewSamples = tableAnalysisJob.buildHighFrequency(sampleTableDataSet,
                    columnDesc.getName(), count, sparkSampleTableHignFrequency);
            if (!dataSkewSamples.isEmpty()) {
                colStats.setDataSkewSamples(dataSkewSamples);
            }*/
            columnStatsList.add(colStats);
        }
        tableExt.setColumnStats(columnStatsList);

        //save sample data
        List<String[]> sampleData = Lists.newArrayList();
        IntStream.range(1, row.length).forEach(i -> {
            String[] data = new String[row[i].length()];
            IntStream.range(0, row[i].length()).forEach(j -> {
                final Object obj = row[i].get(j);
                if (obj == null) {
                    data[j] = null;
                } else if (obj instanceof Timestamp) {
                    data[j] = DateFormat.formatToDateStr(((Timestamp) obj).getTime());
                } else {
                    data[j] = obj.toString();
                }
            });
            sampleData.add(data);
        });
        tableExt.setSampleRows(sampleData);
        tableExt.setJodID(jobId);

        //TODO save Tilt data

        try {
            tableMetadataManager.saveTableExt(tableExt, project);
        } catch (IOException e) {
            logger.error("save {} table found error !", tableExt);
            e.printStackTrace();
        }
        logger.info("Table {} analysis finished, update table ext desc done.", tableDesc.getName());
    }

    @Override
    protected String calculateRequiredCores() throws Exception {
        String tableName = getParam("table");
        Path shareDir = config.getJobTmpShareDir(project, jobId);
        String child = tableName + "_" + ResourceDetectUtils.samplingDetectItemFileSuffix();
        Map<String, String> detectItems = ResourceDetectUtils.readDetectItems(new Path(shareDir, child));
        return ResourceUtils.caculateRequiredCores(detectItems);
    }

    public static void main(String[] args) {
        TableAnalyzerJob job = new TableAnalyzerJob();
        job.execute(args);
    }
}
