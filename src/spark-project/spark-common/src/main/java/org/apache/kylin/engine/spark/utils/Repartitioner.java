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

package org.apache.kylin.engine.spark.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.filter.ParquetBloomFilter;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Repartitioner {
    protected static final Logger logger = LoggerFactory.getLogger(Repartitioner.class);

    private int MB = 1024 * 1024;
    private int shardSize;
    private int fileLengthThreshold;
    private boolean needResetRowGroup;
    private long parquetBlockSize;
    private long parquetPageSizeRowCheckMax;
    private long totalRowCount;
    private long rowCountThreshold;
    private ContentSummary contentSummary;
    private List<Integer> shardByColumns = new ArrayList<>();
    private List<Integer> sortByColumns;
    private boolean optimizeShardEnabled;

    /** because shardByColumns maybe is null, can't use @AllArgsConstructor to fix sonar error */
    @SuppressWarnings("squid:S107")
    public Repartitioner(int shardSize, int fileLengthThreshold, boolean needResetRowGroup, long parquetBlockSize,
            long parquetPageSizeRowCheckMax, long totalRowCount, long rowCountThreshold, ContentSummary contentSummary,
            List<Integer> shardByColumns, List<Integer> sortByColumns, boolean optimizeShardEnabled) {
        this.shardSize = shardSize;
        this.fileLengthThreshold = fileLengthThreshold;
        this.needResetRowGroup = needResetRowGroup;
        this.parquetBlockSize = parquetBlockSize;
        this.parquetPageSizeRowCheckMax = parquetPageSizeRowCheckMax;
        this.totalRowCount = totalRowCount;
        this.rowCountThreshold = rowCountThreshold;
        this.contentSummary = contentSummary;
        if (shardByColumns != null) {
            this.shardByColumns = shardByColumns;
        }
        this.sortByColumns = sortByColumns;
        this.optimizeShardEnabled = optimizeShardEnabled;
    }

    boolean needRepartitionForFileSize() {
        // per file size < threshold file size
        return (contentSummary.getLength() * 1.0 / MB / contentSummary.getFileCount()) < fileLengthThreshold
                && contentSummary.getFileCount() > 1;
    }

    boolean needRepartitionForShardByColumns() {
        return shardByColumns != null && !shardByColumns.isEmpty();
    }

    private boolean needRepartitionForRowCount() {
        return contentSummary.getFileCount() < ((double) totalRowCount / rowCountThreshold) * 0.75;
    }

    @VisibleForTesting
    public boolean needRepartition() {
        if (needRepartitionForShardByColumns()) {
            return true;
        }
        boolean needRepartition = needRepartitionForFileSize() || needRepartitionForRowCount();

        if (needRepartition && getRepartitionNumByStorage() == contentSummary.getFileCount()) {
            needRepartition = false;
        }
        return needRepartition;
    }

    public int getShardSize() {
        return shardSize;
    }

    public int getFileLengthThreshold() {
        return fileLengthThreshold;
    }

    public ContentSummary getContentSummary() {
        return contentSummary;
    }

    private List<Integer> getShardByColumns() {
        return shardByColumns;
    }

    private int getFileLengthRepartitionNum() {
        return (int) Math.ceil(contentSummary.getLength() * 1.0 / MB / shardSize);
    }

    private int getRowCountRepartitionNum() {
        return (int) Math.ceil(1.0 * totalRowCount / rowCountThreshold);
    }

    public int getRepartitionNumByStorage() {
        int fileLengthRepartitionNum = getFileLengthRepartitionNum();
        int rowCountRepartitionNum = getRowCountRepartitionNum();
        logger.info("File length repartition num : {}, Row count Rpartition num: {}", fileLengthRepartitionNum,
                rowCountRepartitionNum);
        int partitionSize = (int) Math.ceil(1.0 * (fileLengthRepartitionNum + rowCountRepartitionNum) / 2);
        logger.info("Repartition size is :{}", partitionSize);
        return partitionSize;
    }

    public void doRepartition(String outputPath, String inputPath, int repartitionNum, SparkSession ss)
            throws IOException {
        Path tempResourcePath = new Path(inputPath);

        FileSystem readFileSystem = HadoopUtil.getWorkingFileSystem();
        if (needRepartition()) {
            // repartition and write to target path
            logger.info("Repartition {} to {}, [repartition number: {}, use shard column: {}]", inputPath, outputPath,
                    repartitionNum, needRepartitionForShardByColumns());
            long start = System.currentTimeMillis();
            Dataset<Row> data;

            if (needRepartitionForShardByColumns()) {
                if (optimizeShardEnabled)
                    ss.sessionState().conf().setLocalProperty("spark.sql.adaptive.skewRepartition.enabled", "true");
                data = ss.read().parquet(inputPath)
                        .repartition(repartitionNum, convertIntegerToColumns(getShardByColumns()))
                        .sortWithinPartitions(convertIntegerToColumns(sortByColumns));
            } else {
                // repartition for single file size is too small
                data = ss.read().parquet(inputPath).repartition(repartitionNum)
                        .sortWithinPartitions(convertIntegerToColumns(sortByColumns));
            }
            DataFrameWriter<Row> writer;
            if (needResetRowGroup) {
                logger.info("set parquet.block.size={}, parquet.page.size.row.check.max={}", parquetBlockSize,
                        parquetPageSizeRowCheckMax);
                writer = data.write().option("parquet.block.size", String.valueOf(parquetBlockSize))
                        .option("parquet.page.size.row.check.max", String.valueOf(parquetPageSizeRowCheckMax))
                        .mode(SaveMode.Overwrite);
            } else {
                writer = data.write().mode(SaveMode.Overwrite);
            }
            ParquetBloomFilter.configBloomColumnIfNeed(data, writer);
            writer.parquet(outputPath);
            if (needRepartitionForShardByColumns()) {
                if (optimizeShardEnabled)
                    ss.sessionState().conf().setLocalProperty("spark.sql.adaptive.skewRepartition.enabled", null);
            }
            if (readFileSystem.delete(tempResourcePath, true)) {
                logger.info("Delete temp cuboid path successful. Temp path: {}.", inputPath);
            } else {
                logger.error("Delete temp cuboid path wrong, leave garbage. Temp path: {}.", inputPath);
            }
            long end = System.currentTimeMillis();
            logger.info("Repartition and rewrite ends. Cost: {} ms.", end - start);
        } else {
            Path goalPath = new Path(outputPath);
            if (readFileSystem.exists(goalPath)) {
                logger.info("Path {} is exists, delete it.", goalPath);
                readFileSystem.delete(goalPath, true);
            }
            if (readFileSystem.rename(new Path(inputPath), goalPath)) {
                logger.info("Rename temp path to target path successfully. Temp path: {}, target path: {}.", inputPath,
                        outputPath);
            } else {
                throw new RuntimeException(String.format(Locale.ROOT,
                        "Rename temp path to target path wrong. Temp path: %s, target path: %s.", inputPath,
                        outputPath));
            }
        }
    }

    private Column[] convertIntegerToColumns(List<Integer> indices) {
        Column[] ret = new Column[indices.size()];
        int index = 0;
        for (Integer i : indices) {
            ret[index] = new Column(String.valueOf(i));
            index++;
        }
        return ret;
    }
}
