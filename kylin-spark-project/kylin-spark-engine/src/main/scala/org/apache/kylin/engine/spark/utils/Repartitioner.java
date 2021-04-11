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

import org.apache.kylin.engine.spark.job.NSparkCubingUtil;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.annotations.VisibleForTesting;

public class Repartitioner {
    private static String tempDirSuffix = "_temp";
    protected static final Logger logger = LoggerFactory.getLogger(Repartitioner.class);

    private int MB = 1024 * 1024;
    private int shardSize;
    private int fileLengthThreshold;
    private long totalRowCount;
    private long rowCountThreshold;
    private long cuboid;
    private ContentSummary contentSummary;
    private List<Integer> shardByColumns = new ArrayList<>();

    public Repartitioner(int shardSize, int fileLengthThreshold, LayoutEntity layoutEntity, long rowCountThreshold,
                         ContentSummary contentSummary, List<Integer> shardByColumns) {
        this.shardSize = shardSize;
        this.fileLengthThreshold = fileLengthThreshold;
        this.totalRowCount = layoutEntity.getRows();
        cuboid = layoutEntity.getId();
        this.rowCountThreshold = rowCountThreshold;
        this.contentSummary = contentSummary;
        if (shardByColumns != null) {
            this.shardByColumns = shardByColumns;
        }
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
        int partitionSize = (int) Math.ceil(1.0 * (fileLengthRepartitionNum + rowCountRepartitionNum) / 2);
        logger.info("Before repartition, cuboid[{}] has {} row, {} bytes and {} files. Partition " +
                        "count calculated by file size is {}, calculated by row count is {}, final is {}.",
                cuboid, totalRowCount, contentSummary.getLength(), contentSummary.getFileCount(),
                fileLengthRepartitionNum, rowCountRepartitionNum, partitionSize);
        return partitionSize;
    }

    public void setShardSize(int shardSize) {
        this.shardSize = shardSize;
    }

    public void setFileLengthThreshold(int fileLengthThreshold) {
        this.fileLengthThreshold = fileLengthThreshold;
    }

    public void setContentSummary(ContentSummary contentSummary) {
        this.contentSummary = contentSummary;
    }

    public void doRepartition(NSparkCubingEngine.NSparkCubingStorage storage, String path, int repartitionNum, Column[] sortCols, SparkSession ss)
            throws IOException {
        String tempPath = path + tempDirSuffix;
        Path tempResourcePath = new Path(tempPath);

        FileSystem readFileSystem = HadoopUtil.getWorkingFileSystem();
        if (needRepartition()) {
            // repartition and write to target path
            logger.info("Start repartition and rewrite");
            long start = System.currentTimeMillis();
            Dataset<Row> data;

            if (needRepartitionForShardByColumns()) {
                logger.info("Cuboid[{}] repartition by column {} to {}", cuboid,
                        NSparkCubingUtil.getColumns(getShardByColumns())[0], repartitionNum);
                data = storage.getFrom(tempPath, ss).repartition(repartitionNum,
                        NSparkCubingUtil.getColumns(getShardByColumns()))
                        .sortWithinPartitions(sortCols);
            } else {
                // repartition for single file size is too small
                logger.info("Cuboid[{}] repartition to {}", cuboid, repartitionNum);
                data = storage.getFrom(tempPath, ss).repartition(repartitionNum)
                        .sortWithinPartitions(sortCols);
            }

            storage.saveTo(path, data, ss);
            if (readFileSystem.delete(tempResourcePath, true)) {
                logger.info("Delete temp cuboid path successful. Temp path: {}.", tempPath);
            } else {
                logger.error("Delete temp cuboid path wrong, leave garbage. Temp path: {}.", tempPath);
            }
            long end = System.currentTimeMillis();
            logger.info("Repartition and rewrite ends. Cost: {} ms.", end - start);
        } else {
            Path goalPath = new Path(path);
            if (readFileSystem.exists(goalPath)) {
                logger.info("Path {} is exists, delete it.", goalPath);
                readFileSystem.delete(goalPath, true);
            }
            if (readFileSystem.rename(new Path(tempPath), goalPath)) {
                logger.info("Rename temp path to target path successfully. Temp path: {}, target path: {}.", tempPath,
                        path);
            } else {
                throw new RuntimeException(String.format(Locale.ROOT,
                        "Rename temp path to target path wrong. Temp path: %s, target path: %s.", tempPath, path));
            }
        }
    }
}
