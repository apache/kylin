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

package org.apache.kylin.engine.spark;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.source.SourceManager;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.kylin.engine.spark.ISparkInput.ISparkBatchCubingInputSide;
import org.apache.kylin.engine.spark.ISparkInput.ISparkBatchMergeInputSide;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveUtils;

public class SparkUtil {

    public static ISparkBatchCubingInputSide getBatchCubingInputSide(CubeSegment seg) {
        IJoinedFlatTableDesc flatDesc = EngineFactory.getJoinedFlatTableDesc(seg);
        return (ISparkBatchCubingInputSide)SourceManager.createEngineAdapter(seg, ISparkInput.class).getBatchCubingInputSide(flatDesc);
    }

    public static ISparkOutput.ISparkBatchCubingOutputSide getBatchCubingOutputSide(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, ISparkOutput.class).getBatchCubingOutputSide(seg);
    }

    public static ISparkOutput.ISparkBatchMergeOutputSide getBatchMergeOutputSide2(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, ISparkOutput.class).getBatchMergeOutputSide(seg);
    }

    public static ISparkBatchMergeInputSide getBatchMergeInputSide(CubeSegment seg) {
        return (ISparkBatchMergeInputSide)SourceManager.createEngineAdapter(seg, ISparkInput.class).getBatchMergeInputSide(seg);
    }

    public static IMROutput2.IMRBatchOptimizeOutputSide2 getBatchOptimizeOutputSide2(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, IMROutput2.class).getBatchOptimizeOutputSide(seg);
    }

    /**
     * Read the given path as a Java RDD; The path can have second level sub folder.
     * @param inputPath
     * @param fs
     * @param sc
     * @param keyClass
     * @param valueClass
     * @return
     * @throws IOException
     */
    public static JavaPairRDD parseInputPath(String inputPath, FileSystem fs, JavaSparkContext sc, Class keyClass,
            Class valueClass) throws IOException {
        List<String> inputFolders = Lists.newArrayList();
        Path inputHDFSPath = new Path(inputPath);
        FileStatus[] fileStatuses = fs.listStatus(inputHDFSPath);
        boolean hasDir = false;
        for (FileStatus stat : fileStatuses) {
            if (stat.isDirectory() && !stat.getPath().getName().startsWith("_")) {
                hasDir = true;
                inputFolders.add(stat.getPath().toString());
            }
        }

        if (!hasDir) {
            return sc.sequenceFile(inputHDFSPath.toString(), keyClass, valueClass);
        }

        return sc.sequenceFile(StringUtil.join(inputFolders, ","), keyClass, valueClass);
    }


    public static int estimateLayerPartitionNum(int level, CubeStatsReader statsReader, KylinConfig kylinConfig) {
        double baseCuboidSize = statsReader.estimateLayerSize(level);
        float rddCut = kylinConfig.getSparkRDDPartitionCutMB();
        int partition = (int) (baseCuboidSize / rddCut);
        partition = Math.max(kylinConfig.getSparkMinPartition(), partition);
        partition = Math.min(kylinConfig.getSparkMaxPartition(), partition);
        return partition;
    }


    public static int estimateTotalPartitionNum(CubeStatsReader statsReader, KylinConfig kylinConfig) {
        double totalSize = 0;
        for (double x: statsReader.getCuboidSizeMap().values()) {
            totalSize += x;
        }
        float rddCut = kylinConfig.getSparkRDDPartitionCutMB();
        int partition = (int) (totalSize / rddCut);
        partition = Math.max(kylinConfig.getSparkMinPartition(), partition);
        partition = Math.min(kylinConfig.getSparkMaxPartition(), partition);
        return partition;
    }


    public static void setHadoopConfForCuboid(Job job, CubeSegment segment, String metaUrl) throws Exception {
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
    }

    public static void modifySparkHadoopConfiguration(SparkContext sc) throws Exception {
        sc.hadoopConfiguration().set("dfs.replication", "2"); // cuboid intermediate files, replication=2
        sc.hadoopConfiguration().set("mapreduce.output.fileoutputformat.compress", "true");
        sc.hadoopConfiguration().set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
        sc.hadoopConfiguration().set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.DefaultCodec"); // or org.apache.hadoop.io.compress.SnappyCodec
    }

    public static JavaRDD<String[]> hiveRecordInputRDD(boolean isSequenceFile, JavaSparkContext sc, String inputPath, String hiveTable) throws IOException {
        JavaRDD<String[]> recordRDD;

        if (isSequenceFile && HadoopUtil.isSequenceDir(sc.hadoopConfiguration(), new Path(inputPath))) {
            recordRDD = getSequenceFormatHiveInput(sc, inputPath);
        } else {
            recordRDD = getOtherFormatHiveInput(sc, hiveTable);
        }

        return recordRDD;
    }

    private static JavaRDD<String[]> getSequenceFormatHiveInput(JavaSparkContext sc, String inputPath) {
        return sc.sequenceFile(inputPath, BytesWritable.class, Text.class).values()
                .map(new Function<Text, String[]>() {
                    @Override
                    public String[] call(Text text) throws Exception {
                        String s = Bytes.toString(text.getBytes(), 0, text.getLength());
                        return s.split(BatchConstants.SEQUENCE_FILE_DEFAULT_DELIMITER, -1);
                    }
                });
    }

    private static JavaRDD<String[]> getOtherFormatHiveInput(JavaSparkContext sc, String hiveTable) {
        SparkSession sparkSession = SparkSession.builder().sparkContext(HiveUtils.withHiveExternalCatalog(sc.sc()))
                .config(sc.getConf()).enableHiveSupport().getOrCreate();
        final Dataset intermediateTable = sparkSession.table(hiveTable);
        return intermediateTable.javaRDD().map(new Function<Row, String[]>() {
            @Override
            public String[] call(Row row) throws Exception {
                String[] result = new String[row.size()];
                for (int i = 0; i < row.size(); i++) {
                    final Object o = row.get(i);
                    if (o != null) {
                        result[i] = o.toString();
                    } else {
                        result[i] = null;
                    }
                }
                return result;
            }
        });
    }

}
