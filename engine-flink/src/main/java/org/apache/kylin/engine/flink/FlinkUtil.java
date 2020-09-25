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
package org.apache.kylin.engine.flink;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.source.SourceManager;
import org.apache.kylin.storage.StorageFactory;

import java.io.IOException;
import java.util.List;

/**
 * A helper class which contains some util methods used by Flink cube engine.
 */
public class FlinkUtil {

    public static IFlinkInput.IFlinkBatchCubingInputSide getBatchCubingInputSide(CubeSegment seg) {
        IJoinedFlatTableDesc flatDesc = EngineFactory.getJoinedFlatTableDesc(seg);
        return SourceManager.createEngineAdapter(seg, IFlinkInput.class).getBatchCubingInputSide(flatDesc);
    }

    public static IFlinkOutput.IFlinkBatchCubingOutputSide getBatchCubingOutputSide(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, IFlinkOutput.class).getBatchCubingOutputSide(seg);
    }

    public static IFlinkOutput.IFlinkBatchMergeOutputSide getBatchMergeOutputSide2(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, IFlinkOutput.class).getBatchMergeOutputSide(seg);
    }

    public static IFlinkInput.IFlinkBatchMergeInputSide getBatchMergeInputSide(CubeSegment seg) {
        return SourceManager.createEngineAdapter(seg, IFlinkInput.class).getBatchMergeInputSide(seg);
    }

    public static IMROutput2.IMRBatchOptimizeOutputSide2 getBatchOptimizeOutputSide2(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, IMROutput2.class).getBatchOptimizeOutputSide(seg);
    }

    public static DataSet parseInputPath(String inputPath, FileSystem fs, ExecutionEnvironment env, Class keyClass,
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
            return env.createInput(HadoopInputs.readSequenceFile(keyClass, valueClass, inputHDFSPath.toString()));
        }

        Job job = Job.getInstance();
        FileInputFormat.setInputPaths(job, StringUtil.join(inputFolders, ","));
        return env.createInput(HadoopInputs.createHadoopInput(new SequenceFileInputFormat(), keyClass, valueClass, job));
    }

    public static void setHadoopConfForCuboid(Job job, CubeSegment segment, String metaUrl) throws Exception {
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
    }

    public static void modifyFlinkHadoopConfiguration(Job job) throws Exception {
        job.getConfiguration().set("dfs.replication", "2"); // cuboid intermediate files, replication=2
        job.getConfiguration().set("mapreduce.output.fileoutputformat.compress", "true");
        job.getConfiguration().set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
        job.getConfiguration().set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.DefaultCodec"); // or org.apache.hadoop.io.compress.SnappyCodec
    }

    public static DataSet<String[]> readHiveRecords(boolean isSequenceFile, ExecutionEnvironment env, String inputPath, String hiveTable, Job job) throws IOException {
        DataSet<String[]> recordDataSet;

        if (isSequenceFile) {
            recordDataSet = env
                    .createInput(HadoopInputs.readHadoopFile(new SequenceFileInputFormat(), BytesWritable.class, Text.class, inputPath, job),
                            TypeInformation.of(new TypeHint<Tuple2<BytesWritable, Text>>() {}))
                    .map(new MapFunction<Tuple2<BytesWritable, Text>, String[]>() {
                        @Override
                        public String[] map(Tuple2<BytesWritable, Text> tuple2) throws Exception {

                            String s = Bytes.toString(tuple2.f1.getBytes(), 0, tuple2.f1.getLength());
                            return s.split(BatchConstants.SEQUENCE_FILE_DEFAULT_DELIMITER);
                        }
                    });
        } else {
            throw new UnsupportedOperationException("Currently, Flink does not support read hive table directly.");
        }

        return recordDataSet;
    }

}
