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
package org.apache.kylin.storage.parquet.steps;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.parquet.example.data.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;


/**
 * Created by Yichen on 11/9/18.
 */
public class MRCubeParquetJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(MRCubeParquetJob.class);

    final static String BY_LAYER_OUTPUT = "ByLayer";
    private Options options;

    public MRCubeParquetJob(){
        options = new Options();
        options.addOption(OPTION_JOB_NAME);
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_OUTPUT_PATH);
    }

    @Override
    public int run(String[] args) throws Exception {
        parseOptions(options, args);

        final Path inputPath = new Path(getOptionValue(OPTION_INPUT_PATH));
        final Path outputPath = new Path(getOptionValue(OPTION_OUTPUT_PATH));
        final String cubeName = getOptionValue(OPTION_CUBE_NAME);
        logger.info("CubeName: ", cubeName);
        final String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        CubeInstance cube = cubeManager.getCube(cubeName);

        CubeSegment cubeSegment = cube.getSegmentById(segmentId);
        logger.info("Input path: {}", inputPath);
        logger.info("Output path: {}", outputPath);

        job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
        setJobClasspath(job, cube.getConfig());
        CuboidToPartitionMapping cuboidToPartitionMapping= new CuboidToPartitionMapping(cubeSegment, kylinConfig);
        String jsonStr = cuboidToPartitionMapping.serialize();
        logger.info("Total Partition: {}", cuboidToPartitionMapping.getNumPartitions());

        final IDimensionEncodingMap dimEncMap = cubeSegment.getDimensionEncodingMap();

        Cuboid baseCuboid = Cuboid.getBaseCuboid(cubeSegment.getCubeDesc());

        MessageType schema = ParquetConvertor.cuboidToMessageType(baseCuboid, dimEncMap, cubeSegment.getCubeDesc());
        logger.info("Schema: {}", schema.toString());

        try {

            job.getConfiguration().set(BatchConstants.ARG_CUBOID_TO_PARTITION_MAPPING, jsonStr);


            addInputDirs(inputPath.toString(), job);
            FileOutputFormat.setOutputPath(job, outputPath);

            job.setJobName("Converting Parquet File for: " + cubeName + " segment " + segmentId);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setPartitionerClass(CuboidPartitioner.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Group.class);
            job.setReducerClass(ConvertToParquetReducer.class);
            job.setNumReduceTasks(cuboidToPartitionMapping.getNumPartitions());

            MultipleOutputs.addNamedOutput(job, BY_LAYER_OUTPUT, ExampleOutputFormat.class, NullWritable.class, Group.class);

            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentId);
            ExampleOutputFormat.setSchema(job, schema);
            LazyOutputFormat.setOutputFormatClass(job, ExampleOutputFormat.class);
            attachCubeMetadataWithDict(cube, job.getConfiguration());

            this.deletePath(job.getConfiguration(), outputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            return waitForCompletion(job);

        } finally {
            if (job != null) {
                cleanupTempConfFile(job.getConfiguration());
            }
        }
    }

    public static class CuboidPartitioner extends Partitioner<Text, Text> implements Configurable {
        private Configuration conf;
        private CuboidToPartitionMapping mapping;

        @Override
        public void setConf(Configuration configuration) {
            this.conf = configuration;
            try {
                mapping = CuboidToPartitionMapping.deserialize(conf.get(BatchConstants.ARG_CUBOID_TO_PARTITION_MAPPING));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return mapping.getPartitionByKey(key.getBytes());
        }

        @Override
        public Configuration getConf() {
            return conf;
        }
    }

    public static void main(String[] args) throws Exception {
        MRCubeParquetJob job = new MRCubeParquetJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
