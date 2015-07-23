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

package org.apache.kylin.engine.mr.steps;

///*
// * Copyright 2013-2014 eBay Software Foundation
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.kylin.index.cube;
//
//import org.apache.commons.cli.Options;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.apache.hadoop.util.ToolRunner;
//
//import org.apache.kylin.cube.CubeInstance;
//import org.apache.kylin.cube.CubeManager;
//import org.apache.kylin.cube.cuboid.Cuboid;
//import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
//import org.apache.kylin.cube.kv.RowKeyEncoder;
//import org.apache.kylin.index.AbstractHadoopJob;
//import org.apache.kylin.metadata.model.cube.CubeDesc;
//
///**
// * @author xjiang
// *
// */
//
//public class KeyDistributionJob extends AbstractHadoopJob {
//
//    public static final String JOB_TITLE = "Kylin Row Key Distribution Job";
//    public static final String KEY_HEADER_LENGTH = "key_header_length";
//    public static final String KEY_COLUMN_PERCENTAGE = "key_column_percentage";
//    public static final String KEY_SPLIT_NUMBER = "key_split_number";
//
//    /* (non-Javadoc)
//     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
//     */
//    @Override
//    public int run(String[] args) throws Exception {
//        Options options = new Options();
//
//        try {
//            options.addOption(OPTION_INPUT_PATH);
//            options.addOption(OPTION_OUTPUT_PATH);
//            options.addOption(OPTION_METADATA_URL);
//            options.addOption(OPTION_CUBE_NAME);
//            options.addOption(OPTION_KEY_COLUMN_PERCENTAGE);
//            options.addOption(OPTION_KEY_SPLIT_NUMBER);
//            parseOptions(options, args);
//
//            // start job
//            String jobName = JOB_TITLE + getOptionsAsString();
//            System.out.println("Starting: " + jobName);
//            Job job = Job.getInstanceFromEnv(getConf(), jobName);
//
//            // set job configuration - basic 
//            setJobClasspath(job);
//            addInputDirs(getOptionValue(OPTION_INPUT_PATH), job);
//
//            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
//            FileOutputFormat.setOutputPath(job, output);
//            //job.getConfiguration().set("dfs.block.size", "67108864");
//
//            // set job configuration - key prefix size & key split number
//            String keyColumnPercentage = getOptionValue(OPTION_KEY_COLUMN_PERCENTAGE);
//            job.getConfiguration().set(KEY_COLUMN_PERCENTAGE, keyColumnPercentage);
//            String metadataUrl = validateMetadataUrl(getOptionValue(OPTION_METADATA_URL));
//            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
//            int keyHeaderLen = getKeyHeaderLength(metadataUrl, cubeName);
//            job.getConfiguration().set(KEY_HEADER_LENGTH, String.valueOf(keyHeaderLen));
//            job.getConfiguration().set(KEY_SPLIT_NUMBER, getOptionValue(OPTION_KEY_SPLIT_NUMBER));
//
//            // Mapper
//            job.setInputFormatClass(SequenceFileInputFormat.class);
//            job.setMapperClass(KeyDistributionMapper.class);
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(LongWritable.class);
//
//            // Combiner, not needed any more as mapper now does the groping
//            //job.setCombinerClass(KeyDistributionCombiner.class);
//
//            // Reducer - only one
//            job.setReducerClass(KeyDistributionReducer.class);
//            // use sequence file as output
//            job.setOutputFormatClass(SequenceFileOutputFormat.class);
//            // key is text
//            job.setOutputKeyClass(Text.class);
//            // value is long
//            job.setOutputValueClass(LongWritable.class);
//            job.setNumReduceTasks(1);
//
//            FileSystem fs = FileSystem.get(job.getConfiguration());
//            if (fs.exists(output))
//                fs.delete(output, true);
//
//            return waitForCompletion(job);
//        } catch (Exception e) {
//            printUsage(options);
//            e.printStackTrace(System.err);
//            return 2;
//        }
//    }
//
//    private int getKeyHeaderLength(String metadataUrl, String cubeName) {
//        CubeManager cubeMgr = CubeManager.getInstanceFromEnv(metadataUrl);
//        CubeInstance cubeInstance = cubeMgr.getCube(cubeName);
//        CubeDesc cubeDesc = cubeInstance.getDescriptor();
//        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
//        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
//        RowKeyEncoder rowKeyEncoder =
//                (RowKeyEncoder) AbstractRowKeyEncoder.createInstance(cubeInstance.getTheOnlySegment(),
//                        baseCuboid);
//
//        return rowKeyEncoder.getHeaderLength();
//
//    }
//
//    public static void main(String[] args) throws Exception {
//        int exitCode = ToolRunner.run(new KeyDistributionJob(), args);
//        System.exit(exitCode);
//    }
// }
