/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job.hadoop.invertedindex;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.job.constant.BatchConstants;
import com.kylinolap.job.hadoop.AbstractHadoopJob;

/**
 * @author yangli9
 */
public class InvertedIndexJob extends AbstractHadoopJob {
    protected static final Logger log = LoggerFactory.getLogger(InvertedIndexJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_INPUT_FORMAT);
            options.addOption(OPTION_INPUT_DELIM);
            options.addOption(OPTION_OUTPUT_PATH);
            parseOptions(options, args);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            String cubeName = getOptionValue(OPTION_CUBE_NAME);
            Path input = new Path(getOptionValue(OPTION_INPUT_PATH));
            String inputFormat = getOptionValue(OPTION_INPUT_FORMAT);
            String inputDelim = getOptionValue(OPTION_INPUT_DELIM);
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));

            // ----------------------------------------------------------------------------

            System.out.println("Starting: " + job.getJobName());
            
            CubeInstance cube = getCube(cubeName);

            setupMapInput(input, inputFormat, inputDelim);
            setupReduceOutput(output, cube.getInvertedIndexDesc().getSharding());
            attachMetadata(cube);

            return waitForCompletion(job);

        } catch (Exception e) {
            printUsage(options);
            log.error(e.getLocalizedMessage(), e);
            return 2;
        }

    }

    /**
     * @param cubeName
     * @return
     */
    private CubeInstance getCube(String cubeName) {
        CubeManager mgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubeInstance cube = mgr.getCube(cubeName);
        if (cube == null)
            throw new IllegalArgumentException("No Inverted Index Cubefound by name " + cubeName);
        return cube;
    }

    private void attachMetadata(CubeInstance cube) throws IOException {

        Configuration conf = job.getConfiguration();
        attachKylinPropsAndMetadata(cube, conf);

        CubeSegment seg = cube.getFirstSegment();
        conf.set(BatchConstants.CFG_CUBE_NAME, cube.getName());
        conf.set(BatchConstants.CFG_CUBE_SEGMENT_NAME, seg.getName());
    }

    private void setupMapInput(Path input, String inputFormat, String inputDelim) throws IOException {
        FileInputFormat.setInputPaths(job, input);

        File JarFile = new File(KylinConfig.getInstanceFromEnv().getKylinJobJarPath());
        if (JarFile.exists()) {
            job.setJar(KylinConfig.getInstanceFromEnv().getKylinJobJarPath());
        } else {
            job.setJarByClass(this.getClass());
        }

        if ("textinputformat".equalsIgnoreCase(inputFormat) || "text".equalsIgnoreCase(inputFormat)) {
            job.setInputFormatClass(TextInputFormat.class);
        } else {
            job.setInputFormatClass(SequenceFileInputFormat.class);
        }

        if ("t".equals(inputDelim)) {
            inputDelim = "\t";
        } else if ("177".equals(inputDelim)) {
            inputDelim = "\177";
        }
        if (inputDelim != null) {
            job.getConfiguration().set(BatchConstants.INPUT_DELIM, inputDelim);
        }

        job.setMapperClass(InvertedIndexMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(ImmutableBytesWritable.class);
        job.setPartitionerClass(InvertedIndexPartitioner.class);
    }

    private void setupReduceOutput(Path output, short sharding) throws IOException {
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(ImmutableBytesWritable.class);
        
        job.setNumReduceTasks(sharding);

        FileOutputFormat.setOutputPath(job, output);

        job.getConfiguration().set(BatchConstants.OUTPUT_PATH, output.toString());

        deletePath(job.getConfiguration(), output);
    }

    public static void main(String[] args) throws Exception {
        InvertedIndexJob job = new InvertedIndexJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
