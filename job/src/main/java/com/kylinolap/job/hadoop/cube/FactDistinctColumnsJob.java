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

package com.kylinolap.job.hadoop.cube;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.job.constant.BatchConstants;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * @author yangli9
 */
public class FactDistinctColumnsJob extends AbstractHadoopJob {
    protected static final Logger log = LoggerFactory.getLogger(FactDistinctColumnsJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_INPUT_FORMAT);
            options.addOption(OPTION_OUTPUT_PATH);
            parseOptions(options, args);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            String cubeName = getOptionValue(OPTION_CUBE_NAME);
            Path input = new Path(getOptionValue(OPTION_INPUT_PATH));
            String inputFormat = getOptionValue(OPTION_INPUT_FORMAT);
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));

            // ----------------------------------------------------------------------------

            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            System.out.println("Starting: " + job.getJobName());

            setupMapInput(input, inputFormat);
            setupReduceOutput(output);

            // add metadata to distributed cache
            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            //CubeSegment seg = cubeMgr.getCube(cubeName).getTheOnlySegment();
            attachKylinPropsAndMetadata(cubeMgr.getCube(cubeName), job.getConfiguration());

            return waitForCompletion(job);

        } catch (Exception e) {
            printUsage(options);
            log.error(e.getLocalizedMessage(), e);
            return 2;
        }

    }

    private void setupMapInput(Path input, String inputFormat) throws IOException {
        FileInputFormat.setInputPaths(job, input);

        File JarFile = new File(KylinConfig.getInstanceFromEnv().getKylinJobJarPath());
        if (JarFile.exists()) {
            job.setJar(KylinConfig.getInstanceFromEnv().getKylinJobJarPath());
        } else {
            job.setJarByClass(this.getClass());
        }

        if ("text".equalsIgnoreCase(inputFormat) || "textinputformat".equalsIgnoreCase(inputFormat)) {
            job.setInputFormatClass(TextInputFormat.class);
        } else {
            job.setInputFormatClass(SequenceFileInputFormat.class);
        }
        job.setMapperClass(FactDistinctColumnsMapper.class);
        job.setCombinerClass(FactDistinctColumnsCombiner.class);
        job.setMapOutputKeyClass(ShortWritable.class);
        job.setMapOutputValueClass(Text.class);
    }

    private void setupReduceOutput(Path output) throws IOException {
        job.setReducerClass(FactDistinctColumnsReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, output);
        job.getConfiguration().set(BatchConstants.OUTPUT_PATH, output.toString());

        job.setNumReduceTasks(1);

        deletePath(job.getConfiguration(), output);
    }

    public static void main(String[] args) throws Exception {
        FactDistinctColumnsJob job = new FactDistinctColumnsJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
