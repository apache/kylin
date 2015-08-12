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

package org.apache.kylin.job.hadoop.invertedindex;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ysong1
 * 
 */
@SuppressWarnings("static-access")
public class RandomKeyDistributionJob extends AbstractHadoopJob {

    protected static final Logger log = LoggerFactory.getLogger(RandomKeyDistributionJob.class);

    static final Option OPTION_KEY_CLASS = OptionBuilder.withArgName("keyclass").hasArg().isRequired(true).withDescription("Key Class").create("keyclass");
    static final Option OPTION_REGION_MB = OptionBuilder.withArgName("regionmb").hasArg().isRequired(true).withDescription("MB per Region").create("regionmb");

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_KEY_CLASS);
            options.addOption(OPTION_REGION_MB);

            parseOptions(options, args);

            // start job
            String jobName = getOptionValue(OPTION_JOB_NAME);
            job = Job.getInstance(getConf(), jobName);

            setJobClasspath(job);

            addInputDirs(getOptionValue(OPTION_INPUT_PATH), job);

            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            FileOutputFormat.setOutputPath(job, output);

            String keyClass = getOptionValue(OPTION_KEY_CLASS);
            Class<?> keyClz = Class.forName(keyClass);

            int regionMB = Integer.parseInt(getOptionValue(OPTION_REGION_MB));

            // Mapper
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapperClass(RandomKeyDistributionMapper.class);
            job.setMapOutputKeyClass(keyClz);
            job.setMapOutputValueClass(NullWritable.class);

            // Reducer - only one
            job.setReducerClass(RandomKeyDistributionReducer.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(keyClz);
            job.setOutputValueClass(NullWritable.class);
            job.setNumReduceTasks(1);

            this.deletePath(job.getConfiguration(), output);

            // total map input MB
            double totalMapInputMB = this.getTotalMapInputMB();
            int regionCount = Math.max(1, (int) (totalMapInputMB / regionMB));
            int mapSampleNumber = 1000;
            System.out.println("Total Map Input MB: " + totalMapInputMB);
            System.out.println("Region Count: " + regionCount);

            // set job configuration
            job.getConfiguration().set(BatchConstants.MAPPER_SAMPLE_NUMBER, String.valueOf(mapSampleNumber));
            job.getConfiguration().set(BatchConstants.REGION_NUMBER, String.valueOf(regionCount));

            return waitForCompletion(job);
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new RandomKeyDistributionJob(), args);
        System.exit(exitCode);
    }

}
