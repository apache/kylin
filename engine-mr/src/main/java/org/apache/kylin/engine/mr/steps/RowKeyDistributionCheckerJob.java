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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;

/**
 * @author ysong1
 * 
 */
public class RowKeyDistributionCheckerJob extends AbstractHadoopJob {

    @SuppressWarnings("static-access")
    protected static final Option ROW_KEY_STATS_FILE_PATH = OptionBuilder.withArgName("path").hasArg().isRequired(true).withDescription("rowKeyStatsFilePath").create("rowKeyStatsFilePath");

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_JOB_NAME);
            options.addOption(ROW_KEY_STATS_FILE_PATH);

            parseOptions(options, args);

            String statsFilePath = getOptionValue(ROW_KEY_STATS_FILE_PATH);

            // start job
            String jobName = getOptionValue(OPTION_JOB_NAME);
            job = Job.getInstance(getConf(), jobName);

            setJobClasspath(job, KylinConfig.getInstanceFromEnv());

            addInputDirs(getOptionValue(OPTION_INPUT_PATH), job);

            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            FileOutputFormat.setOutputPath(job, output);

            // Mapper
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapperClass(RowKeyDistributionCheckerMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            // Reducer - only one
            job.setReducerClass(RowKeyDistributionCheckerReducer.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            job.setNumReduceTasks(1);

            job.getConfiguration().set("rowKeyStatsFilePath", statsFilePath);

            this.deletePath(job.getConfiguration(), output);

            return waitForCompletion(job);
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new RowKeyDistributionCheckerJob(), args);
        System.exit(exitCode);
    }

}
