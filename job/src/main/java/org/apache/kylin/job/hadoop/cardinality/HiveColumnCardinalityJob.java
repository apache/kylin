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

package org.apache.kylin.job.hadoop.cardinality;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;

/**
 * This hadoop job will scan all rows of the hive table and then calculate the cardinality on each column.
 * @author shaoshi
 *
 */
public class HiveColumnCardinalityJob extends AbstractHadoopJob {
    public static final String JOB_TITLE = "Kylin Hive Column Cardinality Job";

    @SuppressWarnings("static-access")
    protected static final Option OPTION_TABLE = OptionBuilder.withArgName("table name").hasArg().isRequired(true).withDescription("The hive table name").create("table");

    public static final String OUTPUT_PATH = "/tmp/cardinality";

    public HiveColumnCardinalityJob() {
    }

    @Override
    public int run(String[] args) throws Exception {

        Options options = new Options();

        try {
            options.addOption(OPTION_TABLE);
            options.addOption(OPTION_OUTPUT_PATH);

            parseOptions(options, args);

            // start job
            String jobName = JOB_TITLE + getOptionsAsString();
            System.out.println("Starting: " + jobName);
            Configuration conf = getConf();
            job = Job.getInstance(conf, jobName);

            setJobClasspath(job);
            
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            FileOutputFormat.setOutputPath(job, output);
            job.getConfiguration().set("dfs.block.size", "67108864");

            // Mapper
            String table = getOptionValue(OPTION_TABLE);
            String[] dbTableNames = HadoopUtil.parseHiveTableName(table);
            HCatInputFormat.setInput(job, dbTableNames[0], dbTableNames[1]);

            job.setInputFormatClass(HCatInputFormat.class);
            job.setMapperClass(ColumnCardinalityMapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(BytesWritable.class);

            // Reducer - only one
            job.setReducerClass(ColumnCardinalityReducer.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(LongWritable.class);
            job.setNumReduceTasks(1);

            this.deletePath(job.getConfiguration(), output);

            System.out.println("Going to submit HiveColumnCardinalityJob for table '" + table + "'");
            int result = waitForCompletion(job);

            return result;
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }

    }

}
