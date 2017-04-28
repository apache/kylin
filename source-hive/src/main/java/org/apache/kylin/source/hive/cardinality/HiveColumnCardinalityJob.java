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

package org.apache.kylin.source.hive.cardinality;

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
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This hadoop job will scan all rows of the hive table and then calculate the cardinality on each column.
 * @author shaoshi
 *
 */
public class HiveColumnCardinalityJob extends AbstractHadoopJob {
    private static final Logger logger = LoggerFactory.getLogger(HiveColumnCardinalityJob.class);
    public static final String JOB_TITLE = "Kylin Hive Column Cardinality Job";

    @SuppressWarnings("static-access")
    protected static final Option OPTION_TABLE = OptionBuilder.withArgName("table name").hasArg().isRequired(true).withDescription("The hive table name").create("table");

    public HiveColumnCardinalityJob() {
    }

    @Override
    public int run(String[] args) throws Exception {

        Options options = new Options();

        options.addOption(OPTION_TABLE);
        options.addOption(OPTION_OUTPUT_PATH);

        parseOptions(options, args);

        // start job
        String jobName = JOB_TITLE + getOptionsAsString();
        logger.info("Starting: " + jobName);
        Configuration conf = getConf();

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        JobEngineConfig jobEngineConfig = new JobEngineConfig(kylinConfig);
        conf.addResource(new Path(jobEngineConfig.getHadoopJobConfFilePath(null)));

        job = Job.getInstance(conf, jobName);

        setJobClasspath(job, kylinConfig);

        String table = getOptionValue(OPTION_TABLE);
        job.getConfiguration().set(BatchConstants.CFG_TABLE_NAME, table);

        Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
        FileOutputFormat.setOutputPath(job, output);
        job.getConfiguration().set("dfs.block.size", "67108864");
        job.getConfiguration().set("mapreduce.output.fileoutputformat.compress", "false");

        // Mapper
        IMRTableInputFormat tableInputFormat = MRUtil.getTableInputFormat(table, true);
        tableInputFormat.configureJob(job);

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

        logger.info("Going to submit HiveColumnCardinalityJob for table '" + table + "'");

        TableDesc tableDesc = MetadataManager.getInstance(kylinConfig).getTableDesc(table);
        attachTableMetadata(tableDesc, job.getConfiguration());
        int result = waitForCompletion(job);

        return result;
    }

}
