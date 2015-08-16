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

import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yangli9
 * 
 */
public class IICreateHFileJob extends AbstractHadoopJob {

    protected static final Logger log = LoggerFactory.getLogger(IICreateHFileJob.class);

    public int run(String[] args) throws Exception {
        Options options = new Options();
        Connection connection = null;
        Table table = null;

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_II_NAME);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_HTABLE_NAME);
            parseOptions(options, args);

            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));

            setJobClasspath(job);

            addInputDirs(getOptionValue(OPTION_INPUT_PATH), job);
            FileOutputFormat.setOutputPath(job, output);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapperClass(IICreateHFileMapper.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(KeyValue.class);

            String tableName = getOptionValue(OPTION_HTABLE_NAME);

            connection = HBaseConnection.get();
            table = connection.getTable(TableName.valueOf(tableName));
            RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(tableName));
            HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

            this.deletePath(job.getConfiguration(), output);

            return waitForCompletion(job);
        } catch (Exception e) {
            printUsage(options);
            throw e;
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    public static void main(String[] args) throws Exception {
        IICreateHFileJob job = new IICreateHFileJob();
        job.setConf(HadoopUtil.getCurrentHBaseConfiguration());
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }

}
