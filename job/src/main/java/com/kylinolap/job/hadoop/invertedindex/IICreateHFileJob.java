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

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.HadoopUtil;
import com.kylinolap.job.hadoop.AbstractHadoopJob;

/**
 * @author yangli9
 * 
 */
public class IICreateHFileJob extends AbstractHadoopJob {

    protected static final Logger log = LoggerFactory.getLogger(IICreateHFileJob.class);

    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_HTABLE_NAME);
            parseOptions(options, args);

            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));

            File JarFile = new File(KylinConfig.getInstanceFromEnv().getKylinJobJarPath());
            if (JarFile.exists()) {
                job.setJar(KylinConfig.getInstanceFromEnv().getKylinJobJarPath());
            } else {
                job.setJarByClass(this.getClass());
            }

            addInputDirs(getOptionValue(OPTION_INPUT_PATH), job);
            FileOutputFormat.setOutputPath(job, output);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapperClass(IICreateHFileMapper.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(KeyValue.class);

            String tableName = getOptionValue(OPTION_HTABLE_NAME);
            HTable htable = new HTable(getConf(), tableName);
            HFileOutputFormat.configureIncrementalLoad(job, htable);

            this.deletePath(job.getConfiguration(), output);

            return waitForCompletion(job);
        } catch (Exception e) {
            printUsage(options);
            log.error(e.getLocalizedMessage(), e);
            return 2;
        }
    }

    public static void main(String[] args) throws Exception {
        IICreateHFileJob job = new IICreateHFileJob();
        job.setConf(HadoopUtil.newHBaseConfiguration(KylinConfig.getInstanceFromEnv().getStorageUrl()));
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }

}
