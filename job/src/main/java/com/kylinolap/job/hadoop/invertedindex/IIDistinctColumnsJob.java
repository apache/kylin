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

import com.kylinolap.common.KylinConfig;
import com.kylinolap.job.constant.BatchConstants;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.ColumnDesc;
import com.kylinolap.metadata.model.TableDesc;

/**
 * @author yangli9
 */
public class IIDistinctColumnsJob extends AbstractHadoopJob {
    protected static final Logger log = LoggerFactory.getLogger(IIDistinctColumnsJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_TABLE_NAME);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_INPUT_FORMAT);
            options.addOption(OPTION_INPUT_DELIM);
            options.addOption(OPTION_OUTPUT_PATH);
            parseOptions(options, args);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            String tableName = getOptionValue(OPTION_TABLE_NAME).toUpperCase();
            Path input = new Path(getOptionValue(OPTION_INPUT_PATH));
            String inputFormat = getOptionValue(OPTION_INPUT_FORMAT);
            String inputDelim = getOptionValue(OPTION_INPUT_DELIM);
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));

            // ----------------------------------------------------------------------------

            System.out.println("Starting: " + job.getJobName());

            setupMapInput(input, inputFormat, inputDelim);
            setupReduceOutput(output);

            // pass table and columns
            MetadataManager metaMgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
            TableDesc table = metaMgr.getTableDesc(tableName);
            job.getConfiguration().set(BatchConstants.TABLE_NAME, tableName);
            job.getConfiguration().set(BatchConstants.TABLE_COLUMNS, getColumns(table));

            return waitForCompletion(job);

        } catch (Exception e) {
            printUsage(options);
            log.error(e.getLocalizedMessage(), e);
            return 2;
        }

    }

    private String getColumns(TableDesc table) {
        StringBuilder buf = new StringBuilder();
        for (ColumnDesc col : table.getColumns()) {
            if (buf.length() > 0)
                buf.append(",");
            buf.append(col.getName());
        }
        return buf.toString();
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

        job.setMapperClass(IIDistinctColumnsMapper.class);
        job.setCombinerClass(IIDistinctColumnsCombiner.class);
        job.setMapOutputKeyClass(ShortWritable.class);
        job.setMapOutputValueClass(Text.class);
    }

    private void setupReduceOutput(Path output) throws IOException {
        job.setReducerClass(IIDistinctColumnsReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, output);
        job.getConfiguration().set(BatchConstants.OUTPUT_PATH, output.toString());

        job.setNumReduceTasks(1);

        deletePath(job.getConfiguration(), output);
    }

    public static void main(String[] args) throws Exception {
        IIDistinctColumnsJob job = new IIDistinctColumnsJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
