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

import java.io.IOException;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.invertedindex.model.IIJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.IntermediateColumnDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
            options.addOption(OPTION_II_NAME);
            options.addOption(OPTION_OUTPUT_PATH);
            parseOptions(options, args);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            String tableName = getOptionValue(OPTION_TABLE_NAME).toUpperCase();
            String iiName = getOptionValue(OPTION_II_NAME);
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));

            // ----------------------------------------------------------------------------

            log.info("Starting: " + job.getJobName() + " on table " + tableName);

            IIManager iiMgr = IIManager.getInstance(KylinConfig.getInstanceFromEnv());
            IIInstance ii = iiMgr.getII(iiName);
            job.getConfiguration().set(BatchConstants.TABLE_NAME, tableName);
            job.getConfiguration().set(BatchConstants.TABLE_COLUMNS, getColumns(ii));

            setJobClasspath(job);
            
            setupMapper();
            setupReducer(output);

            return waitForCompletion(job);

        } catch (Exception e) {
            printUsage(options);
            throw e;
        }

    }

    private String getColumns(IIInstance ii) {
        IIJoinedFlatTableDesc iiflat = new IIJoinedFlatTableDesc(ii.getDescriptor());
        StringBuilder buf = new StringBuilder();
        for (IntermediateColumnDesc col : iiflat.getColumnList()) {
            if (buf.length() > 0)
                buf.append(",");
            buf.append(col.getColumnName());
        }
        return buf.toString();
    }

    private void setupMapper() throws IOException {

        String tableName = job.getConfiguration().get(BatchConstants.TABLE_NAME);
        String[] dbTableNames = HadoopUtil.parseHiveTableName(tableName);

        log.info("setting hcat input format, db name {} , table name {}", dbTableNames[0],dbTableNames[1]);

        HCatInputFormat.setInput(job, dbTableNames[0], dbTableNames[1]);

        job.setInputFormatClass(HCatInputFormat.class);

        job.setMapperClass(IIDistinctColumnsMapper.class);
        job.setCombinerClass(IIDistinctColumnsCombiner.class);
        job.setMapOutputKeyClass(ShortWritable.class);
        job.setMapOutputValueClass(Text.class);
    }

    private void setupReducer(Path output) throws IOException {
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
