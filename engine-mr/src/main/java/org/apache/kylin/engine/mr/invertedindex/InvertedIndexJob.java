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

package org.apache.kylin.engine.mr.invertedindex;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class InvertedIndexJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(InvertedIndexJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_II_NAME);
            options.addOption(OPTION_OUTPUT_PATH);
            parseOptions(options, args);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            String iiname = getOptionValue(OPTION_II_NAME);
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));

            // ----------------------------------------------------------------------------

            System.out.println("Starting: " + job.getJobName());

            IIInstance ii = getII(iiname);
            short sharding = ii.getDescriptor().getSharding();

            setJobClasspath(job);

            setupMapper(ii.getFirstSegment());
            setupReducer(output, sharding);
            attachMetadata(ii);

            return waitForCompletion(job);

        } catch (Exception e) {
            printUsage(options);
            throw e;
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }

    }

    private IIInstance getII(String iiName) {
        IIManager mgr = IIManager.getInstance(KylinConfig.getInstanceFromEnv());
        IIInstance ii = mgr.getII(iiName);
        if (ii == null)
            throw new IllegalArgumentException("No Inverted Index found by name " + iiName);
        return ii;
    }

    private void attachMetadata(IIInstance ii) throws IOException {

        Configuration conf = job.getConfiguration();
        attachKylinPropsAndMetadata(ii, conf);

        IISegment seg = ii.getFirstSegment();
        conf.set(BatchConstants.CFG_II_NAME, ii.getName());
        conf.set(BatchConstants.CFG_II_SEGMENT_NAME, seg.getName());
    }

   
    private void setupMapper(IISegment segment) throws IOException {

        IMRInput.IMRTableInputFormat flatTableInputFormat = MRUtil.getBatchCubingInputSide(segment).getFlatTableInputFormat();
        flatTableInputFormat.configureJob(job);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setPartitionerClass(InvertedIndexPartitioner.class);
    }

    private void setupReducer(Path output, short sharding) throws IOException {
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
