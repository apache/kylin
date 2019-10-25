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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.steps.filter.UHCDictPathFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UHCDictionaryJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(UHCDictionaryJob.class);

    private boolean isSkipped = false;

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_CUBING_JOB_ID);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_INPUT_PATH);
            parseOptions(options, args);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            String job_id = getOptionValue(OPTION_CUBING_JOB_ID);
            String cubeName = getOptionValue(OPTION_CUBE_NAME);
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            Path input = new Path(getOptionValue(OPTION_INPUT_PATH));

            //add metadata to distributed cache
            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);
            attachCubeMetadata(cube, job.getConfiguration());

            List<TblColRef> uhcColumns = cube.getDescriptor().getAllUHCColumns();
            int reducerCount = uhcColumns.size();

            //Note! handle uhc columns is null.
            boolean hasUHCValue = false;
            for (TblColRef tblColRef : uhcColumns) {
                Path path = new Path(input.toString() + "/" + tblColRef.getIdentity());
                if (HadoopUtil.getFileSystem(path).exists(path)) {
                    FileInputFormat.addInputPath(job, path);
                    FileInputFormat.setInputPathFilter(job, UHCDictPathFilter.class);
                    hasUHCValue = true;
                }
            }

            if (!hasUHCValue) {
                isSkipped = true;
                return 0;
            }

            setJobClasspath(job, cube.getConfig());
            setupMapper();
            setupReducer(output, reducerCount);

            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.ARG_CUBING_JOB_ID, job_id);
            job.getConfiguration().set(BatchConstants.CFG_GLOBAL_DICT_BASE_DIR, KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory());
            job.getConfiguration().set(BatchConstants.CFG_MAPRED_OUTPUT_COMPRESS, "false");

            //8G memory is enough for all global dict, because the input is sequential and we handle global dict slice by slice
            job.getConfiguration().set("mapreduce.reduce.memory.mb", "8500");
            job.getConfiguration().set("mapred.reduce.child.java.opts", "-Xmx8g");
            //Copying global dict to working dir in GlobalDictHDFSStore maybe elapsed a long time (Maybe we could improve it)
            //Waiting the global dict lock maybe also take a long time.
            //So we set 8 hours here
            job.getConfiguration().set("mapreduce.task.timeout", "28800000");

            //allow user specially set config for uhc step
            for (Map.Entry<String, String> entry : cube.getConfig().getUHCMRConfigOverride().entrySet()) {
                job.getConfiguration().set(entry.getKey(), entry.getValue());
            }

            return waitForCompletion(job);
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }

    private void setupMapper() throws IOException {
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(UHCDictionaryMapper.class);
        job.setMapOutputKeyClass(SelfDefineSortableKey.class);
        job.setMapOutputValueClass(NullWritable.class);
    }

    private void setupReducer(Path output, int numberOfReducers) throws IOException {
        job.setReducerClass(UHCDictionaryReducer.class);
        job.setPartitionerClass(UHCDictionaryPartitioner.class);
        job.setNumReduceTasks(numberOfReducers);

        MultipleOutputs.addNamedOutput(job, BatchConstants.CFG_OUTPUT_DICT, SequenceFileOutputFormat.class, NullWritable.class, ArrayPrimitiveWritable.class);
        FileOutputFormat.setOutputPath(job, output);
        job.getConfiguration().set(BatchConstants.CFG_OUTPUT_PATH, output.toString());

        //prevent to create zero-sized default output
        LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

        deletePath(job.getConfiguration(), output);
    }

    @Override
    public boolean isSkipped() {
        return isSkipped;
    }

    public static void main(String[] args) throws Exception {
        UHCDictionaryJob job = new UHCDictionaryJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
