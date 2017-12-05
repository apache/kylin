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

package org.apache.kylin.storage.druid.write;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;

import java.util.Map;

public class ConvertToDruidJob extends AbstractHadoopJob {
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OPTION_JOB_NAME);
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_OUTPUT_PATH);
        parseOptions(options, args);

        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubeInstance cube = cubeMgr.getCube(getOptionValue(OPTION_CUBE_NAME));
        CubeSegment segment = cube.getSegmentById(getOptionValue(OPTION_SEGMENT_ID));

        job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
        setJobClasspath(job, cube.getConfig());

        final Configuration jobConf = job.getConfiguration();
        jobConf.set(BatchConstants.CFG_CUBE_NAME, cube.getName());
        jobConf.set(BatchConstants.CFG_CUBE_SEGMENT_ID, segment.getUuid());
        jobConf.set(BatchConstants.CFG_OUTPUT_PATH, getOptionValue(OPTION_OUTPUT_PATH));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        addInputDirs(getOptionValue(OPTION_INPUT_PATH), job);

        job.setPartitionerClass(ShardPartitioner.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(ConvertToDruidRedcuer.class);
        job.setNumReduceTasks(segment.getTotalShards());

        final Path outputPath = new Path(getOptionValue(OPTION_OUTPUT_PATH));
        this.deletePath(jobConf, outputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        //allow user specially set config for Druid step
        for (Map.Entry<String, String> entry : cube.getConfig().getDruidMRConfigOverride().entrySet()) {
            job.getConfiguration().set(entry.getKey(), entry.getValue());
        }

        try {
            // upload segment metadata to distributed cache
            attachSegmentMetadataWithDict(segment, jobConf);
            return waitForCompletion(job);
        } finally {
            this.cleanupTempConfFile(jobConf);
        }
    }

    public static class ShardPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            short shard = BytesUtil.readShort(key.getBytes(), 0, RowConstants.ROWKEY_SHARDID_LEN);
            Preconditions.checkState(shard >= 0 && shard < numPartitions, "Invalid shard id %s given %s total shards", shard, numPartitions);
            return shard;
        }
    }
}
