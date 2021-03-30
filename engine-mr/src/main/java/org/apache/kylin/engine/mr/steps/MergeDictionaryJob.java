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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.Segments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class MergeDictionaryJob extends AbstractHadoopJob {
    private static final Logger logger = LoggerFactory.getLogger(MergeDictionaryJob.class);

    public static final Option OPTION_MERGE_SEGMENT_IDS = OptionBuilder.withArgName("segmentIds").hasArg()
            .isRequired(true).withDescription("Merging Cube Segment Ids").create("segmentIds");
    public static final Option OPTION_OUTPUT_PATH_DICT = OptionBuilder.withArgName("dictOutputPath").hasArg()
            .isRequired(true).withDescription("merged dictionary resource path").create("dictOutputPath");
    public static final Option OPTION_OUTPUT_PATH_STAT = OptionBuilder.withArgName("statOutputPath").hasArg()
            .isRequired(true).withDescription("merged statistics resource path").create("statOutputPath");

    @Override
    public int run(String[] args) throws Exception {
        try {
            Options options = new Options();
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_SEGMENT_ID);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_META_URL);
            options.addOption(OPTION_MERGE_SEGMENT_IDS);
            options.addOption(OPTION_OUTPUT_PATH_DICT);
            options.addOption(OPTION_OUTPUT_PATH_STAT);
            parseOptions(options, args);

            final String segmentId = getOptionValue(OPTION_SEGMENT_ID);
            final String segmentIds = getOptionValue(OPTION_MERGE_SEGMENT_IDS);
            final String cubeName = getOptionValue(OPTION_CUBE_NAME);
            final String metaUrl = getOptionValue(OPTION_META_URL);
            final String dictOutputPath = getOptionValue(OPTION_OUTPUT_PATH_DICT);
            final String statOutputPath = getOptionValue(OPTION_OUTPUT_PATH_STAT);

            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);
            CubeDesc cubeDesc = cube.getDescriptor();
            CubeSegment segment = cube.getSegmentById(segmentId);
            Segments<CubeSegment> mergingSeg = cube.getMergingSegments(segment);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            job.getConfiguration().set(BatchConstants.ARG_CUBE_NAME, cubeName);
            job.getConfiguration().set(OPTION_META_URL.getOpt(), metaUrl);
            job.getConfiguration().set(OPTION_SEGMENT_ID.getOpt(), segmentId);
            job.getConfiguration().set(OPTION_MERGE_SEGMENT_IDS.getOpt(), segmentIds);
            job.getConfiguration().set(OPTION_OUTPUT_PATH_STAT.getOpt(), statOutputPath);
            job.getConfiguration().set("num.map.tasks", String.valueOf(cubeDesc.getAllColumnsNeedDictionaryBuilt().size() + 1));
            job.setNumReduceTasks(1);

            setJobClasspath(job, cube.getConfig());

            // dump metadata to HDFS
            attachSegmentsMetadataWithDict(mergingSeg, metaUrl);

            // clean output dir
            HadoopUtil.deletePath(job.getConfiguration(), new Path(dictOutputPath));

            job.setMapperClass(MergeDictionaryMapper.class);
            job.setReducerClass(MergeDictionaryReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(IndexArrInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.NONE);
            SequenceFileOutputFormat.setOutputPath(job, new Path(dictOutputPath));

            logger.info("Starting: " + job.getJobName());

            return waitForCompletion(job);

        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }

    static class IndexArrInputFormat extends InputFormat<IntWritable, NullWritable> {

        @Override
        public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
            int numMapTasks = jobContext.getConfiguration().getInt("num.map.tasks", 0);
            List<InputSplit> inputSplits = Lists.newArrayListWithCapacity(numMapTasks);

            for (int i = 0; i < numMapTasks; i++) {
                inputSplits.add(new IntInputSplit(i));
            }

            return inputSplits;
        }

        @Override
        public RecordReader<IntWritable, NullWritable> createRecordReader(InputSplit inputSplit,
                TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            return new RecordReader<IntWritable, NullWritable>() {
                private int index;
                private IntWritable key;
                private NullWritable value;

                @Override
                public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
                        throws IOException, InterruptedException {
                    IntInputSplit intInputSplit = (IntInputSplit) inputSplit;
                    index = intInputSplit.getIndex();
                }

                @Override
                public boolean nextKeyValue() throws IOException, InterruptedException {

                    if (key == null) {
                        key = new IntWritable(index);
                        value = NullWritable.get();
                        return true;
                    }

                    return false;
                }

                @Override
                public IntWritable getCurrentKey() throws IOException, InterruptedException {
                    return key;
                }

                @Override
                public NullWritable getCurrentValue() throws IOException, InterruptedException {
                    return value;
                }

                @Override
                public float getProgress() throws IOException, InterruptedException {
                    return 1;
                }

                @Override
                public void close() throws IOException {

                }
            };
        }
    }

    static class IntInputSplit extends InputSplit implements Writable {
        private int index;

        public IntInputSplit() {

        }

        public IntInputSplit(int index) {
            this.index = index;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(index);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.index = dataInput.readInt();
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 1L;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }

        public int getIndex() {
            return index;
        }
    }
}
