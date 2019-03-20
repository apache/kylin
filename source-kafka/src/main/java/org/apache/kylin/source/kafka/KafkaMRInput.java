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
package org.apache.kylin.source.kafka;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.TableDesc;

public class KafkaMRInput extends KafkaInputBase implements IMRInput {

    private CubeSegment cubeSegment;

    @Override
    public IBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
        this.cubeSegment = (CubeSegment) flatDesc.getSegment();
        return new KafkaMRBatchCubingInputSide(cubeSegment, flatDesc);
    }

    @Override
    public IBatchMergeInputSide getBatchMergeInputSide(ISegment seg) {
        return new KafkaMRBatchMergeInputSide((CubeSegment)seg);
    }

    @Override
    public IMRTableInputFormat getTableInputFormat(TableDesc table, String uuid) {
        return new KafkaTableInputFormat(cubeSegment, null);
    }

    public static class KafkaTableInputFormat implements IMRTableInputFormat {
        private final CubeSegment cubeSegment;
        private final JobEngineConfig conf;
        private String delimiter = BatchConstants.SEQUENCE_FILE_DEFAULT_DELIMITER;

        public KafkaTableInputFormat(CubeSegment cubeSegment, JobEngineConfig conf) {
            this.cubeSegment = cubeSegment;
            this.conf = conf;
        }

        @Override
        public void configureJob(Job job) {
            job.setInputFormatClass(SequenceFileInputFormat.class);
            String jobId = job.getConfiguration().get(BatchConstants.ARG_CUBING_JOB_ID);
            IJoinedFlatTableDesc flatHiveTableDesc = new CubeJoinedFlatTableDesc(cubeSegment);
            String inputPath = JoinedFlatTable.getTableDir(flatHiveTableDesc,
                    JobBuilderSupport.getJobWorkingDir(conf, jobId));
            try {
                FileInputFormat.addInputPath(job, new Path(inputPath));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public Collection<String[]> parseMapperInput(Object mapperInput) {
            Text text = (Text) mapperInput;
            String[] columns = Bytes.toString(text.getBytes(), 0, text.getLength()).split(delimiter, -1);
            return Collections.singletonList(columns);
        }

        @Override
        public String getInputSplitSignature(InputSplit inputSplit) {
            FileSplit baseSplit = (FileSplit) inputSplit;
            return baseSplit.getPath().getName() + "_" + baseSplit.getStart() + "_" + baseSplit.getLength();
        }
    }

    public static class KafkaMRBatchCubingInputSide extends BaseBatchCubingInputSide implements IMRBatchCubingInputSide {

        public KafkaMRBatchCubingInputSide(CubeSegment seg, IJoinedFlatTableDesc flatDesc) {
            super(seg, flatDesc);
        }

        @Override
        public IMRTableInputFormat getFlatTableInputFormat() {
            return new KafkaTableInputFormat(seg, conf);
        }
    }

    public static class KafkaMRBatchMergeInputSide extends BaseBatchMergeInputSide implements IMRBatchMergeInputSide {

        KafkaMRBatchMergeInputSide(CubeSegment cubeSegment) {
            super(cubeSegment);
        }
    }
}
