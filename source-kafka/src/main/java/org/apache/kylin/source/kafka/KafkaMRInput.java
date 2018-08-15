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
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class KafkaMRInput extends KafkaInputBase implements IMRInput {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMRInput.class);
    private CubeSegment cubeSegment;

    @Override
    public IMRBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
        this.cubeSegment = (CubeSegment) flatDesc.getSegment();
        return new BatchCubingInputSide(cubeSegment, flatDesc);
    }

    @Override
    public IMRTableInputFormat getTableInputFormat(TableDesc table) {

        return new KafkaTableInputFormat(cubeSegment, null);
    }

    @Override
    public IMRBatchMergeInputSide getBatchMergeInputSide(ISegment seg) {
        return new KafkaMRBatchMergeInputSide((CubeSegment) seg);
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
            String[] columns = Bytes.toString(text.getBytes(), 0, text.getLength()).split(delimiter);
            return Collections.singletonList(columns);
        }

        @Override
        public String getInputSplitSignature(InputSplit inputSplit) {
            FileSplit baseSplit = (FileSplit) inputSplit;
            return baseSplit.getPath().getName() + "_" + baseSplit.getStart() + "_" + baseSplit.getLength();
        }
    }

    public static class BatchCubingInputSide implements IMRBatchCubingInputSide {

        final JobEngineConfig conf;
        final CubeSegment seg;
        private CubeDesc cubeDesc;
        private KylinConfig config;
        protected IJoinedFlatTableDesc flatDesc;
        protected String hiveTableDatabase;
        private List<String> intermediateTables = Lists.newArrayList();
        private List<String> intermediatePaths = Lists.newArrayList();
        private String cubeName;

        public BatchCubingInputSide(CubeSegment seg, IJoinedFlatTableDesc flatDesc) {
            this.conf = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
            this.config = seg.getConfig();
            this.flatDesc = flatDesc;
            this.hiveTableDatabase = config.getHiveDatabaseForIntermediateTable();
            this.seg = seg;
            this.cubeDesc = seg.getCubeDesc();
            this.cubeName = seg.getCubeInstance().getName();
        }

        @Override
        public void addStepPhase1_CreateFlatTable(DefaultChainedExecutable jobFlow) {

            boolean onlyOneTable = cubeDesc.getModel().getLookupTables().size() == 0;
            final String baseLocation = getJobWorkingDir(jobFlow);
            if (onlyOneTable) {
                // directly use flat table location
                final String intermediateFactTable = flatDesc.getTableName();
                final String tableLocation = baseLocation + "/" + intermediateFactTable;
                jobFlow.addTask(createSaveKafkaDataStep(jobFlow.getId(), tableLocation, seg));
                intermediatePaths.add(tableLocation);
            } else {
                final String mockFactTableName = MetadataConstants.KYLIN_INTERMEDIATE_PREFIX + cubeName.toLowerCase()
                        + "_" + seg.getUuid().replaceAll("-", "_") + "_fact";
                jobFlow.addTask(createSaveKafkaDataStep(jobFlow.getId(), baseLocation + "/" + mockFactTableName, seg));
                jobFlow.addTask(createFlatTable(hiveTableDatabase, mockFactTableName, baseLocation, cubeName, cubeDesc, flatDesc, intermediateTables, intermediatePaths));
            }
        }

        protected String getJobWorkingDir(DefaultChainedExecutable jobFlow) {
            return JobBuilderSupport.getJobWorkingDir(config.getHdfsWorkingDirectory(), jobFlow.getId());
        }

        @Override
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
            jobFlow.addTask(createGCStep(intermediateTables, intermediatePaths));

        }

        @Override
        public IMRTableInputFormat getFlatTableInputFormat() {
            return new KafkaTableInputFormat(seg, conf);
        }
    }

    class KafkaMRBatchMergeInputSide implements IMRBatchMergeInputSide {

        private CubeSegment cubeSegment;

        KafkaMRBatchMergeInputSide(CubeSegment cubeSegment) {
            this.cubeSegment = cubeSegment;
        }

        @Override
        public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
            jobFlow.addTask(createMergeOffsetStep(jobFlow.getId(), cubeSegment));
        }
    }

}
