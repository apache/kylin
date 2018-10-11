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
package org.apache.kylin.storage.parquet.steps;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IEngineAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by Yichen on 10/16/18.
 */
public class ParquetMROutput implements IMROutput2 {

    private static final Logger logger = LoggerFactory.getLogger(ParquetMROutput.class);

    @Override
    public IMRBatchCubingOutputSide2 getBatchCubingOutputSide(CubeSegment seg) {

        boolean useSpark = seg.getCubeDesc().getEngineType() == IEngineAware.ID_SPARK;


        // TODO need refactor
        if (!useSpark){
            throw new RuntimeException("Cannot adapt to MR engine");
        }
        final ParquetJobSteps steps = new ParquetSparkSteps(seg);

        return new IMRBatchCubingOutputSide2() {

            @Override
            public void addStepPhase2_BuildDictionary(DefaultChainedExecutable jobFlow) {
            }

            @Override
            public void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow) {
            }

            @Override
            public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
            }

            @Override
            public IMROutputFormat getOuputFormat() {
                return new ParquetMROutputFormat();
            }
        };

    }

    public static class ParquetMROutputFormat implements IMROutputFormat {

        @Override
        public void configureJobInput(Job job, String input) throws Exception {
            job.setInputFormatClass(SequenceFileInputFormat.class);
        }

        @Override
        public void configureJobOutput(Job job, String output, CubeSegment segment, CuboidScheduler cuboidScheduler,
                                       int level) throws Exception {

            Path outputPath = new Path(output);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            HadoopUtil.deletePath(job.getConfiguration(), outputPath);
        }
    }

    @Override
    public IMRBatchMergeOutputSide2 getBatchMergeOutputSide(CubeSegment seg) {
        return null;
    }

    @Override
    public IMRBatchOptimizeOutputSide2 getBatchOptimizeOutputSide(CubeSegment seg) {
        return null;
    }
}
