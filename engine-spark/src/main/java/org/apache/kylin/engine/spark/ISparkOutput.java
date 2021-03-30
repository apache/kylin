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

package org.apache.kylin.engine.spark;

import java.util.List;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

public interface ISparkOutput {

    /** Return a helper to participate in batch cubing job flow. */
    public ISparkBatchCubingOutputSide getBatchCubingOutputSide(CubeSegment seg);

    /**
     * Participate the batch cubing flow as the output side. Responsible for saving
     * the cuboid output to storage at the end of Phase 3.
     * 
     * - Phase 1: Create Flat Table
     * - Phase 2: Build Dictionary
     * - Phase 3: Build Cube
     * - Phase 4: Update Metadata & Cleanup
     */
    public interface ISparkBatchCubingOutputSide {

        /** Add step that executes after build dictionary and before build cube. */
        public void addStepPhase2_BuildDictionary(DefaultChainedExecutable jobFlow);

        /**
         * Add step that saves cuboids from HDFS to storage.
         * 
         * The cuboid output is a directory of sequence files, where key is CUBOID+D1+D2+..+Dn, 
         * value is M1+M2+..+Mm. CUBOID is 8 bytes cuboid ID; Dx is dimension value with
         * dictionary encoding; Mx is measure value serialization form.
         */
        void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow);

        /** Add step that does any necessary clean up. */
        void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow);

    }

    /** Return a helper to participate in batch merge job flow. */
    ISparkBatchMergeOutputSide getBatchMergeOutputSide(CubeSegment seg);

    /**
     * Participate the batch cubing flow as the output side. Responsible for saving
     * the cuboid output to storage at the end of Phase 2.
     * 
     * - Phase 1: Merge Dictionary
     * - Phase 2: Merge Cube
     * - Phase 3: Update Metadata & Cleanup
     */
    interface ISparkBatchMergeOutputSide {

        /** Add step that executes after merge dictionary and before merge cube. */
        void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow);

        /**
         * Add step that saves cuboid output from HDFS to storage.
         * 
         * The cuboid output is a directory of sequence files, where key is CUBOID+D1+D2+..+Dn, 
         * value is M1+M2+..+Mm. CUBOID is 8 bytes cuboid ID; Dx is dimension value with
         * dictionary encoding; Mx is measure value serialization form.
         */
        void addStepPhase2_BuildCube(CubeSegment set, List<CubeSegment> mergingSegments, DefaultChainedExecutable jobFlow);

        /** Add step that does any necessary clean up. */
        void addStepPhase3_Cleanup(DefaultChainedExecutable jobFlow);

    }

    interface ISparkMergeOutputFormat {

        /** Configure the InputFormat of given job. */
        void configureJobInput(Job job, String input) throws Exception;

        /** Configure the OutputFormat of given job. */
        void configureJobOutput(Job job, String output, CubeSegment segment) throws Exception;

        CubeSegment findSourceSegment(FileSplit fileSplit, CubeInstance cube);
    }

    ISparkBatchOptimizeOutputSide getBatchOptimizeOutputSide(CubeSegment seg);

    /**
     * Participate the batch cubing flow as the output side. Responsible for saving
     * the cuboid output to storage at the end of Phase 3.
     *
     * - Phase 1: Filter Recommended Cuboid Data
     * - Phase 2: Copy Dictionary & Calculate Statistics & Update Reused Cuboid Shard
     * - Phase 3: Build Cube
     * - Phase 4: Cleanup Optimize
     * - Phase 5: Update Metadata & Cleanup
     */
    interface ISparkBatchOptimizeOutputSide {

        /** Create HTable based on recommended cuboids & statistics*/
        void addStepPhase2_CreateHTable(DefaultChainedExecutable jobFlow);

        /** Build only missing cuboids*/
        void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow);

        /** Cleanup intermediate cuboid data on HDFS*/
        void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow);

        /** Invoked by Checkpoint job & Cleanup old segments' HTables and related working directory*/
        void addStepPhase5_Cleanup(DefaultChainedExecutable jobFlow);
    }
}
