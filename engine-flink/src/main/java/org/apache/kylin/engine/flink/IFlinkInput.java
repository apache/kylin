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
package org.apache.kylin.engine.flink;

import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;

/**
 * Flink engine (cubing & merge) input side interface.
 */
public interface IFlinkInput {

    /** Return a helper to participate in batch cubing job flow. */
    IFlinkBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc);

    /** Return a helper to participate in batch cubing merge job flow. */
    IFlinkBatchMergeInputSide getBatchMergeInputSide(ISegment seg);

    /**
     * Participate the batch cubing flow as the input side. Responsible for creating
     * intermediate flat table (Phase 1) and clean up any leftover (Phase 4).
     *
     * - Phase 1: Create Flat Table
     * - Phase 2: Build Dictionary (with FlatTableInputFormat)
     * - Phase 3: Build Cube (with FlatTableInputFormat)
     * - Phase 4: Update Metadata & Cleanup
     */
    interface IFlinkBatchCubingInputSide {

        /** Add step that creates an intermediate flat table as defined by CubeJoinedFlatTableDesc */
        void addStepPhase1_CreateFlatTable(DefaultChainedExecutable jobFlow);

        /** Add step that does necessary clean up, like delete the intermediate flat table */
        void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow);
    }

    interface IFlinkBatchMergeInputSide {

        /** Add step that executes before merge dictionary and before merge cube. */
        void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow);

    }

}
