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

package org.apache.kylin.source.hive;

import org.apache.kylin.engine.spark.ISparkInput;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveSparkInput extends HiveInputBase implements ISparkInput {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HiveSparkInput.class);

    @Override
    public IBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
        return new SparkBatchCubingInputSide(flatDesc);
    }

    @Override
    public IBatchMergeInputSide getBatchMergeInputSide(ISegment seg) {
        return new ISparkBatchMergeInputSide() {
            @Override
            public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
                // doing nothing
            }
        };
    }

    public static class SparkBatchCubingInputSide extends BaseBatchCubingInputSide implements ISparkBatchCubingInputSide {

        public SparkBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
            super(flatDesc);
        }
    }
}
