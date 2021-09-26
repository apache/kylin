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
package org.apache.kylin.source.hudi;

import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.hive.HiveMRInput;

public class HudiMRInput extends HudiInputBase implements IMRInput {
    @Override
    public IMRTableInputFormat getTableInputFormat(TableDesc table, String uuid) {
        return new HiveMRInput.HiveTableInputFormat(getTableNameForHCat(table,uuid));
    }

    @Override
    public IBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
        return new HudiInputBase.HudiBaseBatchCubingInputSide(flatDesc);
    }

    @Override
    public IBatchMergeInputSide getBatchMergeInputSide(ISegment seg) {
        return new IMRBatchMergeInputSide() {
            @Override
            public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
                // doing nothing
            }
        };
    }

    public static  class HudiMRBatchCubingInputSide extends HudiBaseBatchCubingInputSide implements IMRBatchCubingInputSide{
        public HudiMRBatchCubingInputSide(IJoinedFlatTableDesc flatTableDesc){super(flatTableDesc);}

        @Override
        public IMRTableInputFormat getFlatTableInputFormat(){
            return new HiveMRInput.HiveTableInputFormat(getIntermediateTableIdentity());
        }
    }

}
