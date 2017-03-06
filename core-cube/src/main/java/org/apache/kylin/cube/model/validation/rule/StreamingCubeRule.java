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

package org.apache.kylin.cube.model.validation.rule;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.cube.model.validation.IValidatorRule;
import org.apache.kylin.cube.model.validation.ResultLevel;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.ISourceAware;

import org.apache.kylin.metadata.model.TblColRef;

/**
 *
 */
public class StreamingCubeRule implements IValidatorRule<CubeDesc> {

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.kylin.metadata.validation.IValidatorRule#validate(java.lang.Object
     * , org.apache.kylin.metadata.validation.ValidateContext)
     */
    @Override
    public void validate(CubeDesc cube, ValidateContext context) {
        DataModelDesc model = cube.getModel();
        
        if (model.getRootFactTable().getTableDesc().getSourceType() != ISourceAware.ID_STREAMING) {
            return;
        }

        if (model.getLookupTables().size() > 0) {
            context.addResult(ResultLevel.ERROR, "Streaming Cube doesn't support star-schema so far; only one fact table is allowed.");
            return;
        }

        if (cube.getEngineType() == IEngineAware.ID_SPARK) {
            context.addResult(ResultLevel.ERROR, "Spark engine doesn't support streaming source, select MapReduce engine instead.");
            return;
        }

        if (model.getPartitionDesc() == null || model.getPartitionDesc().getPartitionDateColumn() == null) {
            context.addResult(ResultLevel.ERROR, "Must define a partition column.");
            return;
        }

        final TblColRef partitionCol = model.getPartitionDesc().getPartitionDateColumnRef();
        boolean found = false;
        for (DimensionDesc dimensionDesc : cube.getDimensions()) {
            for (TblColRef dimCol : dimensionDesc.getColumnRefs()) {
                if (dimCol.equals(partitionCol)) {
                    found = true;
                    break;
                }
            }
        }

        if (found == false) {
            context.addResult(ResultLevel.ERROR, "Partition column '" + partitionCol + "' isn't in dimension list.");
            return;
        }

    }

}
