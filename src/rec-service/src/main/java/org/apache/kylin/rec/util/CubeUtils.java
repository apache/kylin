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

package org.apache.kylin.rec.util;

import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.rec.common.SmartConfig;

public class CubeUtils {

    private CubeUtils() {
    }

    public static FunctionDesc newFunctionDesc(NDataModel modelDesc, String expression, List<ParameterDesc> params,
            String colDataType) {
        String returnType = FunctionDesc.proposeReturnType(expression, colDataType,
                Collections.singletonMap(FunctionDesc.FUNC_COUNT_DISTINCT,
                        SmartConfig.wrap(KylinConfig.getInstanceFromEnv()).getMeasureCountDistinctType()));

        if (FunctionDesc.FUNC_COUNT_DISTINCT.equalsIgnoreCase(expression) && CollectionUtils.isNotEmpty(params)
                && params.size() > 1) {
            returnType = FunctionDesc.FUNC_COUNT_DISTINCT_HLLC10;
        }
        FunctionDesc ret = FunctionDesc.newInstance(expression, params, returnType);
        ret.init(modelDesc);
        return ret;
    }

    public static FunctionDesc newCountStarFuncDesc(NDataModel modelDesc) {
        return newFunctionDesc(modelDesc, FunctionDesc.FUNC_COUNT, Lists.newArrayList(ParameterDesc.newInstance("1")),
                DataType.BIGINT);
    }

    public static NDataModel.Measure newMeasure(FunctionDesc func, String name, int id) {
        NDataModel.Measure measure = new NDataModel.Measure();
        measure.setName(name);
        measure.setFunction(func);
        measure.setId(id);
        return measure;
    }

    public static boolean isValidMeasure(FunctionDesc functionDesc) {
        List<TblColRef> colRefs = functionDesc.getColRefs();
        if (colRefs == null || colRefs.isEmpty())
            return true;

        for (TblColRef colRef : colRefs) {
            if (!colRef.isQualified()) {
                return false;
            }
        }

        return !functionDesc.isGrouping();
    }

}
