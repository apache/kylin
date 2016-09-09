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

package org.apache.kylin.cube;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawQueryLastHacker {

    private static final Logger logger = LoggerFactory.getLogger(RawQueryLastHacker.class);

    public static void hackNoAggregations(SQLDigest sqlDigest, CubeDesc cubeDesc) {
        if (!sqlDigest.isRawQuery) {
            return;
        }

        // If no group by and metric found, then it's simple query like select ... from ... where ...,
        // But we have no raw data stored, in order to return better results, we hack to output sum of metric column
        logger.info("No group by and aggregation found in this query, will hack some result for better look of output...");

        // If it's select * from ...,
        // We need to retrieve cube to manually add columns into sqlDigest, so that we have full-columns results as output.
        boolean isSelectAll = sqlDigest.allColumns.isEmpty() || sqlDigest.allColumns.equals(sqlDigest.filterColumns);
        for (TblColRef col : cubeDesc.listAllColumns()) {
            if (cubeDesc.listDimensionColumnsIncludingDerived().contains(col) || isSelectAll) {
                if (col.getTable().equals(sqlDigest.factTable))
                    sqlDigest.allColumns.add(col);
            }
        }

        for (TblColRef col : sqlDigest.allColumns) {
            if (cubeDesc.listDimensionColumnsExcludingDerived(true).contains(col)) {
                // For dimension columns, take them as group by columns.
                sqlDigest.groupbyColumns.add(col);
            } else {
                // For measure columns, take them as metric columns with aggregation function SUM().
                ParameterDesc colParameter = new ParameterDesc();
                colParameter.setType("column");
                colParameter.setValue(col.getName());
                FunctionDesc sumFunc = new FunctionDesc();
                sumFunc.setExpression("SUM");
                sumFunc.setParameter(colParameter);

                boolean measureHasSum = false;
                for (MeasureDesc colMeasureDesc : cubeDesc.getMeasures()) {
                    if (colMeasureDesc.getFunction().equals(sumFunc)) {
                        measureHasSum = true;
                        break;
                    }
                }
                if (measureHasSum) {
                    sqlDigest.aggregations.add(sumFunc);
                } else {
                    logger.warn("SUM is not defined for measure column " + col + ", output will be meaningless.");
                }

                sqlDigest.metricColumns.add(col);
            }
        }
    }
}