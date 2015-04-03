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

package org.apache.kylin.query.routing.RoutingRules;

import org.apache.kylin.cube.CubeCapabilityChecker;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RoutingRule;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public class AdjustForWeaklyMatchedRealization extends RoutingRule {
    private static final Logger logger = LoggerFactory.getLogger(AdjustForWeaklyMatchedRealization.class);

    @Override
    public void apply(List<IRealization> realizations, OLAPContext olapContext) {
        if (realizations.size() > 0) {
            IRealization first = realizations.get(0);

            if (first instanceof HybridInstance) {
                HybridInstance hybrid = (HybridInstance) first;

                if (hybrid.getHistoryRealizationInstance() instanceof CubeInstance)
                    first = hybrid.getHistoryRealizationInstance();
            }

            if (first instanceof CubeInstance) {
                CubeInstance cube = (CubeInstance) first;
                adjustOLAPContextIfNecessary(cube, olapContext);
            }

            if (first instanceof IIInstance) {
                IIInstance ii = (IIInstance) first;
                adjustOLAPContextIfNecessary(ii, olapContext);
            }
        }
    }

    private static void adjustOLAPContextIfNecessary(IIInstance ii, OLAPContext olapContext) {
        IIDesc iiDesc = ii.getDescriptor();
        Collection<FunctionDesc> iiFuncs = iiDesc.listAllFunctions();
        convertAggregationToDimension(olapContext, iiFuncs, iiDesc.getFactTableName());
    }

    private static void adjustOLAPContextIfNecessary(CubeInstance cube, OLAPContext olapContext) {
        if (CubeCapabilityChecker.check(cube, olapContext.getSQLDigest(), false))
            return;

        CubeDesc cubeDesc = cube.getDescriptor();
        Collection<FunctionDesc> cubeFuncs = cubeDesc.listAllFunctions();
        convertAggregationToDimension(olapContext, cubeFuncs, cubeDesc.getFactTable());
    }

    private static void convertAggregationToDimension(OLAPContext olapContext, Collection<FunctionDesc> availableAggregations, String factTableName) {
        Iterator<FunctionDesc> it = olapContext.aggregations.iterator();
        while (it.hasNext()) {
            FunctionDesc functionDesc = it.next();
            if (!availableAggregations.contains(functionDesc)) {
                // try to convert the metric to dimension to see if it works
                TblColRef col = functionDesc.selectTblColRef(olapContext.metricsColumns, factTableName);
                functionDesc.setDimensionAsMetric(true);
                olapContext.rewriteFields.remove(functionDesc.getRewriteFieldName());
                if (col != null) {
                    olapContext.metricsColumns.remove(col);
                    olapContext.groupByColumns.add(col);
                }
                logger.info("Adjust OLAPContext for " + functionDesc);
            }
        }
    }

}
