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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.filter.UDF.MassInTupleFilter;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.realization.CapabilityResult.CapabilityInfluence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 */
public class CubeCapabilityChecker {
    private static final Logger logger = LoggerFactory.getLogger(CubeCapabilityChecker.class);

    public static CapabilityResult check(CubeInstance cube, SQLDigest digest) {
        CapabilityResult result = new CapabilityResult();
        result.capable = false;

        // match joins
        boolean isJoinMatch = JoinChecker.isJoinMatch(digest.joinDescs, cube);
        if (!isJoinMatch) {
            logger.info("Exclude cube " + cube.getName() + " because unmatched joins");
            return result;
        }

        // dimensions & measures
        Collection<TblColRef> dimensionColumns = getDimensionColumns(digest);
        Collection<FunctionDesc> aggrFunctions = digest.aggregations;
        Collection<TblColRef> unmatchedDimensions = unmatchedDimensions(dimensionColumns, cube);
        Collection<FunctionDesc> unmatchedAggregations = unmatchedAggregations(aggrFunctions, cube);

        // try custom measure types
        // in RAW query, unmatchedDimensions and unmatchedAggregations will null, so can't chose RAW cube well!
        //        if (!unmatchedDimensions.isEmpty() || !unmatchedAggregations.isEmpty()) {
        tryCustomMeasureTypes(unmatchedDimensions, unmatchedAggregations, digest, cube, result);
        //        }

        // try dimension-as-measure
        if (!unmatchedAggregations.isEmpty()) {
            if (cube.getDescriptor().getFactTable().equals(digest.factTable)) {
                tryDimensionAsMeasures(unmatchedAggregations, digest, cube, result, cube.getDescriptor().listDimensionColumnsIncludingDerived());
            } else {
                //deal with query on lookup table, like https://issues.apache.org/jira/browse/KYLIN-2030
                if (cube.getSegments().get(0).getSnapshots().containsKey(digest.factTable)) {
                    TableDesc tableDesc = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getTableDesc(digest.factTable);
                    Set<TblColRef> dimCols = Sets.newHashSet();
                    for (ColumnDesc columnDesc : tableDesc.getColumns()) {
                        dimCols.add(columnDesc.getRef());
                    }
                    tryDimensionAsMeasures(unmatchedAggregations, digest, cube, result, dimCols);
                } else {
                    logger.info("Skip tryDimensionAsMeasures because current cube {} does not touch lookup table {} at all", cube.getName(), digest.factTable);
                }
            }
        }

        if (!unmatchedDimensions.isEmpty()) {
            logger.info("Exclude cube " + cube.getName() + " because unmatched dimensions");
            return result;
        }

        if (!unmatchedAggregations.isEmpty()) {
            logger.info("Exclude cube " + cube.getName() + " because unmatched aggregations");
            return result;
        }

        if (cube.getStorageType() == IStorageAware.ID_HBASE && MassInTupleFilter.containsMassInTupleFilter(digest.filter)) {
            logger.info("Exclude cube " + cube.getName() + " because only v2 storage + v2 query engine supports massin");
            return result;
        }

        if (digest.isRawQuery && cube.getFactTable().equals(digest.factTable)) {
            result.influences.add(new CapabilityInfluence() {
                @Override
                public double suggestCostMultiplier() {
                    return 100;
                }
            });
        }

        // cost will be minded by caller
        result.capable = true;
        return result;
    }

    private static Collection<TblColRef> getDimensionColumns(SQLDigest sqlDigest) {
        Collection<TblColRef> groupByColumns = sqlDigest.groupbyColumns;
        Collection<TblColRef> filterColumns = sqlDigest.filterColumns;

        Collection<TblColRef> dimensionColumns = new HashSet<TblColRef>();
        dimensionColumns.addAll(groupByColumns);
        dimensionColumns.addAll(filterColumns);
        return dimensionColumns;
    }

    private static Set<TblColRef> unmatchedDimensions(Collection<TblColRef> dimensionColumns, CubeInstance cube) {
        HashSet<TblColRef> result = Sets.newHashSet(dimensionColumns);
        CubeDesc cubeDesc = cube.getDescriptor();
        result.removeAll(cubeDesc.listDimensionColumnsIncludingDerived());
        return result;
    }

    private static Set<FunctionDesc> unmatchedAggregations(Collection<FunctionDesc> aggregations, CubeInstance cube) {
        HashSet<FunctionDesc> result = Sets.newHashSet(aggregations);
        CubeDesc cubeDesc = cube.getDescriptor();
        result.removeAll(cubeDesc.listAllFunctions());
        return result;
    }

    private static void tryDimensionAsMeasures(Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, CubeInstance cube, CapabilityResult result, Set<TblColRef> dimCols) {
        CubeDesc cubeDesc = cube.getDescriptor();
        Collection<FunctionDesc> cubeFuncs = cubeDesc.listAllFunctions();

        Iterator<FunctionDesc> it = unmatchedAggregations.iterator();
        while (it.hasNext()) {
            FunctionDesc functionDesc = it.next();

            if (cubeFuncs.contains(functionDesc)) {
                it.remove();
                continue;
            }

            // let calcite handle count
            if (functionDesc.isCount()) {
                it.remove();
                continue;
            }

            // calcite can do aggregation from columns on-the-fly
            ParameterDesc parameterDesc = functionDesc.getParameter();
            if (parameterDesc == null) {
                continue;
            }
            List<TblColRef> neededCols = parameterDesc.getColRefs();
            if (neededCols.size() > 0 && dimCols.containsAll(neededCols) && FunctionDesc.BUILT_IN_AGGREGATIONS.contains(functionDesc.getExpression())) {
                result.influences.add(new CapabilityResult.DimensionAsMeasure(functionDesc));
                it.remove();
                continue;
            }
        }
    }

    // custom measure types can cover unmatched dimensions or measures
    private static void tryCustomMeasureTypes(Collection<TblColRef> unmatchedDimensions, Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, CubeInstance cube, CapabilityResult result) {
        CubeDesc cubeDesc = cube.getDescriptor();
        List<String> influencingMeasures = Lists.newArrayList();
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            //            if (unmatchedDimensions.isEmpty() && unmatchedAggregations.isEmpty())
            //                break;

            MeasureType<?> measureType = measure.getFunction().getMeasureType();
            if (measureType instanceof BasicMeasureType)
                continue;

            CapabilityInfluence inf = measureType.influenceCapabilityCheck(unmatchedDimensions, unmatchedAggregations, digest, measure);
            if (inf != null) {
                result.influences.add(inf);
                influencingMeasures.add(measure.getName() + "@" + measureType.getClass());
            }
        }
        if (influencingMeasures.size() != 0)
            logger.info("Cube {} CapabilityInfluences: {}", cube.getCanonicalName(), StringUtils.join(influencingMeasures, ","));
    }

}
