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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.CapabilityResult.CapabilityInfluence;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 */
public class CubeCapabilityChecker {
    private static final Logger logger = LoggerFactory.getLogger(CubeCapabilityChecker.class);

    public static CapabilityResult check(CubeInstance cube, SQLDigest digest) {
        CapabilityResult result = new CapabilityResult();
        result.capable = false;

        // match joins
        boolean isJoinMatch = isJoinMatch(digest.joinDescs, cube);
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
        if (!unmatchedDimensions.isEmpty() || !unmatchedAggregations.isEmpty()) {
            tryCustomMeasureTypes(unmatchedDimensions, unmatchedAggregations, digest, cube, result);
        }
        
        // try dimension-as-measure
        if (!unmatchedAggregations.isEmpty()) {
            tryDimensionAsMeasures(unmatchedAggregations, digest, cube, result);
        }
        
        if (!unmatchedDimensions.isEmpty()) {
            logger.info("Exclude cube " + cube.getName() + " because unmatched dimensions");
            return result;
        }
        
        if (!unmatchedAggregations.isEmpty()) {
            logger.info("Exclude cube " + cube.getName() + " because unmatched aggregations");
            return result;
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

    private static boolean isJoinMatch(Collection<JoinDesc> joins, CubeInstance cube) {
        CubeDesc cubeDesc = cube.getDescriptor();

        List<JoinDesc> cubeJoins = new ArrayList<JoinDesc>(cubeDesc.getDimensions().size());
        for (DimensionDesc d : cubeDesc.getDimensions()) {
            if (d.getJoin() != null) {
                cubeJoins.add(d.getJoin());
            }
        }
        for (JoinDesc j : joins) {
            // optiq engine can't decide which one is fk or pk
            String pTable = j.getPrimaryKeyColumns()[0].getTable();
            String factTable = cubeDesc.getFactTable();
            if (factTable.equals(pTable)) {
                j.swapPKFK();
            }

            // check primary key, all PK column should refer to same tale, the Fact Table of cube.
            // Using first column's table name to check.
            String fTable = j.getForeignKeyColumns()[0].getTable();
            if (!factTable.equals(fTable)) {
                logger.info("Fact Table" + factTable + " not matched in join: " + j + " on cube " + cube.getName());
                return false;
            }

            // The hashcode() function of JoinDesc has been overwritten,
            // which takes into consideration: pk,fk,jointype
            if (!cubeJoins.contains(j)) {
                logger.info("Query joins don't macth on cube " + cube.getName());
                return false;
            }
        }
        return true;
    }

    private static void tryDimensionAsMeasures(Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, CubeInstance cube, CapabilityResult result) {
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
            List<TblColRef> neededCols = functionDesc.getParameter().getColRefs();
            if (neededCols.size() > 0 && cubeDesc.listDimensionColumnsIncludingDerived().containsAll(neededCols)) {
                result.influences.add(new CapabilityResult.DimensionAsMeasure(functionDesc));
                it.remove();
                continue;
            }
        }
    }

    // custom measure types can cover unmatched dimensions or measures
    private static void tryCustomMeasureTypes(Collection<TblColRef> unmatchedDimensions, Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, CubeInstance cube, CapabilityResult result) {
        CubeDesc cubeDesc = cube.getDescriptor();
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            if (unmatchedDimensions.isEmpty() && unmatchedAggregations.isEmpty())
                break;
            
            MeasureType<?> measureType = measure.getFunction().getMeasureType();
            if (measureType instanceof BasicMeasureType)
                continue;
            
            CapabilityInfluence inf = measureType.influenceCapabilityCheck(unmatchedDimensions, unmatchedAggregations, digest, measure);
            if (inf != null)
                result.influences.add(inf);
        }
    }

}
