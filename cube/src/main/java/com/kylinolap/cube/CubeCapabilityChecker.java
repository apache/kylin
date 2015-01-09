package com.kylinolap.cube;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.cube.model.DimensionDesc;
import com.kylinolap.metadata.model.FunctionDesc;
import com.kylinolap.metadata.model.JoinDesc;
import com.kylinolap.metadata.model.TblColRef;
import com.kylinolap.metadata.realization.SQLDigest;

/**
 * Created by Hongbin Ma(Binmahone) on 1/8/15.
 */
public class CubeCapabilityChecker {
    private static final Logger logger = LoggerFactory.getLogger(CubeCapabilityChecker.class);

    public static boolean check(CubeInstance cube, SQLDigest digest, boolean allowWeekMatch) {

        // retrieve members from olapContext
        Collection<TblColRef> dimensionColumns = CubeDimensionDeriver.getDimensionColumns(digest.groupbyColumns, digest.filterColumns);
        Collection<FunctionDesc> functions = digest.aggregateFunc;
        Collection<TblColRef> metricsColumns = digest.aggregatedColumns;
        Collection<JoinDesc> joins = digest.joinDescs;

        // match dimensions & aggregations & joins

        boolean isOnline = cube.isReady();

        boolean matchDimensions = isMatchedWithDimensions(dimensionColumns, cube);
        boolean matchAggregation = isMatchedWithAggregations(functions, cube);
        boolean matchJoin = isMatchedWithJoins(joins, cube);

        // Some cubes are not "perfectly" match, but still save them in case of usage
        if (allowWeekMatch && isOnline && matchDimensions && !matchAggregation && matchJoin) {
            // sometimes metrics are indeed dimensions
            // e.g. select min(cal_dt) from ..., where cal_dt is actually a dimension
            if (isWeaklyMatchedWithAggregations(functions, metricsColumns, cube)) {
                logger.info("Weakly matched cube found " + cube.getName());
                return true;
            }
        }

        if (!isOnline || !matchDimensions || !matchAggregation || !matchJoin) {
            logger.info("Exclude cube " + cube.getName() + " because " + " isOnlne=" + isOnline + ",matchDimensions=" + matchDimensions + ",matchAggregation=" + matchAggregation + ",matchJoin=" + matchJoin);
            return false;
        }

        return true;
    }

    private static boolean isMatchedWithDimensions(Collection<TblColRef> dimensionColumns, CubeInstance cube) {
        CubeDesc cubeDesc = cube.getDescriptor();
        boolean matchAgg = cubeDesc.listDimensionColumnsIncludingDerived().containsAll(dimensionColumns);
        return matchAgg;
    }

    private static boolean isMatchedWithAggregations(Collection<FunctionDesc> aggregations, CubeInstance cube) {
        CubeDesc cubeDesc = cube.getDescriptor();
        boolean matchAgg = cubeDesc.listAllFunctions().containsAll(aggregations);
        return matchAgg;
    }

    private static boolean isMatchedWithJoins(Collection<JoinDesc> joins, CubeInstance cube) {
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

    private static boolean isWeaklyMatchedWithAggregations(Collection<FunctionDesc> aggregations, Collection<TblColRef> metricColumns, CubeInstance cube) {
        CubeDesc cubeDesc = cube.getDescriptor();
        Collection<FunctionDesc> cubeFuncs = cubeDesc.listAllFunctions();

        boolean matched = true;
        for (FunctionDesc functionDesc : aggregations) {
            if (cubeFuncs.contains(functionDesc))
                continue;

            // only inverted-index cube does not have count, and let calcite handle in this case
            if (functionDesc.isCount())
                continue;

            if (functionDesc.isCountDistinct()) // calcite can not handle distinct count
                matched = false;

            TblColRef col = functionDesc.selectTblColByMetrics(metricColumns, cubeDesc.getFactTable());
            if (col == null || !cubeDesc.listDimensionColumnsIncludingDerived().contains(col)) {
                matched = false;
            }
        }
        return matched;
    }
}
