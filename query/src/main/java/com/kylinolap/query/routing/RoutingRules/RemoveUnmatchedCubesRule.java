package com.kylinolap.query.routing.RoutingRules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.cube.model.DimensionDesc;
import com.kylinolap.metadata.model.FunctionDesc;
import com.kylinolap.metadata.model.JoinDesc;
import com.kylinolap.metadata.model.TblColRef;
import com.kylinolap.metadata.realization.IRealization;
import com.kylinolap.metadata.realization.RealizationType;
import com.kylinolap.query.relnode.OLAPContext;
import com.kylinolap.query.routing.NoRealizationFoundException;
import com.kylinolap.query.routing.RoutingRule;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public class RemoveUnmatchedCubesRule extends RoutingRule {

    private static final Logger logger = LoggerFactory.getLogger(RemoveUnmatchedCubesRule.class);

    @Override
    public void apply(List<IRealization> realizations, OLAPContext olapContext) {

        // retrieve members from olapContext
        String factTableName = olapContext.firstTableScan.getTableName();
        String projectName = olapContext.olapSchema.getProjectName();
        Collection<TblColRef> dimensionColumns = (olapContext).getDimensionColumns();
        Collection<FunctionDesc> functions = olapContext.aggregations;
        Collection<TblColRef> metricsColumns = olapContext.metricsColumns;
        Collection<JoinDesc> joins = olapContext.joins;

        // find cubes by table
        List<CubeInstance> candidates = filterCubes(realizations);
        logger.info("Find candidates by table " + factTableName + " and project=" + projectName + " : " + StringUtils.join(candidates, ","));

        // match dimensions & aggregations & joins
        Iterator<CubeInstance> it = candidates.iterator();

        while (it.hasNext()) {
            CubeInstance cube = it.next();
            boolean isOnline = cube.isReady();

            boolean matchDimensions = isMatchedWithDimensions(dimensionColumns, cube);
            boolean matchAggregation = isMatchedWithAggregations(functions, cube);
            boolean matchJoin = isMatchedWithJoins(joins, cube);

            // Some cubes are not "perfectly" match, but still save them in case of usage
            if (isOnline && matchDimensions && !matchAggregation && matchJoin) {
                // sometimes metrics are indeed dimensions
                // e.g. select min(cal_dt) from ..., where cal_dt is actually a dimension
                if (isWeaklyMatchedWithAggregations(functions, metricsColumns, cube)) {
                    logger.info("Weakly matched cube found " + cube.getName());
                    olapContext.isWeekMatch.put(cube, true);
                    continue;
                }
            }

            if (!isOnline || !matchDimensions || !matchAggregation || !matchJoin) {
                logger.info("Remove cube " + cube.getName() + " because " + " isOnlne=" + isOnline + ",matchDimensions=" + matchDimensions + ",matchAggregation=" + matchAggregation + ",matchJoin=" + matchJoin);
                realizations.remove(cube);
            }
        }
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

    private static boolean isMatchedWithJoins(Collection<JoinDesc> joins, CubeInstance cube) throws NoRealizationFoundException {
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

    static List<CubeInstance> filterCubes(List<IRealization> realizations) {
        return Lists.newArrayList(Iterables.transform(Iterables.filter(realizations, new Predicate<IRealization>() {
            @Override
            public boolean apply(IRealization input) {
                return input.getType() == RealizationType.CUBE;
            }
        }), new Function<IRealization, CubeInstance>() {
            @Nullable
            @Override
            public CubeInstance apply(IRealization input) {
                return (CubeInstance) input;
            }
        }));

    }
}
