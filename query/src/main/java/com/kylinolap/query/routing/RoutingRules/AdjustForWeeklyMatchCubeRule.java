package com.kylinolap.query.routing.RoutingRules;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.metadata.model.FunctionDesc;
import com.kylinolap.metadata.model.TblColRef;
import com.kylinolap.metadata.realization.IRealization;
import com.kylinolap.query.relnode.OLAPContext;
import com.kylinolap.query.routing.QueryRouter;
import com.kylinolap.query.routing.RoutingRule;
import org.eigenbase.reltype.RelDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public class AdjustForWeeklyMatchCubeRule extends RoutingRule {
    private static final Logger logger = LoggerFactory.getLogger(AdjustForWeeklyMatchCubeRule.class);

    @Override
    public void apply(List<IRealization> realizations, OLAPContext olapContext) {
        if (olapContext.isWeekMatch.containsKey(realizations.get(0))) {
            CubeInstance cube = (CubeInstance) realizations.get(0);
            adjustOLAPContext(cube, olapContext);
        }
    }

    private static void adjustOLAPContext(CubeInstance cube, OLAPContext olapContext) {
        Collection<TblColRef> dimensionColumns = (olapContext).getDimensionColumns();
        Collection<FunctionDesc> functions = olapContext.aggregations;
        Collection<TblColRef> metricsColumns = olapContext.metricsColumns;
        Map<String, RelDataType> rewriteFields = olapContext.rewriteFields;

        CubeDesc cubeDesc = cube.getDescriptor();
        Collection<FunctionDesc> cubeFuncs = cubeDesc.listAllFunctions();

        Iterator<FunctionDesc> it = functions.iterator();
        while (it.hasNext()) {
            FunctionDesc functionDesc = it.next();
            if (!cubeFuncs.contains(functionDesc)) {
                // try to convert the metric to dimension to see if it works
                TblColRef col = functionDesc.selectTblColByMetrics(metricsColumns, cubeDesc.getFactTable());
                functionDesc.setAppliedOnDimension(true);
                rewriteFields.remove(functionDesc.getRewriteFieldName());
                if (col != null) {
                    metricsColumns.remove(col);
                    dimensionColumns.add(col);
                    olapContext.storageContext.addOtherMandatoryColumns(col);
                }
                logger.info("Adjust OLAPContext for " + functionDesc);
            }
        }
    }

}
