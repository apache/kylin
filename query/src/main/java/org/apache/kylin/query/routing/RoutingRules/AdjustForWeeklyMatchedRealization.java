package org.apache.kylin.query.routing.RoutingRules;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public class AdjustForWeeklyMatchedRealization extends RoutingRule {
    private static final Logger logger = LoggerFactory.getLogger(AdjustForWeeklyMatchedRealization.class);

    @Override
    public void apply(List<IRealization> realizations, OLAPContext olapContext) {
        if (realizations.size() > 0) {
            IRealization first = realizations.get(0);

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
        convertAggreationToDimension(olapContext, iiFuncs, iiDesc.getFactTableName());
    }

    private static void adjustOLAPContextIfNecessary(CubeInstance cube, OLAPContext olapContext) {
        if (CubeCapabilityChecker.check(cube, olapContext.getSQLDigest(), false))
            return;

        CubeDesc cubeDesc = cube.getDescriptor();
        Collection<FunctionDesc> cubeFuncs = cubeDesc.listAllFunctions();
        convertAggreationToDimension(olapContext, cubeFuncs, cubeDesc.getFactTable());
    }

    private static void convertAggreationToDimension(OLAPContext olapContext, Collection<FunctionDesc> availableAggregations, String factTableName) {
        Iterator<FunctionDesc> it = olapContext.aggregations.iterator();
        while (it.hasNext()) {
            FunctionDesc functionDesc = it.next();
            if (!availableAggregations.contains(functionDesc)) {
                // try to convert the metric to dimension to see if it works
                TblColRef col = functionDesc.selectTblColRef(olapContext.metricsColumns, factTableName);
                functionDesc.setAppliedOnDimension(true);
                olapContext.rewriteFields.remove(functionDesc.getRewriteFieldName());
                if (col != null) {
                    olapContext.metricsColumns.remove(col);
                    olapContext.groupByColumns.add(col);
                    olapContext.storageContext.addOtherMandatoryColumns(col);
                }
                logger.info("Adjust OLAPContext for " + functionDesc);
            }
        }
    }

}
