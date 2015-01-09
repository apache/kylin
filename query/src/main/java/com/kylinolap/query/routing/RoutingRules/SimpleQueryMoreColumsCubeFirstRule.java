package com.kylinolap.query.routing.RoutingRules;

import java.util.Comparator;
import java.util.List;

import com.kylinolap.common.util.PartialSorter;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.metadata.realization.IRealization;
import com.kylinolap.metadata.realization.RealizationType;
import com.kylinolap.query.relnode.OLAPContext;
import com.kylinolap.query.routing.RoutingRule;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public class SimpleQueryMoreColumsCubeFirstRule extends RoutingRule {
    @Override
    public void apply(List<IRealization> realizations, OLAPContext olapContext) {
        List<Integer> itemIndexes = super.findRealizationsOf(realizations, RealizationType.CUBE);

        if (olapContext.isSimpleQuery()) {
            PartialSorter.partialSort(realizations, itemIndexes, new Comparator<IRealization>() {
                @Override
                public int compare(IRealization o1, IRealization o2) {
                    CubeInstance c1 = (CubeInstance) o1;
                    CubeInstance c2 = (CubeInstance) o2;
                    return c1.getDescriptor().listDimensionColumnsIncludingDerived().size() - c2.getDescriptor().listDimensionColumnsIncludingDerived().size();
                }
            });
        }
    }
}
