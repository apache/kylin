package com.kylinolap.query.routing.RoutingRules;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.kylinolap.common.util.PartialSorter;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.metadata.realization.IRealization;
import com.kylinolap.metadata.realization.RealizationType;
import com.kylinolap.query.relnode.OLAPContext;
import com.kylinolap.query.routing.RoutingRule;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public class ExactMatchCubePrecedeRule extends RoutingRule {

    @Override
    public void apply(List<IRealization> realizations, OLAPContext olapContext) {
        List<Integer> itemIndexes = super.findRealizationsOf(realizations, RealizationType.CUBE);
        final Map<IRealization, Boolean> isWeekMatch = olapContext.isWeekMatch;

        PartialSorter.partialSort(realizations, itemIndexes, new Comparator<IRealization>() {
            @Override
            public int compare(IRealization o1, IRealization o2) {
                CubeInstance c1 = (CubeInstance) o1;
                CubeInstance c2 = (CubeInstance) o2;

                if (isWeekMatch.containsKey(c1) && !isWeekMatch.containsKey(c2)) {
                    return 1;
                } else if (!isWeekMatch.containsKey(c1) && isWeekMatch.containsKey(c2)) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });
    }
}
