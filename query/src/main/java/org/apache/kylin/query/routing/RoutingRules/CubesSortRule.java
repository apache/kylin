package org.apache.kylin.query.routing.RoutingRules;

import java.util.Comparator;
import java.util.List;

import org.apache.kylin.common.util.PartialSorter;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.model.CubeDesc;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RoutingRule;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public class CubesSortRule extends RoutingRule {
    @Override
    public void apply(List<IRealization> realizations, OLAPContext olapContext) {

        // sort cube candidates, 0) the cost indicator, 1) the lesser header
        // columns the better, 2) the lesser body columns the better
        List<Integer> items = super.findRealizationsOf(realizations, RealizationType.CUBE);
        PartialSorter.partialSort(realizations, items, new Comparator<IRealization>() {
            @Override
            public int compare(IRealization o1, IRealization o2) {
                CubeInstance c1 = (CubeInstance) o1;
                CubeInstance c2 = (CubeInstance) o2;
                int comp = 0;
                comp = c1.getCost() - c2.getCost();
                if (comp != 0) {
                    return comp;
                }

                CubeDesc schema1 = c1.getDescriptor();
                CubeDesc schema2 = c2.getDescriptor();

                comp = schema1.listDimensionColumnsIncludingDerived().size() - schema2.listDimensionColumnsIncludingDerived().size();
                if (comp != 0)
                    return comp;

                comp = schema1.getMeasures().size() - schema2.getMeasures().size();
                return comp;
            }
        });

    }

}
