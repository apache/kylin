package com.kylinolap.query.routing.RoutingRules;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.Maps;
import com.kylinolap.metadata.realization.IRealization;
import com.kylinolap.metadata.realization.RealizationType;
import com.kylinolap.query.relnode.OLAPContext;
import com.kylinolap.query.routing.RoutingRule;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public class RealizationPriorityRule extends RoutingRule {
    public void apply(List<IRealization> realizations, OLAPContext olapContext) {

        final HashMap<RealizationType, Integer> priority = Maps.newHashMap();
        priority.put(RealizationType.CUBE, 1);
        priority.put(RealizationType.INVERTED_INDEX, 0);

        Collections.sort(realizations, new Comparator<IRealization>() {
            @Override
            public int compare(IRealization o1, IRealization o2) {
                int i1 = priority.get(o1.getType());
                int i2 = priority.get(o2.getType());
                return i1 - i2;
            }
        });
    }
}
