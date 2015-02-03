package org.apache.kylin.query.routing.RoutingRules;

import java.util.*;

import com.google.common.collect.Maps;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RoutingRule;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public class RealizationPriorityRule extends RoutingRule {
    static Map<RealizationType, Integer> priorities = Maps.newHashMap();

    static {
        priorities.put(RealizationType.CUBE, 0);
        priorities.put(RealizationType.INVERTED_INDEX, 1);
    }

    public static void setPriorities(Map<RealizationType, Integer> priorities) {
        RealizationPriorityRule.priorities = priorities;
    }

    public void apply(List<IRealization> realizations, OLAPContext olapContext) {

        Collections.sort(realizations, new Comparator<IRealization>() {
            @Override
            public int compare(IRealization o1, IRealization o2) {
                int i1 = priorities.get(o1.getType());
                int i2 = priorities.get(o2.getType());
                return i1 - i2;
            }
        });
    }
}
