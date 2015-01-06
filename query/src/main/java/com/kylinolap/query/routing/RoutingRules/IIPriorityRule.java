package com.kylinolap.query.routing.RoutingRules;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.kylinolap.invertedindex.IIInstance;
import com.kylinolap.metadata.realization.IRealization;
import com.kylinolap.query.relnode.OLAPContext;
import com.kylinolap.query.routing.RoutingRule;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public class IIPriorityRule extends RoutingRule {
    public void apply(List<IRealization> realizations, OLAPContext olapContext) {
        List<IRealization> temp = Lists.newLinkedList();
        for (Iterator<IRealization> iter = realizations.iterator(); iter.hasNext();) {
            IRealization realization = iter.next();
            if (realization instanceof IIInstance) {
                temp.add(realization);
                iter.remove();
            }
        }
        for (IRealization realization : temp) {
            if (olapContext.isSimpleQuery()) {
                realizations.add(0, realization);
            } else {
                realizations.add(realization);
            }
        }
    }
}
