package org.apache.kylin.query.routing.RoutingRules;

import java.util.Iterator;
import java.util.List;

import com.kylinolap.metadata.realization.IRealization;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RoutingRule;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public class RemoveUncapableRealizationsRule extends RoutingRule {
    @Override
    public void apply(List<IRealization> realizations, OLAPContext olapContext) {
        for (Iterator<IRealization> iterator = realizations.iterator(); iterator.hasNext();) {
            IRealization realization = iterator.next();
            if (!realization.isCapable(olapContext.getSQLDigest())) {
                iterator.remove();
            }
        }
    }

}
