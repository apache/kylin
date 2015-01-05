package com.kylinolap.query.routing.RoutingRules;

import java.util.List;

import com.kylinolap.metadata.realization.IRealization;
import com.kylinolap.query.relnode.OLAPContext;
import com.kylinolap.query.routing.RoutingRule;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public class RemoveUnmatchedIIRule extends RoutingRule {
    @Override
    public void apply(List<IRealization> realizations, OLAPContext olapContext) {
        //TODO: currently II is omnipotent
    }
}
