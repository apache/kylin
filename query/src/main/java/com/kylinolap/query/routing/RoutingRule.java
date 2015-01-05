package com.kylinolap.query.routing;

import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.kylinolap.metadata.realization.IRealization;
import com.kylinolap.metadata.realization.RealizationType;
import com.kylinolap.query.relnode.OLAPContext;
import com.kylinolap.query.routing.RoutingRules.*;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public abstract class RoutingRule {
    private static final Logger logger = LoggerFactory.getLogger(QueryRouter.class);
    private static List<RoutingRule> rules = Lists.newLinkedList();

    static {
        rules.add(new RemoveUnmatchedCubesRule());
        rules.add(new RemoveUnmatchedIIRule());
        rules.add(new IIPriorityRule());
        rules.add(new SimpleQueryMoreColumsCubeFirstRule());
        rules.add(new CubesSortRule());
        rules.add(new ExactMatchCubePrecedeRule());
        rules.add(new AdjustForWeeklyMatchCubeRule());//this rule might modify olapcontext content, better put it at last
    }

    public static void goThroughRules(List<IRealization> realizations, OLAPContext olapContext) {
        for (RoutingRule rule : rules) {
            logger.info("Initial realizations order:");
            logger.info(getPrintableText(realizations));
            logger.info("Applying rule " + rule);

            rule.apply(realizations, olapContext);

            logger.info(getPrintableText(realizations));
            logger.info("===================================================");
        }
    }

    public static String getPrintableText(List<IRealization> realizations) {
        StringBuffer sb = new StringBuffer();
        sb.append("[");
        for (IRealization r : realizations) {
            sb.append(r.getName());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");
        return sb.toString();
    }

    /**
     *
     * @param rule
     * @param applyOrder RoutingRule are applied in order, latter rules can override previous rules
     */
    public static void registerRule(RoutingRule rule, int applyOrder) {
        if (applyOrder > rules.size()) {
            logger.warn("apply order " + applyOrder + "  is larger than rules size " + rules.size() + ", will put the new rule at the end");
            rules.add(rule);
        }

        rules.add(applyOrder, rule);
    }

    public static void removeRule(RoutingRule rule) {
        for (Iterator<RoutingRule> iter = rules.iterator(); iter.hasNext();) {
            RoutingRule r = iter.next();
            if (r.getClass() == rule.getClass()) {
                iter.remove();
            }
        }
    }

    protected List<Integer> findRealizationsOf(List<IRealization> realizations, RealizationType type) {
        List<Integer> itemIndexes = Lists.newArrayList();
        for (int i = 0; i < realizations.size(); ++i) {
            if (realizations.get(i).getType() == type) {
                itemIndexes.add(i);
            }
        }
        return itemIndexes;
    }

    @Override
    public String toString() {
        return this.getClass().toString();
    }

    public abstract void apply(List<IRealization> realizations, OLAPContext olapContext);

}
