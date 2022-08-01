/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.query.routing;

import java.util.Iterator;
import java.util.List;

import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.routing.rules.PartitionPruningRule;
import org.apache.kylin.query.routing.rules.RealizationSortRule;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.apache.kylin.query.routing.rules.RemoveUncapableRealizationsRule;
import org.apache.kylin.query.routing.rules.SegmentPruningRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 */
public abstract class RoutingRule {
    private static final Logger logger = LoggerFactory.getLogger(QueryRouter.class);
    private static List<RoutingRule> rules = Lists.newLinkedList();

    static {
        rules.add(new RemoveBlackoutRealizationsRule());
        rules.add(new SegmentPruningRule());
        rules.add(new PartitionPruningRule());
        rules.add(new RemoveUncapableRealizationsRule());
        rules.add(new RealizationSortRule());
    }

    public static void applyRules(List<Candidate> candidates) {
        for (RoutingRule rule : rules) {
            String before = getPrintableText(candidates);
            rule.apply(candidates);
            String after = getPrintableText(candidates);
            if (!before.equals(after)) {
                logger.info("Applying rule: {}, realizations before:{}, realizations after: {}", rule, before, after);
            }
        }
    }

    public static String getPrintableText(List<Candidate> candidates) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Candidate candidate : candidates) {
            IRealization r = candidate.realization;
            sb.append(r.getCanonicalName());
            sb.append(",");
        }
        if (sb.charAt(sb.length() - 1) != '[')
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
            logger.warn("apply order {} is larger than rules size {}, will put the new rule at the end", applyOrder,
                    rules.size());
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

    protected List<Integer> findRealizationsOf(List<IRealization> realizations, String type) {
        List<Integer> itemIndices = Lists.newArrayList();
        for (int i = 0; i < realizations.size(); ++i) {
            if (realizations.get(i).getType().equals(type)) {
                itemIndices.add(i);
            }
        }
        return itemIndices;
    }

    @Override
    public String toString() {
        return this.getClass().toString();
    }

    public abstract void apply(List<Candidate> candidates);

}
