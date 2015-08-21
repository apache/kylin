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

package org.apache.kylin.query.routing.RoutingRules;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RoutingRule;
import org.apache.kylin.storage.hybrid.HybridInstance;

/**
 */
public class RealizationSortRule extends RoutingRule {
    @Override
    public void apply(List<IRealization> realizations, final OLAPContext olapContext) {

        // sort cube candidates, 0) the priority 1) the cost indicator, 2) the lesser header
        // columns the better, 3) the lesser body columns the better 4) the larger date range the better

        Collections.sort(realizations, new Comparator<IRealization>() {
            @Override
            public int compare(IRealization o1, IRealization o2) {
                int i1 = RealizationPriorityRule.priorities.get(o1.getType());
                int i2 = RealizationPriorityRule.priorities.get(o2.getType());
                int comp = i1 - i2;
                if (comp != 0) {
                    return comp;
                }

                comp = o1.getCost(olapContext.getSQLDigest()) - o2.getCost(olapContext.getSQLDigest());
                if (comp != 0) {
                    return comp;
                }

                if (o1 instanceof HybridInstance)
                    return -1;
                else if (o2 instanceof HybridInstance)
                    return 1;

                return 0;
            }
        });

    }

}
