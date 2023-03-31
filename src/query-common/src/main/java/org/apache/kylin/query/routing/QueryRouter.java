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

import java.util.List;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

/**
 * @author xjiang
 */
public class QueryRouter {

    private QueryRouter() {
    }

    private static final List<RoutingRule> LAYOUT_CHOOSING_RULES = Lists.newLinkedList();

    static {
        LAYOUT_CHOOSING_RULES.add(new SegmentPruningRule());
        LAYOUT_CHOOSING_RULES.add(new PartitionPruningRule());
        LAYOUT_CHOOSING_RULES.add(new RemoveIncapableRealizationsRule());
    }

    public static void applyRules(Candidate candidate) {
        for (RoutingRule rule : LAYOUT_CHOOSING_RULES) {
            rule.apply(candidate);
        }
    }

}
