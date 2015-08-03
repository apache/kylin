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
import java.util.Map;

import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RoutingRule;

import com.google.common.collect.Maps;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public class RealizationPriorityRule extends RoutingRule {
    static Map<RealizationType, Integer> priorities = Maps.newHashMap();

    static {
        priorities.put(RealizationType.CUBE, 1);
        priorities.put(RealizationType.HYBRID, 1);
        priorities.put(RealizationType.INVERTED_INDEX, 2);
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
