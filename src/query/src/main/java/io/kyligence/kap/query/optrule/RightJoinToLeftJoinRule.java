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

package io.kyligence.kap.query.optrule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.JoinCommuteRule;

/**
 * Convert all right joins to left joins with the left and right operands swapped.
 * Note that
 * <li>A project rel may will be inserted to ensure the col ordering</li>
 * </p>
 */
public class RightJoinToLeftJoinRule extends JoinCommuteRule {

    public static final RelOptRule INSTANCE = new RightJoinToLeftJoinRule(Join.class);

    private RightJoinToLeftJoinRule(Class<? extends Join> clazz) {
        super(clazz, RelFactories.LOGICAL_BUILDER, true);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Join join = call.rel(0);
        return join.getJoinType() == JoinRelType.RIGHT;
    }
}
