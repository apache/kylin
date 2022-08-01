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

package org.apache.kylin.query.engine;

import java.util.LinkedList;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import io.kyligence.kap.query.optrule.KapProjectJoinTransposeRule;

public class QueryOptimizer {

    private final RelOptPlanner planner;

    public QueryOptimizer(RelOptPlanner planner) {
        this.planner = planner;
    }

    public RelRoot optimize(RelRoot relRoot) {
        // Work around
        //   [CALCITE-1774] Allow rules to be registered during planning process
        // by briefly creating each kind of physical table to let it register its
        // rules. The problem occurs when plans are created via RelBuilder, not
        // the usual process (SQL and SqlToRelConverter.Config.isConvertTableAccess
        // = true).
        final RelVisitor visitor = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableScan) {
                    final RelOptCluster cluster = node.getCluster();
                    final RelOptTable.ToRelContext context = RelOptUtil.getContext(cluster);
                    final RelNode r = node.getTable().toRel(context);
                    planner.registerClass(r);
                }
                if (node instanceof Union) {
                    // @see https://olapio.atlassian.net/browse/AL-2127
                    // the optimize step is extremely slow with this rule
                    planner.removeRule(KapProjectJoinTransposeRule.INSTANCE);
                }
                super.visit(node, ordinal, parent);
            }
        };
        visitor.go(relRoot.rel);
        Program program = Programs.standard();
        return relRoot.withRel(program.run(planner, relRoot.rel, getDesiredRootTraitSet(relRoot), new LinkedList<>(),
                new LinkedList<>()));
    }

    private RelTraitSet getDesiredRootTraitSet(RelRoot root) {
        // Make sure non-CallingConvention traits, if any, are preserved
        return root.rel.getTraitSet().replace(EnumerableConvention.INSTANCE).replace(root.collation).simplify();
    }

}
