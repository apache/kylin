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

package org.apache.kylin.query.relnode;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.EnumerableUnion;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.ExtendedEnumerable;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Method;
import java.util.List;

/**
 * KYLIN-2200
 */
public class KylinEnumerableUnion extends EnumerableUnion {
    private Method unionArray;
    private Method arrayComparer;

    public KylinEnumerableUnion(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        super(cluster, traitSet, inputs, all);

        unionArray = Types.lookupMethod(ExtendedEnumerable.class, "union", Enumerable.class, EqualityComparer.class);
        arrayComparer = Types.lookupMethod(Functions.class, "arrayComparer");
    }

    private Expression createUnionExpression(Expression left, Expression right, boolean arrayInput) {
        if (all) {
            return Expressions.call(left, BuiltInMethod.CONCAT.method, right);
        }

        return arrayInput
                ? Expressions.call(left, unionArray, right, Expressions.call(arrayComparer))
                : Expressions.call(left, BuiltInMethod.UNION.method, right);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        Expression unionExp = null;
        for (Ord<RelNode> ord : Ord.zip(inputs)) {
            EnumerableRel input = (EnumerableRel) ord.e;
            final Result result = implementor.visitChild(this, ord.i, input, pref);
            Expression childExp =
                    builder.append(
                            "child" + ord.i,
                            result.block);

            if (unionExp == null) {
                unionExp = childExp;
            } else {
                unionExp = createUnionExpression(unionExp, childExp, result.format == JavaRowFormat.ARRAY);
            }
        }

        builder.add(unionExp);
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.prefer(JavaRowFormat.CUSTOM));
        return implementor.result(physType, builder.toBlock());
    }
}
