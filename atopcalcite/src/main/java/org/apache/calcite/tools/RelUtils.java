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

package org.apache.calcite.tools;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;

import com.google.common.collect.Lists;

public class RelUtils {
    public static boolean findOLAPRel(RelNode rel) {
        Class aClass;
        try {
            aClass = Thread.currentThread().getContextClassLoader().loadClass("org.apache.kylin.query.relnode.OLAPRel");
        } catch (ClassNotFoundException e) {
            return false;
        }
        return findRel(rel, Lists.newArrayList(aClass)) != null;
    }

    private static RelNode findRel(RelNode rel, List<Class> candidate) {
        for (Class clazz : candidate) {
            if (clazz.isInstance(rel)) {
                return rel;
            }
        }

        if (rel.getInputs().size() < 1) {
            return null;
        }

        return findRel(rel.getInput(0), candidate);
    }

    public static int countOperatorCall(final SqlOperator operator, RexNode node) {
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {
            public Void visitCall(RexCall call) {
                if (call.getOperator().equals(operator)) {
                    atomicInteger.incrementAndGet();
                }
                return super.visitCall(call);
            }
        };
        node.accept(visitor);
        return atomicInteger.get();
    }
}
