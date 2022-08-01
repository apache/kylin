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

package org.apache.kylin.metadata.model.tool;

import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.NonEquiJoinConditionType;

public interface NonEquiJoinConditionVisitor {

    default NonEquiJoinCondition visit(NonEquiJoinCondition nonEquiJoinCondition) {
        if (nonEquiJoinCondition == null) {
            return null;
        }

        if (nonEquiJoinCondition.getType() == NonEquiJoinConditionType.LITERAL) {
            return visitLiteral(nonEquiJoinCondition);
        } else if (nonEquiJoinCondition.getType() == NonEquiJoinConditionType.COLUMN) {
            return visitColumn(nonEquiJoinCondition);
        } else if (nonEquiJoinCondition.getType() == NonEquiJoinConditionType.EXPRESSION) {
            return visitExpression(nonEquiJoinCondition);
        } else {
            return visit(nonEquiJoinCondition);
        }
    }

    default NonEquiJoinCondition visitExpression(NonEquiJoinCondition nonEquiJoinCondition) {
        NonEquiJoinCondition[] ops = new NonEquiJoinCondition[nonEquiJoinCondition.getOperands().length];
        for (int i = 0; i < nonEquiJoinCondition.getOperands().length; i++) {
            ops[i] = visit(nonEquiJoinCondition.getOperands()[i]);
        }
        nonEquiJoinCondition.setOperands(ops);
        return nonEquiJoinCondition;
    }

    default NonEquiJoinCondition visitColumn(NonEquiJoinCondition nonEquiJoinCondition) {
        return nonEquiJoinCondition;
    }

    default NonEquiJoinCondition visitLiteral(NonEquiJoinCondition nonEquiJoinCondition) {
        return nonEquiJoinCondition;
    }

}
