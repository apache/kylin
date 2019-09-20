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
package org.apache.kylin.query.optrule.visitor;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class TimezoneRewriteVisitor extends RexVisitorImpl<RexNode> {
    public static final Logger logger = LoggerFactory.getLogger(TimezoneRewriteVisitor.class);

    public TimezoneRewriteVisitor(boolean deep) {
        super(deep);
    }

    @Override
    public RexNode visitCall(RexCall call) {
        List<RexNode> subList = new ArrayList<>();
        SqlTypeName type = call.getType().getSqlTypeName();
        if (call.getKind() == SqlKind.CAST) {
            if (type.getFamily() == SqlTypeFamily.DATE || type.getFamily() == SqlTypeFamily.DATETIME
                    || type.getFamily() == SqlTypeFamily.TIMESTAMP) {
                for (RexNode node : call.getOperands()) {
                    if (node instanceof RexLiteral) {
                        RexLiteral literal = (RexLiteral) node;
                        String toBeModify = literal.getValue2().toString();
                        logger.info(toBeModify);
                        if (toBeModify.length() != 19) {
                            subList.add(node);
                            continue;
                        }
                        // minus offset by timezone in RelNode level
                        // this will affect code gen
                        int minusHours = 0;
                        String afetrModify = LocalDateTime
                                .parse(toBeModify, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).minusHours(minusHours)
                                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                        logger.info("{}  ->  {}", toBeModify, afetrModify);
                        RexLiteral newliteral = RexLiteral.fromJdbcString(literal.getType(), literal.getTypeName(),
                                afetrModify);
                        subList.add(newliteral);
                    } else {
                        subList.add(node);
                    }
                }
            }
            return call.clone(call.type, subList);
        }

        for (RexNode operand : call.operands) {
            RexNode node = operand.accept(this);
            subList.add(node);
        }
        return call.clone(call.type, subList);
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
        return literal;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        return inputRef;
    }

    @Override
    public RexNode visitLocalRef(RexLocalRef localRef) {
        return localRef;
    }

    @Override
    public RexNode visitOver(RexOver over) {
        return over;
    }

    @Override
    public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
        return correlVariable;
    }

    @Override
    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
        return dynamicParam;
    }

    @Override
    public RexNode visitRangeRef(RexRangeRef rangeRef) {
        return rangeRef;
    }

    @Override
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
        return fieldAccess;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
        return subQuery;
    }

    @Override
    public RexNode visitTableInputRef(RexTableInputRef ref) {
        return ref;
    }

    @Override
    public RexNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        return fieldRef;
    }

    public static void main(String[] args) {

    }
}
