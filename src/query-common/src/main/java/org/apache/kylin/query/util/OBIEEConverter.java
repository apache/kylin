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

package org.apache.kylin.query.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.source.adhocquery.IPushDownConverter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OBIEEConverter implements KapQueryUtil.IQueryTransformer, IPushDownConverter {

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        return doConvert(sql, project);
    }

    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        return doConvert(originSql, project);
    }

    public String doConvert(String originSql, String project) {
        try {
            SqlNode sqlNode = CalciteParser.parse(originSql, project);
            List<SqlNumericLiteral> sqlNumericLiterals = new ArrayList<>(numbersToTrim(sqlNode));
            CalciteParser.descSortByPosition(sqlNumericLiterals);
            final StringBuilder sb = new StringBuilder(originSql);
            for (SqlNumericLiteral numLit : sqlNumericLiterals) {
                if (numLit.getTypeName() == SqlTypeName.DECIMAL && numLit.getPrec() >= 0 && numLit.getScale() > 0) {
                    BigDecimal number = ((BigDecimal) numLit.getValue());
                    String numStr = number.toString();
                    int i = numStr.length() - numLit.getScale();
                    for (; i < numStr.length(); i++) {
                        if (numStr.charAt(i) != '0') {
                            break;
                        }
                    }
                    if (i == numStr.length()) {
                        Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(numLit, originSql);
                        String trimmedNumStr = number.setScale(0, RoundingMode.FLOOR).toString();
                        sb.replace(replacePos.getFirst(), replacePos.getSecond(), trimmedNumStr);
                    }
                }
            }
            return sb.toString();
        } catch (SqlParseException e) {
            log.warn("Error converting sql for OBIEE", e);
            return originSql;
        }
    }

    private Set<SqlNumericLiteral> numbersToTrim(SqlNode node) {
        if (node instanceof SqlBasicCall) {
            return numbersToTrimFromBasicCall((SqlBasicCall) node);
        } else if (node instanceof SqlCall) {
            Set<SqlNumericLiteral> numbers = new HashSet<>();
            for (SqlNode op : ((SqlCall) node).getOperandList()) {
                numbers.addAll(numbersToTrim(op));
            }
            return numbers;
        } else {
            return Collections.emptySet();
        }
    }

    private Set<SqlNumericLiteral> numbersToTrimFromBasicCall(SqlBasicCall call) {
        if (call.getOperator() == SqlStdOperatorTable.EQUALS) {
            Set<SqlNumericLiteral> numbers = new HashSet<>();
            for (SqlNode op : call.getOperands()) {
                if (op instanceof SqlNumericLiteral) {
                    numbers.add((SqlNumericLiteral) op);
                }
            }
            return numbers.size() == 1 ? numbers : Collections.emptySet();
        } else if (call.getOperator() == SqlStdOperatorTable.IN) {
            Set<SqlNumericLiteral> numbers = new HashSet<>();
            for (SqlNode op : ((SqlNodeList) call.getOperands()[1]).getList()) {
                if (op instanceof SqlNumericLiteral) {
                    numbers.add((SqlNumericLiteral) op);
                }
            }
            return numbers;
        } else {
            Set<SqlNumericLiteral> numbers = new HashSet<>();
            for (SqlNode op : call.getOperands()) {
                numbers.addAll(numbersToTrim(op));
            }
            return numbers;
        }
    }

}
