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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.util.Litmus;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.sdk.datasource.framework.def.DataSourceDef;
import org.apache.kylin.sdk.datasource.framework.def.TypeDef;
import org.apache.kylin.metadata.model.alias.ExpressionComparator;

import com.google.common.collect.Maps;

public class ConvMaster {
    private final DataSourceDef sourceDS;
    private final DataSourceDef targetDS;

    public ConvMaster(DataSourceDef sourceDS, DataSourceDef targetDS) {
        this.sourceDS = sourceDS;
        this.targetDS = targetDS;
    }

    Pair<SqlNode, SqlNode> matchSqlFunc(SqlNode sourceFunc) {
        if (sourceFunc == null || sourceDS == null || targetDS == null)
            return null;

        if (sourceFunc instanceof SqlCall || sourceFunc instanceof SqlIdentifier) {
            String funcName = sourceFunc instanceof SqlCall ? ((SqlCall) sourceFunc).getOperator().getName()
                    : sourceFunc.toString();
            List<String> validDefIds = sourceDS.getFuncDefsByName(funcName);
            if (validDefIds != null) {
                for (String defId : validDefIds) {
                    SqlNode sourceCandidate = sourceDS.getFuncDefSqlNode(defId);
                    if (ExpressionComparator.isNodeEqual(sourceFunc, sourceCandidate, new ParamSqlNodeComparator())) {
                        SqlNode targetTmpl = targetDS.getFuncDefSqlNode(defId);
                        if (targetTmpl != null)
                            return new Pair<>(sourceCandidate, targetDS.getFuncDefSqlNode(defId));
                    }
                }
            }
        }
        return null;
    }

    SqlDataTypeSpec findTargetSqlDataTypeSpec(SqlDataTypeSpec typeSpec) {
        if (sourceDS == null || targetDS == null || typeSpec == null)
            return null;

        List<TypeDef> validTypeDefs = sourceDS.getTypeDefsByName(typeSpec.getTypeName().toString());
        if (validTypeDefs != null) {
            for (TypeDef typeDef : validTypeDefs) {
                if (typeDef.getMaxPrecision() >= typeSpec.getPrecision()) {
                    TypeDef targetType = targetDS.getTypeDef(typeDef.getId());
                    if (targetType == null) {
                        return null;
                    }
                    return new SqlDataTypeSpec(new SqlIdentifier(targetType.getName(), typeSpec.getParserPosition()),
                            targetType.getDefaultPrecision() >= 0 ? targetType.getDefaultPrecision()
                                    : typeSpec.getPrecision(),
                            targetType.getDefaultScale() >= 0 ? targetType.getDefaultScale() : typeSpec.getScale(),
                            typeSpec.getCharSetName(), typeSpec.getTimeZone(), typeSpec.getParserPosition());
                }
            }
        }
        return null;
    }

    boolean checkNodeEqual(SqlNode node1, SqlNode node2) {
        return ExpressionComparator.isNodeEqual(node1, node2, new ParamSqlNodeComparator());
    }

    private static class ParamSqlNodeComparator extends ExpressionComparator.SqlNodeComparator {
        private final Map<Integer, SqlNode> matchedNodesMap;

        private ParamSqlNodeComparator() {
            this(Maps.<Integer, SqlNode> newHashMap());
        }

        private ParamSqlNodeComparator(Map<Integer, SqlNode> matchedNodesMap) {
            this.matchedNodesMap = matchedNodesMap;
        }

        @Override
        public boolean isSqlIdentifierEqual(SqlIdentifier querySqlIdentifier, SqlIdentifier exprSqlIdentifier) {
            int parsedIdx = ParamNodeParser.parseParamIdx(exprSqlIdentifier.toString());
            if (parsedIdx >= 0) {
                SqlNode matchedBefore = matchedNodesMap.get(parsedIdx);
                if (matchedBefore != null) {
                    return ExpressionComparator.isNodeEqual(querySqlIdentifier, matchedBefore, this);
                } else {
                    matchedNodesMap.put(parsedIdx, querySqlIdentifier);
                    return true;
                }
            } else {
                return querySqlIdentifier.equalsDeep(exprSqlIdentifier, Litmus.IGNORE);
            }
        }

        @Override
        public boolean isSqlNodeEqual(SqlNode queryNode, SqlNode exprNode) {
            if (queryNode != null && exprNode != null) {
                if (exprNode instanceof SqlIdentifier) {
                    int parsedIdx = ParamNodeParser.parseParamIdx(exprNode.toString());
                    if (parsedIdx >= 0) {
                        SqlNode matchedBefore = matchedNodesMap.get(parsedIdx);
                        if (matchedBefore != null) {
                            return ExpressionComparator.isNodeEqual(queryNode, matchedBefore, this);
                        } else {
                            matchedNodesMap.put(parsedIdx, queryNode);
                            return true;
                        }
                    }
                } else if (exprNode instanceof SqlIntervalQualifier) {
                    if (!(queryNode instanceof SqlIntervalQualifier)) {
                        return false;
                    }
                    SqlIntervalQualifier thisNode = (SqlIntervalQualifier) queryNode;
                    SqlIntervalQualifier thatNode = (SqlIntervalQualifier) exprNode;
                    return thisNode.toString().equals(thatNode.toString());
                } else if (exprNode instanceof SqlWindow) {
                    if (!(queryNode instanceof SqlWindow)) {
                        return false;
                    }
                    if (((SqlWindow) exprNode).getRefName() instanceof SqlIdentifier) {
                        return true;
                    }
                }

            }

            return super.isSqlNodeEqual(queryNode, exprNode);
        }
    }
}
