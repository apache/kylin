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
package org.apache.kylin.metadata.model.alias;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ExpressionComparator {

    private static final Logger logger = LoggerFactory.getLogger(ExpressionComparator.class);

    private ExpressionComparator() {
    }

    /**
     *
     * @param queryNode
     * @param exprNode
     * @param aliasMapping
     * @param aliasDeduce is only required if column reference in queryNode might be COL instead of ALIAS.COL
     * @return
     */
    public static boolean isNodeEqual(SqlNode queryNode, SqlNode exprNode, final AliasMapping aliasMapping,
            final AliasDeduce aliasDeduce) {
        if (aliasMapping == null) {
            return false;
        }
        return isNodeEqual(queryNode, exprNode, new AliasMatchingSqlNodeComparator(aliasMapping, aliasDeduce));
    }

    public static boolean isNodeEqual(SqlNode queryNode, SqlNode exprNode, SqlNodeComparator nodeComparator) {
        try {
            Preconditions.checkNotNull(nodeComparator);
            return nodeComparator.isSqlNodeEqual(queryNode, exprNode);
        } catch (Exception e) {
            logger.error("Exception while running isNodeEqual, return false", e);
            return false;
        }
    }

    public static class AliasMatchingSqlNodeComparator extends SqlNodeComparator {
        private final AliasMapping aliasMapping;
        private final AliasDeduce aliasDeduce;

        public AliasMatchingSqlNodeComparator(AliasMapping aliasMapping, AliasDeduce aliasDeduce) {
            this.aliasMapping = aliasMapping;
            this.aliasDeduce = aliasDeduce;
        }

        protected boolean isSqlIdentifierEqual(SqlIdentifier querySqlIdentifier, SqlIdentifier exprSqlIdentifier) {
            if (aliasMapping == null || aliasMapping.getAliasMap() == null) {
                return false;
            }
            Preconditions.checkState(exprSqlIdentifier.names.size() == 2);
            String queryAlias = null, queryCol = null;
            if (querySqlIdentifier.isStar()) {
                return exprSqlIdentifier.isStar();
            } else if (exprSqlIdentifier.isStar()) {
                return false;
            }

            try {
                if (querySqlIdentifier.names.size() == 1) {
                    queryCol = querySqlIdentifier.names.get(0);
                    queryAlias = aliasDeduce.deduceAlias(queryCol);
                } else if (querySqlIdentifier.names.size() == 2) {
                    queryCol = querySqlIdentifier.names.get(1);
                    queryAlias = querySqlIdentifier.names.get(0);
                }

                //translate user alias to alias in model
                String modelAlias = aliasMapping.getAliasMap().get(queryAlias);
                Preconditions.checkNotNull(modelAlias);
                Preconditions.checkNotNull(queryCol);

                String identity = modelAlias + "." + queryCol;
                if (aliasMapping.getExcludedColumns().contains(identity)) {
                    return false;
                }

                return StringUtils.equalsIgnoreCase(modelAlias, exprSqlIdentifier.names.get(0))
                        && StringUtils.equalsIgnoreCase(queryCol, exprSqlIdentifier.names.get(1));
            } catch (NullPointerException | IllegalStateException e) {
                logger.trace("met exception when doing expressions[{}, {}] comparison", querySqlIdentifier,
                        exprSqlIdentifier, e);
                return false;
            }
        }
    }

    public abstract static class SqlNodeComparator {
        protected abstract boolean isSqlIdentifierEqual(SqlIdentifier querySqlIdentifier,
                SqlIdentifier exprSqlIdentifier);

        public boolean isSqlNodeEqual(SqlNode queryNode, SqlNode exprNode) {
            if (queryNode == null) {
                return exprNode == null;
            }

            if (exprNode == null) {
                return false;
            }

            if (!Objects.equals(queryNode.getClass().getSimpleName(), exprNode.getClass().getSimpleName())) {
                return false;
            }

            if (queryNode instanceof SqlCall) {
                SqlCall thisNode = (SqlCall) queryNode;
                SqlCall thatNode = (SqlCall) exprNode;
                if (!thisNode.getOperator().getName().equalsIgnoreCase(thatNode.getOperator().getName())) {
                    return false;
                }
                if (isCommutativeOperator(thisNode.getOperator())) {
                    return isNodeListEqualRegardlessOfOrdering(thisNode.getOperandList(), thatNode.getOperandList());
                } else {
                    return isNodeListEqual(thisNode.getOperandList(), thatNode.getOperandList());
                }
            }
            if (queryNode instanceof SqlLiteral) {
                SqlLiteral thisNode = (SqlLiteral) queryNode;
                SqlLiteral thatNode = (SqlLiteral) exprNode;
                return Objects.equals(thisNode.getValue(), thatNode.getValue());
            }
            if (queryNode instanceof SqlNodeList) {
                SqlNodeList thisNode = (SqlNodeList) queryNode;
                SqlNodeList thatNode = (SqlNodeList) exprNode;
                if (thisNode.getList().size() != thatNode.getList().size()) {
                    return false;
                }
                for (int i = 0; i < thisNode.getList().size(); i++) {
                    SqlNode thisChild = thisNode.getList().get(i);
                    final SqlNode thatChild = thatNode.getList().get(i);
                    if (!isSqlNodeEqual(thisChild, thatChild)) {
                        return false;
                    }
                }
                return true;
            }

            if (queryNode instanceof SqlIdentifier) {
                SqlIdentifier thisNode = (SqlIdentifier) queryNode;
                SqlIdentifier thatNode = (SqlIdentifier) exprNode;
                return isSqlIdentifierEqual(thisNode, thatNode);
            }

            if (queryNode instanceof SqlDataTypeSpec) {
                SqlDataTypeSpec thisNode = (SqlDataTypeSpec) queryNode;
                SqlDataTypeSpec thatNode = (SqlDataTypeSpec) exprNode;
                return isSqlDataTypeSpecEqual(thisNode, thatNode);
            }

            if (queryNode instanceof SqlIntervalQualifier) {
                SqlIntervalQualifier thisNode = (SqlIntervalQualifier) queryNode;
                SqlIntervalQualifier thatNode = (SqlIntervalQualifier) exprNode;
                return isSqlIntervalQualifierEqual(thisNode, thatNode);
            }

            return false;
        }

        private boolean isSqlIntervalQualifierEqual(SqlIntervalQualifier querySqlIntervalQualifier,
                SqlIntervalQualifier exprIntervalQualifier) {
            return querySqlIntervalQualifier.getUnit() == exprIntervalQualifier.getUnit();
        }

        protected boolean isSqlDataTypeSpecEqual(SqlDataTypeSpec querySqlDataTypeSpec,
                SqlDataTypeSpec exprSqlDataTypeSpec) {
            if (querySqlDataTypeSpec.getTypeName() == null
                    || CollectionUtils.isEmpty(querySqlDataTypeSpec.getTypeName().names))
                return false;
            if (querySqlDataTypeSpec.getTypeName().names.size() != exprSqlDataTypeSpec.getTypeName().names.size())
                return false;

            for (int i = 0; i < querySqlDataTypeSpec.getTypeName().names.size(); i++) {
                String queryName = querySqlDataTypeSpec.getTypeName().names.get(i);
                if (!exprSqlDataTypeSpec.getTypeName().names.contains(queryName)) {
                    return false;
                }
            }

            return querySqlDataTypeSpec.getScale() == exprSqlDataTypeSpec.getScale()
                    && querySqlDataTypeSpec.getPrecision() == exprSqlDataTypeSpec.getPrecision();
        }

        // maintain a very limited set of COMMUTATIVE_OPERATORS
        // as the equal assertion on commutative operation is more costly (O(n^2)) than non commutative operation (O(n))
        // add new operators here only when necessary
        private static final Set<SqlKind> COMMUTATIVE_OPERATORS = Sets.newHashSet(SqlKind.PLUS, SqlKind.TIMES);

        private boolean isCommutativeOperator(SqlOperator operator) {
            return COMMUTATIVE_OPERATORS.contains(operator.getKind());
        }

        private boolean isNodeListEqualRegardlessOfOrdering(List<SqlNode> queryNodeList, List<SqlNode> exprNodeList) {
            if (queryNodeList.size() != exprNodeList.size()) {
                return false;
            }

            List<SqlNode> remExprNodes = Lists.newLinkedList(exprNodeList);
            for (SqlNode queryNode : queryNodeList) {
                int found = -1;
                for (int i = 0; i < remExprNodes.size(); i++) {
                    if (isSqlNodeEqual(queryNode, remExprNodes.get(i))) {
                        found = i;
                        break;
                    }
                }

                if (found == -1) {
                    return false;
                }
                remExprNodes.remove(found);
            }
            return true;
        }

        private boolean isNodeListEqual(List<SqlNode> queryNodeList, List<SqlNode> exprNodeList) {
            if (queryNodeList.size() != exprNodeList.size()) {
                return false;
            }
            for (int i = 0; i < queryNodeList.size(); i++) {
                if (!isSqlNodeEqual(queryNodeList.get(i), exprNodeList.get(i))) {
                    return false;
                }
            }
            return true;
        }
    }

}
