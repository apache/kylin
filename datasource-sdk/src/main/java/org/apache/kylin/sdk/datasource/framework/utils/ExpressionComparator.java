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
package org.apache.kylin.sdk.datasource.framework.utils;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class ExpressionComparator {

    private static final Logger logger = LoggerFactory.getLogger(ExpressionComparator.class);


    public static boolean isNodeEqual(SqlNode queryNode, SqlNode exprNode, SqlNodeComparator nodeComparator) {
        try {
            Preconditions.checkNotNull(nodeComparator);
            return nodeComparator.isSqlNodeEqual(queryNode, exprNode);
        } catch (Exception e) {
            logger.error("Exception while running isNodeEqual, return false", e);
            return false;
        }
    }



    public static abstract class SqlNodeComparator {
        protected abstract boolean isSqlIdentifierEqual(SqlIdentifier querySqlIdentifier,
                SqlIdentifier exprSqlIdentifier);

        public boolean isSqlNodeEqual(SqlNode queryNode, SqlNode exprNode) {
            if (queryNode == null) {
                return exprNode == null;
            } else if (exprNode == null) {
                return false;
            }

            if (!Objects.equals(queryNode.getClass().getSimpleName(), exprNode.getClass().getSimpleName())) {
                return false;
            }

            if (queryNode instanceof SqlCall) {
                if (!(exprNode instanceof SqlCall)) {
                    return false;
                }

                SqlCall thisNode = (SqlCall) queryNode;
                SqlCall thatNode = (SqlCall) exprNode;

                if (!thisNode.getOperator().getName().equalsIgnoreCase(thatNode.getOperator().getName())) {
                    return false;
                }
                return isNodeListEqual(thisNode.getOperandList(), thatNode.getOperandList());
            }
            if (queryNode instanceof SqlLiteral) {
                if (!(exprNode instanceof SqlLiteral)) {
                    return false;
                }

                SqlLiteral thisNode = (SqlLiteral) queryNode;
                SqlLiteral thatNode = (SqlLiteral) exprNode;

                return Objects.equals(thisNode.getValue(), thatNode.getValue());
            }
            if (queryNode instanceof SqlNodeList) {
                if (!(exprNode instanceof SqlNodeList)) {
                    return false;
                }

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
                if (!(exprNode instanceof SqlIdentifier)) {
                    return false;
                }
                SqlIdentifier thisNode = (SqlIdentifier) queryNode;
                SqlIdentifier thatNode = (SqlIdentifier) exprNode;

                return isSqlIdentifierEqual(thisNode, thatNode);
            }

            if (queryNode instanceof SqlDataTypeSpec) {
                if (!(exprNode instanceof SqlDataTypeSpec))
                    return false;

                SqlDataTypeSpec thisNode = (SqlDataTypeSpec) queryNode;
                SqlDataTypeSpec thatNode = (SqlDataTypeSpec) exprNode;
                return isSqlDataTypeSpecEqual(thisNode, thatNode);
            }

            return false;
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

        protected boolean isNodeListEqual(List<SqlNode> queryNodeList, List<SqlNode> exprNodeList) {
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
