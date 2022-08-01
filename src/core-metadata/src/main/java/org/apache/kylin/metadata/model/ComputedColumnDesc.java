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

package org.apache.kylin.metadata.model;

import static org.apache.kylin.common.exception.ServerErrorCode.COLUMN_NOT_EXIST;

import java.io.Serializable;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.common.util.ModifyTableNameSqlVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ComputedColumnDesc implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ComputedColumnDesc.class);

    private static final String CC_PREFIX = "_CC_";

    // the table identity DB.TABLE (ignoring alias) in the model where the computed column belong to
    // this field is more useful for frontend, for backend code, usage should be avoided
    @JsonProperty
    private String tableIdentity;

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String tableAlias;
    @JsonProperty
    private String columnName; // the new col name
    @JsonProperty
    private String expression;
    @JsonProperty
    private String innerExpression; // QueryUtil massaged expression
    @JsonProperty
    private String datatype;
    @JsonProperty
    @EqualsAndHashCode.Exclude
    private String comment;
    @JsonProperty("rec_uuid")
    @EqualsAndHashCode.Exclude
    private String uuid;

    public void init(NDataModel model, String rootFactTableName) {
        Map<String, TableRef> aliasMap = model.getAliasMap();
        Set<String> aliasSet = aliasMap.keySet();

        Preconditions.checkNotNull(tableIdentity, "tableIdentity is null");
        Preconditions.checkNotNull(columnName, "columnName is null");
        if (!model.isSeekingCCAdvice())
            Preconditions.checkNotNull(expression, "expression is null");
        Preconditions.checkNotNull(datatype, "datatype is null");

        if (tableAlias == null) // refer to comment of handleLegacyCC()
            tableAlias = tableIdentity.substring(tableIdentity.indexOf('.') + 1);

        Preconditions.checkState(tableIdentity.equals(tableIdentity.trim()),
                "tableIdentity of ComputedColumnDesc has heading/tailing whitespace");
        Preconditions.checkState(tableAlias.equals(tableAlias.trim()),
                "tableAlias of ComputedColumnDesc has heading/tailing whitespace");
        Preconditions.checkState(columnName.equals(columnName.trim()),
                "columnName of ComputedColumnDesc has heading/tailing whitespace");
        Preconditions.checkState(datatype.equals(datatype.trim()),
                "datatype of ComputedColumnDesc has heading/tailing whitespace");

        tableIdentity = tableIdentity.toUpperCase(Locale.ROOT);
        tableAlias = tableAlias.toUpperCase(Locale.ROOT);
        columnName = columnName.toUpperCase(Locale.ROOT);

        TableRef hostTblRef = model.findTable(tableAlias);
        if (!model.isFactTable(hostTblRef)) {
            throw new IllegalArgumentException(
                    "Computed column has to be defined on fact table or limited lookup table");
        }

        if ("true".equals(System.getProperty("needCheckCC"))) {
            try {
                simpleParserCheck(expression, aliasSet);
            } catch (Exception e) {
                String legacyHandled = handleLegacyCC(expression, rootFactTableName, aliasSet);
                if (legacyHandled != null) {
                    expression = legacyHandled;
                } else {
                    throw e;
                }
            }
        }
    }

    @VisibleForTesting
    public static String getComputedColumnInternalNamePrefix() {
        return CC_PREFIX;
    }

    public static String getOriginCcName(String ccNameWithPrefix) {
        return ccNameWithPrefix.startsWith(ComputedColumnDesc.CC_PREFIX)
                ? ccNameWithPrefix.replaceFirst(ComputedColumnDesc.CC_PREFIX, "")
                : ccNameWithPrefix;
    }

    public String getInternalCcName() {
        return ComputedColumnDesc.CC_PREFIX + columnName;
    }

    private String handleLegacyCC(String expr, String rootFact, Set<String> aliasSet) {
        try {
            CalciteParser.ensureNoAliasInExpr(expr);
            String ret = CalciteParser.insertAliasInExpr(expr, rootFact);
            simpleParserCheck(ret, aliasSet);
            return ret;
        } catch (Exception e) {
            logger.error("failed to handle legacy CC '{}' for {}", expr, e.getMessage());
            return null;
        }
    }

    public static void simpleParserCheck(final String expr, final Set<String> aliasSet) {

        SqlNode sqlNode = CalciteParser.getReadonlyExpNode(expr);

        SqlVisitor<Object> sqlVisitor = new SqlBasicVisitor<Object>() {
            @Override
            public Object visit(SqlIdentifier id) {
                if (id.names.size() != 2 || !aliasSet.contains(id.names.get(0))) {
                    throw new KylinException(COLUMN_NOT_EXIST,
                            String.format(Locale.ROOT, MsgPicker.getMsg().getColumnUnrecognized(), id));
                }
                return null;
            }

            @Override
            public Object visit(SqlCall call) {
                if (call instanceof SqlBasicCall) {
                    if (call.getOperator() instanceof SqlAsOperator) {
                        throw new IllegalArgumentException("Computed column expression should not contain keyword AS");
                    }

                    if (call.getOperator() instanceof SqlAggFunction
                            || MeasureTypeFactory.getUDAFs().containsKey(call.getOperator().getName())) {
                        throw new IllegalArgumentException(
                                "Computed column expression should not contain any aggregate functions: "
                                        + call.getOperator().getName());
                    }
                }
                return call.getOperator().acceptCall(this, call);
            }
        };

        sqlNode.accept(sqlVisitor);
    }

    public String getFullName() {
        return tableAlias + "." + columnName;
    }

    public String getIdentName() {
        return tableIdentity + "." + columnName;
    }

    public void setInnerExpression(String innerExpression) {
        this.innerExpression = innerExpression;
    }

    public String getInnerExpression() {
        if (StringUtils.isEmpty(innerExpression)) {
            return expression;
        }
        return innerExpression;
    }

    public void changeTableAlias(String oldAlias, String newAlias) {
        SqlVisitor<Object> modifyAlias = new ModifyTableNameSqlVisitor(oldAlias, newAlias);
        SqlNode sqlNode = CalciteParser.getExpNode(getExpression());
        sqlNode.accept(modifyAlias);
        setExpression(sqlNode.toSqlString(HiveSqlDialect.DEFAULT).toString());
    }

}
