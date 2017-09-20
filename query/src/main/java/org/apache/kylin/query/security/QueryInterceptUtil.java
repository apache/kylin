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

package org.apache.kylin.query.security;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPTableScan;

import com.google.common.base.Preconditions;

public class QueryInterceptUtil {
    private static List<QueryIntercept> queryIntercepts = new ArrayList<>();

    private static void setQueryIntercept() {
        if (queryIntercepts.size() > 0) {
            return;
        }
        String[] classes = KylinConfig.getInstanceFromEnv().getQueryIntercept();
        for (String clz : classes) {
            try {
                QueryIntercept i = (QueryIntercept) ClassUtil.newInstance(clz);
                queryIntercepts.add(i);
            } catch (Exception e) {
                throw new RuntimeException("Failed to load query intercept", e);
            }
        }
    }

    public static List<QueryIntercept> getQueryIntercepts() {
        setQueryIntercept();
        return queryIntercepts;
    }

    public static Set<String> getAllColsWithTblAndSchema(String project, List<OLAPContext> contexts) {
        // all columns with table and DB. Like DB.TABLE.COLUMN
        Set<String> allColWithTblAndSchema = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        for (OLAPContext context : contexts) {
            for (TblColRef tblColRef : context.allColumns) {
                ColumnDesc columnDesc = tblColRef.getColumnDesc();
                //computed column
                if (columnDesc.isComputedColumnn()) {
                    allColWithTblAndSchema.addAll(getCCUsedCols(project, columnDesc));
                }
                //normal column
                allColWithTblAndSchema.add(tblColRef.getColumWithTableAndSchema());
            }
        }
        return allColWithTblAndSchema;
    }

    private static Set<String> getCCUsedCols(String project, ColumnDesc columnDesc) {
        Set<String> usedCols = new HashSet<>();
        Map<String, String> aliasTableMap = getAliasTableMap(project, columnDesc.getName());
        Preconditions.checkState(aliasTableMap.size() > 0, "can not find cc:" + columnDesc.getName() + "'s table alias");

        List<Pair<String, String>> colsWithAlias = ExprIdentifierFinder.getExprIdentifiers(columnDesc.getComputedColumnExpr());
        for (Pair<String, String> cols : colsWithAlias) {
            String tableIdentifier = aliasTableMap.get(cols.getFirst());
            usedCols.add(tableIdentifier + "." + cols.getSecond());
        }
        //Preconditions.checkState(usedCols.size() > 0, "can not find cc:" + columnDesc.getName() + "'s used cols");
        return usedCols;
    }

    private static  Map<String, String> getAliasTableMap(String project, String ccName) {
        DataModelDesc model = getModel(project, ccName);
        Map<String, String> tableWithAlias = new HashMap<>();
        for (String alias : model.getAliasMap().keySet()) {
            String tableName = model.getAliasMap().get(alias).getTableDesc().getIdentity();
            tableWithAlias.put(alias, tableName);
        }
        return tableWithAlias;
    }

    private static  DataModelDesc getModel(String project, String ccName) {
        List<DataModelDesc> models = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getModels(project);
        for (DataModelDesc model : models) {
            Set<String> computedColumnNames = model.getComputedColumnNames();
            if (computedColumnNames.contains(ccName)) {
                return model;
            }
        }
        return null;
    }

    static class ExprIdentifierFinder extends SqlBasicVisitor<SqlNode> {
        List<Pair<String, String>> columnWithTableAlias;

        ExprIdentifierFinder() {
            this.columnWithTableAlias = new ArrayList<>();
        }

        List<Pair<String, String>> getIdentifiers() {
            return columnWithTableAlias;
        }

        static List<Pair<String, String>> getExprIdentifiers(String expr) {
            SqlNode exprNode = CalciteParser.getExpNode(expr);
            ExprIdentifierFinder id = new ExprIdentifierFinder();
            exprNode.accept(id);
            return id.getIdentifiers();
        }

        @Override
        public SqlNode visit(SqlCall call) {
            for (SqlNode operand : call.getOperandList()) {
                if (operand != null) {
                    operand.accept(this);
                }
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            //Preconditions.checkState(id.names.size() == 2, "error when get identifier in cc's expr");
            if (id.names.size() == 2) {
                columnWithTableAlias.add(Pair.newPair(id.names.get(0), id.names.get(1)));
            }
            return null;
        }
    }

    public static Set<String> getAllTblsWithSchema(List<OLAPContext> contexts) {
        // all tables with DB, Like DB.TABLE
        Set<String> tableWithSchema = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (OLAPContext context : contexts) {
            for (OLAPTableScan tableScan : context.allTableScans) {
                tableWithSchema.add(tableScan.getTableRef().getTableIdentity());
            }
        }
        return tableWithSchema;
    }
}