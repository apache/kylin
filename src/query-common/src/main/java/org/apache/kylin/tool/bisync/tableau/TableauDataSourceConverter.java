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
package org.apache.kylin.tool.bisync.tableau;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.Types;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.input.XmlStreamReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModel.Measure;
import org.apache.kylin.tool.bisync.BISyncModelConverter;
import org.apache.kylin.tool.bisync.SyncContext;
import org.apache.kylin.tool.bisync.model.ColumnDef;
import org.apache.kylin.tool.bisync.model.JoinTreeNode;
import org.apache.kylin.tool.bisync.model.MeasureDef;
import org.apache.kylin.tool.bisync.model.SyncModel;
import org.apache.kylin.tool.bisync.tableau.datasource.DrillPath;
import org.apache.kylin.tool.bisync.tableau.datasource.DrillPaths;
import org.apache.kylin.tool.bisync.tableau.datasource.TableauDatasource;
import org.apache.kylin.tool.bisync.tableau.datasource.column.Calculation;
import org.apache.kylin.tool.bisync.tableau.datasource.column.Column;
import org.apache.kylin.tool.bisync.tableau.datasource.connection.Col;
import org.apache.kylin.tool.bisync.tableau.datasource.connection.Cols;
import org.apache.kylin.tool.bisync.tableau.datasource.connection.Connection;
import org.apache.kylin.tool.bisync.tableau.datasource.connection.NamedConnection;
import org.apache.kylin.tool.bisync.tableau.datasource.connection.relation.Clause;
import org.apache.kylin.tool.bisync.tableau.datasource.connection.relation.Expression;
import org.apache.kylin.tool.bisync.tableau.datasource.connection.relation.Relation;
import org.apache.kylin.tool.bisync.tableau.mapping.FunctionMapping;
import org.apache.kylin.tool.bisync.tableau.mapping.Mappings;
import org.apache.kylin.tool.bisync.tableau.mapping.TypeMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class TableauDataSourceConverter implements BISyncModelConverter {

    private static final String ODBC_CONNECTION_PROJECT_PREFIX = "PROJECT=";
    private static final String ODBC_CONNECTION_MODEL_PREFIX = "CUBE=";

    private static final String ODBC_CONN_TDS_TEMPLATE_PATH = "/bisync/tds/tableau.template.xml";
    private static final String CONNECTOR_CONN_TDS_TEMPLATE_PATH = "/bisync/tds/tableau.connector.template.xml";

    private static final Logger logger = LoggerFactory.getLogger(TableauDataSourceConverter.class);

    public static InputStream getResourceAsStream(Class<?> clz, String path) {
        InputStream result = null;

        while (path.startsWith("/")) {
            path = path.substring(1);
        }

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        if (classLoader == null) {
            classLoader = clz.getClassLoader();
            result = classLoader.getResourceAsStream(path);
        } else {
            result = classLoader.getResourceAsStream(path);

            if (result == null) {
                classLoader = clz.getClassLoader();
                if (classLoader != null)
                    result = classLoader.getResourceAsStream(path);
            }
        }
        return result;
    }

    @Override
    public TableauDatasourceModel convert(SyncModel sourceSyncModel, SyncContext syncContext) {
        TableauDatasource tds = getTdsTemplate(syncContext.getTargetBI());
        fillTemplate(tds, sourceSyncModel);
        return new TableauDatasourceModel(tds);
    }

    private TableauDatasource getTdsTemplate(SyncContext.BI targetBI) {
        String templatePath;
        switch (targetBI) {
        case TABLEAU_CONNECTOR_TDS:
            templatePath = CONNECTOR_CONN_TDS_TEMPLATE_PATH;
            break;
        case TABLEAU_ODBC_TDS:
            templatePath = ODBC_CONN_TDS_TEMPLATE_PATH;
            break;
        default:
            throw new IllegalStateException();
        }
        XmlMapper xmlMapper = new XmlMapper();
        try {
            XmlStreamReader reader = new XmlStreamReader(
                    getResourceAsStream(TableauDataSourceConverter.class, templatePath));
            return xmlMapper.readValue(reader, TableauDatasource.class);
        } catch (IOException e) {
            logger.error("can not find file : {}", templatePath, e);
            return null;
        }
    }

    protected void fillTemplate(TableauDatasource tds, SyncModel syncModel) {
        fillConnectionProperties(tds, syncModel.getHost(), syncModel.getPort(), syncModel.getProject(),
                syncModel.getModelName());
        Map<String, Pair<Col, ColumnDef>> colMap = fillCols(tds, syncModel.getColumnDefMap());
        fillColumns(tds, colMap);
        fillJoinTables(tds, syncModel.getJoinTree());
        fillHierarchies(tds, syncModel.getHierarchies(), colMap);
        fillCalculations(tds, syncModel.getMetrics(), colMap);
    }

    private void fillConnectionProperties(TableauDatasource tds, String host, String port, String project,
            String modelName) {
        NamedConnection namedConnection = tds.getTableauConnection().getNamedConnectionList().getNamedConnections()
                .get(0);
        Connection connection = namedConnection.getConnection();
        String connectionStr = ODBC_CONNECTION_PROJECT_PREFIX + project + ";" + ODBC_CONNECTION_MODEL_PREFIX
                + modelName;
        namedConnection.setCaption(host);
        connection.setOdbcConnectStringExtras(connectionStr);
        connection.setServer(host);
        connection.setPort(port);
        connection.setDbName(StringUtils.EMPTY);
        connection.setVendor1(project);
        connection.setVendor2(modelName);
    }

    private void fillCalculations(TableauDatasource tds, List<MeasureDef> metrics,
            Map<String, Pair<Col, ColumnDef>> colMap) {
        List<Column> columns = tds.getColumns();
        if (columns == null) {
            columns = new LinkedList<>();
        }

        for (MeasureDef measureDef : metrics) {
            // TODO check if multi param measure is possible in tableau
            NDataModel.Measure measure = measureDef.getMeasure();
            String mColName = measure.getFunction().getParameters().get(0).getValue();
            Pair<Col, ColumnDef> colPair = colMap.get(mColName);
            String calcFieldName = (colPair == null ? mColName : colPair.getFirst().getKey());
            String dataType = TypeConverter.convertKylinType(measure.getFunction().getReturnType());
            String kylinFuncName = measure.getFunction().getExpression();
            String aggregationFunc = TypeConverter.convertKylinFunction(kylinFuncName);
            String caption = getCaption(measure);
            if (aggregationFunc == null) {
                logger.debug("tableau can not support function : {}", kylinFuncName);
                continue;
            }
            String formula = aggregationFunc + "(" + calcFieldName + ")";
            Calculation calculation = new Calculation();
            calculation.setClassName("tableau");
            calculation.setFormula(formula);

            Column column = new Column();
            column.setHidden(measureDef.isHidden() ? "true" : null);
            column.setCalculation(calculation);
            column.setRole(TdsConstant.ROLE_TYPE_MEASURE);
            column.setName('[' + measure.getName() + ']');
            column.setCaption(caption);
            column.setDatatype(dataType);
            column.setType(TdsConstant.ORDER_TYPE_QUANTITATIVE);
            columns.add(column);
        }

    }

    private String getCaption(Measure measure) {
        return measure.getComment() != null ? measure.getComment() : measure.getName();
    }

    private void fillHierarchies(TableauDatasource tds, Set<String[]> hierarchies,
            Map<String, Pair<Col, ColumnDef>> colMap) {
        DrillPaths drillPaths = new DrillPaths();
        List<DrillPath> drillPathList = new LinkedList<>();

        for (String[] hierarchy : hierarchies) {
            DrillPath drillPath = new DrillPath();
            List<String> fields = new LinkedList<>();
            StringBuilder sb = new StringBuilder();

            for (String column : hierarchy) {
                String filedName = colMap.get(column).getKey().getKey();
                fields.add(filedName);
                sb.append(filedName);
                sb.append(", ");
            }
            String hierarchyName = sb.substring(0, sb.length() - 2);

            drillPath.setFields(fields);
            drillPath.setName(hierarchyName);
            drillPathList.add(drillPath);
        }
        drillPaths.setDrillPathList(drillPathList);
        tds.setDrillPaths(drillPaths);
    }

    private void fillJoinTables(TableauDatasource tds, JoinTreeNode joinTree) {
        Relation relation = createRelation(tds, joinTree);
        tds.getTableauConnection().setRelation(relation);
    }

    private Relation createRelation(TableauDatasource tds, JoinTreeNode joinTree) {
        String connectionName = tds.getTableauConnection().getNamedConnectionList().getNamedConnections().get(0)
                .getName();
        return new RelationBuilder(joinTree, connectionName).build();
    }

    private Map<String, Pair<Col, ColumnDef>> fillCols(TableauDatasource tds, Map<String, ColumnDef> columnMetaMap) {
        Map<String, Pair<Col, ColumnDef>> colMap = new HashMap<>();
        Cols cols = new Cols();
        List<Col> colList = new LinkedList<>();

        // find repeated column in all tables
        Map<String, Integer> colNameRepeatTimes = new HashMap<>();
        for (Map.Entry<String, ColumnDef> entry : columnMetaMap.entrySet()) {
            String fullColName = entry.getKey();
            String colName = fullColName.substring(fullColName.indexOf('.') + 1);
            if (!colNameRepeatTimes.containsKey(colName)) {
                colNameRepeatTimes.put(colName, 1);
            } else {
                Integer repeatTimes = colNameRepeatTimes.get(colName);
                colNameRepeatTimes.put(colName, repeatTimes + 1);
            }
        }

        for (Map.Entry<String, ColumnDef> entry : columnMetaMap.entrySet()) {
            Col col = new Col();
            String fullColName = entry.getKey();
            ColumnDef columnDef = entry.getValue();
            String colName = fullColName.substring(fullColName.indexOf('.') + 1);
            String key = '[' + colName + ']';
            if (colNameRepeatTimes.get(colName) > 1) {
                String tableAlias = columnDef.getTableAlias();
                key = '[' + colName + " (" + tableAlias + ")]";
            }
            String value = '[' + columnDef.getTableAlias() + "].[" + columnDef.getColumnName() + ']';
            col.setKey(key);
            col.setValue(value);

            colList.add(col);
            Pair<Col, ColumnDef> colPair = new Pair<>(col, columnDef);
            colMap.put(fullColName, colPair);
        }
        cols.setCols(colList);
        tds.getTableauConnection().setCols(cols);
        return colMap;
    }

    private void fillColumns(TableauDatasource tds, Map<String, Pair<Col, ColumnDef>> colMap) {
        List<Column> columns = new LinkedList<>();
        for (Map.Entry<String, Pair<Col, ColumnDef>> entry : colMap.entrySet()) {
            Column column = new Column();

            String colName = entry.getValue().getFirst().getKey();
            ColumnDef columnDef = entry.getValue().getSecond();
            String role = columnDef.getRole();
            String dataType = TypeConverter.convertKylinType(columnDef.getColumnType());
            String hidden = columnDef.isHidden() ? "true" : null;
            String columnAlias = (columnDef.getColumnAlias() == null ? colName.substring(1, colName.length() - 1)
                    : columnDef.getColumnAlias());

            column.setName(colName);
            column.setCaption(columnAlias);
            column.setRole(role);
            column.setDatatype(dataType);
            column.setType(TypeConverter.getOrderType(role, dataType));
            column.setHidden(hidden);
            columns.add(column);
        }
        tds.setColumns(columns);
    }

    public static class RelationBuilder {

        private JoinTreeNode joinTree;

        private String connectionName;

        public RelationBuilder(JoinTreeNode joinTree, String connectionName) {
            this.joinTree = joinTree;
            this.connectionName = connectionName;
        }

        public Relation build() {
            if (joinTree == null) {
                return null;
            } else {
                return convertTree2Relation(joinTree);
            }
        }

        private Relation convertTree2Relation(JoinTreeNode joinTree) {
            if (joinTree == null || joinTree.getValue() == null) {
                return null;
            }
            List<JoinTableDesc> tableDescs = joinTree.iteratorAsList();
            Relation left = buildRelationTable(tableDescs.get(0));
            for (int i = 1; i < tableDescs.size(); i++) {
                left = buildJoinRelation(left, tableDescs.get(i));
            }
            return left;
        }

        private Relation buildJoinRelation(Relation leftTable, JoinTableDesc rightJoin) {
            Relation rightTable = buildRelationTable(rightJoin);
            Relation joinRelation = new Relation();
            List<Relation> relations = new LinkedList<>();
            JoinDesc joinDesc = rightJoin.getJoin();
            String joinType = joinDesc.getType().toLowerCase(Locale.ROOT);
            joinRelation.setType(TdsConstant.JOIN_TYPE_JOIN);
            joinRelation.setJoin(joinType);
            relations.add(leftTable);
            relations.add(rightTable);
            joinRelation.setRelationList(relations);
            joinRelation.setClause(buildJoinClause(joinDesc));
            return joinRelation;
        }

        private Relation buildRelationTable(JoinTableDesc table) {
            Relation relation = new Relation();
            relation.setType(TdsConstant.JOIN_TYPE_TABLE);
            relation.setConnection(this.connectionName);
            relation.setName(table.getAlias());
            relation.setTable(formatName(table.getTable()));
            return relation;
        }

        private Clause buildJoinClause(JoinDesc joinDesc) {
            String[] pks = joinDesc.getPrimaryKey();
            String[] fks = joinDesc.getForeignKey();
            if (pks.length == 0) {
                return null;
            } else {
                Clause clause = new Clause();
                clause.setType("join");
                if (pks.length == 1) {
                    clause.setExpression(buildExpression(formatName(fks[0]), formatName(pks[0])));
                } else {
                    Expression expression = new Expression();
                    expression.setOp("AND");
                    List<Expression> expressionList = new LinkedList<>();
                    for (int i = 0; i < pks.length; i++) {
                        expressionList.add(buildExpression(formatName(fks[i]), formatName(pks[i])));
                    }
                    expression.setExpressionList(expressionList);
                    clause.setExpression(expression);
                }
                return clause;
            }
        }

        private Expression buildExpression(String left, String right) {
            Expression expression = new Expression();
            Expression leftExp = new Expression();
            Expression rightExp = new Expression();
            List<Expression> expressionList = new LinkedList<>();
            expression.setOp("=");
            leftExp.setOp(left);
            rightExp.setOp(right);
            expressionList.add(leftExp);
            expressionList.add(rightExp);
            expression.setExpressionList(expressionList);
            return expression;
        }

        private String formatName(String origin) {
            int index = origin.indexOf('.');
            if (index == -1) {
                return origin;
            } else {
                String left = origin.substring(0, index);
                String right = origin.substring(index + 1);
                return "[" + left + "].[" + right + "]";
            }
        }
    }

    public static class TypeConverter {
        private static final Map<Integer, String> TYPE_MAP;
        private static final Map<String, String> TYPE_NAME_MAP;
        private static final Map<String, String> FUNC_MAP;
        private static final Map<String, Integer> TYPE_VALUES_MAP;

        static {
            Class<Types> clazz = java.sql.Types.class;
            Field[] fields = java.sql.Types.class.getDeclaredFields();
            TYPE_VALUES_MAP = new HashMap<>(fields.length);
            TYPE_MAP = new HashMap<>();
            FUNC_MAP = new HashMap<>();
            TYPE_NAME_MAP = new HashMap<>();

            try {
                // load java.sql.types fields
                for (Field field : fields) {
                    TYPE_VALUES_MAP.put(field.getName().toUpperCase(Locale.ROOT), field.getInt(clazz));
                }

                String filePath = "/bisync/tds/tableau.mappings.xml";
                XmlMapper xmlMapper = new XmlMapper();
                XmlStreamReader reader = new XmlStreamReader(
                        getResourceAsStream(TableauDataSourceConverter.class, filePath));
                Mappings mappings = xmlMapper.readValue(reader, Mappings.class);

                // load dataType mappings
                for (TypeMapping mapping : mappings.getTypeMappings()) {
                    TYPE_NAME_MAP.put(mapping.getKylinType().toUpperCase(Locale.ROOT), mapping.getTargetType());
                    TYPE_MAP.put(TYPE_VALUES_MAP.get(mapping.getKylinType().toUpperCase(Locale.ROOT)),
                            mapping.getTargetType());
                }

                // load function mappings
                for (FunctionMapping functionMapping : mappings.getFuncMappings()) {
                    FUNC_MAP.put(functionMapping.getKylinFuncName().toUpperCase(Locale.ROOT),
                            functionMapping.getTargetFunName());
                }
            } catch (IllegalAccessException | IOException e) {
                logger.error("can not init tableau mappings", e);
            }
        }

        private TypeConverter() {
        }

        public static String convertKylinType(String typeName) {
            String trimmedTypeName = typeName.trim().toUpperCase(Locale.ROOT);
            if (typeName.indexOf('(') > -1) {
                trimmedTypeName = trimmedTypeName.substring(0, trimmedTypeName.indexOf('(')); // strip off brackets
            }
            return TYPE_NAME_MAP.get(trimmedTypeName);
        }

        public static String convertKylinFunction(String funcName) {
            return FUNC_MAP.get(funcName.toUpperCase(Locale.ROOT));
        }

        public static String getOrderType(String role, String dataType) {
            if (role == null || dataType == null) {
                return null;
            }
            if (role.equals(TdsConstant.ROLE_TYPE_DIMENSION)) {
                if (dataType.equals(TdsConstant.DATA_TYPE_DATE) || dataType.equals(TdsConstant.DATA_TYPE_INTEGER)
                        || dataType.equals(TdsConstant.DATA_TYPE_REAL)) {
                    return TdsConstant.ORDER_TYPE_ORDINAL;
                }
            } else if (role.equals(TdsConstant.ROLE_TYPE_MEASURE)) {
                return TdsConstant.ORDER_TYPE_QUANTITATIVE;
            }

            return TdsConstant.ORDER_TYPE_NOMINAL;
        }
    }

    public static class TdsConstant {
        // data type
        public static final String DATA_TYPE_INTEGER = "integer";
        public static final String DATA_TYPE_DATE = "date";
        public static final String DATA_TYPE_REAL = "real";
        public static final String DATA_TYPE_STRING = "string";
        // join type
        public static final String JOIN_TYPE_JOIN = "join";
        public static final String JOIN_TYPE_TABLE = "table";
        // order type
        public static final String ORDER_TYPE_NOMINAL = "nominal";
        public static final String ORDER_TYPE_ORDINAL = "ordinal";
        public static final String ORDER_TYPE_QUANTITATIVE = "quantitative";
        // role type
        public static final String ROLE_TYPE_DIMENSION = "dimension";
        public static final String ROLE_TYPE_MEASURE = "measure";

        private TdsConstant() {
        }
    }
}
