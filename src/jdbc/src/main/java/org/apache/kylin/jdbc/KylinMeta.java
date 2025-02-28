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

package org.apache.kylin.jdbc;

import static org.apache.kylin.jdbc.LoggerUtils.entry;
import static org.apache.kylin.jdbc.LoggerUtils.exit;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Avatica interface
 */
public class KylinMeta extends MetaImpl {

    private static final Logger logger = LoggerFactory.getLogger(KylinMeta.class);
    private KMetaProject metaProject;

    public KylinMeta(KylinConnection connection) {
        super(connection);
        entry(logger);
        exit(logger);
    }

    private KylinConnection connection() {
        return (KylinConnection) connection;
    }

    // insert/update/delete go this path, ignorable for Kylin
    @Override
    public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
        entry(logger);
        StatementHandle result = super.createStatement(ch);
        result.signature = connection().mockPreparedSignature(sql);
        exit(logger);
        return result;
    }

    @Override
    public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle sh, List<String> sqlCommands) {
        entry(logger);
        ExecuteBatchResult result = new ExecuteBatchResult(new long[] {});
        exit(logger);
        return result;
    }

    @Override
    public ExecuteBatchResult executeBatch(StatementHandle sh, List<List<TypedValue>> parameterValues)
            throws NoSuchStatementException {
        entry(logger);
        ExecuteBatchResult result = new ExecuteBatchResult(new long[] {});
        exit(logger);
        return result;
    }

    /**
     * @deprecated (2022-11-20, real execution happens in KylinResultSet.execute())
     */
    @Override
    @Deprecated
    public ExecuteResult execute(StatementHandle sh, List<TypedValue> parameterValues, long maxRowCount)
            throws NoSuchStatementException {
        entry(logger);
        final MetaResultSet metaResultSet = MetaResultSet.create(sh.connectionId, sh.id, false, sh.signature, null);
        ExecuteResult result = new ExecuteResult(Collections.singletonList(metaResultSet));
        exit(logger);
        return result;
    }

    @Override
    public ExecuteResult execute(StatementHandle sh, List<TypedValue> parameterValues, int maxRowsInFirstFrame)
            throws NoSuchStatementException {
        entry(logger);
        final MetaResultSet metaResultSet = MetaResultSet.create(sh.connectionId, sh.id, false, sh.signature, null);
        ExecuteResult result = new ExecuteResult(Collections.singletonList(metaResultSet));
        exit(logger);
        return result;
    }

    /**
     * @deprecated (2022-11-20, mimic from CalciteMetaImpl, real execution happens via callback in KylinResultSet.execute())
     */
    @Override
    @Deprecated
    public ExecuteResult prepareAndExecute(StatementHandle sh, String sql, long maxRowCount, PrepareCallback callback) {
        entry(logger);
        try {
            synchronized (callback.getMonitor()) {
                callback.clear();
                sh.signature = connection().mockPreparedSignature(sql);
                callback.assign(sh.signature, null, -1);
            }
            callback.execute();
            final MetaResultSet metaResultSet = MetaResultSet.create(sh.connectionId, sh.id, false, sh.signature, null);
            ExecuteResult result = new ExecuteResult(Collections.singletonList(metaResultSet));
            exit(logger);
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle sh, String sql, long maxRowCount, int maxRowsInFirstFrame,
            PrepareCallback callback) throws NoSuchStatementException {
        entry(logger);
        try {
            synchronized (callback.getMonitor()) {
                callback.clear();
                sh.signature = connection().mockPreparedSignature(sql);
                callback.assign(sh.signature, null, -1);
            }
            callback.execute();
            final MetaResultSet metaResultSet = MetaResultSet.create(sh.connectionId, sh.id, false, sh.signature, null);
            ExecuteResult result = new ExecuteResult(Collections.singletonList(metaResultSet));
            exit(logger);
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeStatement(StatementHandle h) {
        entry(logger);
        // nothing to do
        exit(logger);
    }

    private KMetaProject getMetaProject() {
        entry(logger);
        try {
            if (metaProject == null) {
                KylinConnection conn = connection();
                metaProject = conn.getRemoteClient().retrieveMetaData(conn.getProject());
            }
            exit(logger);
            return metaProject;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getTableTypes(ConnectionHandle ch) {
        entry(logger);
        MetaResultSet resultSet = createResultSet(metaTableTypes, MetaTableType.class, "TABLE_TYPE");
        exit(logger);
        return resultSet;
    }

    @Override
    public MetaResultSet getCatalogs(ConnectionHandle ch) {
        entry(logger);
        List<KMetaCatalog> catalogs = getMetaProject().catalogs;
        MetaResultSet resultSet = createResultSet(catalogs, KMetaCatalog.class, "TABLE_CAT");
        exit(logger);
        return resultSet;
    }

    @Override
    public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
        entry(logger);
        List<KMetaSchema> schemas = getMetaProject().getSchemas(catalog, schemaPattern);
        MetaResultSet resultSet = createResultSet(schemas, KMetaSchema.class, "TABLE_SCHEM", "TABLE_CATALOG");
        entry(logger);
        return resultSet;
    }

    @Override
    public MetaResultSet getTables(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern,
            List<String> typeList) {
        entry(logger);
        List<KMetaTable> tables = getMetaProject().getTables(catalog, schemaPattern, tableNamePattern, typeList);
        MetaResultSet resultSet = createResultSet(tables, KMetaTable.class, //
                "TABLE_CAT", //
                "TABLE_SCHEM", //
                "TABLE_NAME", //
                "TABLE_TYPE", //
                "REMARKS", //
                "TYPE_CAT", //
                "TYPE_SCHEM", //
                "TYPE_NAME", //
                "SELF_REFERENCING_COL_NAME", //
                "REF_GENERATION");
        exit(logger);
        return resultSet;
    }

    @Override
    public MetaResultSet getColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern,
            Pat columnNamePattern) {
        entry(logger);
        List<KMetaColumn> columns = getMetaProject().getColumns(catalog, schemaPattern, tableNamePattern,
                columnNamePattern);
        MetaResultSet resultSet = createResultSet(columns, KMetaColumn.class, //
                "TABLE_CAT", //
                "TABLE_SCHEM", //
                "TABLE_NAME", //
                "COLUMN_NAME", //
                "DATA_TYPE", //
                "TYPE_NAME", //
                "COLUMN_SIZE", //
                "BUFFER_LENGTH", //
                "DECIMAL_DIGITS", //
                "NUM_PREC_RADIX", //
                "NULLABLE", //
                "REMARKS", //
                "COLUMN_DEF", //
                "SQL_DATA_TYPE", //
                "SQL_DATETIME_SUB", //
                "CHAR_OCTET_LENGTH", //
                "ORDINAL_POSITION", //
                "IS_NULLABLE", //
                "SCOPE_CATALOG", //
                "SCOPE_TABLE", //
                "SOURCE_DATA_TYPE", //
                "IS_AUTOINCREMENT", //
                "IS_GENERATEDCOLUMN");
        exit(logger);
        return resultSet;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private MetaResultSet createResultSet(List iterable, Class clazz, String... names) {
        entry(logger);
        final List<ColumnMetaData> columns = new ArrayList<>();
        final List<Field> fields = new ArrayList<>();
        final List<String> fieldNames = new ArrayList<>();
        for (String name : names) {
            final int index = fields.size();
            final String fieldName = AvaticaUtils.toCamelCase(name);
            final Field field;
            try {
                field = clazz.getField(fieldName);
            } catch (NoSuchFieldException e) {

                throw new RuntimeException(e);
            }
            columns.add(columnMetaData(name, index, field.getType(), true));
            fields.add(field);
            fieldNames.add(fieldName);
        }

        CursorFactory cursorFactory = CursorFactory.record(clazz, fields, fieldNames);
        Signature signature = new Signature(columns, "", null, Collections.<String, Object> emptyMap(), cursorFactory,
                StatementType.SELECT);
        StatementHandle sh = this.createStatement(connection().handle);
        Frame frame = new Frame(0, true, iterable);
        MetaResultSet resultSet = MetaResultSet.create(connection().id, sh.id, true, signature, frame);
        exit(logger);
        return resultSet;
    }

    // ============================================================================

    public static interface NamedWithChildren extends Named {
        List<? extends NamedWithChildren> getChildren();
    }

    public static List<? extends NamedWithChildren> searchByPatterns(NamedWithChildren parent, Pat... patterns) {
        if (patterns == null || patterns.length <= 0) {
            throw new AssertionError();
        }

        List<? extends NamedWithChildren> children = findChildren(parent, patterns[0]);
        if (patterns.length == 1) {
            return children;
        } else {
            List<NamedWithChildren> result = new ArrayList<>();
            Pat[] subPatterns = Arrays.copyOfRange(patterns, 1, patterns.length);
            for (NamedWithChildren c : children) {
                result.addAll(searchByPatterns(c, subPatterns));
            }
            return result;
        }
    }

    private static List<? extends NamedWithChildren> findChildren(NamedWithChildren parent, Pat pattern) {
        if (null == pattern.s || pattern.s.equals("%")) {
            return parent.getChildren();
        }

        List<NamedWithChildren> result = new ArrayList<>();
        Pattern regex = likeToRegex(pattern);

        for (NamedWithChildren c : parent.getChildren()) {
            if (regex.matcher(c.getName()).matches()) {
                result.add(c);
            }
        }
        return result;
    }

    /**
     * Converts a LIKE-style pattern (where '%' represents a wild-card,
     * escaped using '\') to a Java regex.
     */
    private static Pattern likeToRegex(Pat pattern) {
        StringBuilder buf = new StringBuilder("^");
        char[] charArray = pattern.s.toCharArray();
        int slash = -2;
        for (int i = 0; i < charArray.length; i++) {
            char c = charArray[i];
            if (slash == i - 1) {
                buf.append('[').append(c).append(']');
            } else {
                switch (c) {
                case '\\':
                    slash = i;
                    break;
                case '%':
                    buf.append(".*");
                    break;
                case '[':
                    buf.append("\\[");
                    break;
                case ']':
                    buf.append("\\]");
                    break;
                default:
                    buf.append('[').append(c).append(']');
                }
            }
        }
        buf.append("$");

        return Pattern.compile(buf.toString());
    }

    // ============================================================================

    private static final List<MetaTableType> metaTableTypes = new ArrayList<MetaTableType>();
    static {
        metaTableTypes.add(new MetaTableType("TABLE"));
    }

    public static class KMetaProject implements NamedWithChildren {
        public final String projectName;
        public final List<KMetaCatalog> catalogs;

        public KMetaProject(String projectName, List<KMetaCatalog> catalogs) {
            this.projectName = projectName;
            this.catalogs = catalogs;
        }

        @SuppressWarnings("unchecked")
        public List<KMetaSchema> getSchemas(String catalog, Pat schemaPattern) {
            return (List<KMetaSchema>) searchByPatterns(this, Pat.of(catalog), schemaPattern);
        }

        @SuppressWarnings("unchecked")
        public List<KMetaTable> getTables(String catalog, Pat schemaPattern, Pat tableNamePattern,
                List<String> typeList) {
            return (List<KMetaTable>) searchByPatterns(this, Pat.of(catalog), schemaPattern, tableNamePattern);
        }

        @SuppressWarnings("unchecked")
        public List<KMetaColumn> getColumns(String catalog, Pat schemaPattern, Pat tableNamePattern,
                Pat columnNamePattern) {
            return (List<KMetaColumn>) searchByPatterns(this, Pat.of(catalog), schemaPattern, tableNamePattern,
                    columnNamePattern);
        }

        @Override
        public String getName() {
            return projectName;
        }

        @Override
        public List<? extends NamedWithChildren> getChildren() {
            return catalogs;
        }
    }

    public static class KMetaCatalog implements NamedWithChildren {
        public final String tableCat;
        public final List<KMetaSchema> schemas;

        public KMetaCatalog(String tableCatalog, List<KMetaSchema> schemas) {
            this.tableCat = tableCatalog;
            this.schemas = schemas;
        }

        @Override
        public String getName() {
            return tableCat;
        }

        @Override
        public List<? extends NamedWithChildren> getChildren() {
            return schemas;
        }
    }

    public static class KMetaSchema extends MetaSchema implements NamedWithChildren {
        public final List<KMetaTable> tables;

        public KMetaSchema(String tableCatalog, String tableSchem, List<KMetaTable> tables) {
            super(tableCatalog, tableSchem);
            this.tables = tables;
        }

        @Override
        public List<? extends NamedWithChildren> getChildren() {
            return tables;
        }
    }

    public static class KMetaTable extends MetaTable implements NamedWithChildren {
        public final List<KMetaColumn> columns;

        public KMetaTable(String tableCat, String tableSchem, String tableName, String tableType,
                List<KMetaColumn> columns) {
            super(tableCat, tableSchem, tableName, tableType);
            this.columns = columns;
        }

        @Override
        public List<? extends NamedWithChildren> getChildren() {
            return columns;
        }
    }

    public static class KMetaColumn implements NamedWithChildren {
        public final String tableCat;
        public final String tableSchem;
        @MetaImpl.ColumnNoNulls
        public final String tableName;
        @MetaImpl.ColumnNoNulls
        public final String columnName;
        public final int dataType;
        @MetaImpl.ColumnNoNulls
        public final String typeName;
        public final Integer columnSize;
        @MetaImpl.ColumnNullableUnknown
        public final Integer bufferLength = null;
        public final Integer decimalDigits;
        public final Integer numPrecRadix;
        public final int nullable;
        public final String remarks;
        public final String columnDef = null;
        @MetaImpl.ColumnNullableUnknown
        public final Integer sqlDataType = null;
        @MetaImpl.ColumnNullableUnknown
        public final Integer sqlDatetimeSub = null;
        public final Integer charOctetLength;
        public final int ordinalPosition;
        @MetaImpl.ColumnNoNulls
        public final String isNullable;
        public final String scopeCatalog = null;
        public final String scopeSchema = null;
        public final String scopeTable = null;
        public final Short sourceDataType = null;
        @MetaImpl.ColumnNoNulls
        public final String isAutoincrement = "";
        @MetaImpl.ColumnNoNulls
        public final String isGeneratedcolumn = "";

        public KMetaColumn(String tableCat, String tableSchem, String tableName, String columnName, int dataType,
                String typeName, int columnSize, Integer decimalDigits, int numPrecRadix, int nullable,
                int charOctetLength, int ordinalPosition, String isNullable, String remarks) {
            this.tableCat = tableCat;
            this.tableSchem = tableSchem;
            this.tableName = tableName;
            this.columnName = columnName;
            this.dataType = dataType;
            this.typeName = typeName;
            this.columnSize = columnSize;
            this.decimalDigits = decimalDigits;
            this.numPrecRadix = numPrecRadix;
            this.nullable = nullable;
            this.charOctetLength = charOctetLength;
            this.ordinalPosition = ordinalPosition;
            this.isNullable = isNullable;
            this.remarks = remarks;
        }

        @Override
        public String getName() {
            return this.columnName;
        }

        @Override
        public List<NamedWithChildren> getChildren() {
            return Collections.emptyList();
        }
    }

    @Override
    public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean syncResults(StatementHandle sh, QueryState state, long offset) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void commit(ConnectionHandle ch) {
        // TODO Auto-generated method stub

    }

    @Override
    public void rollback(ConnectionHandle ch) {
        // TODO Auto-generated method stub

    }
}
