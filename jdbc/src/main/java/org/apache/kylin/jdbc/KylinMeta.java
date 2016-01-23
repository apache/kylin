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
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;

import com.google.common.collect.ImmutableList;

/**
 * Implementation of Avatica interface
 */
public class KylinMeta extends MetaImpl {

    private KMetaProject metaProject;

    public KylinMeta(KylinConnection connection) {
        super(connection);
    }

    private KylinConnection connection() {
        return (KylinConnection) connection;
    }

    // insert/update/delete go this path, ignorable for Kylin
    @Override
    public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
        StatementHandle result = super.createStatement(ch);
        result.signature = connection().mockPreparedSignature(sql);
        return result;
    }

    // real execution happens in KylinResultSet.execute()
    @Override
    public ExecuteResult execute(StatementHandle sh, List<TypedValue> parameterValues, long maxRowCount) throws NoSuchStatementException {
        final MetaResultSet metaResultSet = MetaResultSet.create(sh.connectionId,  sh.id, false, sh.signature, null);
        return new ExecuteResult(ImmutableList.of(metaResultSet));
    }

    // mimic from CalciteMetaImpl, real execution happens via callback in KylinResultSet.execute()
    @Override
    public ExecuteResult prepareAndExecute(StatementHandle sh, String sql, long maxRowCount, PrepareCallback callback) {
        try {
            synchronized (callback.getMonitor()) {
                callback.clear();
                sh.signature = connection().mockPreparedSignature(sql);
                callback.assign(sh.signature, null, -1);
            }
            callback.execute();
            final MetaResultSet metaResultSet = MetaResultSet.create(sh.connectionId, sh.id, false, sh.signature, null);
            return new ExecuteResult(ImmutableList.of(metaResultSet));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeStatement(StatementHandle h) {
        // nothing to do
    }

    private KMetaProject getMetaProject() {
        try {
            if (metaProject == null) {
                KylinConnection conn = connection();
                metaProject = conn.getRemoteClient().retrieveMetaData(conn.getProject());
            }
            return metaProject;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getTableTypes(ConnectionHandle ch) {
        return createResultSet(metaTableTypes, MetaTableType.class, "TABLE_TYPE");
    }

    @Override
    public MetaResultSet getCatalogs(ConnectionHandle ch) {
        List<KMetaCatalog> catalogs = getMetaProject().catalogs;
        return createResultSet(catalogs, KMetaCatalog.class, "TABLE_CAT");
    }

    @Override
    public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
        List<KMetaSchema> schemas = getMetaProject().getSchemas(catalog, schemaPattern);
        return createResultSet(schemas, KMetaSchema.class, "TABLE_SCHEM", "TABLE_CATALOG");
    }

    @Override
    public MetaResultSet getTables(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, List<String> typeList) {
        List<KMetaTable> tables = getMetaProject().getTables(catalog, schemaPattern, tableNamePattern, typeList);
        return createResultSet(tables, KMetaTable.class, //
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
    }

    @Override
    public MetaResultSet getColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
        List<KMetaColumn> columns = getMetaProject().getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        return createResultSet(columns, KMetaColumn.class, //
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
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private MetaResultSet createResultSet(List iterable, Class clazz, String... names) {
        final List<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();
        final List<Field> fields = new ArrayList<Field>();
        final List<String> fieldNames = new ArrayList<String>();
        for (String name : names) {
            final int index = fields.size();
            final String fieldName = AvaticaUtils.toCamelCase(name);
            final Field field;
            try {
                field = clazz.getField(fieldName);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(e);
            }
            columns.add(columnMetaData(name, index, field.getType()));
            fields.add(field);
            fieldNames.add(fieldName);
        }

        CursorFactory cursorFactory = CursorFactory.record(clazz, fields, fieldNames);
        Signature signature = new Signature(columns, "", null, Collections.<String, Object> emptyMap(), cursorFactory, StatementType.SELECT);
        StatementHandle sh = this.createStatement(connection().handle);
        Frame frame = new Frame(0, true, iterable);

        return MetaResultSet.create(connection().id, sh.id, true, signature, frame);
    }

    // ============================================================================

    public static interface NamedWithChildren extends Named {
        List<? extends NamedWithChildren> getChildren();
    }

    public static List<? extends NamedWithChildren> searchByPatterns(NamedWithChildren parent, Pat... patterns) {
        assert patterns != null && patterns.length > 0;

        List<? extends NamedWithChildren> children = findChildren(parent, patterns[0]);
        if (patterns.length == 1) {
            return children;
        } else {
            List<NamedWithChildren> result = new ArrayList<NamedWithChildren>();
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

        List<NamedWithChildren> result = new ArrayList<NamedWithChildren>();
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
        public List<KMetaTable> getTables(String catalog, Pat schemaPattern, Pat tableNamePattern, List<String> typeList) {
            return (List<KMetaTable>) searchByPatterns(this, Pat.of(catalog), schemaPattern, tableNamePattern);
        }

        @SuppressWarnings("unchecked")
        public List<KMetaColumn> getColumns(String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
            return (List<KMetaColumn>) searchByPatterns(this, Pat.of(catalog), schemaPattern, tableNamePattern, columnNamePattern);
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

        public KMetaTable(String tableCat, String tableSchem, String tableName, String tableType, List<KMetaColumn> columns) {
            super(tableCat, tableSchem, tableName, tableType);
            this.columns = columns;
        }

        @Override
        public List<? extends NamedWithChildren> getChildren() {
            return columns;
        }
    }

    public static class KMetaColumn extends MetaColumn implements NamedWithChildren {

        public KMetaColumn(String tableCat, String tableSchem, String tableName, String columnName, int dataType, String typeName, int columnSize, Integer decimalDigits, int numPrecRadix, int nullable, int charOctetLength, int ordinalPosition, String isNullable) {
            super(tableCat, tableSchem, tableName, columnName, dataType, typeName, columnSize, decimalDigits, numPrecRadix, nullable, charOctetLength, ordinalPosition, isNullable);
        }

        @Override
        public List<NamedWithChildren> getChildren() {
            return Collections.<NamedWithChildren> emptyList();
        }
    }

    @Override
    public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount) throws NoSuchStatementException, MissingResultsException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean syncResults(StatementHandle sh, QueryState state, long offset) throws NoSuchStatementException {
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
