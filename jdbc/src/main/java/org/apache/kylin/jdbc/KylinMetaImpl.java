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

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.kylin.jdbc.stub.DataSet;
import org.apache.kylin.jdbc.stub.KylinColumnMetaData;
import org.apache.kylin.jdbc.stub.RemoteClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kylin.jdbc.util.SQLTypeMap;

/**
 * Implementation of avatica interface
 *
 * @author xduo
 */
public class KylinMetaImpl implements Meta {

    private static final Logger logger = LoggerFactory.getLogger(KylinMetaImpl.class);

    private final KylinConnectionImpl conn;

    private final KylinJdbc41Factory factory;

    /**
     * @param conn
     */
    public KylinMetaImpl(KylinConnectionImpl conn, KylinJdbc41Factory factory) {
        super();
        this.conn = conn;
        this.factory = factory;
    }

    private ResultSet mockEmptyResultSet() {
        AvaticaResultSet resultSet = null;
        try {
            List<ColumnMetaData> columnMetas = new ArrayList<ColumnMetaData>();
            List<Object[]> data = new ArrayList<Object[]>();
            resultSet = this.conn.getFactory().newResultSet(this.conn.createStatement(), new KylinPrepare.PrepareResult(null, null, new KylinEnumerator<Object[]>(data), ColumnMetaData.struct(columnMetas)), this.conn.getTimeZone());
            KylinConnectionImpl.TROJAN.execute(resultSet);
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage(), e);
        }

        return resultSet;
    }

    public String getSqlKeywords() {
        return SqlParser.create("").getMetadata().getJdbcKeywords();
    }

    public String getNumericFunctions() {
        return SqlJdbcFunctionCall.getNumericFunctions();
    }

    public String getStringFunctions() {
        return SqlJdbcFunctionCall.getStringFunctions();
    }

    public String getSystemFunctions() {
        return SqlJdbcFunctionCall.getSystemFunctions();
    }

    public String getTimeDateFunctions() {
        return SqlJdbcFunctionCall.getTimeDateFunctions();
    }

    public ResultSet getTables(String catalog, Pat schemaPattern, Pat tableNamePattern, List<String> typeList) {
        logger.debug("Get tables with conn " + conn);
        MetaProject metaProject = conn.getMetaProject();

        if (null != metaProject) {
            final DataSet<MetaTable> tables = metaProject.getMetaTables(catalog, schemaPattern, tableNamePattern);
            final NamedFieldGetter<MetaTable> tableGetter = new NamedFieldGetter<MetaTable>(MetaTable.class, tables.getMeta(), "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION");

            AvaticaResultSet resultSet = null;
            try {
                resultSet = this.conn.getFactory().newResultSet(this.conn.createStatement(), new KylinPrepare.PrepareResult(null, null, null, tableGetter.structType) {
                    @Override
                    public Cursor createCursor() {
                        return tableGetter.cursor(tables.getEnumerator());
                    }
                }, this.conn.getTimeZone());
                KylinConnectionImpl.TROJAN.execute(resultSet);
            } catch (SQLException e) {
                logger.error(e.getLocalizedMessage(), e);
            }

            return resultSet;
        } else {
            return mockEmptyResultSet();
        }
    }

    public ResultSet getColumns(String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
        logger.debug("Get columns with conn " + conn);
        MetaProject metaProject = conn.getMetaProject();

        if (null != metaProject) {
            final DataSet<MetaColumn> columns = metaProject.getMetaColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
            final NamedFieldGetter<MetaColumn> columnGetter = new NamedFieldGetter<MetaColumn>(MetaColumn.class, columns.getMeta(), "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN");

            AvaticaResultSet resultSet = null;
            try {
                resultSet = this.conn.getFactory().newResultSet(this.conn.createStatement(), new KylinPrepare.PrepareResult(null, null, null, columnGetter.structType) {
                    @Override
                    public Cursor createCursor() {
                        return columnGetter.cursor(columns.getEnumerator());
                    }
                }, this.conn.getTimeZone());

                KylinConnectionImpl.TROJAN.execute(resultSet);
            } catch (SQLException e) {
                logger.error(e.getLocalizedMessage(), e);
            }

            return resultSet;
        } else {
            return mockEmptyResultSet();
        }
    }

    public ResultSet getSchemas(String catalog, Pat schemaPattern) {
        logger.debug("Get schemas with conn " + conn);
        MetaProject metaProject = conn.getMetaProject();

        if (null != metaProject) {
            final DataSet<MetaSchema> schemas = metaProject.getMetaSchemas(catalog, schemaPattern);
            final NamedFieldGetter<MetaSchema> schemaGetter = new NamedFieldGetter<MetaSchema>(MetaSchema.class, schemas.getMeta(), "TABLE_SCHEM", "TABLE_CATALOG");

            AvaticaResultSet resultSet = null;
            try {
                resultSet = this.conn.getFactory().newResultSet(this.conn.createStatement(), new KylinPrepare.PrepareResult(null, null, null, schemaGetter.structType) {
                    @Override
                    public Cursor createCursor() {
                        return schemaGetter.cursor(schemas.getEnumerator());
                    }
                }, this.conn.getTimeZone());

                KylinConnectionImpl.TROJAN.execute(resultSet);
            } catch (SQLException e) {
                logger.error(e.getLocalizedMessage(), e);
            }

            return resultSet;
        } else {
            return mockEmptyResultSet();
        }
    }

    public ResultSet getCatalogs() {
        MetaProject metaProject = conn.getMetaProject();

        if (null != metaProject) {
            final DataSet<MetaCatalog> catalogs = metaProject.getMetaCatalogs();
            final NamedFieldGetter<MetaCatalog> catalogGetter = new NamedFieldGetter<MetaCatalog>(MetaCatalog.class, catalogs.getMeta(), "TABLE_CATALOG");

            AvaticaResultSet resultSet = null;
            try {
                resultSet = this.conn.getFactory().newResultSet(this.conn.createStatement(), new KylinPrepare.PrepareResult(null, null, null, catalogGetter.structType) {
                    @Override
                    public Cursor createCursor() {
                        return catalogGetter.cursor(catalogs.getEnumerator());
                    }
                }, this.conn.getTimeZone());

                KylinConnectionImpl.TROJAN.execute(resultSet);
            } catch (SQLException e) {
                logger.error(e.getLocalizedMessage(), e);
            }

            return resultSet;
        } else {
            return mockEmptyResultSet();
        }
    }

    public ResultSet getTableTypes() {
        List<ColumnMetaData> tableTypeMeta = new ArrayList<ColumnMetaData>();
        tableTypeMeta.add(ColumnMetaData.dummy(ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), false));
        List<Object[]> data = new ArrayList<Object[]>();
        Object[] row = new Object[1];
        row[0] = "TABLE";
        data.add(row);

        AvaticaResultSet resultSet = null;
        try {
            resultSet = this.conn.getFactory().newResultSet(this.conn.createStatement(), new KylinPrepare.PrepareResult(null, null, new KylinEnumerator<Object[]>(data), ColumnMetaData.struct(tableTypeMeta)), this.conn.getTimeZone());
            KylinConnectionImpl.TROJAN.execute(resultSet);
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage(), e);
        }

        return resultSet;
    }

    public ResultSet getProcedures(String catalog, Pat schemaPattern, Pat procedureNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getProcedureColumns(String catalog, Pat schemaPattern, Pat procedureNamePattern, Pat columnNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getColumnPrivileges(String catalog, String schema, String table, Pat columnNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getTablePrivileges(String catalog, Pat schemaPattern, Pat tableNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) {
        return mockEmptyResultSet();
    }

    public ResultSet getVersionColumns(String catalog, String schema, String table) {
        return mockEmptyResultSet();
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
        return mockEmptyResultSet();
    }

    public ResultSet getImportedKeys(String catalog, String schema, String table) {
        return mockEmptyResultSet();
    }

    public ResultSet getExportedKeys(String catalog, String schema, String table) {
        return mockEmptyResultSet();
    }

    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) {
        return mockEmptyResultSet();
    }

    public ResultSet getTypeInfo() {
        return mockEmptyResultSet();
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) {
        return mockEmptyResultSet();
    }

    public ResultSet getUDTs(String catalog, Pat schemaPattern, Pat typeNamePattern, int[] types) {
        return mockEmptyResultSet();
    }

    public ResultSet getSuperTypes(String catalog, Pat schemaPattern, Pat typeNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getSuperTables(String catalog, Pat schemaPattern, Pat tableNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getAttributes(String catalog, Pat schemaPattern, Pat typeNamePattern, Pat attributeNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getClientInfoProperties() {
        return mockEmptyResultSet();
    }

    public ResultSet getFunctions(String catalog, Pat schemaPattern, Pat functionNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getFunctionColumns(String catalog, Pat schemaPattern, Pat functionNamePattern, Pat columnNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getPseudoColumns(String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
        return mockEmptyResultSet();
    }

    public Cursor createCursor(AvaticaResultSet resultSet) {

        if (!(resultSet instanceof KylinResultSet))
            throw new IllegalStateException("resultSet is not KylinResultSet");

        KylinPrepare.PrepareResult result = ((KylinResultSet) resultSet).getPrepareResult();

        return result.createCursor();
    }

    /* 
     * Client could request metadata after prepare
     * 
     * (non-Javadoc)
     * @see org.apache.calcite.avatica.Meta#prepare(org.apache.calcite.avatica.AvaticaStatement, java.lang.String)
     */
    public AvaticaPrepareResult prepare(AvaticaStatement statement, String sql) {
        RemoteClient client = factory.newRemoteClient(conn);
        DataSet<Object[]> result = null;

        try {
            result = (DataSet<Object[]>) client.query(statement, sql);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new RuntimeException("Failed to query kylin server with exception " + e.getLocalizedMessage());
        }

        return new KylinPrepare.PrepareResult(sql, null, (Enumerator<Object[]>) result.getEnumerator(), ColumnMetaData.struct(result.getMeta()));
    }

    /**
     * Tree node used by project tree-like structure
     *
     * @author xduo
     */
    interface Node {
        /**
         * Get the node name
         *
         * @return
         */
        public String getName();

        /**
         * Get direct children of the node.
         *
         * @return
         */
        public List<? extends Node> getChildren();

        /**
         * Search the subtree of the node with patterns. One pattern, one level.
         *
         * @param patterns
         * @return
         */
        public List<? extends Node> searchByPatterns(Pat... patterns);
    }

    /**
     * Abstract of the tree-like structure
     *
     * @author xduo
     */
    public static abstract class AbstractNode implements Node {

        public List<? extends Node> searchByPatterns(Pat... patterns) {
            if (patterns.length == 1) {
                return findChildren(patterns[0]);
            } else {
                List<Node> children = new ArrayList<Node>();

                for (Node child : this.findChildren(patterns[0])) {
                    children.addAll(child.searchByPatterns(Arrays.copyOfRange(patterns, 1, patterns.length)));
                }

                return children;
            }
        }

        private List<? extends Node> findChildren(Pat pattern) {
            if (null == pattern.s || pattern.s.equals("%")) {
                return this.getChildren();
            }

            List<Node> list = new ArrayList<Node>();

            for (Node c : this.getChildren()) {
                if (likeToRegex(pattern).matcher(c.getName()).matches()) {
                    list.add(c);
                }
            }

            return list;
        }

        ;

        /**
         * Converts a LIKE-style pattern (where '%' represents a wild-card,
         * escaped using '\') to a Java regex.
         */
        private Pattern likeToRegex(Pat pattern) {
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
    }

    public static class MetaProject extends AbstractNode {
        public final String project;
        public final List<MetaCatalog> catalogs;

        public MetaProject(String project, List<MetaCatalog> catalogs) {
            super();
            this.project = project;
            this.catalogs = catalogs;
        }

        public DataSet<MetaCatalog> getMetaCatalogs() {
            return new DataSet<MetaCatalog>(MetaCatalog.meta, new KylinEnumerator<MetaCatalog>(catalogs));
        }

        /**
         * facade method to search schemas in current project.
         *
         * @param catalog
         * @param schemaPattern
         * @return
         */
        @SuppressWarnings("unchecked")
        public DataSet<MetaSchema> getMetaSchemas(String catalog, Pat schemaPattern) {
            List<? extends Node> metaSchemas = this.searchByPatterns(Pat.of(catalog), schemaPattern);

            return new DataSet<MetaSchema>(MetaSchema.meta, new KylinEnumerator<MetaSchema>((Collection<MetaSchema>) metaSchemas));
        }

        /**
         * facade method to search tables in current project
         *
         * @param catalog
         * @param schemaPattern
         * @param tableNamePattern
         * @return
         */
        @SuppressWarnings("unchecked")
        public DataSet<MetaTable> getMetaTables(String catalog, Pat schemaPattern, Pat tableNamePattern) {
            logger.debug("getMetaTables with catalog:" + catalog + ", schema:" + schemaPattern.s + ", table:" + tableNamePattern.s);
            List<? extends Node> tables = this.searchByPatterns(Pat.of(catalog), schemaPattern, tableNamePattern);

            return new DataSet<MetaTable>(MetaTable.meta, new KylinEnumerator<MetaTable>((Collection<MetaTable>) tables));
        }

        /**
         * facade method to search columns in current project
         *
         * @param catalog
         * @param schemaPattern
         * @param tableNamePattern
         * @param columnNamePattern
         * @return
         */
        @SuppressWarnings("unchecked")
        public DataSet<MetaColumn> getMetaColumns(String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
            logger.debug("getMetaColumns with catalog:" + catalog + ", schema:" + schemaPattern.s + ", table:" + tableNamePattern.s + ", column:" + columnNamePattern.s);
            List<? extends Node> columns = this.searchByPatterns(Pat.of(catalog), schemaPattern, tableNamePattern, columnNamePattern);

            return new DataSet<MetaColumn>(MetaColumn.meta, new KylinEnumerator<MetaColumn>((Collection<MetaColumn>) columns));
        }

        @Override
        public String getName() {
            return project;
        }

        @Override
        public List<? extends Node> getChildren() {
            return this.catalogs;
        }
    }

    /**
     * Metadata describing a catalog.
     */
    public static class MetaCatalog extends AbstractNode {
        public static final List<ColumnMetaData> meta = new ArrayList<ColumnMetaData>();
        public final String tableCatalog;
        public final List<MetaSchema> schemas;

        static {
            meta.add(KylinColumnMetaData.dummy(0, "TABLE_CAT", "TABLE_CAT", ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING), true));
        }

        public MetaCatalog(String tableCatalog, List<MetaSchema> schemas) {
            this.tableCatalog = tableCatalog;
            this.schemas = schemas;
        }

        public String getName() {
            return tableCatalog;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((tableCatalog == null) ? 0 : tableCatalog.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            MetaCatalog other = (MetaCatalog) obj;
            if (tableCatalog == null) {
                if (other.tableCatalog != null)
                    return false;
            } else if (!tableCatalog.equals(other.tableCatalog))
                return false;
            return true;
        }

        @Override
        public List<? extends Node> getChildren() {
            return schemas;
        }

    }

    /**
     * Metadata describing a schema.
     */
    public static class MetaSchema extends AbstractNode {
        public static final List<ColumnMetaData> meta = new ArrayList<ColumnMetaData>();
        public final String tableCatalog;
        public final String tableSchem;
        public final List<MetaTable> tables;

        static {
            for (ColumnMetaData cmd : SQLTypeMap.schemaMetaTypeMapping.values()) {
                meta.add(cmd);
            }
        }

        public MetaSchema(String tableCatalog, String tableSchem, List<MetaTable> tables) {
            this.tableCatalog = tableCatalog;
            this.tableSchem = tableSchem;
            this.tables = tables;
        }

        public String getName() {
            return tableSchem;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((tableCatalog == null) ? 0 : tableCatalog.hashCode());
            result = prime * result + ((tableSchem == null) ? 0 : tableSchem.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            MetaSchema other = (MetaSchema) obj;
            if (tableCatalog == null) {
                if (other.tableCatalog != null)
                    return false;
            } else if (!tableCatalog.equals(other.tableCatalog))
                return false;
            if (tableSchem == null) {
                if (other.tableSchem != null)
                    return false;
            } else if (!tableSchem.equals(other.tableSchem))
                return false;
            return true;
        }

        @Override
        public List<MetaTable> getChildren() {
            return this.tables;
        }
    }

    /**
     * Metadata describing a table type.
     */
    public static class MetaTableType {
        public final String tableType;

        public MetaTableType(String tableType) {
            this.tableType = tableType;
        }
    }

    /**
     * Metadata describing a table.
     */
    public static class MetaTable extends AbstractNode {
        public static final List<ColumnMetaData> meta = new ArrayList<ColumnMetaData>();
        public final String tableCat;
        public final String tableSchem;
        public final String tableName;
        public final String tableType;
        public final String remarks;
        public final String typeCat;
        public final String typeSchem;
        public final String typeName;
        public final String selfReferencingColName;
        public final String refGeneration;
        public final List<MetaColumn> columns;

        static {
            for (ColumnMetaData cmd : SQLTypeMap.tableMetaTypeMapping.values()) {
                meta.add(cmd);
            }
        }

        public MetaTable(String tableCat, String tableSchem, String tableName, String tableType, String remarks, String typeCat, String typeSchem, String typeName, String selfReferencingColName, String refGeneration, List<MetaColumn> columns) {
            this.tableCat = tableCat;
            this.tableSchem = tableSchem;
            this.tableName = tableName;
            this.tableType = tableType;
            this.remarks = remarks;
            this.typeCat = typeCat;
            this.typeSchem = typeSchem;
            this.typeName = typeName;
            this.selfReferencingColName = selfReferencingColName;
            this.refGeneration = refGeneration;
            this.columns = columns;
        }

        public String getName() {
            return tableName;
        }

        @Override
        public List<? extends Node> getChildren() {
            return this.columns;
        }
    }

    /**
     * Metadata describing a column.
     */
    public static class MetaColumn implements Node {
        public static final List<ColumnMetaData> meta = new ArrayList<ColumnMetaData>();
        public final String tableCat;
        public final String tableSchem;
        public final String tableName;
        public final String columnName;
        public final int dataType;
        public final String typeName;
        public final int columnSize;
        public final int bufferLength;
        public final int decimalDigits;
        public final int numPrecRadix;
        public final int nullable;
        public final String remarks;
        public final String columnDef;
        public final int sqlDataType;
        public final int sqlDatetimeSub;
        public final int charOctetLength;
        public final int ordinalPosition;
        public final String isNullable;
        public final String scopeCatalog;
        public final String scopeTable;
        public final int sourceDataType;
        public final String isAutoincrement;
        public final String isGeneratedcolumn;

        static {
            for (ColumnMetaData cmd : SQLTypeMap.columnMetaTypeMapping.values()) {
                meta.add(cmd);
            }
        }

        public MetaColumn(String tableCat, String tableSchem, String tableName, String columnName, int dataType, String typeName, int columnSize, int bufferLength, int decimalDigits, int numPrecRadix, int nullable, String remarks, String columnDef, int sqlDataType, int sqlDatetimeSub, int charOctetLength, int ordinalPosition, String isNullable, String scopeCatalog, String scopeTable, int sourceDataType, String isAutoincrement, String isGeneratedcolumn) {
            super();
            this.tableCat = tableCat;
            this.tableSchem = tableSchem;
            this.tableName = tableName;
            this.columnName = columnName;
            this.dataType = dataType;
            this.typeName = typeName;
            this.columnSize = columnSize;
            this.bufferLength = bufferLength;
            this.decimalDigits = decimalDigits;
            this.numPrecRadix = numPrecRadix;
            this.nullable = nullable;
            this.remarks = remarks;
            this.columnDef = columnDef;
            this.sqlDataType = sqlDataType;
            this.sqlDatetimeSub = sqlDatetimeSub;
            this.charOctetLength = charOctetLength;
            this.ordinalPosition = ordinalPosition;
            this.isNullable = isNullable;
            this.scopeCatalog = scopeCatalog;
            this.scopeTable = scopeTable;
            this.sourceDataType = sourceDataType;
            this.isAutoincrement = isAutoincrement;
            this.isGeneratedcolumn = isGeneratedcolumn;
        }

        public String getName() {
            return columnName;
        }

        @Override
        public List<? extends Node> getChildren() {
            return Collections.emptyList();
        }

        @Override
        public List<? extends Node> searchByPatterns(Pat... patterns) {
            return Collections.emptyList();
        }
    }

    /**
     * Accesses fields by name.
     */
    private static class NamedFieldGetter<T> {
        private final List<Field> fields = new ArrayList<Field>();
        private final ColumnMetaData.StructType structType;

        public NamedFieldGetter(Class<T> clazz, List<ColumnMetaData> columns, String... names) {
            init(clazz, names, fields);
            structType = ColumnMetaData.struct(columns);
        }

        private void init(Class<T> clazz, String[] names, List<Field> fields) {
            for (String name : names) {
                final String fieldName = Util.toCamelCase(name);
                final Field field;
                try {
                    field = clazz.getField(fieldName);
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException(e);
                }
                fields.add(field);
            }
        }

        Object get(Object o, int columnIndex) {
            try {
                return fields.get(columnIndex).get(o);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        public Cursor cursor(Enumerator<T> enumerator) {
            // noinspection unchecked
            return new EnumeratorCursor<T>(enumerator) {
                protected Getter createGetter(final int ordinal) {
                    return new Getter() {
                        public Object getObject() {
                            return get(current(), ordinal);
                        }

                        public boolean wasNull() {
                            return getObject() == null;
                        }
                    };
                }
            };
        }
    }
}
