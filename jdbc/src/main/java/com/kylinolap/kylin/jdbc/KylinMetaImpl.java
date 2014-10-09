/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.kylin.jdbc;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaResultSet;
import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.ColumnMetaData;
import net.hydromatic.avatica.ColumnMetaData.Rep;
import net.hydromatic.avatica.Cursor;
import net.hydromatic.avatica.Meta;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.optiq.runtime.EnumeratorCursor;

import org.eigenbase.sql.SqlJdbcFunctionCall;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.kylin.jdbc.stub.DataSet;
import com.kylinolap.kylin.jdbc.stub.MetaProject;
import com.kylinolap.kylin.jdbc.stub.RemoteClient;

/**
 * Implementation of avatica interface
 * 
 * @author xduo
 * 
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
            resultSet =
                    this.conn.getFactory().newResultSet(
                            this.conn.createStatement(),
                            new KylinPrepare.PrepareResult(null, null, new KylinEnumerator<Object[]>(data
                                    .iterator()), ColumnMetaData.struct(columnMetas)),
                            this.conn.getTimeZone());
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
        MetaProject metaProject = conn.getMetaProject();

        if (null != metaProject) {
            // todo: apply patterns
            final DataSet<MetaTable> tables = metaProject.getMetaTables();
            final NamedFieldGetter<MetaTable> tableGetter =
                    new NamedFieldGetter<MetaTable>(MetaTable.class, tables.getMeta(), "TABLE_CAT",
                            "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM",
                            "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION");

            AvaticaResultSet resultSet = null;
            try {
                resultSet =
                        this.conn.getFactory().newResultSet(this.conn.createStatement(),
                                new KylinPrepare.PrepareResult(null, null, null, tableGetter.structType) {
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
        MetaProject metaProject = conn.getMetaProject();

        if (null != metaProject) {
            // todo: apply patterns
            final DataSet<MetaColumn> columns = metaProject.getMetaColumns();
            final NamedFieldGetter<MetaColumn> columnGetter =
                    new NamedFieldGetter<MetaColumn>(MetaColumn.class, columns.getMeta(), "TABLE_CAT",
                            "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                            "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
                            "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
                            "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG",
                            "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN");

            AvaticaResultSet resultSet = null;
            try {
                resultSet =
                        this.conn.getFactory().newResultSet(this.conn.createStatement(),
                                new KylinPrepare.PrepareResult(null, null, null, columnGetter.structType) {
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
        MetaProject metaProject = conn.getMetaProject();

        if (null != metaProject) {
            // todo: apply patterns
            final DataSet<MetaSchema> schemas = metaProject.getMetaSchemas();
            final NamedFieldGetter<MetaSchema> schemaGetter =
                    new NamedFieldGetter<MetaSchema>(MetaSchema.class, schemas.getMeta(), "TABLE_SCHEM",
                            "TABLE_CATALOG");

            AvaticaResultSet resultSet = null;
            try {
                resultSet =
                        this.conn.getFactory().newResultSet(this.conn.createStatement(),
                                new KylinPrepare.PrepareResult(null, null, null, schemaGetter.structType) {
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
        return mockEmptyResultSet();
        //        MetaProject metaProject = conn.getMetaProject();
        //
        //        if (null != metaProject) {
        //            // todo: apply patterns
        //            final DataSet<MetaCatalog> catalogs = metaProject.getMetaCatalogs();
        //            final NamedFieldGetter<MetaCatalog> catalogGetter =
        //                    new NamedFieldGetter<MetaCatalog>(MetaCatalog.class, catalogs.getMeta(), "TABLE_CATALOG");
        //
        //            AvaticaResultSet resultSet = null;
        //            try {
        //                resultSet =
        //                        this.conn.getFactory().newResultSet(this.conn.createStatement(),
        //                                new KylinPrepare.PrepareResult(null, null, null, catalogGetter.structType) {
        //                                    @Override
        //                                    public Cursor createCursor() {
        //                                        return catalogGetter.cursor(catalogs.getEnumerator());
        //                                    }
        //                                }, this.conn.getTimeZone());
        //
        //                KylinConnectionImpl.TROJAN.execute(resultSet);
        //            } catch (SQLException e) {
        //                logger.error(e.getLocalizedMessage(), e);
        //            }
        //
        //            return resultSet;
        //        } else {
        //            return mockEmptyResultSet();
        //        }
    }

    public ResultSet getTableTypes() {
        List<ColumnMetaData> tableTypeMeta = new ArrayList<ColumnMetaData>();
        tableTypeMeta.add(ColumnMetaData.dummy(ColumnMetaData.scalar(Types.VARCHAR, "varchar", Rep.STRING),
                false));
        List<Object[]> data = new ArrayList<Object[]>();
        Object[] row = new Object[1];
        row[0] = "TABLE";
        data.add(row);

        AvaticaResultSet resultSet = null;
        try {
            resultSet =
                    this.conn.getFactory().newResultSet(
                            this.conn.createStatement(),
                            new KylinPrepare.PrepareResult(null, null, new KylinEnumerator<Object[]>(data
                                    .iterator()), ColumnMetaData.struct(tableTypeMeta)),
                            this.conn.getTimeZone());
            KylinConnectionImpl.TROJAN.execute(resultSet);
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage(), e);
        }

        return resultSet;
    }

    public ResultSet getProcedures(String catalog, Pat schemaPattern, Pat procedureNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getProcedureColumns(String catalog, Pat schemaPattern, Pat procedureNamePattern,
            Pat columnNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getColumnPrivileges(String catalog, String schema, String table, Pat columnNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getTablePrivileges(String catalog, Pat schemaPattern, Pat tableNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
            boolean nullable) {
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

    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema, String foreignTable) {
        return mockEmptyResultSet();
    }

    public ResultSet getTypeInfo() {
        return mockEmptyResultSet();
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
            boolean approximate) {
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

    public ResultSet getAttributes(String catalog, Pat schemaPattern, Pat typeNamePattern,
            Pat attributeNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getClientInfoProperties() {
        return mockEmptyResultSet();
    }

    public ResultSet getFunctions(String catalog, Pat schemaPattern, Pat functionNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getFunctionColumns(String catalog, Pat schemaPattern, Pat functionNamePattern,
            Pat columnNamePattern) {
        return mockEmptyResultSet();
    }

    public ResultSet getPseudoColumns(String catalog, Pat schemaPattern, Pat tableNamePattern,
            Pat columnNamePattern) {
        return mockEmptyResultSet();
    }

    public Cursor createCursor(AvaticaResultSet resultSet) {
        KylinPrepare.PrepareResult result = ((KylinResultSet) resultSet).getPrepareResult();

        return result.createCursor();
    }

    public AvaticaPrepareResult prepare(AvaticaStatement statement, String sql) {
        RemoteClient client = factory.newRemoteClient(conn);
        DataSet<Object[]> result = null;

        try {
            result = (DataSet<Object[]>) client.query(statement, sql);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new RuntimeException("Failed to query kylin server with exception "
                    + e.getLocalizedMessage());
        }

        return new KylinPrepare.PrepareResult(sql, null, (Enumerator<Object[]>) result.getEnumerator(),
                ColumnMetaData.struct(result.getMeta()));
    }

    /** An object that has a name. */
    interface Named {
        String getName();
    }

    /** Metadata describing a catalog. */
    public static class MetaCatalog implements Named {
        public final String tableCatalog;

        public MetaCatalog(String tableCatalog) {
            this.tableCatalog = tableCatalog;
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

    }

    /** Metadata describing a schema. */
    public static class MetaSchema implements Named {
        public final String tableCatalog;
        public final String tableSchem;

        public MetaSchema(String tableCatalog, String tableSchem) {
            this.tableCatalog = tableCatalog;
            this.tableSchem = tableSchem;
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

    }

    /** Metadata describing a table type. */
    public static class MetaTableType {
        public final String tableType;

        public MetaTableType(String tableType) {
            this.tableType = tableType;
        }
    }

    /** Metadata describing a table. */
    public static class MetaTable implements Named {
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

        public MetaTable(String tableCat, String tableSchem, String tableName, String tableType,
                String remarks, String typeCat, String typeSchem, String typeName,
                String selfReferencingColName, String refGeneration) {
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
        }

        public String getName() {
            return tableName;
        }
    }

    /** Metadata describing a column. */
    public static class MetaColumn implements Named {
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

        public MetaColumn(String tableCat, String tableSchem, String tableName, String columnName,
                int dataType, String typeName, int columnSize, int bufferLength, int decimalDigits,
                int numPrecRadix, int nullable, String remarks, String columnDef, int sqlDataType,
                int sqlDatetimeSub, int charOctetLength, int ordinalPosition, String isNullable,
                String scopeCatalog, String scopeTable, int sourceDataType, String isAutoincrement,
                String isGeneratedcolumn) {
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
    }

    /** Accesses fields by name. */
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
            //noinspection unchecked
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
