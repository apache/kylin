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

package org.apache.kylin.source.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.hive.DBConnConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JdbcExplorer implements ISourceMetadataExplorer, ISampleDataDeployer {
    private static final Logger logger = LoggerFactory.getLogger(JdbcExplorer.class);
    
    public static final String DIALECT_VERTICA="vertica";
    public static final String DIALECT_ORACLE="oracle";
    public static final String DIALECT_MYSQL="mysql";
    public static final String DIALECT_HIVE="hive";
    
    public static final String TABLE_TYPE_TABLE="TABLE";
    public static final String TABLE_TYPE_VIEW="VIEW";
    
    private KylinConfig config;
    private DBConnConf dbconf;
    private String dialect;

    public JdbcExplorer() {
        config = KylinConfig.getInstanceFromEnv();
        String connectionUrl = config.getJdbcConnectionUrl();
        String driverClass = config.getJdbcDriver();
        String jdbcUser = config.getJdbcUser();
        String jdbcPass = config.getJdbcPass();
        dbconf = new DBConnConf(driverClass, connectionUrl, jdbcUser, jdbcPass);
        this.dialect = config.getJdbcDialect();
    }
    
    private String getSqlDataType(String javaDataType) {
        if (DIALECT_VERTICA.equals(dialect)){
            if (javaDataType.toLowerCase().equals("double")){
                return "float";
            }
        }

        return javaDataType.toLowerCase();
    }
    
    @Override
    public void createSampleDatabase(String database) throws Exception {
        executeSQL(generateCreateSchemaSql(database));
    }

    private String generateCreateSchemaSql(String schemaName){
        if (DIALECT_VERTICA.equals(dialect)){
            return String.format("CREATE schema IF NOT EXISTS %s", schemaName);
        }else{
            logger.error(String.format("unsupported dialect %s.", dialect));
            return null;
        }
    }
    
    @Override
    public void loadSampleData(String tableName, String tmpDataDir) throws Exception {
        executeSQL(generateLoadDataSql(tableName, tmpDataDir));
    }

    private String generateLoadDataSql(String tableName, String tableFileDir) {
        if (DIALECT_VERTICA.equals(dialect)){
            return String.format("copy %s from local '%s/%s.csv' delimiter as ',';", tableName, tableFileDir, tableName);
        }else{
            logger.error(String.format("unsupported dialect %s.", dialect));
            return null;
        }
    }

    @Override
    public void createSampleTable(TableDesc table) throws Exception {
        executeSQL(generateCreateTableSql(table));
    }

    private String[] generateCreateTableSql(TableDesc tableDesc) {
        logger.info(String.format("gen create table sql:%s", tableDesc));
        String tableIdentity = String.format("%s.%s", tableDesc.getDatabase().toUpperCase(), tableDesc.getName()).toUpperCase();
        String dropsql = "DROP TABLE IF EXISTS " + tableIdentity;
        String dropsql2 = "DROP VIEW IF EXISTS " + tableIdentity;

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE " + tableIdentity + "\n");
        ddl.append("(" + "\n");

        for (int i = 0; i < tableDesc.getColumns().length; i++) {
            ColumnDesc col = tableDesc.getColumns()[i];
            if (i > 0) {
                ddl.append(",");
            }
            ddl.append(col.getName() + " " + getSqlDataType((col.getDatatype())) + "\n");
        }

        ddl.append(")");

        return new String[] { dropsql, dropsql2, ddl.toString() };
    }

    @Override
    public void createWrapperView(String origTableName, String viewName) throws Exception {
        executeSQL(generateCreateViewSql(viewName, origTableName));
    }

    private String[] generateCreateViewSql(String viewName, String tableName) {

        String dropView = "DROP VIEW IF EXISTS " + viewName;
        String dropTable = "DROP TABLE IF EXISTS " + viewName;

        String createSql = ("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);

        return new String[] { dropView, dropTable, createSql };
    }

    private void executeSQL(String sql) throws CommandNeedRetryException, IOException {
        Connection con = SqlUtil.getConnection(dbconf);
        logger.info(String.format(sql));
        SqlUtil.execUpdateSQL(con, sql);
        SqlUtil.closeResources(con, null);
    }

    private void executeSQL(String[] sqls) throws CommandNeedRetryException, IOException {
        Connection con = SqlUtil.getConnection(dbconf);
        for (String sql : sqls){
            logger.info(String.format(sql));
            SqlUtil.execUpdateSQL(con, sql);
        }
        SqlUtil.closeResources(con, null);
    }

    @Override
    public List<String> listDatabases() throws Exception {
        Connection con = SqlUtil.getConnection(dbconf);
        DatabaseMetaData dbmd = con.getMetaData();
        ResultSet rs = dbmd.getSchemas();
        List<String> ret = new ArrayList<String>();
        /*
        The schema columns are: 
            - TABLE_SCHEM String => schema name 
            - TABLE_CATALOG String => catalog name (may be null) 
        */
        while (rs.next()){
            String schema = rs.getString(1);
            String catalog = rs.getString(2);
            logger.info(String.format("%s,%s", schema, catalog));
            ret.add(schema);
        }
        SqlUtil.closeResources(con, null);
        return ret;
    }

    @Override
    public List<String> listTables(String database) throws Exception {
        Connection con = SqlUtil.getConnection(dbconf);
        DatabaseMetaData dbmd = con.getMetaData();
        ResultSet rs = dbmd.getTables(null, database, null, null);
        List<String> ret = new ArrayList<String>();
        /*
    - TABLE_CAT String => table catalog (may be null) 
    - TABLE_SCHEM String => table schema (may be null) 
    - TABLE_NAME String => table name 
    - TABLE_TYPE String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL 
     TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM". 
    - REMARKS String => explanatory comment on the table 
    - TYPE_CAT String => the types catalog (may be null) 
    - TYPE_SCHEM String => the types schema (may be null) 
    - TYPE_NAME String => type name (may be null) 
    - SELF_REFERENCING_COL_NAME String => name of the designated "identifier" column of a typed 
     table (may be null) 
    - REF_GENERATION String => specifies how values in SELF_REFERENCING_COL_NAME are created. 
     Values are "SYSTEM", "USER", "DERIVED". (may be null) 
         */
        while (rs.next()){
            String catalog = rs.getString(1);
            String schema = rs.getString(2);
            String name = rs.getString(3);
            String type = rs.getString(4);
            logger.info(String.format("%s,%s,%s,%s", schema, catalog, name, type));
            ret.add(name);
        }
        SqlUtil.closeResources(con, null);
        return ret;
    }

    @Override
    public Pair<TableDesc, TableExtDesc> loadTableMetadata(String database, String table, String prj) throws Exception {

        TableDesc tableDesc = new TableDesc();
        tableDesc.setDatabase(database.toUpperCase());
        tableDesc.setName(table.toUpperCase());
        tableDesc.setUuid(UUID.randomUUID().toString());
        tableDesc.setLastModified(0);
        
        Connection con = SqlUtil.getConnection(dbconf);
        DatabaseMetaData dbmd = con.getMetaData();
        ResultSet rs = dbmd.getTables(null, database, table, null);
        String tableType=null;
        while (rs.next()){
            tableType = rs.getString(4);
        }
        DBUtils.closeQuietly(rs);
        if (tableType!=null){
            tableDesc.setTableType(tableType);
        }else{
            logger.error(String.format("table %s not found in schema:%s", table, database));
        }
        /*
    - 1. TABLE_CAT String => table catalog (may be null) 
    - 2. TABLE_SCHEM String => table schema (may be null) 
    - 3. TABLE_NAME String => table name 
    - 4. COLUMN_NAME String => column name 
    - 5. DATA_TYPE int => SQL type from java.sql.Types 
    - 6. TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified 
    - 7. COLUMN_SIZE int => column size. 
    - 8. BUFFER_LENGTH is not used. 
    - 9. DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable. 
    - 10.NUM_PREC_RADIX int => Radix (typically either 10 or 2) 
    - 11.NULLABLE int => is NULL allowed. 
        - columnNoNulls - might not allow NULL values 
        - columnNullable - definitely allows NULL values 
        - columnNullableUnknown - nullability unknown 
    - 12.REMARKS String => comment describing column (may be null) 
    - 13.COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null) 
    - 14.SQL_DATA_TYPE int => unused 
    - 15.SQL_DATETIME_SUB int => unused 
    - 16.CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column 
    - 17.ORDINAL_POSITION int => index of column in table (starting at 1) 
    - 18.IS_NULLABLE String => ISO rules are used to determine the nullability for a column. 
        - YES --- if the column can include NULLs 
        - NO --- if the column cannot include NULLs 
        - empty string --- if the nullability for the column is unknown
         */
        List<ColumnDesc> columns = new ArrayList<ColumnDesc>();
        rs = dbmd.getColumns(null, database, table, null);
        while (rs.next()){
            String tname = rs.getString(3);
            String cname = rs.getString(4);
            int type=rs.getInt(5);
            String typeName=rs.getString(6);
            int csize=rs.getInt(7);
            int digits = rs.getInt(9);
            int nullable = rs.getInt(11);
            String comment = rs.getString(12);
            int pos = rs.getInt(17);
            logger.info(String.format("%s,%s,%d,%d,%d,%d,%s,%d", tname, cname, type, csize, digits, nullable, comment, pos));
            
            ColumnDesc cdesc = new ColumnDesc();
            cdesc.setName(cname.toUpperCase());
            // use "double" in kylin for "float"
            cdesc.setDatatype(typeName);
            cdesc.setId(String.valueOf(pos));
            columns.add(cdesc);
        }
        DBUtils.closeQuietly(rs);

        tableDesc.setColumns(columns.toArray(new ColumnDesc[columns.size()]));

        TableExtDesc tableExtDesc = new TableExtDesc();
        tableExtDesc.setIdentity(tableDesc.getIdentity());
        tableExtDesc.setUuid(UUID.randomUUID().toString());
        tableExtDesc.setLastModified(0);
        tableExtDesc.init(prj);

        return Pair.newPair(tableDesc, tableExtDesc);
    }

    @Override
    public List<String> getRelatedKylinResources(TableDesc table) {
        return Collections.emptyList();
    }

}
