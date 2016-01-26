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

package org.apache.kylin.query;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class H2Database {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(H2Database.class);

    private static final String[] ALL_TABLES = new String[] { "edw.test_cal_dt", "default.test_category_groupings", "default.test_kylin_fact", "edw.test_seller_type_dim", "edw.test_sites", "default.streaming_table" };
    private static final Map<String, String> javaToH2DataTypeMapping = new HashMap<String, String>();

    static {
        javaToH2DataTypeMapping.put("short", "smallint");
        javaToH2DataTypeMapping.put("long", "bigint");
        javaToH2DataTypeMapping.put("byte", "tinyint");
        javaToH2DataTypeMapping.put("string", "varchar");
    }

    private final Connection h2Connection;

    private final KylinConfig config;

    public H2Database(Connection h2Connection, KylinConfig config) {
        this.h2Connection = h2Connection;
        this.config = config;
    }

    public void loadAllTables() throws SQLException {
        for (String tableName : ALL_TABLES) {
            loadH2Table(tableName);
        }
    }

    private void loadH2Table(String tableName) throws SQLException {
        MetadataManager metaMgr = MetadataManager.getInstance(config);
        TableDesc tableDesc = metaMgr.getTableDesc(tableName.toUpperCase());
        File tempFile = null;

        try {
            tempFile = File.createTempFile("tmp_h2", ".csv");
            FileOutputStream tempFileStream = new FileOutputStream(tempFile);
            String normalPath = "/data/" + tableDesc.getIdentity() + ".csv";
            InputStream csvStream = metaMgr.getStore().getResource(normalPath).inputStream;

            org.apache.commons.io.IOUtils.copy(csvStream, tempFileStream);

            csvStream.close();
            tempFileStream.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        String cvsFilePath = tempFile.getPath();
        Statement stmt = h2Connection.createStatement();

        String createDBSql = "CREATE SCHEMA IF NOT EXISTS DEFAULT;\nCREATE SCHEMA IF NOT EXISTS EDW;\nSET SCHEMA DEFAULT;\n";
        stmt.executeUpdate(createDBSql);

        String sql = generateCreateH2TableSql(tableDesc, cvsFilePath);
        stmt.executeUpdate(sql);

        if (tempFile != null)
            tempFile.delete();
    }

    private String generateCreateH2TableSql(TableDesc tableDesc, String csvFilePath) {
        StringBuilder ddl = new StringBuilder();
        StringBuilder csvColumns = new StringBuilder();

        ddl.append("CREATE TABLE " + tableDesc.getIdentity() + "\n");
        ddl.append("(" + "\n");

        for (int i = 0; i < tableDesc.getColumns().length; i++) {
            ColumnDesc col = tableDesc.getColumns()[i];
            if (i > 0) {
                ddl.append(",");
                csvColumns.append(",");
            }
            ddl.append(col.getName() + " " + getH2DataType((col.getDatatype())) + "\n");
            csvColumns.append(col.getName());
        }
        ddl.append(")" + "\n");
        ddl.append("AS SELECT * FROM CSVREAD('" + csvFilePath + "', '" + csvColumns + "', 'charset=UTF-8 fieldSeparator=,');");

        return ddl.toString();
    }

    private static String getH2DataType(String javaDataType) {
        String hiveDataType = javaToH2DataTypeMapping.get(javaDataType.toLowerCase());
        if (hiveDataType == null) {
            hiveDataType = javaDataType;
        }
        return hiveDataType.toLowerCase();
    }

}
