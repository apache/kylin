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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class H2Database {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(H2Database.class);

    private static final String[] ALL_TABLES = new String[] { //
            "edw.test_cal_dt", //
            "default.test_category_groupings", //
            "default.test_kylin_fact", //
            "default.test_order", //
            "edw.test_seller_type_dim", //
            "edw.test_sites", //
            "default.test_account", //
            "default.test_country", "ssb.customer", "ssb.dates", "ssb.p_lineorder", "ssb.lineorder", "ssb.part",
            "ssb.supplier" }; //
    private static final Map<String, String> javaToH2DataTypeMapping = new HashMap<String, String>();

    static {
        javaToH2DataTypeMapping.put("short", "smallint");
        javaToH2DataTypeMapping.put("long", "bigint");
        javaToH2DataTypeMapping.put("byte", "tinyint");
        javaToH2DataTypeMapping.put("string", "varchar");
    }

    private final Connection h2Connection;
    private final KylinConfig config;
    private final String project;

    public H2Database(Connection h2Connection, KylinConfig config, String prj) {
        this.h2Connection = h2Connection;
        this.config = config;
        this.project = prj;
    }

    private static String getH2DataType(String javaDataType) {
        String hiveDataType = javaToH2DataTypeMapping.get(javaDataType.toLowerCase(Locale.ROOT));
        if (hiveDataType == null) {
            hiveDataType = javaDataType;
        }
        return hiveDataType.toLowerCase(Locale.ROOT);
    }

    public void loadAllTables() throws SQLException {
        for (String tableName : ALL_TABLES) {
            loadH2Table(tableName);
        }
    }

    public void dropAll() throws SQLException {
        try (Statement stmt = h2Connection.createStatement()) {
            StringBuilder sqlBuilder = new StringBuilder();
            for (String tblName : ALL_TABLES)
                sqlBuilder.append("DROP TABLE ").append(tblName).append(";\n");
            sqlBuilder.append("DROP SCHEMA DEFAULT;\nDROP SCHEMA EDW;\n");

            stmt.executeUpdate(sqlBuilder.toString());
        }
    }

    private void loadH2Table(String tableName) throws SQLException {
        NTableMetadataManager metaMgr = NTableMetadataManager.getInstance(config, project);
        TableDesc tableDesc = metaMgr.getTableDesc(tableName.toUpperCase(Locale.ROOT));

        String path = path(tableDesc);
        try {
            File tempFile = File.createTempFile("tmp_h2", ".csv");
            try (FileOutputStream tempFileStream = new FileOutputStream(tempFile)) {
                try (InputStream is = new FileInputStream(
                        Paths.get(config.getMetadataUrl().getIdentifier(), "..", path).toFile())) {
                    IOUtils.copy(is, tempFileStream);
                }

                String cvsFilePath = tempFile.getPath();
                try (Statement stmt = h2Connection.createStatement()) {

                    String createDBSql = "CREATE SCHEMA IF NOT EXISTS DEFAULT;\n" + "CREATE SCHEMA IF NOT EXISTS EDW;\n"
                            + "CREATE SCHEMA IF NOT EXISTS SSB;\n" + "SET SCHEMA DEFAULT;\n";
                    stmt.executeUpdate(createDBSql);

                    String sql = generateCreateH2TableSql(tableDesc, cvsFilePath);
                    stmt.executeUpdate(sql);

                    List<String> createIndexStatements = generateCreateH2IndexSql(tableDesc);
                    for (String indexSql : createIndexStatements) {
                        stmt.executeUpdate(indexSql);
                    }
                }
            }

            FileUtils.deleteQuietly(tempFile);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private String path(TableDesc tableDesc) {
        if ("EDW.TEST_SELLER_TYPE_DIM".equals(tableDesc.getIdentity())) // it is a view of table below
            return "/data/" + "EDW.TEST_SELLER_TYPE_DIM" + ".csv";
        else
            return "/data/" + tableDesc.getIdentity() + ".csv";
    }

    private String generateCreateH2TableSql(TableDesc tableDesc, String csvFilePath) {
        StringBuilder ddl = new StringBuilder();
        StringBuilder csvColumns = new StringBuilder();

        ddl.append("CREATE TABLE IF NOT EXISTS " + tableDesc.getIdentity() + "\n");
        ddl.append("(" + "\n");

        for (int i = 0; i < tableDesc.getColumns().length; i++) {
            ColumnDesc col = tableDesc.getColumns()[i];
            if (col.isComputedColumn()) {
                continue;
            }
            if (i > 0) {
                ddl.append(",");
                csvColumns.append(",");
            }
            ddl.append(col.getName() + " " + getH2DataType((col.getDatatype())) + "\n");
            csvColumns.append(col.getName());
        }
        ddl.append(")" + "\n");
        ddl.append("AS SELECT * FROM CSVREAD('" + csvFilePath + "', '" + csvColumns
                + "', 'charset=UTF-8 fieldSeparator=,');");

        return ddl.toString();
    }

    private List<String> generateCreateH2IndexSql(TableDesc tableDesc) {
        List<String> result = Lists.newArrayList();
        int x = 0;
        for (ColumnDesc col : tableDesc.getColumns()) {
            if ("T".equalsIgnoreCase(col.getIndex())) {
                StringBuilder ddl = new StringBuilder();
                ddl.append("CREATE INDEX IF NOT EXISTS IDX_" + tableDesc.getName() + "_" + x + " ON "
                        + tableDesc.getIdentity() + "(" + col.getName() + ")");
                ddl.append("\n");
                result.add(ddl.toString());
                x++;
            }
        }

        return result;
    }

}
