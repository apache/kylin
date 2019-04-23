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

package org.apache.kylin.source.hive;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.SourceConfigurationUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class BeelineHiveClient implements IHiveClient {

    private static final String HIVE_AUTH_USER = "user";
    private static final String HIVE_AUTH_PASSWD = "password";
    private Connection cnct;
    private Statement stmt;
    private DatabaseMetaData metaData;

    public BeelineHiveClient(String beelineParams) {
        if (StringUtils.isEmpty(beelineParams)) {
            throw new IllegalArgumentException("BeelineParames cannot be empty");
        }
        String[] splits = StringUtils.split(beelineParams);
        String url = "", username = "", password = "";
        for (int i = 0; i < splits.length; i++) {
            if ("-u".equals(splits[i])) {
                url = stripQuotes(splits[i + 1]);
            }
            if ("-n".equals(splits[i])) {
                username = stripQuotes(splits[i + 1]);
            }
            if ("-p".equals(splits[i])) {
                password = stripQuotes(splits[i + 1]);
            }
            if ("-w".equals(splits[i])) {
                File file = new File(splits[i + 1]);
                BufferedReader br = null;
                try {
                    br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
                    password = br.readLine();
                    br.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        Properties jdbcProperties = SourceConfigurationUtil.loadHiveJDBCProperties();
        jdbcProperties.put(HIVE_AUTH_PASSWD, password);
        jdbcProperties.put(HIVE_AUTH_USER, username);
        this.init(url, jdbcProperties);
    }

    private void init(String url, Properties hiveProperties) {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            cnct = DriverManager.getConnection(url, hiveProperties);
            stmt = cnct.createStatement();
            metaData = cnct.getMetaData();
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private String stripQuotes(String input) {
        if (input.startsWith("'") && input.endsWith("'")) {
            return StringUtils.strip(input, "'");
        } else if (input.startsWith("\"") && input.endsWith("\"")) {
            return StringUtils.strip(input, "\"");
        } else {
            return input;
        }
    }

    public List<String> getHiveDbNames() throws Exception {
        List<String> ret = Lists.newArrayList();
        ResultSet schemas = metaData.getSchemas();
        while (schemas.next()) {
            ret.add(String.valueOf(schemas.getObject(1)));
        }
        DBUtils.closeQuietly(schemas);
        return ret;
    }

    public List<String> getHiveTableNames(String database) throws Exception {
        List<String> ret = Lists.newArrayList();
        ResultSet tables = metaData.getTables(null, database, null, null);
        while (tables.next()) {
            ret.add(String.valueOf(tables.getObject(3)));
        }
        DBUtils.closeQuietly(tables);
        return ret;
    }

    @Override
    public long getHiveTableRows(String database, String tableName) throws Exception {
        ResultSet resultSet = null;
        long count = 0;
        try {
            String query = "select count(*) from ";
            resultSet = stmt.executeQuery(query.concat(database + "." + tableName));
            if (resultSet.next()) {
                count = resultSet.getLong(1);
            }
        } finally {
            DBUtils.closeQuietly(resultSet);
        }
        return count;
    }

    @Override
    public List<Object[]> getHiveResult(String hql) throws Exception {
        ResultSet resultSet = null;
        List<Object[]> datas = new ArrayList<>();
        try {
            resultSet = stmt.executeQuery(hql);
            int columnCtn = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                Object[] data = new Object[columnCtn];
                for (int i = 0; i < columnCtn; i++) {
                    data[i] = resultSet.getObject(i + 1);
                }
                datas.add(data);
            }
        } finally {
            DBUtils.closeQuietly(resultSet);
        }
        return datas;
    }

    @Override
    public void executeHQL(String hql) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void executeHQL(String[] hqls) throws IOException {
        throw new UnsupportedOperationException();
    }

    public HiveTableMeta getHiveTableMeta(String database, String tableName) throws SQLException {
        ResultSet columns = metaData.getColumns(null, database, tableName, null);
        HiveTableMetaBuilder builder = new HiveTableMetaBuilder();
        builder.setTableName(tableName);

        List<HiveTableMeta.HiveTableColumnMeta> allColumns = Lists.newArrayList();
        while (columns.next()) {
            String columnName = columns.getString(4);
            String dataType = columns.getString(6);
            String comment = columns.getString(12);
            dataType = considerDataTypePrecision(dataType, columns.getString(7), columns.getString(9));
            allColumns.add(new HiveTableMeta.HiveTableColumnMeta(columnName, dataType, comment));
        }
        builder.setAllColumns(allColumns);
        DBUtils.closeQuietly(columns);
        String exe = "use ";
        stmt.execute(exe.concat(database));
        String des = "describe formatted ";
        ResultSet resultSet = stmt.executeQuery(des.concat(tableName));
        extractHiveTableMeta(resultSet, builder);
        DBUtils.closeQuietly(resultSet);
        return builder.createHiveTableMeta();
    }

    public static String considerDataTypePrecision(String dataType, String precision, String scale) {
        if ("VARCHAR".equalsIgnoreCase(dataType) || "CHAR".equalsIgnoreCase(dataType)) {
            if (null != precision)
                dataType = new StringBuilder(dataType).append("(").append(precision).append(")").toString();
        }
        if ("DECIMAL".equalsIgnoreCase(dataType) || "NUMERIC".equalsIgnoreCase(dataType)) {
            if (precision != null && scale != null)
                dataType = new StringBuilder(dataType).append("(").append(precision).append(",").append(scale)
                        .append(")").toString();
        }
        return dataType;
    }

    private void extractHiveTableMeta(ResultSet resultSet, HiveTableMetaBuilder builder) throws SQLException {
        while (resultSet.next()) {
            parseResultEntry(resultSet, builder);
        }
    }

    private void parseResultEntry(ResultSet resultSet, HiveTableMetaBuilder builder) throws  SQLException{
        List<HiveTableMeta.HiveTableColumnMeta> partitionColumns = Lists.newArrayList();
        if ("# Partition Information".equals(resultSet.getString(1).trim())) {
            resultSet.next();
            Preconditions.checkArgument("# col_name".equals(resultSet.getString(1).trim()));
            resultSet.next();
        if ("".equals(resultSet.getString(1).trim()))
            resultSet.next();
        do {
             if ("".equals(resultSet.getString(1).trim())) {
                 break;
             }
             partitionColumns.add(new HiveTableMeta.HiveTableColumnMeta(resultSet.getString(1).trim(),
                     resultSet.getString(2).trim(), resultSet.getString(3).trim()));
        } while (resultSet.next());
            builder.setPartitionColumns(partitionColumns);
        }

        if ("Owner:".equals(resultSet.getString(1).trim())) {
            builder.setOwner(resultSet.getString(2).trim());
        }
        if ("LastAccessTime:".equals(resultSet.getString(1).trim())) {
            try {
                int i = Integer.parseInt(resultSet.getString(2).trim());
                builder.setLastAccessTime(i);
            } catch (NumberFormatException e) {
                builder.setLastAccessTime(0);
            }
        }
        if ("Location:".equals(resultSet.getString(1).trim())) {
            builder.setSdLocation(resultSet.getString(2).trim());
        }
        if ("Table Type:".equals(resultSet.getString(1).trim())) {
            builder.setTableType(resultSet.getString(2).trim());
        }
        if ("Table Parameters:".equals(resultSet.getString(1).trim())) {
            extractTableParam(resultSet, builder);
        }
        if ("InputFormat:".equals(resultSet.getString(1).trim())) {
            builder.setSdInputFormat(resultSet.getString(2).trim());
        }
        if ("OutputFormat:".equals(resultSet.getString(1).trim())) {
            builder.setSdOutputFormat(resultSet.getString(2).trim());
        }
    }

    private void extractTableParam(ResultSet resultSet, HiveTableMetaBuilder builder) throws SQLException {
        while (resultSet.next()) {
            if (resultSet.getString(2) == null) {
                break;
            }
            if ("storage_handler".equals(resultSet.getString(2).trim())) {
                builder.setIsNative(false);//default is true
            }
            if ("totalSize".equals(resultSet.getString(2).trim())) {
                builder.setFileSize(Long.parseLong(resultSet.getString(3).trim()));//default is false
            }
            if ("numFiles".equals(resultSet.getString(2).trim())) {
                builder.setFileNum(Long.parseLong(resultSet.getString(3).trim()));
            }
            if ("skip.header.line.count".equals(resultSet.getString(2).trim())) {
                builder.setSkipHeaderLineCount(resultSet.getString(3).trim());
            }
        }
    }

    public void close() {
        DBUtils.closeQuietly(stmt);
        DBUtils.closeQuietly(cnct);
    }

    public static void main(String[] args) throws SQLException {

        BeelineHiveClient loader = new BeelineHiveClient(
                "-n root --hiveconf hive.security.authorization.sqlstd.confwhitelist.append='mapreduce.job.*|dfs.*' -u 'jdbc:hive2://sandbox:10000'");
        //BeelineHiveClient loader = new BeelineHiveClient(StringUtils.join(args, " "));
        HiveTableMeta hiveTableMeta = loader.getHiveTableMeta("default", "test_kylin_fact_part");
        System.out.println(hiveTableMeta);
        loader.close();
    }
}
