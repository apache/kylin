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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.LogManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.dbunit.Assertion;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.SortedTable;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.DataTypeException;
import org.dbunit.ext.h2.H2Connection;
import org.dbunit.ext.h2.H2DataTypeFactory;
import org.junit.Assert;

import com.google.common.io.Files;

/**
 */
public class KylinTestBase {

    // Hack for the different constant integer type between optiq (INTEGER) and
    // h2 (BIGINT)
    public static class TestH2DataTypeFactory extends H2DataTypeFactory {
        @Override
        public DataType createDataType(int sqlType, String sqlTypeName, String tableName, String columnName) throws DataTypeException {

            if ((columnName.startsWith("COL") || columnName.startsWith("col")) && sqlType == Types.BIGINT) {
                return DataType.INTEGER;
            }
            return super.createDataType(sqlType, sqlTypeName);
        }
    }

    protected static final String resultTableName = "query result of ";
    protected static KylinConfig config = null;
    protected static Connection cubeConnection = null;
    protected static Connection h2Connection = null;
    protected static String joinType = "default";
    protected static int h2InstanceCount = 0;

    protected static int compQueryCount = 0;
    protected static ArrayList<String> zeroResultQueries = new ArrayList<String>();

    protected static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @param folder
     * @param fileType
     *            specify the interested file type by file extension
     * @return
     */
    protected static List<File> getFilesFromFolder(final File folder, final String fileType) {
        List<File> files = new ArrayList<File>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.getName().toLowerCase().endsWith(fileType.toLowerCase())) {
                files.add(fileEntry);
            }
        }
        return files;
    }

    protected static void getFilesFromFolderR(final String directoryStr, List<File> files, final String fileType) {
        File folder = new File(directoryStr);
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                getFilesFromFolderR(fileEntry.getAbsolutePath(), files, fileType);
            } else if (fileEntry.isFile()) {
                if (fileEntry.getName().toLowerCase().endsWith(fileType.toLowerCase())) {
                    files.add(fileEntry);
                }
            }
        }
    }

    protected static void putTextTofile(File file, String sql) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write(sql, 0, sql.length());
        writer.close();
    }

    protected static String getTextFromFile(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        StringBuilder stringBuilder = new StringBuilder();
        String ls = System.getProperty("line.separator");
        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
            stringBuilder.append(ls);
        }
        reader.close();
        return stringBuilder.toString();
    }

    protected static List<String> getParameterFromFile(File sqlFile) throws IOException {
        String sqlFileName = sqlFile.getAbsolutePath();
        int prefixIndex = sqlFileName.lastIndexOf(".sql");
        String dataFielName = sqlFileName.substring(0, prefixIndex) + ".dat";
        File dataFile = new File(dataFielName);
        List<String> parameters = Files.readLines(dataFile, Charset.defaultCharset());
        return parameters;
    }

    protected static void printInfo(String info) {
        System.out.println(new Timestamp(System.currentTimeMillis()) + " - " + info);
    }

    protected static void printResult(ITable resultTable) throws DataSetException {
        StringBuilder sb = new StringBuilder();

        int columnCount = resultTable.getTableMetaData().getColumns().length;
        String[] columns = new String[columnCount];

        for (int i = 0; i < columnCount; i++) {
            sb.append(resultTable.getTableMetaData().getColumns()[i].getColumnName());
            sb.append("-");
            sb.append(resultTable.getTableMetaData().getColumns()[i].getDataType());
            sb.append("\t");
            columns[i] = resultTable.getTableMetaData().getColumns()[i].getColumnName();
        }
        sb.append("\n");

        for (int i = 0; i < resultTable.getRowCount(); i++) {
            for (int j = 0; j < columns.length; j++) {
                sb.append(resultTable.getValue(i, columns[j]));
                sb.append("\t");
            }
            sb.append("\n");
        }
        System.out.println(sb.toString());
    }

    protected Set<String> buildExclusiveSet(String[] exclusiveQuerys) {
        Set<String> exclusiveSet = new HashSet<String>();
        if (exclusiveQuerys != null) {
            for (String query : exclusiveQuerys) {
                exclusiveSet.add(query);
            }
        }
        return exclusiveSet;
    }

    // ////////////////////////////////////////////////////////////////////////////////////////
    // execute

    protected ITable executeQuery(IDatabaseConnection dbConn, String queryName, String sql, boolean needSort) throws Exception {

        // change join type to match current setting
        sql = changeJoinType(sql, joinType);

        ITable queryTable = dbConn.createQueryTable(resultTableName + queryName, sql);
        String[] columnNames = new String[queryTable.getTableMetaData().getColumns().length];
        for (int i = 0; i < columnNames.length; i++) {
            columnNames[i] = queryTable.getTableMetaData().getColumns()[i].getColumnName();
        }
        if (needSort) {
            queryTable = new SortedTable(queryTable, columnNames);
        }
        //printResult(queryTable);

        return queryTable;
    }

    protected int executeQuery(String sql, boolean needDisplay) throws SQLException {

        // change join type to match current setting
        sql = changeJoinType(sql, joinType);

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            printInfo("start running...");
            statement = cubeConnection.createStatement();
            resultSet = statement.executeQuery(sql);
            printInfo("stop running...");

            return output(resultSet, needDisplay);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }

    }

    protected ITable executeDynamicQuery(IDatabaseConnection dbConn, String queryName, String sql, List<String> parameters, boolean needSort) throws Exception {

        // change join type to match current setting
        sql = changeJoinType(sql, joinType);

        PreparedStatement prepStat = dbConn.getConnection().prepareStatement(sql);
        for (int j = 1; j <= parameters.size(); ++j) {
            prepStat.setString(j, parameters.get(j - 1).trim());
        }

        ITable queryTable = dbConn.createTable(resultTableName + queryName, prepStat);
        String[] columnNames = new String[queryTable.getTableMetaData().getColumns().length];
        for (int i = 0; i < columnNames.length; i++) {
            columnNames[i] = queryTable.getTableMetaData().getColumns()[i].getColumnName();
        }
        if (needSort) {
            queryTable = new SortedTable(queryTable, columnNames);
        }
        //printResult(queryTable);
        return queryTable;
    }

    // end of execute
    // ////////////////////////////////////////////////////////////////////////////////////////

    protected static String changeJoinType(String sql, String targetType) {

        if (targetType.equalsIgnoreCase("default"))
            return sql;

        String specialStr = "changeJoinType_DELIMITERS";
        sql = sql.replaceAll(System.getProperty("line.separator"), " " + specialStr + " ");

        String[] tokens = StringUtils.split(sql, null);// split white spaces
        for (int i = 0; i < tokens.length - 1; ++i) {
            if ((tokens[i].equalsIgnoreCase("inner") || tokens[i].equalsIgnoreCase("left")) && tokens[i + 1].equalsIgnoreCase("join")) {
                tokens[i] = targetType.toLowerCase();
            }
        }

        String ret = StringUtils.join(tokens, " ");
        ret = ret.replaceAll(specialStr, System.getProperty("line.separator"));
        System.out.println("The actual sql executed is: " + ret);

        return ret;
    }

    protected static void batchChangeJoinType(String targetType) throws IOException {
        List<File> files = new LinkedList<File>();
        getFilesFromFolderR("src/test/resources/query", files, ".sql");
        for (File file : files) {
            String x = changeJoinType(getTextFromFile(file), targetType);
            putTextTofile(file, x);
        }
    }

    protected void execQueryUsingH2(String queryFolder, boolean needSort) throws Exception {
        printInfo("---------- Running H2 queries: " + queryFolder);

        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            String sql = getTextFromFile(sqlFile);

            // execute H2
            printInfo("Query Result from H2 - " + queryName);
            H2Connection h2Conn = new H2Connection(h2Connection, null);
            h2Conn.getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new TestH2DataTypeFactory());
            executeQuery(h2Conn, queryName, sql, needSort);
        }
    }

    protected void verifyResultRowCount(String queryFolder) throws Exception {
        printInfo("---------- verify result count in folder: " + queryFolder);

        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            String sql = getTextFromFile(sqlFile);

            File expectResultFile = new File(sqlFile.getParent(), sqlFile.getName() + ".expected");
            int expectRowCount = Integer.parseInt(Files.readFirstLine(expectResultFile, Charset.defaultCharset()));

            // execute Kylin
            printInfo("Query Result from Kylin - " + queryName + "  (" + queryFolder + ")");
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            ITable kylinTable = executeQuery(kylinConn, queryName, sql, false);

            // compare the result
            Assert.assertEquals(expectRowCount, kylinTable.getRowCount());
            // Assertion.assertEquals(expectRowCount, kylinTable.getRowCount());
        }
    }

    protected void execAndCompQuery(String queryFolder, String[] exclusiveQuerys, boolean needSort) throws Exception {
        printInfo("---------- test folder: " + queryFolder);
        Set<String> exclusiveSet = buildExclusiveSet(exclusiveQuerys);

        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            if (exclusiveSet.contains(queryName)) {
                continue;
            }
            String sql = getTextFromFile(sqlFile);

            // execute Kylin
            printInfo("Query Result from Kylin - " + queryName + "  (" + queryFolder + ")");
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            ITable kylinTable = executeQuery(kylinConn, queryName, sql, needSort);

            // execute H2
            printInfo("Query Result from H2 - " + queryName);
            H2Connection h2Conn = new H2Connection(h2Connection, null);
            h2Conn.getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new TestH2DataTypeFactory());
            ITable h2Table = executeQuery(h2Conn, queryName, sql, needSort);

            // compare the result
            Assertion.assertEquals(h2Table, kylinTable);

            compQueryCount++;
            if (kylinTable.getRowCount() == 0) {
                zeroResultQueries.add(sql);
            }
        }
    }


    protected void execAndCompResultSize(String queryFolder, String[] exclusiveQuerys, boolean needSort) throws Exception {
        printInfo("---------- test folder: " + queryFolder);
        Set<String> exclusiveSet = buildExclusiveSet(exclusiveQuerys);

        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            if (exclusiveSet.contains(queryName)) {
                continue;
            }
            String sql = getTextFromFile(sqlFile);

            // execute Kylin
            printInfo("Query Result from Kylin - " + queryName + "  (" + queryFolder + ")");
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            ITable kylinTable = executeQuery(kylinConn, queryName, sql, needSort);

            // execute H2
            printInfo("Query Result from H2 - " + queryName);
            H2Connection h2Conn = new H2Connection(h2Connection, null);
            h2Conn.getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new TestH2DataTypeFactory());
            ITable h2Table = executeQuery(h2Conn, queryName, sql, needSort);

            // compare the result
            Assert.assertEquals(h2Table.getRowCount(), kylinTable.getRowCount());

            compQueryCount++;
            if (kylinTable.getRowCount() == 0) {
                zeroResultQueries.add(sql);
            }
        }
    }

    protected void execAndCompDynamicQuery(String queryFolder, String[] exclusiveQuerys, boolean needSort) throws Exception {
        printInfo("---------- test folder: " + queryFolder);
        Set<String> exclusiveSet = buildExclusiveSet(exclusiveQuerys);

        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            if (exclusiveSet.contains(queryName)) {
                continue;
            }
            String sql = getTextFromFile(sqlFile);
            List<String> parameters = getParameterFromFile(sqlFile);

            // execute Kylin
            printInfo("Query Result from Kylin - " + queryName + "  (" + queryFolder + ")");
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            ITable kylinTable = executeDynamicQuery(kylinConn, queryName, sql, parameters, needSort);

            // execute H2
            printInfo("Query Result from H2 - " + queryName);
            IDatabaseConnection h2Conn = new DatabaseConnection(h2Connection);
            h2Conn.getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new TestH2DataTypeFactory());
            ITable h2Table = executeDynamicQuery(h2Conn, queryName, sql, parameters, needSort);

            // compare the result
            Assertion.assertEquals(h2Table, kylinTable);
        }
    }

    protected int runSqlFile(String file) throws Exception {
        return runSQL(new File(file), true, false);
    }

    protected int runSQL(File sqlFile, boolean debug, boolean explain) throws Exception {
        if (debug) {
            System.setProperty("calcite.debug", "true");
            InputStream inputStream = new FileInputStream("src/test/resources/logging.properties");
            LogManager.getLogManager().readConfiguration(inputStream);
        }

        String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
        printInfo("Testing Query " + queryName);
        String sql = getTextFromFile(sqlFile);
        if (explain) {
            sql = "explain plan for " + sql;
        }
        int count = executeQuery(sql, true);

        if (debug) {
            System.clearProperty("calcite.debug");
        }
        return count;
    }

    protected void batchExecuteQuery(String queryFolder) throws Exception {
        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            runSQL(sqlFile, false, false);
        }
    }

    protected int output(ResultSet resultSet, boolean needDisplay) throws SQLException {
        int count = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder sb = new StringBuilder("\n");
        if (needDisplay) {
            for (int i = 1; i <= columnCount; i++) {
                sb.append(metaData.getColumnName(i));
                sb.append("-");
                sb.append(metaData.getTableName(i));
                sb.append("-");
                sb.append(metaData.getColumnTypeName(i));
                if (i < columnCount) {
                    sb.append("\t");
                } else {
                    sb.append("\n");
                }
            }
        }

        while (resultSet.next()) {
            if (needDisplay) {
                for (int i = 1; i <= columnCount; i++) {
                    sb.append(resultSet.getString(i));
                    if (i < columnCount) {
                        sb.append("\t");
                    } else {
                        sb.append("\n");
                    }
                }
            }
            count++;
        }
        printInfo(sb.toString());
        return count;
    }
}
