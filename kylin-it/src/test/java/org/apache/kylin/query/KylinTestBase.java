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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.LogManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.apache.kylin.query.schema.OLAPSchemaFactory;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.SortedTable;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.DataTypeException;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.ext.h2.H2Connection;
import org.dbunit.ext.h2.H2DataTypeFactory;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;

/**
 */
public class KylinTestBase {

    private static final Logger logger = LoggerFactory.getLogger(KylinTestBase.class);
    public static boolean PRINT_RESULT = false;

    class ObjectArray {
        Object[] data;

        public ObjectArray(Object[] data) {
            this.data = data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            ObjectArray that = (ObjectArray) o;

            // Probably incorrect - comparing Object[] arrays with Arrays.equals
            return Arrays.equals(data, that.data);

        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }
    }

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
    protected static String ITDirHeader = "";

    protected static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static class FileByNameComparator implements Comparator<File> {
        @Override
        public int compare(File o1, File o2) {
            return String.CASE_INSENSITIVE_ORDER.compare(o1.getName(), o2.getName());
        }
    }

    /**
     * @param folder
     * @param fileType specify the interested file type by file extension
     * @return
     */
    protected static List<File> getFilesFromFolder(final File folder, final String fileType) {
        System.out.println(folder.getAbsolutePath());
        Set<File> set = new TreeSet<>(new FileByNameComparator());
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.getName().toLowerCase().endsWith(fileType.toLowerCase())) {
                set.add(fileEntry);
            }
        }
        return new ArrayList<>(set);
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
        if (needSort) {
            String[] columnNames = new String[queryTable.getTableMetaData().getColumns().length];
            for (int i = 0; i < columnNames.length; i++) {
                columnNames[i] = queryTable.getTableMetaData().getColumns()[i].getColumnName();
            }

            queryTable = new SortedTable(queryTable, columnNames);
        }
        if (PRINT_RESULT)
            printResult(queryTable);

        return queryTable;
    }

    protected int executeQuery(String sql, boolean needDisplay) throws SQLException {

        // change join type to match current setting
        sql = changeJoinType(sql, joinType);

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            logger.info("start running...");
            statement = cubeConnection.createStatement();
            resultSet = statement.executeQuery(sql);
            logger.info("stop running...");

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
        if (PRINT_RESULT)
            printResult(queryTable);
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
        logger.info("The actual sql executed is: " + ret);

        return ret;
    }

    protected void execQueryUsingH2(String queryFolder, boolean needSort) throws Exception {
        logger.info("---------- Running H2 queries: " + queryFolder);

        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            String sql = getTextFromFile(sqlFile);

            // execute H2
            logger.info("Query Result from H2 - " + queryName);
            executeQuery(newH2Connection(), queryName, sql, needSort);
        }
    }

    protected void verifyResultRowColCount(String queryFolder) throws Exception {
        logger.info("---------- verify result count in folder: " + queryFolder);

        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            String sql = getTextFromFile(sqlFile);

            File expectResultFile = new File(sqlFile.getParent(), sqlFile.getName() + ".expected");
            Pair<Integer, Integer> pair = getExpectedRowAndCol(expectResultFile);
            int expectRowCount = pair.getFirst();
            int expectColCount = pair.getSecond();

            // execute Kylin
            logger.info("Query Result from Kylin - " + queryName + "  (" + queryFolder + ")");
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            ITable kylinTable = executeQuery(kylinConn, queryName, sql, false);

            // compare the result
            if (expectRowCount >= 0)
                Assert.assertEquals(queryName, expectRowCount, kylinTable.getRowCount());

            if (expectColCount >= 0)
                Assert.assertEquals(queryName, expectColCount, kylinTable.getTableMetaData().getColumns().length);
        }
    }

    private Pair<Integer, Integer> getExpectedRowAndCol(File expectResultFile) throws IOException {
        List<String> lines = Files.readLines(expectResultFile, Charset.forName("UTF-8"));
        int row = -1;
        int col = -1;
        try {
            row = Integer.parseInt(lines.get(0).trim());
        } catch (Exception ex) {
        }
        try {
            col = Integer.parseInt(lines.get(1).trim());
        } catch (Exception ex) {
        }
        return Pair.newPair(row, col);
    }

    protected void verifyResultContent(String queryFolder) throws Exception {
        logger.info("---------- verify result content in folder: " + queryFolder);

        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            String sql = getTextFromFile(sqlFile);

            File expectResultFile = new File(sqlFile.getParent(), sqlFile.getName() + ".expected.xml");
            IDataSet expect = new FlatXmlDataSetBuilder().build(expectResultFile);
            // Get expected table named "expect". FIXME Only support default table name
            ITable expectTable = expect.getTable("expect");

            // execute Kylin
            logger.info("Query Result from Kylin - " + queryName + "  (" + queryFolder + ")");
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            ITable kylinTable = executeQuery(kylinConn, queryName, sql, false);

            // compare the result
            assertTableEquals(expectTable, kylinTable);
        }
    }

    protected void execAndCompResultSize(String queryFolder, String[] exclusiveQuerys, boolean needSort) throws Exception {
        logger.info("---------- test folder: " + queryFolder);
        Set<String> exclusiveSet = buildExclusiveSet(exclusiveQuerys);

        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            if (exclusiveSet.contains(queryName)) {
                continue;
            }
            String sql = getTextFromFile(sqlFile);

            // execute Kylin
            logger.info("Query Result from Kylin - " + queryName + "  (" + queryFolder + ")");
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            ITable kylinTable = executeQuery(kylinConn, queryName, sql, needSort);

            // execute H2
            logger.info("Query Result from H2 - " + queryName);
            ITable h2Table = executeQuery(newH2Connection(), queryName, sql, needSort);

            try {
                // compare the result
                Assert.assertEquals(h2Table.getRowCount(), kylinTable.getRowCount());
            } catch (Throwable t) {
                logger.info("execAndCompResultSize failed on: " + sqlFile.getAbsolutePath());
                throw t;
            }

            compQueryCount++;
            if (kylinTable.getRowCount() == 0) {
                zeroResultQueries.add(sql);
            }
        }
    }

    protected void execAndCompColumnCount(String input, int expectedColumnCount) throws Exception {
        logger.info("---------- test column count: " + input);
        Set<String> sqlSet = ImmutableSet.of(input);

        for (String sql : sqlSet) {
            // execute Kylin
            logger.info("Query Result from Kylin - " + sql);
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            ITable kylinTable = executeQuery(kylinConn, sql, sql, false);

            try {
                // compare the result
                Assert.assertEquals(expectedColumnCount, kylinTable.getTableMetaData().getColumns().length);
            } catch (Throwable t) {
                logger.info("execAndCompColumnCount failed on: " + sql);
                throw t;
            }
        }
    }

    protected void execLimitAndValidate(String queryFolder) throws Exception {
        logger.info("---------- test folder: " + new File(queryFolder).getAbsolutePath());

        int appendLimitQueries = 0;
        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            String sql = getTextFromFile(sqlFile);

            String sqlWithLimit;
            if (sql.toLowerCase().contains("limit ")) {
                sqlWithLimit = sql;
            } else {
                sqlWithLimit = sql + " limit 5";
                appendLimitQueries++;
            }

            // execute Kylin
            logger.info("Query Result from Kylin - " + queryName + "  (" + queryFolder + ")");
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            ITable kylinTable = executeQuery(kylinConn, queryName, sqlWithLimit, false);

            // execute H2
            logger.info("Query Result from H2 - " + queryName);
            ITable h2Table = executeQuery(newH2Connection(), queryName, sql, false);

            try {
                assertTableContains(h2Table, kylinTable);
            } catch (Throwable t) {
                logger.info("execAndCompQuery failed on: " + sqlFile.getAbsolutePath());
                throw t;
            }

            compQueryCount++;
            if (kylinTable.getRowCount() == 0) {
                zeroResultQueries.add(sql);
            }
        }
        logger.info("Queries appended with limit: " + appendLimitQueries);
    }

    protected void execAndCompQuery(String queryFolder, String[] exclusiveQuerys, boolean needSort) throws Exception {
        logger.info("---------- test folder: " + new File(queryFolder).getAbsolutePath());
        Set<String> exclusiveSet = buildExclusiveSet(exclusiveQuerys);

        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            if (exclusiveSet.contains(queryName)) {
                continue;
            }
            String sql = getTextFromFile(sqlFile);

            // execute Kylin
            logger.info("Query Result from Kylin - " + queryName + "  (" + queryFolder + ")");
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            ITable kylinTable = executeQuery(kylinConn, queryName, sql, needSort);

            // execute H2
            logger.info("Query Result from H2 - " + queryName);
            long currentTime = System.currentTimeMillis();
            ITable h2Table = executeQuery(newH2Connection(), queryName, sql, needSort);
            logger.info("H2 spent " + (System.currentTimeMillis() - currentTime) + " mili-seconds.");

            try {
                // compare the result
                assertTableEquals(h2Table, kylinTable);
            } catch (Throwable t) {
                logger.info("execAndCompQuery failed on: " + sqlFile.getAbsolutePath());
                throw t;
            }

            compQueryCount++;
            if (kylinTable.getRowCount() == 0) {
                zeroResultQueries.add(sql);
            }
        }
    }

    protected void execAndCompDynamicQuery(String queryFolder, String[] exclusiveQuerys, boolean needSort) throws Exception {
        logger.info("---------- test folder: " + queryFolder);
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
            logger.info("Query Result from Kylin - " + queryName + "  (" + queryFolder + ")");
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            ITable kylinTable = executeDynamicQuery(kylinConn, queryName, sql, parameters, needSort);

            // execute H2
            logger.info("Query Result from H2 - " + queryName);
            ITable h2Table = executeDynamicQuery(newH2Connection(), queryName, sql, parameters, needSort);

            // compare the result
            assertTableEquals(h2Table, kylinTable);
        }
    }

    protected void assertTableEquals(ITable h2Table, ITable kylinTable) throws DatabaseUnitException {
        HackedDbUnitAssert dbUnit = new HackedDbUnitAssert();
        dbUnit.hackIgnoreIntBigIntMismatch();
        dbUnit.assertEquals(h2Table, kylinTable);
    }

    protected void assertTableContains(ITable h2Table, ITable kylinTable) throws DatabaseUnitException {
        HackedDbUnitAssert dbUnit = new HackedDbUnitAssert();
        dbUnit.hackIgnoreIntBigIntMismatch();
        dbUnit.hackCheckContains();
        dbUnit.assertEquals(h2Table, kylinTable);
    }

    @SuppressWarnings("deprecation")
    protected static H2Connection newH2Connection() throws DatabaseUnitException {
        H2Connection h2Conn = new H2Connection(h2Connection, null);
        h2Conn.getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new TestH2DataTypeFactory());
        h2Conn.getConfig().setFeature(DatabaseConfig.FEATURE_DATATYPE_WARNING, false);
        return h2Conn;
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
        logger.info("Testing Query " + queryName);
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
        logger.info(sb.toString());
        return count;
    }

    protected static void setupAll() throws Exception {
        //setup env
        HBaseMetadataTestCase.staticCreateTestMetadata();
        config = KylinConfig.getInstanceFromEnv();

        //setup cube conn
        File olapTmp = OLAPSchemaFactory.createTempOLAPJson(ProjectInstance.DEFAULT_PROJECT_NAME, config);
        cubeConnection = DriverManager.getConnection("jdbc:calcite:model=" + olapTmp.getAbsolutePath());

        //setup h2
        h2Connection = DriverManager.getConnection("jdbc:h2:mem:db" + (h2InstanceCount++) + ";CACHE_SIZE=32072", "sa", "");
        // Load H2 Tables (inner join)
        H2Database h2DB = new H2Database(h2Connection, config);
        h2DB.loadAllTables();
    }

    protected static void clean() {
        if (cubeConnection != null)
            closeConnection(cubeConnection);
        if (h2Connection != null)
            closeConnection(h2Connection);

        HBaseMetadataTestCase.staticCleanupTestMetadata();
        RemoveBlackoutRealizationsRule.blackList.clear();

    }

    protected boolean checkFinalPushDownLimit() {
        OLAPContext context = getFirstOLAPContext();
        return context.storageContext.isLimitPushDownEnabled();

    }

    private OLAPContext getFirstOLAPContext() {
        return OLAPContext.getThreadLocalContexts().iterator().next();
    }

    protected String getQueryFolderPrefix() {
        return "";
    }

    protected Throwable findRoot(Throwable throwable) {
        while (true) {
            if (throwable.getCause() != null) {
                throwable = throwable.getCause();
            } else {
                break;
            }
        }
        return throwable;
    }

}
