package org.apache.kylin.jdbc;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.*;

import java.io.File;
import java.sql.*;
import java.util.List;
import java.util.Properties;

/**
 * Created by shaoshi on 2/5/15.
 */
public class JDBCDriverTest extends HBaseMetadataTestCase {

    private static Server server = null;

    private static String previousSpringProfile = null;

    private static String SPRING_PROFILE_PROPERTY = "spring.profiles.active";

    @BeforeClass
    public static void beforeClass() throws Exception {
        previousSpringProfile = System.getProperty(SPRING_PROFILE_PROPERTY);
        System.setProperty(SPRING_PROFILE_PROPERTY, "testing");
        staticCreateTestMetadata();
        startJetty();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        stopJetty();
        staticCleanupTestMetadata();
        if (previousSpringProfile == null) {
            System.clearProperty(SPRING_PROFILE_PROPERTY);
        } else {
            System.setProperty(SPRING_PROFILE_PROPERTY, previousSpringProfile);
        }
    }

    protected static void stopJetty() throws Exception {
        if (server != null)
            server.stop();

        File workFolder = new File("work");
        if(workFolder.isDirectory() && workFolder.exists()) {
            FileUtils.deleteDirectory(workFolder);
        }
    }

    protected static void startJetty() throws Exception {

        server = new Server(7070);

        WebAppContext context = new WebAppContext();
        context.setDescriptor("./src/main/webapp/WEB-INF/web.xml");
        context.setResourceBase("./src/main/webapp");
        context.setContextPath("/kylin");
        context.setParentLoaderPriority(true);

        server.setHandler(context);

        server.start();

    }

    protected Connection getConnection() throws Exception {

        Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
        Properties info = new Properties();
        info.put("user", "ADMIN");
        info.put("password", "KYLIN");
        Connection conn = driver.connect("jdbc:kylin://localhost:7070/default", info);

        return conn;
    }

    @Test
    public void testMetadata1() throws Exception {

        // check the JDBC API here: http://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html
        Connection conn = getConnection();

        // test getSchemas();
        List<String> schemaList = Lists.newArrayList();
        DatabaseMetaData dbMetadata = conn.getMetaData();
        ResultSet resultSet = dbMetadata.getSchemas();
        while (resultSet.next()) {
            String schema = resultSet.getString("TABLE_SCHEM");
            String catalog = resultSet.getString("TABLE_CATALOG");

            System.out.println("Get schema: schema=" + schema + ", catalog=" + catalog);
            schemaList.add(schema);

        }

        resultSet.close();
        Assert.assertTrue(schemaList.contains("DEFAULT"));
        Assert.assertTrue(schemaList.contains("EDW"));

        // test getCatalogs();
        resultSet = dbMetadata.getCatalogs();

        List<String> catalogList = Lists.newArrayList();
        while (resultSet.next()) {
            String catalog = resultSet.getString("TABLE_CAT");

            System.out.println("Get catalog: catalog=" + catalog);
            catalogList.add(catalog);

        }
        Assert.assertTrue(catalogList.size() > 0 && catalogList.contains("defaultCatalog"));

        /** //Disable the test on getTableTypes() as it is not ready
         resultSet = dbMetadata.getTableTypes();

         List<String> tableTypes = Lists.newArrayList();
         while (resultSet.next()) {
         String type = resultSet.getString("TABLE_TYPE");

         System.out.println("Get table type: type=" + type);
         tableTypes.add(type);

         }

         Assert.assertTrue(tableTypes.size() > 0 && tableTypes.contains("TABLE"));
         resultSet.close();

         **/
        conn.close();
    }

    @Test
    public void testMetadata2() throws Exception {
        Connection conn = getConnection();

        List<String> tableList = Lists.newArrayList();
        DatabaseMetaData dbMetadata = conn.getMetaData();
        ResultSet resultSet = dbMetadata.getTables(null, "%", "%", new String[]{"TABLE"});
        while (resultSet.next()) {
            String schema = resultSet.getString("TABLE_SCHEM");
            String name = resultSet.getString("TABLE_NAME");

            System.out.println("Get table: schema=" + schema + ", name=" + name);
            tableList.add(schema + "." + name);

        }

        resultSet.close();
        Assert.assertTrue(tableList.contains("DEFAULT.TEST_KYLIN_FACT"));

        resultSet = dbMetadata.getColumns(null, "%", "TEST_KYLIN_FACT", "%");

        List<String> columns = Lists.newArrayList();
        while (resultSet.next()) {
            String name = resultSet.getString("COLUMN_NAME");
            String type = resultSet.getString("TYPE_NAME");

            System.out.println("Get column: name=" + name + ", data_type=" + type);
            columns.add(name);

        }

        Assert.assertTrue(columns.size() > 0 && columns.contains("CAL_DT"));
        resultSet.close();
        conn.close();
    }

    @Test
    public void testSimpleStatement() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();

        statement.execute("select count(*) from test_kylin_fact");

        ResultSet rs = statement.getResultSet();

        Assert.assertTrue(rs.next());
        int result = rs.getInt(1);

        Assert.assertTrue(result > 0);

        rs.close();
        statement.close();
        conn.close();

    }


    @Test
    public void testPreparedStatement() throws Exception {
        Connection conn = getConnection();

        PreparedStatement statement = conn.prepareStatement("select LSTG_FORMAT_NAME, sum(price) as GMV, count(1) as TRANS_CNT from test_kylin_fact " +
                "where LSTG_FORMAT_NAME = ? group by LSTG_FORMAT_NAME");

        statement.setString(1, "FP-GTC");

        ResultSet rs = statement.executeQuery();

        Assert.assertTrue(rs.next());

        String format_name = rs.getString(1);

        Assert.assertTrue("FP-GTC".equals(format_name));

        rs.close();
        statement.close();
        conn.close();

    }

    @Test
    public void testResultSet() throws Exception {
        String sql = "select LSTG_FORMAT_NAME, sum(price) as GMV, count(1) as TRANS_CNT from test_kylin_fact \n" +
                " group by LSTG_FORMAT_NAME ";

        Connection conn = getConnection();
        Statement statement = conn.createStatement();

        statement.execute(sql);

        ResultSet rs = statement.getResultSet();

        int count = 0;
        while (rs.next()) {
            count++;
            String lstg = rs.getString(1);
            double gmv = rs.getDouble(2);
            int trans_count = rs.getInt(3);

            System.out.println("Get a line: LSTG_FORMAT_NAME=" + lstg + ", GMV=" + gmv + ", TRANS_CNT=" + trans_count);
        }

        Assert.assertTrue(count > 0);
        statement.close();
        rs.close();
        conn.close();

    }

}
