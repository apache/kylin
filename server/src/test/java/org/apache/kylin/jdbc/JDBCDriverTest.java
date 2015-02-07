package org.apache.kylin.jdbc;

import com.google.common.collect.Lists;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.*;

import java.sql.*;
import java.util.List;
import java.util.Properties;

/**
 * Created by shaoshi on 2/5/15.
 */
public class JDBCDriverTest extends HBaseMetadataTestCase {

    private Server server = null;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        startJetty();
    }

    @After
    public void tearDown() throws Exception {
        stopJetty();
        this.cleanupTestMetadata();
    }

    public void stopJetty() throws Exception {
        if (server != null)
            server.stop();
    }

    public void startJetty() throws Exception {

        String jetty_home = System.getProperty("jetty.home", "..");

        server = new Server(7070);

        WebAppContext context = new WebAppContext();
        context.setDescriptor("./src/main/webapp/WEB-INF/web.xml");
        context.setResourceBase("./src/main/webapp");
        context.setContextPath("/kylin");
        context.setParentLoaderPriority(true);

        server.setHandler(context);

        server.start();

    }

    private Connection getConnection() throws Exception {

        Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
        Properties info = new Properties();
        info.put("user", "ADMIN");
        info.put("password", "KYLIN");
        Connection conn = driver.connect("jdbc:kylin://localhost:7070/default", info);

        return conn;
    }

    @Test
    public void test() throws Exception {
        testMetadata();
        testSimpleStatement();
        testResultSet();
        testPreparedStatement();
    }

    public void testMetadata() throws Exception {
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
        conn.close();
    }

    public void testSimpleStatement() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();

        statement.execute("select count(*) from test_kylin_fact");

        ResultSet rs = statement.getResultSet();

        Assert.assertTrue(rs.next());
        int result = rs.getInt(1);

        Assert.assertTrue(result > 0);

        rs.close();
        conn.close();

    }


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
        conn.close();

    }

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
        rs.close();
        conn.close();

    }

}
