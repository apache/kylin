package org.apache.kylin.jdbc;

import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.xml.transform.Result;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by shaoshi on 2/5/15.
 */
@Ignore("Not ready yet")
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
        context.setDescriptor("../server/src/main/webapp/WEB-INF/web.xml");
        context.setResourceBase("../server/src/main/webapp");
        context.setContextPath("/kylin");
        context.setParentLoaderPriority(true);

        server.setHandler(context);

        server.start();
        //server.join();

    }

    @Test
    public void test() throws Exception {
        Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
        Properties info = new Properties();
        info.put("user", "KYLIN");
        info.put("password", "ADMIN");
        Connection conn = driver.connect("jdbc:kylin://localhost/default", info);

        Statement statement = conn.createStatement();

        statement.execute("select count(*) from test_kylin_fact");

        ResultSet rs = statement.getResultSet();
        System.out.println(rs.next());
    }

}
