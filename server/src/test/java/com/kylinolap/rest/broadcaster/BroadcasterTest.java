package com.kylinolap.rest.broadcaster;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.CubeDescManager;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.project.ProjectInstance;
import com.kylinolap.metadata.project.ProjectManager;
import com.kylinolap.rest.controller.CacheController;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.*;

import java.lang.reflect.Method;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by qianzhou on 1/16/15.
 */

public class BroadcasterTest extends LocalFileMetadataTestCase {

    private Server server;
    private CacheController cacheController = new CacheController();

    private KylinConfig configA;
    private KylinConfig configB;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();

        server = new Server(8080);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);

        // http://localhost:8080/hello
        context.addServlet(new ServletHolder(new BroadcasterReceiveServlet(null)), "/");
        server.start();

        configA = KylinConfig.getInstanceFromEnv();
        configB = KylinConfig.getKylinConfigFromInputStream(KylinConfig.getKylinPropertiesAsInputSteam());
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
        server.stop();
    }

    @BeforeClass
    public static void startServer() throws Exception {

    }

    @AfterClass
    public static void stopServer() throws Exception {

    }

    private CubeManager getCubeManager(KylinConfig config) throws Exception {
        return CubeManager.getInstance(config);
    }
    private ProjectManager getProjectManager(KylinConfig config) throws Exception {
        return ProjectManager.getInstance(config);
    }
    private CubeDescManager getCubeDescManager(KylinConfig config) throws Exception {
        return CubeDescManager.getInstance(config);
    }
    private MetadataManager getMetadataManager(KylinConfig config) throws Exception {
        return MetadataManager.getInstance(config);
    }

    @Test
    public void testBasic() throws Exception {
        assertTrue(!configA.equals(configB));

        assertNotNull(getCubeManager(configA));
        assertNotNull(getCubeManager(configB));
        assertNotNull(getCubeDescManager(configA));
        assertNotNull(getCubeDescManager(configB));
        assertNotNull(getProjectManager(configB));
        assertNotNull(getProjectManager(configB));
        assertNotNull(getMetadataManager(configB));
        assertNotNull(getMetadataManager(configB));

        assertTrue(!getCubeManager(configA).equals(getCubeManager(configB)));
        assertTrue(!getCubeDescManager(configA).equals(getCubeDescManager(configB)));
        assertTrue(!getProjectManager(configA).equals(getProjectManager(configB)));
        assertTrue(!getMetadataManager(configA).equals(getMetadataManager(configB)));
    }

    @Test
    public void test() throws Exception {


        getStore().deleteResource("/cube/a_whole_new_cube.json");

        CubeDesc desc = getCubeDescManager(configA).getCubeDesc("test_kylin_cube_with_slr_desc");
        CubeInstance createdCube = getCubeManager(configA).createCube("a_whole_new_cube", ProjectInstance.DEFAULT_PROJECT_NAME, desc, null);
        assertTrue(createdCube == getCubeManager(configA).getCube("a_whole_new_cube"));

        getStore().deleteResource("/cube/a_whole_new_cube.json");
    }
}
