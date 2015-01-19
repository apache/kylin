package com.kylinolap.rest.service;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.restclient.Broadcaster;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.CubeDescManager;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.project.ProjectInstance;
import com.kylinolap.metadata.project.ProjectManager;
import com.kylinolap.rest.broadcaster.BroadcasterReceiveServlet;
import com.kylinolap.rest.service.CacheService;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.*;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by qianzhou on 1/16/15.
 */

public class CacheServiceTest extends LocalFileMetadataTestCase {

    private Server server;

    private KylinConfig configA;
    private KylinConfig configB;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();

        server = new Server(8080);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);

        configA = KylinConfig.getInstanceFromEnv();
        configB = KylinConfig.getKylinConfigFromInputStream(KylinConfig.getKylinPropertiesAsInputSteam());
        configB.setMetadataUrl(tempTestMetadataUrl);

        context.addServlet(new ServletHolder(new BroadcasterReceiveServlet(new BroadcasterReceiveServlet.BroadcasterHandler() {
            @Override
            public void handle(String type, String name, String event) {
                final CacheService cacheService = new CacheService() {
                    @Override
                    public KylinConfig getConfig() {
                        return configB;
                    }
                };
                Broadcaster.TYPE wipeType = Broadcaster.TYPE.getType(type);
                Broadcaster.EVENT wipeEvent = Broadcaster.EVENT.getEvent(event);
                final String log = "wipe cache type: " + wipeType + " event:" + wipeEvent + " name:" + name;
                switch (wipeEvent) {
                    case CREATE:
                    case UPDATE:
                        cacheService.rebuildCache(wipeType, name);
                        break;
                    case DROP:
                        cacheService.removeCache(wipeType, name);
                        break;
                    default:
                        throw new RuntimeException("invalid type:" + wipeEvent);
                }
            }
        })), "/");
        server.start();
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
        final String cubeName = "a_whole_new_cube";
        assertTrue(getCubeManager(configA).getCube(cubeName) == null);
        assertTrue(getCubeManager(configB).getCube(cubeName) == null);
        getCubeManager(configA).createCube(cubeName, ProjectInstance.DEFAULT_PROJECT_NAME, desc, null);
        assertNotNull(getCubeManager(configA).getCube(cubeName));
        Thread.sleep(1000);
        assertNotNull(getCubeManager(configB).getCube(cubeName));

        getStore().deleteResource("/cube/a_whole_new_cube.json");
    }
}
