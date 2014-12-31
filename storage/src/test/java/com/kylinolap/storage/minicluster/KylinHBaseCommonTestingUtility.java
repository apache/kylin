package com.kylinolap.storage.minicluster;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.mapred.JobConf;

import com.kylinolap.common.util.AbstractKylinTestCase;

/**
 * HBase minicluster Testing utility class for Kylin, extended from HBase's;
 * @author shaoshi
 *
 */
public class KylinHBaseCommonTestingUtility extends HBaseTestingUtility {
    protected static String LOG_DIR = "/tmp/logs";

    protected File dataTestDir = null;

    protected File clusterTestDir = null;

    protected MiniZooKeeperCluster zkCluster = null;

    private Path getBaseTestDir() {
        String PathName = System.getProperty(BASE_TEST_DIRECTORY_KEY, DEFAULT_BASE_TEST_DIRECTORY);

        return new Path(PathName);
    }

    protected Path setupDataTestDir() {

        Path testPath = new Path(getBaseTestDir(), "testPath");

        dataTestDir = new File(testPath.toString()).getAbsoluteFile();
        // Set this property so if mapreduce jobs run, they will use this as their home dir.
        System.setProperty("test.build.dir", dataTestDir.toString());

        createSubDir("hbase.local.dir", testPath, "hbase-local-dir");

        return testPath;
    }

    public Path getDataTestDirOnTestFS() throws IOException {
        FileSystem fs = getTestFileSystem();
        return new Path(fs.getWorkingDirectory(), "testPath");
    }

    public void init() {
        System.setProperty("hadoop.log.dir", LOG_DIR);
        System.setProperty("javax.xml.parsers.SAXParserFactory", "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
        System.setProperty("javax.xml.parsers.DocumentBuilderFactory", "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
        Properties props = new Properties();
        props.setProperty("dfs.datanode.data.dir.perm", "775");

        File miniclusterFolder = new File(AbstractKylinTestCase.MINICLUSTER_TEST_DATA);
        System.out.println("----" + miniclusterFolder.getAbsolutePath());

        //save the dfs data to minicluster folder
        //System.setProperty("test.build.data", miniclusterFolder.getAbsolutePath());
        System.setProperty("test.build.data.basedirectory", miniclusterFolder.getAbsolutePath());
        System.setProperty("hbase.testing.preserve.testdir", "true");

    }

    private void setupClusterTestDir() {

        if (clusterTestDir != null)
            return;

        Path testDir = getDataTestDir("dfscluster_kylin");
        clusterTestDir = new File(testDir.toString()).getAbsoluteFile();
        // Have it cleaned up on exit
        conf.set("test.build.data", clusterTestDir.getPath());
    }

    public Path getDataTestDir() {
        if (this.dataTestDir == null) {
            setupDataTestDir();
        }
        return new Path(this.dataTestDir.getAbsolutePath());
    }

    public MiniHBaseCluster startMiniCluster(final int numMasters, final int numSlaves, int numDataNodes, final String[] dataNodeHosts, Class<? extends HMaster> masterClass, Class<? extends MiniHBaseCluster.MiniHBaseClusterRegionServer> regionserverClass) throws Exception {
        if (dataNodeHosts != null && dataNodeHosts.length != 0) {
            numDataNodes = dataNodeHosts.length;
        }

        LOG.info("Starting up minicluster with " + numMasters + " master(s) and " + numSlaves + " regionserver(s) and " + numDataNodes + " datanode(s)");

        setupClusterTestDir();

        // Bring up mini dfs cluster. This spews a bunch of warnings about missing
        // scheme. Complaints are 'Scheme is undefined for build/test/data/dfs/name1'.
        startMiniDFSCluster(numDataNodes, dataNodeHosts);

        // Start up a zk cluster.
        startMiniZKCluster(clusterTestDir, 1);

        // Start the MiniHBaseCluster
        return startMiniHBaseCluster(numMasters, numSlaves, masterClass, regionserverClass);
    }

    /**
     * Start a mini ZK cluster. If the property "test.hbase.zookeeper.property.clientPort" is set
     *  the port mentionned is used as the default port for ZooKeeper.
     */
    private MiniZooKeeperCluster startMiniZKCluster(final File dir, int zooKeeperServerNum) throws Exception {
        if (this.zkCluster != null) {
            throw new IOException("Cluster already running at " + dir);
        }
        this.zkCluster = new MiniZooKeeperCluster(this.getConfiguration());
        final int defPort = this.conf.getInt("test.hbase.zookeeper.property.clientPort", 0);
        if (defPort > 0) {
            // If there is a port in the config file, we use it.
            this.zkCluster.setDefaultClientPort(defPort);
        }
        int clientPort = this.zkCluster.startup(dir, zooKeeperServerNum);
        this.conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(clientPort));
        return this.zkCluster;
    }

    public MiniHBaseCluster startMiniCluster() throws Exception {
        this.init();
        return startMiniCluster(1, 1, 1, null, null, null);
    }

    public static void main(String[] args) throws Exception {
        KylinHBaseCommonTestingUtility util = new KylinHBaseCommonTestingUtility();
        util.startMiniCluster();

        Thread.sleep(5 * 1000);
        util.shutdownMiniCluster();
    }

}
