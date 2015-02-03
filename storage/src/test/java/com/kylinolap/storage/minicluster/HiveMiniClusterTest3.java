package com.kylinolap.storage.minicluster;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

import org.apache.kylin.common.util.AbstractKylinTestCase;

public class HiveMiniClusterTest3 extends HiveJDBCClientTest {
    public static final File HIVE_BASE_DIR = new File("target/hive");
    public static final File HIVE_SCRATCH_DIR = new File(HIVE_BASE_DIR + "/scratchdir");
    public static final File HIVE_LOCAL_SCRATCH_DIR = new File(HIVE_BASE_DIR + "/localscratchdir");
    public static final File HIVE_METADB_DIR = new File(HIVE_BASE_DIR + "/metastoredb");
    public static final File HIVE_LOGS_DIR = new File(HIVE_BASE_DIR + "/logs");
    public static final File HIVE_TMP_DIR = new File(HIVE_BASE_DIR + "/tmp");
    public static final File HIVE_WAREHOUSE_DIR = new File(HIVE_BASE_DIR + "/warehouse");
    public static final File HIVE_TESTDATA_DIR = new File(HIVE_BASE_DIR + "/testdata");
    public static final File HIVE_HADOOP_TMP_DIR = new File(HIVE_BASE_DIR + "/hadooptmp");
    protected HiveInterface client;

    private static final HBaseTestingUtility testUtil = new HBaseTestingUtility();

    protected static MiniDFSCluster dfsCluster = null;
    protected static MiniMRCluster mrCluster = null;
    protected static MiniHBaseCluster hbaseCluster = null;
    protected static JobConf conf = null;
    private static final int NAMENODE_PORT = 9010;
    private static final int JOBTRACKER_PORT = 9011;
    protected static String LOG_DIR = "/tmp/logs";

    private static class ConfigurableMiniMRCluster extends MiniMRCluster {
        private static Properties config;

        public static void setConfiguration(Properties props) {
            config = props;
        }

        public ConfigurableMiniMRCluster(int numTaskTrackers, String namenode, int numDir, JobConf conf) throws Exception {
            super(JOBTRACKER_PORT, 0, numTaskTrackers, namenode, numDir, null, null, null, conf);
        }

        public JobConf createJobConf() {
            JobConf conf = super.createJobConf();
            if (config != null) {
                for (Map.Entry entry : config.entrySet()) {
                    conf.set((String) entry.getKey(), (String) entry.getValue());
                }
            }
            return conf;
        }
    }

    //@Before
    public void setup() {
        try {
            setUpBeforeClass();
        } catch (Exception e) {
            e.printStackTrace();
        }
        super.setup();
        startHiveMiniCluster();
    }

    protected void startHiveMiniCluster() {
        //Create and configure location for hive to dump junk in target folder
        try {
            FileUtils.forceMkdir(HIVE_BASE_DIR);
            FileUtils.forceMkdir(HIVE_SCRATCH_DIR);
            FileUtils.forceMkdir(HIVE_LOCAL_SCRATCH_DIR);
            FileUtils.forceMkdir(HIVE_LOGS_DIR);
            FileUtils.forceMkdir(HIVE_TMP_DIR);
            FileUtils.forceMkdir(HIVE_WAREHOUSE_DIR);
            FileUtils.forceMkdir(HIVE_HADOOP_TMP_DIR);
            FileUtils.forceMkdir(HIVE_TESTDATA_DIR);
        } catch (IOException e1) {
            e1.printStackTrace();
            System.exit(1);
        }

        System.setProperty("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + HIVE_METADB_DIR.getAbsolutePath() + ";create=true");
        System.setProperty("hive.metastore.warehouse.dir", HIVE_WAREHOUSE_DIR.getAbsolutePath());
        System.setProperty("hive.exec.scratchdir", HIVE_SCRATCH_DIR.getAbsolutePath());
        System.setProperty("hive.exec.local.scratchdir", HIVE_LOCAL_SCRATCH_DIR.getAbsolutePath());
        System.setProperty("hive.metastore.metadb.dir", HIVE_METADB_DIR.getAbsolutePath());
        System.setProperty("test.log.dir", HIVE_LOGS_DIR.getAbsolutePath());
        System.setProperty("hive.querylog.location", HIVE_TMP_DIR.getAbsolutePath());
        System.setProperty("hadoop.tmp.dir", HIVE_HADOOP_TMP_DIR.getAbsolutePath());
        System.setProperty("derby.stream.error.file", HIVE_BASE_DIR.getAbsolutePath() + "/derby.log");
        System.setProperty("mapred.job.tracker", "localhost:9011");

        // custom properties
        System.setProperty("hive.server2.long.polling.timeout", "5000");

    }

    //@BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // make sure the log dir exists
        File logPath = new File(LOG_DIR);
        if (!logPath.exists()) {
            logPath.mkdirs();
        }
        // configure and start the cluster
        System.setProperty("hadoop.log.dir", LOG_DIR);
        System.setProperty("javax.xml.parsers.SAXParserFactory", "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
        System.setProperty("javax.xml.parsers.DocumentBuilderFactory", "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
        Properties props = new Properties();
        props.setProperty("dfs.datanode.data.dir.perm", "775");
        conf = new JobConf();
        //        String hadoopConfDir = "test" + File.separator + "resources" + File.separator + "hadoop" + File.separator + "conf";
        //        File hadoopConfFolder = new File(hadoopConfDir);

        File miniclusterFolder = new File(AbstractKylinTestCase.MINICLUSTER_TEST_DATA);
        System.out.println("----" + miniclusterFolder.getAbsolutePath());
        if (!miniclusterFolder.exists()) {
            System.err.println("Couldn't find " + miniclusterFolder + ", exit...");
            System.exit(1);
        }

        String coreSitePath = miniclusterFolder + File.separator + "core-site.xml";
        conf.addResource(new Path(coreSitePath));
        String hdfsSitePath = miniclusterFolder + File.separator + "hdfs-site.xml";
        conf.addResource(new Path(hdfsSitePath));
        String mrSitePath = miniclusterFolder + File.separator + "mapred-site.xml";
        conf.addResource(new Path(mrSitePath));

        //save the dfs data to minicluster folder
        //conf.set("test.build.data", miniclusterFolder.getAbsolutePath());
        System.setProperty("test.build.data", miniclusterFolder.getAbsolutePath());

        //System.setProperty("test.build.data.basedirectory",miniclusterFolder.getAbsolutePath() + File.separator + "testdata");
        startCluster(true, conf, props);

    }

    public void tearDown() {
        try {
            tearDownAfterClass();
        } catch (Exception e) {
            e.printStackTrace();
        }
        super.tearDown();
    }

    //@AfterClass
    public static void tearDownAfterClass() throws Exception {
        stopCluster();
        // clean up the hdfs files created by mini cluster
        //        String baseTempDir = "build" + File.separator + "test" + File.separator;
        String baseTempDir = dfsCluster.getBaseDirectory();
        String dfsDir = baseTempDir + "data";
        System.out.println("------" + new File(dfsDir).getAbsolutePath());
        FileUtils.deleteDirectory(new File(dfsDir));
        String mrDir = baseTempDir + "mapred";
        FileUtils.deleteDirectory(new File(mrDir));

        FileUtils.cleanDirectory(new File(LOG_DIR));
    }

    protected static synchronized void startCluster(boolean reformatDFS, JobConf conf, Properties props) throws Exception {
        if (dfsCluster == null) {

            if (props != null) {
                for (Map.Entry entry : props.entrySet()) {
                    conf.set((String) entry.getKey(), (String) entry.getValue());
                }
            }
            dfsCluster = new MiniDFSCluster(NAMENODE_PORT, conf, 2, reformatDFS, true, null, null);
            ConfigurableMiniMRCluster.setConfiguration(props);
            mrCluster = new ConfigurableMiniMRCluster(2, dfsCluster.getFileSystem().getName(), 1, conf);
        }

        HBaseTestingUtility testUtil = new HBaseTestingUtility();
        testUtil.setDFSCluster(dfsCluster);
        hbaseCluster = testUtil.startMiniCluster();

        System.out.println("dfs uri: -------" + dfsCluster.getFileSystem().getUri().toString());

        Configuration config = hbaseCluster.getConf();
        String host = config.get(HConstants.ZOOKEEPER_QUORUM);
        String port = config.get(HConstants.ZOOKEEPER_CLIENT_PORT);
        String parent = config.get(HConstants.ZOOKEEPER_ZNODE_PARENT);

        // reduce rpc retry
        config.set(HConstants.HBASE_CLIENT_PAUSE, "3000");
        config.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "5");
        config.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "60000");

        String hbaseconnectionUrl = "hbase:" + host + ":" + port + ":" + parent;
        System.out.println("hbase connection url: -----" + hbaseconnectionUrl);
    }

    protected static void stopCluster() throws Exception {
        if (mrCluster != null) {
            mrCluster.shutdown();
            mrCluster = null;
        }
        if (dfsCluster != null) {
            dfsCluster.shutdown();
            dfsCluster = null;
        }

        if (hbaseCluster != null) {
            hbaseCluster.shutdown();
            hbaseCluster = null;
        }
    }

    protected Connection getHiveConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:hive2:///", "hive", "");
    }

    public static void main(String[] args) throws SQLException {
        HiveMiniClusterTest3 test = new HiveMiniClusterTest3();
        test.runTests();
    }

}
