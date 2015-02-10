package com.kylinolap.storage.minicluster;

import java.io.File;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

import com.kylinolap.common.util.AbstractKylinTestCase;

public class HBaseMiniClusterTest {
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

    private static HBaseTestingUtility testUtil;

    protected static MiniDFSCluster dfsCluster = null;
    protected static MiniMRCluster mrCluster = null;
    protected static MiniHBaseCluster hbaseCluster = null;
    protected static JobConf conf = null;
    protected static String LOG_DIR = "/tmp/logs";

    //@Before
    public void setup() {
        try {
            setUpBeforeClass();
        } catch (Exception e) {
            e.printStackTrace();
        }
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

        File miniclusterFolder = new File(AbstractKylinTestCase.MINICLUSTER_TEST_DATA);
        System.out.println("----" + miniclusterFolder.getAbsolutePath());

        //        String coreSitePath = miniclusterFolder + File.separator + "core-site.xml";
        //        conf.addResource(new Path(coreSitePath));
        //        String hdfsSitePath = miniclusterFolder + File.separator + "hdfs-site.xml";
        //        conf.addResource(new Path(hdfsSitePath));
        //        String mrSitePath = miniclusterFolder + File.separator + "mapred-site.xml";
        //        conf.addResource(new Path(mrSitePath));
        //        String hbSitePath = miniclusterFolder + File.separator + "hbase-site.xml";
        //        conf.addResource(new Path(hbSitePath));

        //save the dfs data to minicluster folder
        //System.setProperty("test.build.data", miniclusterFolder.getAbsolutePath());
        System.setProperty("test.build.data.basedirectory", miniclusterFolder.getAbsolutePath());
        System.setProperty("hbase.testing.preserve.testdir", "true");

        startCluster(true, conf, props);

    }

    public void tearDown() {
        try {
            tearDownAfterClass();
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        //        if (dfsCluster == null) {
        //            
        //            if (props != null) {
        //                for (Map.Entry entry : props.entrySet()) {
        //                    conf.set((String) entry.getKey(), (String) entry.getValue());
        //                }
        //            }
        //            dfsCluster = new  MiniDFSCluster(NAMENODE_PORT, conf, 2, reformatDFS, true, null, null);
        ////            ConfigurableMiniMRCluster.setConfiguration(props);
        ////            mrCluster = new ConfigurableMiniMRCluster(2, dfsCluster.getFileSystem().getName(),
        ////                    1, conf);
        //        }

        if (hbaseCluster == null) {
            //            if (props != null) {
            //                for (Map.Entry entry : props.entrySet()) {
            //                    conf.set((String) entry.getKey(), (String) entry.getValue());
            //                }
            //            }
            testUtil = new HBaseTestingUtility();
            // dfsCluster = testUtil.startMiniDFSCluster(1);
            System.out.println("-----" + testUtil.getDataTestDir());
            hbaseCluster = testUtil.startMiniCluster();
            dfsCluster = testUtil.getDFSCluster();
        }

        System.out.println("dfs uri: -------" + dfsCluster.getFileSystem().getUri().toString());
        System.out.println("dfs base directory: -------" + dfsCluster.getBaseDirectory().toString());

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
        //        if (mrCluster != null) {
        //            mrCluster.shutdown();
        //            mrCluster = null;
        //        }
        //        if (dfsCluster != null) {
        //            dfsCluster.shutdown();
        //            dfsCluster = null;
        //        }
        //        
        //        if(hbaseCluster !=null) {
        //            hbaseCluster.shutdown();
        //            hbaseCluster = null;
        //        }
        if (testUtil != null) {
            testUtil.shutdownMiniCluster();
        }
    }

    public void runTests() throws SQLException {
        try {
            setup();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            tearDown();
        }

    }

    public static void main(String[] args) throws SQLException {
        HBaseMiniClusterTest test = new HBaseMiniClusterTest();
        test.runTests();
    }

}
