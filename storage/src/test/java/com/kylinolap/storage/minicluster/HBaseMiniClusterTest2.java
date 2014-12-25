package com.kylinolap.storage.minicluster;

import java.io.File;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

import com.kylinolap.common.util.AbstractKylinTestCase;

public class HBaseMiniClusterTest2 {
    protected HiveInterface client;

    private static HBaseTestingUtility testUtil;

    protected static MiniDFSCluster dfsCluster = null;
    protected static MiniMRCluster mrCluster = null;
    protected static MiniHBaseCluster hbaseCluster = null;
    protected static JobConf conf = null;
    private static final int NAMENODE_PORT = 9010;
    private static final int JOBTRACKER_PORT = 9011;
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

        String coreSitePath = miniclusterFolder + File.separator + "core-site.xml";
        conf.addResource(new Path(coreSitePath));
        String hdfsSitePath = miniclusterFolder + File.separator + "hdfs-site.xml";
        conf.addResource(new Path(hdfsSitePath));
        String mrSitePath = miniclusterFolder + File.separator + "mapred-site.xml";
        conf.addResource(new Path(mrSitePath));
        String hbSitePath = miniclusterFolder + File.separator + "hbase-site.xml";
        conf.addResource(new Path(hbSitePath));

        Configuration c = new Configuration();
        c.addResource(new Path(coreSitePath));
        c.addResource(new Path(hdfsSitePath));
        c.addResource(new Path(mrSitePath));
        c.addResource(new Path(hbSitePath));

        //save the dfs data to minicluster folder
        System.setProperty("test.build.data", miniclusterFolder.getAbsolutePath());
        System.setProperty("test.build.data.basedirectory", miniclusterFolder.getAbsolutePath());
        startCluster(true, c, conf, props);

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
        //        String dfsDir = baseTempDir + "data";
        //        System.out.println("------" + new File(dfsDir).getAbsolutePath());
        //        FileUtils.deleteDirectory(new File(dfsDir));
        //        String mrDir = baseTempDir + "mapred";
        //        FileUtils.deleteDirectory(new File(mrDir));

        FileUtils.cleanDirectory(new File(LOG_DIR));
    }

    protected static synchronized void startCluster(boolean reformatDFS, Configuration c, JobConf conf, Properties props) throws Exception {
        if (dfsCluster == null) {

            if (props != null) {
                for (Map.Entry entry : props.entrySet()) {
                    conf.set((String) entry.getKey(), (String) entry.getValue());
                }
            }

            dfsCluster = new MiniDFSCluster(NAMENODE_PORT, conf, 2, reformatDFS, true, null, null);
            //            ConfigurableMiniMRCluster.setConfiguration(props);
            //            mrCluster = new ConfigurableMiniMRCluster(2, dfsCluster.getFileSystem().getName(),
            //                    1, conf);

            hbaseCluster = new MiniHBaseCluster(c, 1, 1, null, null);
            // Don't leave here till we've done a successful scan of the hbase:meta
            HTable t = new HTable(c, TableName.META_TABLE_NAME);
            ResultScanner s = t.getScanner(new Scan());
            while (s.next() != null) {
                continue;
            }
            s.close();
            t.close();
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
        HBaseMiniClusterTest2 test = new HBaseMiniClusterTest2();
        test.runTests();
    }

}
