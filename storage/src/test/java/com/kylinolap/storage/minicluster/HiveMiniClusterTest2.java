package com.kylinolap.storage.minicluster;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

/**
 * This is a similar test with HiveMiniClusterTest.java ; except need set HADOOP_HOME, it also need give bigger JVM heap size, like 
 * "-Xmx2048m -Xms1024m -XX:PermSize=256M -XX:MaxPermSize=512m"
 * 
 * @deprecated Please see HiveMiniClusterTest3.java
 * @author shaoshi
 *
 */
public class HiveMiniClusterTest2 extends HiveJDBCClientTest {
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

    //@Before
    public void setup() {
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

        // custom properties
        System.setProperty("hive.server2.long.polling.timeout", "5000");

        HiveConf conf = new HiveConf();

        try {
            MiniHBaseCluster miniCluster = testUtil.startMiniCluster(1);
            //MiniDFSCluster dfsCluster = testUtil.getDFSCluster();
            MiniMRCluster miniMR = testUtil.startMiniMapReduceCluster();

            Configuration config = miniCluster.getConf();
            //            System.out.println("-----------------------------");
            //            System.out.println(config.get("yarn.resourcemanager.scheduler.address"));
            //            System.out.println(config.get("mapreduce.jobtracker.address"));
            //            System.out.println(config.get("yarn.resourcemanager.address"));
            //            System.out.println(config.get("mapreduce.jobhistory.address"));
            //            System.out.println(config.get("yarn.resourcemanager.webapp.address"));
            //            System.out.println("-----------------------------");
            //
            //System.setProperty("mapred.job.tracker", miniMR.createJobConf(new JobConf(config)).get("mapred.job.tracker"));
            String jobTracker = miniMR.createJobConf(new JobConf(config)).get("mapreduce.jobtracker.address");
            System.setProperty("mapred.job.tracker", jobTracker);
            System.setProperty("mapreduce.jobtracker.address", jobTracker);
            System.setProperty("mapred.job.tracker", miniMR.createJobConf(new JobConf(conf)).get("mapred.job.tracker"));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    //@After
    public void tearDown() {

        try {
            //testUtil.shutdownMiniMapReduceCluster();
            testUtil.shutdownMiniCluster();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        super.tearDown();
    }

    protected Connection getHiveConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:hive2:///", "", "");
    }

    public static void main(String[] args) throws SQLException {
        HiveMiniClusterTest2 test = new HiveMiniClusterTest2();
        test.runTests();
    }

}
