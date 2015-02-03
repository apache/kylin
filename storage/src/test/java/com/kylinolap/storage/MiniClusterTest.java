package com.kylinolap.storage;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;

import org.apache.kylin.common.util.AbstractKylinTestCase;

public class MiniClusterTest {

    private static HBaseTestingUtility testUtil = new HBaseTestingUtility();

    public static void main(String[] args) throws Exception {

        File miniclusterFolder = new File(AbstractKylinTestCase.MINICLUSTER_TEST_DATA);
        System.out.println("----" + miniclusterFolder.getAbsolutePath());

        //save the dfs data to minicluster folder
        System.setProperty("test.build.data", miniclusterFolder.getAbsolutePath());

        MiniHBaseCluster hbCluster = testUtil.startMiniCluster(1);
        testUtil.startMiniMapReduceCluster();
        System.out.println("Minicluster started.");

        Configuration conf = hbCluster.getConf();
        String host = conf.get(HConstants.ZOOKEEPER_QUORUM);
        String port = conf.get(HConstants.ZOOKEEPER_CLIENT_PORT);
        String parent = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT);

        // reduce rpc retry
        conf.set(HConstants.HBASE_CLIENT_PAUSE, "3000");
        conf.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "5");
        conf.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "60000");

        String connectionUrl = "hbase:" + host + ":" + port + ":" + parent;

        System.out.println("hbase connection url:" + connectionUrl);

        testUtil.getDFSCluster().getFileSystem();
        testUtil.shutdownMiniMapReduceCluster();
        testUtil.shutdownMiniCluster();
    }
}
