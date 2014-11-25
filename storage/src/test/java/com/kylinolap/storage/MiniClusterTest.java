package com.kylinolap.storage;

import org.apache.hadoop.hbase.HBaseTestingUtility;

public class MiniClusterTest {

    private static final HBaseTestingUtility testUtil = new HBaseTestingUtility();
    
    public static void main(String[] args) throws Exception {
        
        testUtil.startMiniCluster(1);
        testUtil.startMiniMapReduceCluster();
        
        testUtil.shutdownMiniMapReduceCluster();
        testUtil.shutdownMiniCluster();
    }
}
