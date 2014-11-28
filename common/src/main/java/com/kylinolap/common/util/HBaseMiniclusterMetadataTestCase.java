/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.common.util;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.ResourceTool;

/**
 * @author shaoshi
 */
public class HBaseMiniclusterMetadataTestCase extends AbstractKylinTestCase {
    
    private static HBaseTestingUtility testUtil ;

    protected static MiniDFSCluster dfsCluster = null;
    protected static MiniMRCluster mrCluster = null;
    protected static MiniHBaseCluster hbaseCluster = null;
    
    public static void staticCleanupTestMetadata() {
        System.clearProperty(KylinConfig.KYLIN_CONF);
        KylinConfig.destoryInstance();
        
    }

    @Override
    public void createTestMetadata() {
        staticCreateTestMetadata(MINICLUSTER_TEST_DATA);

        startupMinicluster();
        importHBaseData();
    }
    
    public void startupMinicluster() {

        if(testUtil == null) {
            testUtil = new HBaseTestingUtility();
        }
        
        try {
            hbaseCluster = testUtil.startMiniCluster();
        } catch (Exception e) {
            e.printStackTrace();
        }

        Configuration config = hbaseCluster.getConf();
        String host = config.get(HConstants.ZOOKEEPER_QUORUM);
        String port = config.get(HConstants.ZOOKEEPER_CLIENT_PORT);
        String parent = config.get(HConstants.ZOOKEEPER_ZNODE_PARENT);

        // reduce rpc retry
        config.set(HConstants.HBASE_CLIENT_PAUSE, "3000");
        config.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "5");
        config.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "60000");

        String hbaseconnectionUrl = "hbase:" + host + ":" + port + ":" + parent;
        
        KylinConfig.getInstanceFromEnv().setMetadataUrl(hbaseconnectionUrl);
        KylinConfig.getInstanceFromEnv().setStorageUrl(hbaseconnectionUrl);
    }
    
    public void importHBaseData() {
        File dataFolder = new File(MINICLUSTER_TEST_DATA + File.separator + "b-kylin" + File.separator + "meta");
        try {
            ResourceTool.copy(KylinConfig.createInstanceFromUri(dataFolder.getAbsolutePath()), KylinConfig.getInstanceFromEnv());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }

    @Override
    public void cleanupTestMetadata() {
        staticCleanupTestMetadata();
        
        try {
            testUtil.shutdownMiniCluster();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        HBaseMiniclusterMetadataTestCase t = new HBaseMiniclusterMetadataTestCase();
        t.createTestMetadata();
        t.cleanupTestMetadata();
    }
}
