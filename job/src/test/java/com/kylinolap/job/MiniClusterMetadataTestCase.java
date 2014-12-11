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

package com.kylinolap.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.HBaseMetadataTestCase;

/**
 * @author shaoshi
 */
public class MiniClusterMetadataTestCase extends HBaseMetadataTestCase {
    
    private static final HBaseTestingUtility testUtil = new HBaseTestingUtility();
    private static MiniHBaseCluster hbCluster = null;

    @Override
    public void createTestMetadata() {
        staticCreateTestMetadata(MINICLUSTER_TEST_DATA);
        //String connectionUrl = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        try {
            hbCluster = testUtil.startMiniCluster();
            //testUtil.startMiniMapReduceCluster();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        assert hbCluster != null;

        Configuration conf = hbCluster.getConf();
        String host = conf.get(HConstants.ZOOKEEPER_QUORUM);
        String port = conf.get(HConstants.ZOOKEEPER_CLIENT_PORT);
        String parent = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
        
        // reduce rpc retry
        conf.set(HConstants.HBASE_CLIENT_PAUSE, "3000");
        conf.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "5");
        conf.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "60000");
        
        
        String connectionUrl = "hbase:" + host + ":" + port + ":" + parent;
        
        KylinConfig.getInstanceFromEnv().setMetadataUrl(connectionUrl);
        KylinConfig.getInstanceFromEnv().setStorageUrl(connectionUrl);

    }
    
    protected void importHBaseData() {
        
    }
    
    

    @Override
    public void cleanupTestMetadata() {
       
        System.clearProperty(KylinConfig.KYLIN_CONF);
        KylinConfig.destoryInstance();
        try {
//            testUtil.shutdownMiniMapReduceCluster();
            testUtil.shutdownMiniCluster();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


}
