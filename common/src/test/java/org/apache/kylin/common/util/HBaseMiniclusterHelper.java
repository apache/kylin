/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.common.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.HBaseResourceStore;

/**
 * a helper class to start and shutdown hbase mini cluster
 *
 * @author shaoshi
 */
public class HBaseMiniclusterHelper {

    public static final String SHARED_STORAGE_PREFIX = "KYLIN_";
    public static final String CUBE_STORAGE_PREFIX = "KYLIN_";
    public static final String II_STORAGE_PREFIX = "KYLIN_II";
    public static final String TEST_METADATA_TABLE = "kylin_metadata";

    private static final String hbaseTarLocation = "../examples/test_case_data/minicluster/hbase-export.tar.gz";
    private static final String iiEndpointClassName = "org.apache.kylin.storage.hbase.coprocessor.endpoint.IIEndpoint";

    public static HBaseTestingUtility UTIL = new HBaseTestingUtility();
    private static volatile boolean clusterStarted = false;
    private static String hbaseconnectionUrl = "";

    private static final Log logger = LogFactory.getLog(HBaseMiniclusterHelper.class);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                shutdownMiniCluster();
            }
        });
    }

    /**
     * Start the minicluster; Sub-classes should invoke this in BeforeClass method.
     *
     * @throws Exception
     */
    public static void startupMinicluster() throws Exception {
        HBaseMetadataTestCase.staticCreateTestMetadata(HBaseMetadataTestCase.MINICLUSTER_TEST_DATA);

        if (!clusterStarted) {
            synchronized (HBaseMiniclusterHelper.class) {
                if (!clusterStarted) {
                    startupMiniClusterAndImportData();
                    clusterStarted = true;
                }
            }
        } else {
            updateKylinConfigWithMinicluster();
        }
    }

    private static void updateKylinConfigWithMinicluster() {

        KylinConfig.getInstanceFromEnv().setMetadataUrl(TEST_METADATA_TABLE + "@" + hbaseconnectionUrl);
        KylinConfig.getInstanceFromEnv().setStorageUrl(hbaseconnectionUrl);
    }

    private static void startupMiniClusterAndImportData() throws Exception {

        logger.info("Going to start mini cluster.");

        if (existInClassPath(iiEndpointClassName)) {
            HBaseMiniclusterHelper.UTIL.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, iiEndpointClassName);
        }

        //https://issues.apache.org/jira/browse/HBASE-11711
        UTIL.getConfiguration().setInt("hbase.master.info.port", -1);//avoid port clobbering

        MiniHBaseCluster hbaseCluster = UTIL.startMiniCluster();

        Configuration config = hbaseCluster.getConf();
        String host = config.get(HConstants.ZOOKEEPER_QUORUM);
        String port = config.get(HConstants.ZOOKEEPER_CLIENT_PORT);
        String parent = config.get(HConstants.ZOOKEEPER_ZNODE_PARENT);

        // see in: https://hbase.apache.org/book.html#trouble.rs.runtime.zkexpired
        config.set("zookeeper.session.timeout", "1200000");
        config.set("hbase.zookeeper.property.tickTime", "6000");
        // reduce rpc retry
        config.set(HConstants.HBASE_CLIENT_PAUSE, "3000");
        config.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "1");
        config.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "60000");

        hbaseconnectionUrl = "hbase:" + host + ":" + port + ":" + parent;
        updateKylinConfigWithMinicluster();

        UTIL.startMiniMapReduceCluster();

        // create the metadata htables;
        @SuppressWarnings("unused")
        HBaseResourceStore store = new HBaseResourceStore(KylinConfig.getInstanceFromEnv());

        // import the table content
        HbaseImporter.importHBaseData(hbaseTarLocation, UTIL.getConfiguration());

    }

    private static boolean existInClassPath(String className) {
        try {
            Class.forName(className);
        } catch (ClassNotFoundException e) {
            return false;
        }
        return true;
    }

    /**
     * Shutdown the minicluster; 
     */
    public static void shutdownMiniCluster() {

        logger.info("Going to shutdown mini cluster.");

        try {
            UTIL.shutdownMiniMapReduceCluster();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            UTIL.shutdownMiniCluster();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        HBaseMiniclusterHelper t = new HBaseMiniclusterHelper();
        logger.info(t);
        try {
            HBaseMiniclusterHelper.startupMinicluster();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            HBaseMiniclusterHelper.shutdownMiniCluster();
        }
    }
}
