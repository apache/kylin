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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.mapreduce.Import;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.common.persistence.HBaseResourceStore;

/**
 * a helper class to start and shutdown hbase mini cluster
 *
 * @author shaoshi
 */
public class HBaseMiniclusterHelper {

    private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
    private static MiniHBaseCluster hbaseCluster = null;
    private static volatile boolean clusterStarted = false;
    private static Configuration config = null;
    private static String hbaseconnectionUrl = "";
    private static final String TEST_METADATA_TABLE = "kylin_metadata_qa";
    private static final String SHARED_STORAGE_PREFIX = "KYLIN_";

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

        System.out.println("Going to start mini cluster.");
        hbaseCluster = UTIL.startMiniCluster();

        config = hbaseCluster.getConf();
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
        importHBaseData();

    }

    public static void importHBaseData() throws IOException, ClassNotFoundException, InterruptedException {

        if (System.getenv("JAVA_HOME") == null) {
            System.err.println("Didn't find $JAVA_HOME, this will cause HBase data import failed. Please set $JAVA_HOME.");
            System.err.println("Skip table import...");
            return;
        }

        File exportFile = new File("../examples/test_case_data/minicluster/hbase-export.tar.gz");
        if (!exportFile.exists()) {
            logger.error("Didn't find the export archieve file on " + exportFile.getAbsolutePath());
            return;
        }

        File folder = new File("/tmp/hbase-export/");
        if (folder.exists()) {
            FileUtils.deleteDirectory(folder);
        }

        folder.mkdirs();
        folder.deleteOnExit();

        TarGZUtil.uncompressTarGZ(exportFile, folder);
        String[] child = folder.list();
        assert child.length == 1;
        String backupFolderName = child[0];
        File backupFolder = new File(folder, backupFolderName);
        String[] tableNames = backupFolder.list();

        for (String table : tableNames) {

            if (!(table.equalsIgnoreCase(TEST_METADATA_TABLE) || table.startsWith(SHARED_STORAGE_PREFIX))) {
                continue;
            }

            if (table.startsWith(SHARED_STORAGE_PREFIX)) {
                // create the cube table; otherwise the import will fail.
                HBaseConnection.createHTableIfNeeded(KylinConfig.getInstanceFromEnv().getStorageUrl(), table, "F1", "F2");
            }

            // directly import from local fs, no need to copy to hdfs
            String importLocation = "file://" + backupFolder.getAbsolutePath() + "/" + table;
            String[] args = new String[] { table, importLocation };
            boolean result = runImport(args);
            System.out.println("---- import table '" + table + "' result:" + result);

            if (!result)
                break;
        }

    }

    private static boolean runImport(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // need to make a copy of the configuration because to make sure different temp dirs are used.
        GenericOptionsParser opts = new GenericOptionsParser(new Configuration(UTIL.getConfiguration()), args);
        Configuration conf = opts.getConfiguration();
        args = opts.getRemainingArgs();
        Job job = Import.createSubmittableJob(conf, args);
        job.waitForCompletion(false);
        return job.isSuccessful();
    }

    /**
     * Shutdown the minicluster; 
     */
    public static void shutdownMiniCluster() {

        System.out.println("Going to shutdown mini cluster.");
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
        System.out.println(t);
        try {
            HBaseMiniclusterHelper.startupMinicluster();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            HBaseMiniclusterHelper.shutdownMiniCluster();
        }
    }
}
