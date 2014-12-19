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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.mapreduce.Import;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.HBaseConnection;
import com.kylinolap.common.persistence.HBaseResourceStore;

/**
 * A base class for running unit tests with HBase minicluster;
 * @author shaoshi
 */
public class HBaseMiniclusterMetadataTestCase extends AbstractKylinTestCase {

    private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

    protected static MiniHBaseCluster hbaseCluster = null;

    private static boolean clusterStarted = false;

    protected static Configuration config = null;

    protected static String hbaseconnectionUrl = "";

    private static final Log logger = LogFactory.getLog(HBaseMiniclusterMetadataTestCase.class);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    UTIL.shutdownMiniMapReduceCluster();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    UTIL.shutdownMiniCluster();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void createTestMetadata() {
        // do nothing, as the Test metadata has been initialized in BeforeClass
        staticCreateTestMetadata(MINICLUSTER_TEST_DATA);

        // Overwrite the hbase url with the minicluster's
        KylinConfig.getInstanceFromEnv().setMetadataUrl("kylin_metadata_qa@" + hbaseconnectionUrl);
        KylinConfig.getInstanceFromEnv().setStorageUrl(hbaseconnectionUrl);
    }

    /**
     * Start the minicluster; Sub-classes should invoke this in BeforeClass method.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void startupMinicluster() throws IOException, ClassNotFoundException, InterruptedException {
        staticCreateTestMetadata(MINICLUSTER_TEST_DATA);

        if (!clusterStarted) {
            synchronized (HBaseMiniclusterMetadataTestCase.class) {
                if (!clusterStarted) {
                    startupMiniClusterAndImportData();
                    clusterStarted = true;
                }
            }
        }

        KylinConfig.getInstanceFromEnv().setMetadataUrl("kylin_metadata_qa@" + hbaseconnectionUrl);
        KylinConfig.getInstanceFromEnv().setStorageUrl(hbaseconnectionUrl);
    }

    private static void startupMiniClusterAndImportData() {

        System.out.println("Going to start mini cluster.");
        try {
            hbaseCluster = UTIL.startMiniCluster();

            UTIL.startMiniMapReduceCluster();
            config = hbaseCluster.getConf();
            String host = config.get(HConstants.ZOOKEEPER_QUORUM);
            String port = config.get(HConstants.ZOOKEEPER_CLIENT_PORT);
            String parent = config.get(HConstants.ZOOKEEPER_ZNODE_PARENT);

            // reduce rpc retry
            config.set(HConstants.HBASE_CLIENT_PAUSE, "3000");
            config.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "5");
            config.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "60000");

            hbaseconnectionUrl = "hbase:" + host + ":" + port + ":" + parent;
            
            // create the metadata htables;
            HBaseResourceStore store = new HBaseResourceStore(KylinConfig.getInstanceFromEnv());

            // import the table content
            importHBaseData(true, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    
    }
    
    
    public static void importHBaseData(boolean importMetadataTables, boolean importCubeTables) throws IOException, ClassNotFoundException, InterruptedException {

        if (!importMetadataTables && !importCubeTables)
            return;

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
            folder.delete();
        }

        folder.mkdirs();
        folder.deleteOnExit();

        TarGZUtil.uncompressTarGZ(exportFile, folder);

        String[] child = folder.list();

        assert child.length == 1;

        String backupTime = child[0];

        File backupFolder = new File(folder, backupTime);

        String[] tableNames = backupFolder.list();

        for (String table : tableNames) {

            if (!(table.startsWith("kylin_metadata_qa") && importMetadataTables || table.startsWith("KYLIN_") && importCubeTables)) {
                continue;
            }

            if (table.startsWith("KYLIN_")) {
                // create the cube table; otherwise the import will fail.
                HBaseConnection.createHTableIfNeeded(KylinConfig.getInstanceFromEnv().getStorageUrl(), table, "F1", "F2");
            }
            // directly import from local fs, no need to copy to hdfs
            //String importLocation = copyTableBackupToHDFS(backupFolder, table);
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

    @Override
    public void cleanupTestMetadata() {
        staticCleanupTestMetadata();
    }

    /**
     * Shutdown the minicluster; Sub-classes should invoke this method in AfterClass method.
     */
    public static void shutdownMiniCluster() {

        /*
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
        */
    }

    public static void main(String[] args) {
        HBaseMiniclusterMetadataTestCase t = new HBaseMiniclusterMetadataTestCase();
        try {
            HBaseMiniclusterMetadataTestCase.startupMinicluster();
            t.createTestMetadata();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            t.cleanupTestMetadata();
            HBaseMiniclusterMetadataTestCase.shutdownMiniCluster();
        }
    }
}
