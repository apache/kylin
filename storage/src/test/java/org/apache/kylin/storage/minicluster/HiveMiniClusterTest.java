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

package org.apache.kylin.storage.minicluster;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

/**
 * This is a test case to verify whether the query can be executed on Hive minicluster;
 * You need set $HADOOP_HOME environment parameter before run it;
 * @author shaoshi
 *
 */
public class HiveMiniClusterTest extends HiveJDBCClientTest {
    public static final File HIVE_BASE_DIR = new File("target/hive");
    public static final File HIVE_SCRATCH_DIR = new File(HIVE_BASE_DIR + "/scratchdir");
    public static final File HIVE_LOCAL_SCRATCH_DIR = new File(HIVE_BASE_DIR + "/localscratchdir");
    public static final File HIVE_METADB_DIR = new File(HIVE_BASE_DIR + "/metastoredb");
    public static final File HIVE_LOGS_DIR = new File(HIVE_BASE_DIR + "/logs");
    public static final File HIVE_TMP_DIR = new File(HIVE_BASE_DIR + "/tmp");
    public static final File HIVE_WAREHOUSE_DIR = new File(HIVE_BASE_DIR + "/warehouse");
    public static final File HIVE_TESTDATA_DIR = new File(HIVE_BASE_DIR + "/testdata");
    public static final File HIVE_HADOOP_TMP_DIR = new File(HIVE_BASE_DIR + "/hadooptmp");
    //protected HiveInterface client;

    protected MiniDFSCluster miniDFS;
    protected MiniMRCluster miniMR;

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

        /* Build MiniDFSCluster */
        try {
            miniDFS = new MiniDFSCluster.Builder(conf).build();

            /* Build MiniMR Cluster */
            int numTaskTrackers = 1;
            int numTaskTrackerDirectories = 1;
            String[] racks = null;
            String[] hosts = null;
            miniMR = new MiniMRCluster(numTaskTrackers, miniDFS.getFileSystem().getUri().toString(), numTaskTrackerDirectories, racks, hosts, new JobConf(conf));
            JobConf jobConf = miniMR.createJobConf(new JobConf(conf));
            System.out.println("-------" + jobConf.get("fs.defaultFS"));
            System.out.println("-------" + miniDFS.getFileSystem().getUri().toString());
            System.setProperty("mapred.job.tracker", jobConf.get("mapred.job.tracker"));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    //@After
    public void tearDown() {
        if (miniMR != null)
            miniMR.shutdown();

        if (miniDFS != null)
            miniDFS.shutdown();

        super.tearDown();
    }

    protected Connection getHiveConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:hive2:///", "", "");
    }

    public static void main(String[] args) throws SQLException {
        HiveMiniClusterTest test = new HiveMiniClusterTest();
        test.runTests();
    }
}
