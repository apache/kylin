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

package org.apache.kylin.storage;

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
