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

package org.apache.kylin.job;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HBaseMiniclusterHelper;
import org.apache.kylin.common.util.SSHClient;
import org.codehaus.plexus.util.FileUtils;

public class ExportHBaseData {

    KylinConfig kylinConfig;
    HTableDescriptor[] allTables;
    Configuration config;
    HBaseAdmin hbase;
    CliCommandExecutor cli = null;
    String exportFolder;
    String backupArchive = null;
    String tableNameBase;

    public ExportHBaseData() {
        try {
            setup();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void setup() throws IOException {
        long currentTIME = System.currentTimeMillis();
        exportFolder = "/tmp/hbase-export/" + currentTIME + "/";
        backupArchive = "/tmp/kylin_" + currentTIME + ".tar.gz";

        KylinConfig.destoryInstance();
        System.setProperty(KylinConfig.KYLIN_CONF, AbstractKylinTestCase.SANDBOX_TEST_DATA);

        kylinConfig = KylinConfig.getInstanceFromEnv();
        cli = kylinConfig.getCliCommandExecutor();

        String metadataUrl = kylinConfig.getMetadataUrl();
        // split TABLE@HBASE_URL
        int cut = metadataUrl.indexOf('@');
        tableNameBase = metadataUrl.substring(0, cut);
        String hbaseUrl = cut < 0 ? metadataUrl : metadataUrl.substring(cut + 1);

        HConnection conn = HBaseConnection.get(hbaseUrl);
        try {
            hbase = new HBaseAdmin(conn);
            config = hbase.getConfiguration();
            allTables = hbase.listTables();
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void tearDown() {

        // cleanup hdfs
        try {
            if (cli != null && exportFolder != null) {
                cli.execute("hadoop fs -rm -r " + exportFolder);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // cleanup sandbox disk
        try {
            if (cli != null && exportFolder != null) {
                cli.execute("rm -r " + exportFolder);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // delete archive file on sandbox
        try {
            if (cli != null && backupArchive != null) {
                cli.execute("rm " + backupArchive);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        KylinConfig.destoryInstance();

    }

    public void exportTables() throws IOException {
        cli.execute("mkdir -p " + exportFolder);

        for (HTableDescriptor table : allTables) {
            String tName = table.getNameAsString();
            if (!tName.equals(tableNameBase) && !tName.startsWith(HBaseMiniclusterHelper.SHARED_STORAGE_PREFIX))
                continue;

            cli.execute("hbase org.apache.hadoop.hbase.mapreduce.Export " + tName + " " + exportFolder + tName);
        }

        cli.execute("hadoop fs -copyToLocal " + exportFolder + " " + exportFolder);
        cli.execute("tar -zcvf " + backupArchive + " --directory=" + exportFolder + " .");
        downloadToLocal();
    }

    public void downloadToLocal() throws IOException {
        String localArchive = "../examples/test_case_data/minicluster/hbase-export.tar.gz";

        if (kylinConfig.getRunAsRemoteCommand()) {
            SSHClient ssh = new SSHClient(kylinConfig.getRemoteHadoopCliHostname(), kylinConfig.getRemoteHadoopCliUsername(), kylinConfig.getRemoteHadoopCliPassword());
            try {
                ssh.scpFileToLocal(backupArchive, localArchive);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            FileUtils.copyFile(new File(backupArchive), new File(localArchive));
        }
    }

    public static void main(String[] args) {
        ExportHBaseData export = new ExportHBaseData();
        try {
            export.exportTables();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            export.tearDown();
        }
    }
}
