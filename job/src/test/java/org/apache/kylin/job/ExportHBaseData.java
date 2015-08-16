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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HBaseMiniclusterHelper;
import org.apache.kylin.common.util.SSHClient;
import org.apache.kylin.job.constant.BatchConstants;

public class ExportHBaseData {

    KylinConfig kylinConfig;
    HTableDescriptor[] allTables;
    Configuration config;
    Admin admin;
    CliCommandExecutor cli;
    String exportHdfsFolder;
    String exportLocalFolderParent;
    String exportLocalFolder;
    String backupArchive;
    String tableNameBase;
    long currentTIME;

    public ExportHBaseData() {
        try {
            setup();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void setup() throws IOException {

        KylinConfig.destoryInstance();
        System.setProperty(KylinConfig.KYLIN_CONF, AbstractKylinTestCase.SANDBOX_TEST_DATA);

        kylinConfig = KylinConfig.getInstanceFromEnv();
        cli = kylinConfig.getCliCommandExecutor();

        currentTIME = System.currentTimeMillis();
        exportHdfsFolder = kylinConfig.getHdfsWorkingDirectory() + "hbase-export/" + currentTIME + "/";
        exportLocalFolderParent = BatchConstants.CFG_KYLIN_LOCAL_TEMP_DIR + "hbase-export/";
        exportLocalFolder = exportLocalFolderParent + currentTIME + "/";
        backupArchive = exportLocalFolderParent + "hbase-export-at-" + currentTIME + ".tar.gz";

        String metadataUrl = kylinConfig.getMetadataUrl();
        // split TABLE@HBASE_URL
        int cut = metadataUrl.indexOf('@');
        tableNameBase = metadataUrl.substring(0, cut);
        String hbaseUrl = cut < 0 ? metadataUrl : metadataUrl.substring(cut + 1);
        Connection conn = HBaseConnection.get(hbaseUrl);
        try {
            admin = conn.getAdmin();
            config = admin.getConfiguration();
            allTables = admin.listTables();
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void tearDown() {

        // close hbase admin
        IOUtils.closeQuietly(admin);
        // cleanup hdfs
        try {
            if (cli != null && exportHdfsFolder != null) {
                cli.execute("hadoop fs -rm -r " + exportHdfsFolder);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // cleanup sandbox disk
        try {
            if (cli != null && exportLocalFolder != null) {
                cli.execute("rm -r " + exportLocalFolder);
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
    }

    public void exportTables() throws IOException {
        cli.execute("mkdir -p " + exportLocalFolderParent);

        for (HTableDescriptor table : allTables) {
            String tName = table.getNameAsString();
            if (!tName.equals(tableNameBase) && !tName.startsWith(HBaseMiniclusterHelper.SHARED_STORAGE_PREFIX))
                continue;

            cli.execute("hbase org.apache.hadoop.hbase.mapreduce.Export " + tName + " " + exportHdfsFolder + tName);
        }

        cli.execute("hadoop fs -copyToLocal " + exportHdfsFolder + " " + exportLocalFolderParent);
        cli.execute("tar -zcvf " + backupArchive + " --directory=" + exportLocalFolderParent + " " + currentTIME);

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
