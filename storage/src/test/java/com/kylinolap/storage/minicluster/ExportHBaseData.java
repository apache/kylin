package com.kylinolap.storage.minicluster;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.HBaseConnection;
import com.kylinolap.common.util.CliCommandExecutor;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.common.util.SSHClient;

public class ExportHBaseData extends HBaseMetadataTestCase {

    KylinConfig kylinConfig;
    HTableDescriptor[] allTables;
    Configuration config;
    HBaseAdmin hbase;
    CliCommandExecutor cli = null;
    String exportRemoteDiskFolder = "/tmp/hbase-export";
    String backupArchive = null;
    String tableNameBase;

    @Before
    public void setup() throws IOException {
        long currentTIME = System.currentTimeMillis();
        exportRemoteDiskFolder = "/tmp/hbase-export/" + currentTIME + "/";
        backupArchive = "/tmp/kylin_" + currentTIME + ".tar.gz";
        this.createTestMetadata();

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

    @After
    public void tearDown() {

/*
        try {
            if (cli != null && exportRemoteDiskFolder != null) {
                cli.execute("hadoop fs -rm -r " + exportRemoteDiskFolder);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            if (cli != null && exportRemoteDiskFolder != null) {
                cli.execute("rm -r " + exportRemoteDiskFolder);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        */

//        if (backupArchive != null) {
//            new File(backupArchive).delete();
//        }
    }

    @Test
    public void exportTables() throws IOException {
        for (HTableDescriptor table : allTables) {
            String tName = table.getNameAsString();
            if (!tName.startsWith(tableNameBase) && !tName.startsWith("KYLIN_"))
//                if (!tName.equals(tableNameBase))
                continue;
            
            cli.execute("hbase org.apache.hadoop.hbase.mapreduce.Export " + tName + " " + exportRemoteDiskFolder + tName);
        }
        
        
        cli.execute("mkdir -p "  + exportRemoteDiskFolder);

        cli.execute("hadoop fs -copyToLocal " + exportRemoteDiskFolder + " " + exportRemoteDiskFolder);
        cli.execute("tar -zcvf " + backupArchive + " --directory=" + exportRemoteDiskFolder + " .");
        downloadToLocal();
    }

    public void downloadToLocal() throws IOException {

        SSHClient ssh = new SSHClient(kylinConfig.getRemoteHadoopCliHostname(), kylinConfig.getRemoteHadoopCliUsername(), kylinConfig.getRemoteHadoopCliPassword(), null);
        try {
            ssh.scpFileToLocal(backupArchive, "../examples/test_case_data/minicluster/hbase-export.tar.gz");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
