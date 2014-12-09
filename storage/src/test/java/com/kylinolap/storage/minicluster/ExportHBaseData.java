package com.kylinolap.storage.minicluster;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;

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
    String exportFolder = "/tmp/hbase-export";
    String backupArchive = null;
    String tableNameBase;

    public void setup() throws IOException {
        long currentTIME = System.currentTimeMillis();
        exportFolder = "/tmp/hbase-export/" + currentTIME + "/";
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

    }

    public void exportTables() throws IOException {
        for (HTableDescriptor table : allTables) {
            String tName = table.getNameAsString();
            if (!tName.startsWith(tableNameBase) && !tName.startsWith("KYLIN_"))
                continue;
            
            cli.execute("hbase org.apache.hadoop.hbase.mapreduce.Export " + tName + " " + exportFolder + tName);
        }
        
        
        cli.execute("mkdir -p "  + exportFolder);

        cli.execute("hadoop fs -copyToLocal " + exportFolder + " " + exportFolder);
        cli.execute("tar -zcvf " + backupArchive + " --directory=" + exportFolder + " .");
        downloadToLocal();
    }

    public void downloadToLocal() throws IOException {

        SSHClient ssh = new SSHClient(kylinConfig.getRemoteHadoopCliHostname(), kylinConfig.getRemoteHadoopCliUsername(), kylinConfig.getRemoteHadoopCliPassword(), null);
        try {
            ssh.scpFileToLocal(backupArchive, "../examples/test_case_data/minicluster/hbase-export.tar.gz");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        ExportHBaseData export = new ExportHBaseData();
        try {
            export.setup();
            export.exportTables();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            export.tearDown();
        }
    }
}
