package com.kylinolap.job;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.HBaseConnection;
import com.kylinolap.common.persistence.HBaseResourceStore;
import com.kylinolap.common.util.AbstractKylinTestCase;
import com.kylinolap.common.util.CliCommandExecutor;
import com.kylinolap.common.util.SSHClient;
import com.kylinolap.common.util.TarGZUtil;

public class ImportHBaseData {

    KylinConfig kylinConfig;
    HTableDescriptor[] allTables;
    Configuration config;
    HBaseAdmin hbase;
    CliCommandExecutor cli = null;
    String importFolder = "/tmp/hbase-export/";
    String backupArchive = null;
    String tableNameBase;

    public void setup() throws IOException {
        
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
            //allTables = hbase.listTables();
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }

        uploadTarballToRemote();
    }

    public void tearDown() {
        // cleanup sandbox disk
        try {
            if (cli != null && importFolder != null) {
                cli.execute("rm -r " + importFolder);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        KylinConfig.destoryInstance();

    }

    public void importTables() throws IOException {
        // create the metadata htables;
        HBaseResourceStore store = new HBaseResourceStore(KylinConfig.getInstanceFromEnv());

        List<String> tablelocations = getTablesBackupLocations(importFolder);
        for (String tableLocation : tablelocations) {
            String table = tableLocation.substring(tableLocation.lastIndexOf("/") + 1);
            
            if (!(table.equalsIgnoreCase("kylin_metadata_qa") || table.startsWith("KYLIN_"))) {
                continue;
            }
            
            if (table.startsWith("KYLIN_")) {
                // create the cube table; otherwise the import will fail.
                HBaseConnection.createHTableIfNeeded(KylinConfig.getInstanceFromEnv().getStorageUrl(), table, "F1", "F2");
            }
            cli.execute("hbase org.apache.hadoop.hbase.mapreduce.Import " + table + " file://" + tableLocation);
        }
        
    }

    public void uploadTarballToRemote() throws IOException {

        cli.execute("mkdir -p /tmp/hbase-export/");
        SSHClient ssh = new SSHClient(kylinConfig.getRemoteHadoopCliHostname(), kylinConfig.getRemoteHadoopCliUsername(), kylinConfig.getRemoteHadoopCliPassword());
        try {
           // ssh.scpFileToRemote("../examples/test_case_data/minicluster/hbase-export.tar.gz", importFolder);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        cli.execute("tar -xzf /tmp/hbase-export/hbase-export.tar.gz  --directory=" + importFolder);
    }
    
    private List<String> getTablesBackupLocations(String exportBase) throws IOException {
        File exportFile = new File("../examples/test_case_data/minicluster/hbase-export.tar.gz");

        if (!exportFile.exists()) {
            return null;
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
        
        List<String> locations = new ArrayList<String>(15);
        
        for(String t: tableNames) {
            locations.add(exportBase + backupTime + "/" + t);
        }
        
        return locations;
    }
    
    public static void main(String[] args) {
        ImportHBaseData export = new ImportHBaseData();
        try {
            export.setup();
            export.importTables();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //export.tearDown();
        }
    }
}
