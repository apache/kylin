package com.kylinolap.job;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.common.util.SSHClient;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.dataGen.FactTableGenerator;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.hadoop.hive.SqlHiveDataTypeMapping;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.schema.ColumnDesc;
import com.kylinolap.metadata.model.schema.TableDesc;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by honma on 10/15/14.
 */
public class CubeDevelopTestCase extends HBaseMetadataTestCase {
    private static final Logger logger = LoggerFactory.getLogger(CubeDevelopTestCase.class);


    protected static final String TABLE_CAL_DT = "test_cal_dt";
    protected static final String TABLE_CATEGORY_GROUPINGS = "test_category_groupings";
    protected static final String TABLE_KYLIN_FACT = "test_kylin_fact";
    protected static final String TABLE_SELLER_TYPE_DIM = "test_seller_type_dim";
    protected static final String TABLE_SITES = "test_sites";

    String[] TABLE_NAMES = new String[] { TABLE_CAL_DT, TABLE_CATEGORY_GROUPINGS, TABLE_KYLIN_FACT, TABLE_SELLER_TYPE_DIM, TABLE_SITES };

    protected String jobJarName;
    protected String coprocessorJarName;

    public void scpFilesToHadoopCli(String[] files, String remoteDir) throws Exception {
        for (String file : files) {
            File f = new File(file);
            String filename = f.getName();
            execCommand("mkdir -p " + remoteDir);
            execCommand("rm -f " + remoteDir + "/" + filename);
            execCopy(file, remoteDir);
            execCommand("chmod 755 " + remoteDir + "/" + filename);
        }
    }

    public void removeHdfsDir(SSHClient hadoopCli, String hdfsDir) throws Exception {
        String cmd = "hadoop fs -rm -f -r " + hdfsDir;
        assertEquals(0, hadoopCli.execCommand(cmd));
    }

    public void createHdfsDir(SSHClient hadoopCli, String hdfsDir) throws Exception {
        String cmd = "hadoop fs -mkdir -p " + hdfsDir;
        assertEquals(0, hadoopCli.execCommand(cmd));
        assertEquals(0, hadoopCli.execCommand("hadoop fs -chmod 777 " + hdfsDir));
    }

    public String getHadoopCliWorkingDir() {
        return KylinConfig.getInstanceFromEnv().getCliWorkingDir();
    }

    public String getHadoopCliHostname() {
        return getHadoopCliAsRemote() ? KylinConfig.getInstanceFromEnv().getRemoteHadoopCliHostname() : null;
    }

    public String getHadoopCliUsername() {
        return getHadoopCliAsRemote() ? KylinConfig.getInstanceFromEnv().getRemoteHadoopCliUsername() : null;
    }

    public String getHadoopCliPassword() {
        return getHadoopCliAsRemote() ? KylinConfig.getInstanceFromEnv().getRemoteHadoopCliPassword() : null;
    }

    public boolean getHadoopCliAsRemote() {
        return KylinConfig.getInstanceFromEnv().getRunAsRemoteCommand();
    }

    public void retrieveJarName() throws FileNotFoundException, IOException, XmlPullParserException {
        MavenXpp3Reader pomReader = new MavenXpp3Reader();
        Model model = pomReader.read(new FileReader("../pom.xml"));
        String version = model.getVersion();

        this.jobJarName = "kylin-job-" + version + "-job.jar";
        this.coprocessorJarName = "kylin-storage-" + version + "-coprocessor.jar";
    }

    public void scpFilesToHdfs(SSHClient hadoopCli, String[] localFiles, String hdfsDir) throws Exception {
        String remoteTempDir = "/tmp/";

        List<String> nameList = new ArrayList<String>();
        for (int i = 0; i < localFiles.length; i++) {
            File file = new File(localFiles[i]);
            String filename = file.getName();
            nameList.add(filename);
        }
        for (String f : localFiles) {
            hadoopCli.scpFileToRemote(f, remoteTempDir);
        }
        for (String f : nameList) {
            hadoopCli.execCommand("hadoop fs -put -f " + remoteTempDir + f + " " + hdfsDir + "/" + f);
            hadoopCli.execCommand("hadoop fs -chmod 777 " + hdfsDir + "/" + f);
        }
    }

    public void execHiveCommand(String hql) throws Exception {
        String hiveCmd = "hive -e \"" + hql + "\"";
        execCommand(hiveCmd);
    }

    public String getTextFromFile(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        StringBuilder stringBuilder = new StringBuilder();
        String ls = System.getProperty("line.separator");
        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
            stringBuilder.append(ls);
        }
        reader.close();
        return stringBuilder.toString();
    }

    public String generateLoadDataHql(String tableName) {
        return "LOAD DATA LOCAL INPATH '" + this.getHadoopCliWorkingDir() + "/" + tableName.toUpperCase() + ".csv' OVERWRITE INTO TABLE " + tableName.toUpperCase();
    }

    public String generateCreateTableHql(TableDesc tableDesc) {
        StringBuilder ddl = new StringBuilder();

        ddl.append("DROP TABLE IF EXISTS " + tableDesc.getName() + ";\n");
        ddl.append("CREATE TABLE " + tableDesc.getName() + "\n");
        ddl.append("(" + "\n");

        for (int i = 0; i < tableDesc.getColumns().length; i++) {
            ColumnDesc col = tableDesc.getColumns()[i];
            if (i > 0) {
                ddl.append(",");
            }
            ddl.append(col.getName() + " " + SqlHiveDataTypeMapping.getHiveDataType((col.getDatatype())) + "\n");
        }

        ddl.append(")" + "\n");
        ddl.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" + "\n");
        ddl.append("STORED AS TEXTFILE;");

        return ddl.toString();
    }

    public void execCommand(String cmd) throws IOException {
        int exitCode = 0;
        CommandExecutor cmdExecutor = new CommandExecutor(cmd, new VerboseConsumer());
        cmdExecutor.setRunAtRemote(getHadoopCliHostname(), getHadoopCliUsername(), getHadoopCliPassword());
        exitCode = cmdExecutor.execute(true);
        assertEquals(0, exitCode);
    }

    public void execCopy(String localFile, String destDir) throws IOException {
        int exitCode = 0;
        CopyExecutor cpExecutor = new CopyExecutor(localFile, destDir, new VerboseConsumer());
        cpExecutor.setRunAtRemote(getHadoopCliHostname(), getHadoopCliUsername(), getHadoopCliPassword());
        exitCode = cpExecutor.execute(true);
        assertEquals(0, exitCode);
    }

    public class VerboseConsumer implements CliOutputConsumer {
        @Override
        public void consume(String message) {
            logger.debug(message);
        }

        @Override
        public void log(String message) {
            consume(message);
        }
    }

    private void cleanUp() throws Exception {
        this.execCommand("rm -rf " + this.getHadoopCliWorkingDir());
    }

    private void deployJarToHadoopCli() throws Exception {
        scpFilesToHadoopCli(new String[] { ".." + File.separator + "job" + File.separator + "target" + File.separator + jobJarName }, this.getHadoopCliWorkingDir());
    }

    private void deployJarToLocalDir() throws IOException {
        File targetFile = new File(KylinConfig.getInstanceFromEnv().getCoprocessorLocalJar());
        File parent = targetFile.getParentFile();
        if (!parent.exists() && !parent.mkdirs()) {
            throw new IllegalStateException("Couldn't create dir: " + parent);
        }

        FileUtils.copyFile(new File(".." + File.separator + "storage" + File.separator + "target" + File.separator + coprocessorJarName), targetFile);
    }

    private void deployKylinProperty() throws Exception {
        scpFilesToHadoopCli(new String[] { ".." + File.separator + "examples" + File.separator + "test_case_data" + File.separator + "kylin.properties" }, "/etc/kylin");
    }

    private void deployJobConf(boolean jobEnableLzo) throws Exception {
        String srcFile;

        if (jobEnableLzo) {
            logger.info("job conf: the lzo enabled version is deployed to /etc/kylin");

            srcFile = getExampleTestCaseDataFolder() + JobEngineConfig.HADOOP_JOB_CONF_FILENAME + ".xml";

        } else {
            logger.info("job conf: the lzo disabled version is deployed to /etc/kylin");

            File temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
            temp.delete();
            temp.mkdir();
            File src = new File(getExampleTestCaseDataFolder() + JobEngineConfig.HADOOP_JOB_CONF_FILENAME + ".lzo_disabled.xml");
            File dest = new File(temp, JobEngineConfig.HADOOP_JOB_CONF_FILENAME + ".xml");
            FileUtils.copyFile(src, dest);
            srcFile = dest.toString();
            dest.deleteOnExit();
        }

        scpFilesToHadoopCli(new String[] { srcFile }, "/etc/kylin");
    }

    private String getExampleTestCaseDataFolder() {
        return ".." + File.separator + "examples" + File.separator + "test_case_data" + File.separator;
    }


    private void deployTestData() throws Exception {

        MetadataManager metaMgr = MetadataManager.getInstance(this.getTestConfig());

        // scp data files, use the data from hbase, instead of local files
        File temp = File.createTempFile("temp", ".csv");
        temp.createNewFile();
        for (String tablename : TABLE_NAMES) {
            tablename = tablename.toUpperCase();

            File localBufferFile = new File(temp.getParent() + File.separator + tablename + ".csv");
            localBufferFile.createNewFile();

            InputStream hbaseDataStream = metaMgr.getStore().getResource("/data/" + tablename + ".csv");
            FileOutputStream localFileStream = new FileOutputStream(localBufferFile);
            IOUtils.copy(hbaseDataStream, localFileStream);

            hbaseDataStream.close();
            localFileStream.close();

            this.scpFilesToHadoopCli(new String[] { localBufferFile.getPath() }, this.getHadoopCliWorkingDir());

            localBufferFile.delete();
        }
        temp.delete();

        // create hive tables
        this.execHiveCommand(this.generateCreateTableHql(metaMgr.getTableDesc(TABLE_CAL_DT.toUpperCase())));
        this.execHiveCommand(this.generateCreateTableHql(metaMgr.getTableDesc(TABLE_CATEGORY_GROUPINGS.toUpperCase())));
        this.execHiveCommand(this.generateCreateTableHql(metaMgr.getTableDesc(TABLE_KYLIN_FACT.toUpperCase())));
        this.execHiveCommand(this.generateCreateTableHql(metaMgr.getTableDesc(TABLE_SELLER_TYPE_DIM.toUpperCase())));
        this.execHiveCommand(this.generateCreateTableHql(metaMgr.getTableDesc(TABLE_SITES.toUpperCase())));

        // load data to hive tables
        // LOAD DATA LOCAL INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
        this.execHiveCommand(this.generateLoadDataHql(TABLE_CAL_DT));
        this.execHiveCommand(this.generateLoadDataHql(TABLE_CATEGORY_GROUPINGS));
        this.execHiveCommand(this.generateLoadDataHql(TABLE_KYLIN_FACT));
        this.execHiveCommand(this.generateLoadDataHql(TABLE_SELLER_TYPE_DIM));
        this.execHiveCommand(this.generateLoadDataHql(TABLE_SITES));
    }

    protected void initEnv(boolean deployKylinProperties, boolean jobEnableLzo) throws Exception {
        cleanUp();

        // create log dir
        this.execCommand("mkdir -p " + KylinConfig.getInstanceFromEnv().getKylinJobLogDir());
        retrieveJarName();

        // install metadata to hbase
        installMetadataToHBase();

        // update cube desc signature.
        for (CubeInstance cube : CubeManager.getInstance(this.getTestConfig()).listAllCubes()) {
            cube.getDescriptor().setSignature(cube.getDescriptor().calculateSignature());
            CubeManager.getInstance(this.getTestConfig()).updateCube(cube);
        }

        deployJarToHadoopCli();
        deployJarToLocalDir();

        if (deployKylinProperties)
            deployKylinProperty();

        deployJobConf(jobEnableLzo);
    }

    protected void prepareTestData(String joinType) throws Exception {
        if (joinType.equalsIgnoreCase("inner")) {
            // put data to cli
            FactTableGenerator.generate("test_kylin_cube_with_slr_empty", "10000", "1", null, "inner");
        } else if (joinType.equalsIgnoreCase("left")) {
            FactTableGenerator.generate("test_kylin_cube_with_slr_left_join_empty", "10000", "0.6", null, "left");
        } else {
            throw new Exception("Unsupported join type : " + joinType);
        }

        deployTestData();
    }
}
