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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.dataGen.FactTableGenerator;
import org.apache.kylin.job.streaming.StreamDataLoader;
import org.apache.kylin.job.streaming.StreamingTableDataGenerator;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.hive.HiveClient;
import org.apache.kylin.source.hive.HiveCmdBuilder;
import org.apache.kylin.source.kafka.TimedJsonStreamParser;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import kafka.message.Message;
import kafka.message.MessageAndOffset;

public class DeployUtil {
    private static final Logger logger = LoggerFactory.getLogger(DeployUtil.class);

    public static void initCliWorkDir() throws IOException {
        execCliCommand("rm -rf " + getHadoopCliWorkingDir());
        execCliCommand("mkdir -p " + config().getKylinJobLogDir());
    }

    public static void deployMetadata() throws IOException {
        // install metadata to hbase
        ResourceTool.reset(config());
        ResourceTool.copy(KylinConfig.createInstanceFromUri(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA), config());

        // update cube desc signature.
        for (CubeInstance cube : CubeManager.getInstance(config()).listAllCubes()) {
            CubeDescManager.getInstance(config()).updateCubeDesc(cube.getDescriptor());//enforce signature updating
        }
    }

    public static void overrideJobJarLocations() {
        File jobJar = getJobJarFile();
        File coprocessorJar = getCoprocessorJarFile();

        config().overrideMRJobJarPath(jobJar.getAbsolutePath());
        config().overrideCoprocessorLocalJar(coprocessorJar.getAbsolutePath());
        config().overrideSparkJobJarPath(getSparkJobJarFile().getAbsolutePath());
    }

    private static String getPomVersion() {
        try {
            MavenXpp3Reader pomReader = new MavenXpp3Reader();
            Model model = pomReader.read(new FileReader("../pom.xml"));
            return model.getVersion();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static File getJobJarFile() {
        return new File("../assembly/target", "kylin-assembly-" + getPomVersion() + "-job.jar");
    }

    private static File getCoprocessorJarFile() {
        return new File("../storage-hbase/target", "kylin-storage-hbase-" + getPomVersion() + "-coprocessor.jar");
    }

    private static File getSparkJobJarFile() {
        return new File("../engine-spark/target", "kylin-engine-spark-" + getPomVersion() + "-job.jar");
    }

    private static void execCliCommand(String cmd) throws IOException {
        config().getCliCommandExecutor().execute(cmd);
    }

    private static String getHadoopCliWorkingDir() {
        return config().getCliWorkingDir();
    }

    private static KylinConfig config() {
        return KylinConfig.getInstanceFromEnv();
    }

    // ============================================================================

    static final String TABLE_CAL_DT = "edw.test_cal_dt";
    static final String VIEW_CAL_DT = "edw.v_test_cal_dt";
    static final String TABLE_CATEGORY_GROUPINGS = "default.test_category_groupings";
    static final String TABLE_KYLIN_FACT = "default.test_kylin_fact";
    static final String TABLE_SELLER_TYPE_DIM = "edw.test_seller_type_dim";
    static final String TABLE_SITES = "edw.test_sites";

    static final String[] TABLE_NAMES = new String[] { TABLE_CAL_DT, TABLE_CATEGORY_GROUPINGS, TABLE_KYLIN_FACT, TABLE_SELLER_TYPE_DIM, TABLE_SITES };

    public static void prepareTestDataForNormalCubes(String cubeName) throws Exception {

        String factTableName = TABLE_KYLIN_FACT.toUpperCase();
        String content = null;

        boolean buildCubeUsingProvidedData = Boolean.parseBoolean(System.getProperty("buildCubeUsingProvidedData"));
        if (!buildCubeUsingProvidedData) {
            System.out.println("build cube with random dataset");
            // data is generated according to cube descriptor and saved in resource store
            content = FactTableGenerator.generate(cubeName, "10000", "0.6", null);
            assert content != null;
            overrideFactTableData(content, factTableName);
        } else {
            System.out.println("build normal cubes with provided dataset");
        }

        deployHiveTables();
    }

    public static void prepareTestDataForStreamingCube(long startTime, long endTime, String cubeName, StreamDataLoader streamDataLoader) throws IOException {
        CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        List<String> data = StreamingTableDataGenerator.generate(10000, startTime, endTime, cubeInstance.getFactTable());
        List<String> data2 = StreamingTableDataGenerator.generate(10, endTime, endTime + 300000, cubeInstance.getFactTable());
        TableDesc tableDesc = cubeInstance.getFactTableDesc();
        //load into kafka
        streamDataLoader.loadIntoKafka(data);
        streamDataLoader.loadIntoKafka(data2);
        logger.info("Write {} messages into {}", data.size(), streamDataLoader.toString());

        //csv data for H2 use
        List<TblColRef> tableColumns = Lists.newArrayList();
        for (ColumnDesc columnDesc : tableDesc.getColumns()) {
            tableColumns.add(columnDesc.getRef());
        }
        TimedJsonStreamParser timedJsonStreamParser = new TimedJsonStreamParser(tableColumns, "formatTs=true");
        StringBuilder sb = new StringBuilder();
        for (String json : data) {
            List<String> rowColumns = timedJsonStreamParser.parse(ByteBuffer.wrap(json.getBytes())).getData();
            sb.append(StringUtils.join(rowColumns, ","));
            sb.append(System.getProperty("line.separator"));
        }
        overrideFactTableData(sb.toString(), cubeInstance.getFactTable());
    }

    public static void overrideFactTableData(String factTableContent, String factTableName) throws IOException {
        // Write to resource store
        ResourceStore store = ResourceStore.getStore(config());

        InputStream in = new ByteArrayInputStream(factTableContent.getBytes("UTF-8"));
        String factTablePath = "/data/" + factTableName + ".csv";
        store.deleteResource(factTablePath);
        store.putResource(factTablePath, in, System.currentTimeMillis());
        in.close();
    }


    private static void deployHiveTables() throws Exception {

        MetadataManager metaMgr = MetadataManager.getInstance(config());

        // scp data files, use the data from hbase, instead of local files
        File temp = File.createTempFile("temp", ".csv");
        temp.createNewFile();
        for (String tablename : TABLE_NAMES) {
            tablename = tablename.toUpperCase();

            File localBufferFile = new File(temp.getParent() + "/" + tablename + ".csv");
            localBufferFile.createNewFile();

            InputStream hbaseDataStream = metaMgr.getStore().getResource("/data/" + tablename + ".csv").inputStream;
            FileOutputStream localFileStream = new FileOutputStream(localBufferFile);
            IOUtils.copy(hbaseDataStream, localFileStream);

            hbaseDataStream.close();
            localFileStream.close();

            localBufferFile.deleteOnExit();
        }
        String tableFileDir = temp.getParent();
        temp.delete();

        HiveClient hiveClient = new HiveClient();
        // create hive tables
        hiveClient.executeHQL("CREATE DATABASE IF NOT EXISTS EDW");
        hiveClient.executeHQL(generateCreateTableHql(metaMgr.getTableDesc(TABLE_CAL_DT.toUpperCase())));
        hiveClient.executeHQL(generateCreateTableHql(metaMgr.getTableDesc(TABLE_CATEGORY_GROUPINGS.toUpperCase())));
        hiveClient.executeHQL(generateCreateTableHql(metaMgr.getTableDesc(TABLE_KYLIN_FACT.toUpperCase())));
        hiveClient.executeHQL(generateCreateTableHql(metaMgr.getTableDesc(TABLE_SELLER_TYPE_DIM.toUpperCase())));
        hiveClient.executeHQL(generateCreateTableHql(metaMgr.getTableDesc(TABLE_SITES.toUpperCase())));

        // load data to hive tables
        // LOAD DATA LOCAL INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
        hiveClient.executeHQL(generateLoadDataHql(TABLE_CAL_DT, tableFileDir));
        hiveClient.executeHQL(generateLoadDataHql(TABLE_CATEGORY_GROUPINGS, tableFileDir));
        hiveClient.executeHQL(generateLoadDataHql(TABLE_KYLIN_FACT, tableFileDir));
        hiveClient.executeHQL(generateLoadDataHql(TABLE_SELLER_TYPE_DIM, tableFileDir));
        hiveClient.executeHQL(generateLoadDataHql(TABLE_SITES, tableFileDir));

        final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatements(generateCreateViewHql(VIEW_CAL_DT, metaMgr.getTableDesc(TABLE_CAL_DT.toUpperCase())));

        config().getCliCommandExecutor().execute(hiveCmdBuilder.build());
    }

    private static String generateLoadDataHql(String tableName, String tableFileDir) {
        return "LOAD DATA LOCAL INPATH '" + tableFileDir + "/" + tableName.toUpperCase() + ".csv' OVERWRITE INTO TABLE " + tableName.toUpperCase();
    }

    private static String[] generateCreateTableHql(TableDesc tableDesc) {

        String dropsql = "DROP TABLE IF EXISTS " + tableDesc.getIdentity();
        StringBuilder ddl = new StringBuilder();

        ddl.append("CREATE TABLE " + tableDesc.getIdentity() + "\n");
        ddl.append("(" + "\n");

        for (int i = 0; i < tableDesc.getColumns().length; i++) {
            ColumnDesc col = tableDesc.getColumns()[i];
            if (i > 0) {
                ddl.append(",");
            }
            ddl.append(col.getName() + " " + getHiveDataType((col.getDatatype())) + "\n");
        }

        ddl.append(")" + "\n");
        ddl.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" + "\n");
        ddl.append("STORED AS TEXTFILE");

        return new String[] { dropsql, ddl.toString() };
    }

    private static String[] generateCreateViewHql(String viewName, TableDesc tableDesc) {

        String dropsql = "DROP VIEW IF EXISTS " + viewName + ";";
        StringBuilder ddl = new StringBuilder();

        ddl.append("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableDesc.getIdentity() + ";\n");

        return new String[] { dropsql, ddl.toString() };
    }

    private static String getHiveDataType(String javaDataType) {
        String hiveDataType = javaDataType.toLowerCase().startsWith("varchar") ? "string" : javaDataType;
        hiveDataType = javaDataType.toLowerCase().startsWith("integer") ? "int" : hiveDataType;

        return hiveDataType.toLowerCase();
    }
}
