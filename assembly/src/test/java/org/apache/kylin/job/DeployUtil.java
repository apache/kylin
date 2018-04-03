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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.streaming.StreamDataLoader;
import org.apache.kylin.job.streaming.StreamingTableDataGenerator;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.SourceManager;
import org.apache.kylin.source.datagen.ModelDataGenerator;
import org.apache.kylin.source.kafka.TimedJsonStreamParser;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class DeployUtil {
    private static final Logger logger = LoggerFactory.getLogger(DeployUtil.class);

    public static void initCliWorkDir() throws IOException {
        execCliCommand("rm -rf " + getHadoopCliWorkingDir());
        execCliCommand("mkdir -p " + config().getKylinJobLogDir());
    }

    public static void deployMetadata(String localMetaData) throws IOException {
        // install metadata to hbase
        ResourceTool.reset(config());
        ResourceTool.copy(KylinConfig.createInstanceFromUri(localMetaData), config());

        // update cube desc signature.
        for (CubeInstance cube : CubeManager.getInstance(config()).listAllCubes()) {
            CubeDescManager.getInstance(config()).updateCubeDesc(cube.getDescriptor());//enforce signature updating
        }
    }
    
    public static void deployMetadata() throws IOException {
        deployMetadata(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA);
    }

    public static void overrideJobJarLocations() {
        File jobJar = getJobJarFile();
        File coprocessorJar = getCoprocessorJarFile();

        config().overrideMRJobJarPath(jobJar.getAbsolutePath());
        config().overrideCoprocessorLocalJar(coprocessorJar.getAbsolutePath());
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

    public static void prepareTestDataForNormalCubes(String modelName) throws Exception {

        boolean buildCubeUsingProvidedData = Boolean.parseBoolean(System.getProperty("buildCubeUsingProvidedData"));
        if (!buildCubeUsingProvidedData) {
            System.out.println("build cube with random dataset");

            // data is generated according to cube descriptor and saved in resource store
            DataModelManager mgr = DataModelManager.getInstance(KylinConfig.getInstanceFromEnv());
            ModelDataGenerator gen = new ModelDataGenerator(mgr.getDataModelDesc(modelName), 10000);
            gen.generate();
        } else {
            System.out.println("build normal cubes with provided dataset");
        }

        deployTables(modelName);
    }

    public static void prepareTestDataForStreamingCube(long startTime, long endTime, int numberOfRecords, String cubeName, StreamDataLoader streamDataLoader) throws IOException {
        CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        List<String> data = StreamingTableDataGenerator.generate(numberOfRecords, startTime, endTime, cubeInstance.getRootFactTable(), cubeInstance.getProject());
        //load into kafka
        streamDataLoader.loadIntoKafka(data);
        logger.info("Write {} messages into {}", data.size(), streamDataLoader.toString());

        //csv data for H2 use
        TableRef factTable = cubeInstance.getModel().getRootFactTable();
        List<TblColRef> tableColumns = Lists.newArrayList(factTable.getColumns());
        TimedJsonStreamParser timedJsonStreamParser = new TimedJsonStreamParser(tableColumns, null);
        StringBuilder sb = new StringBuilder();
        for (String json : data) {
            List<String> rowColumns = timedJsonStreamParser.parse(ByteBuffer.wrap(json.getBytes())).get(0).getData();
            sb.append(StringUtils.join(rowColumns, ","));
            sb.append(System.getProperty("line.separator"));
        }
        appendFactTableData(sb.toString(), cubeInstance.getRootFactTable());
    }

    public static void appendFactTableData(String factTableContent, String factTableName) throws IOException {
        // Write to resource store
        ResourceStore store = ResourceStore.getStore(config());

        InputStream in = new ByteArrayInputStream(factTableContent.getBytes("UTF-8"));
        String factTablePath = "/data/" + factTableName + ".csv";

        File tmpFile = File.createTempFile(factTableName, "csv");
        FileOutputStream out = new FileOutputStream(tmpFile);

        InputStream tempIn = null;
        try {
            if (store.exists(factTablePath)) {
                InputStream oldContent = store.getResource(factTablePath).inputStream;
                IOUtils.copy(oldContent, out);
            }
            IOUtils.copy(in, out);
            IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(out);

            store.deleteResource(factTablePath);
            tempIn = new FileInputStream(tmpFile);
            store.putResource(factTablePath, tempIn, System.currentTimeMillis());
        } finally {
            IOUtils.closeQuietly(out);
            IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(tempIn);
        }

    }

    private static void deployTables(String modelName) throws Exception {
        // the special VIEW_SELLER_TYPE_DIM is a wrapper of TABLE_SELLER_TYPE_DIM_TABLE
        final String VIEW_SELLER_TYPE_DIM = "EDW.TEST_SELLER_TYPE_DIM";
        final String TABLE_SELLER_TYPE_DIM_TABLE = "EDW.TEST_SELLER_TYPE_DIM_TABLE";

        TableMetadataManager metaMgr = TableMetadataManager.getInstance(config());
        DataModelManager modelMgr = DataModelManager.getInstance(config());
        DataModelDesc model = modelMgr.getDataModelDesc(modelName);

        Set<TableRef> tables = model.getAllTables();
        Set<String> TABLE_NAMES = new HashSet<String>();
        for (TableRef tr:tables){
            if (!tr.getTableDesc().isView()){
                String tableName = tr.getTableName();
                String schema = tr.getTableDesc().getDatabase();
                String identity = String.format("%s.%s", schema, tableName);
                TABLE_NAMES.add(identity);
            }
        }
        TABLE_NAMES.add(TABLE_SELLER_TYPE_DIM_TABLE); // the wrapper view VIEW_SELLER_TYPE_DIM need this table
        
        // scp data files, use the data from hbase, instead of local files
        File tempDir = Files.createTempDir();
        String tempDirAbsPath = tempDir.getAbsolutePath();
        for (String tablename : TABLE_NAMES) {
            tablename = tablename.toUpperCase();

            File localBufferFile = new File(tempDirAbsPath + "/" + tablename + ".csv");
            localBufferFile.createNewFile();

            logger.info(String.format("get resource from hbase:/data/%s.csv", tablename));
            InputStream hbaseDataStream = metaMgr.getStore().getResource("/data/" + tablename + ".csv").inputStream;
            FileOutputStream localFileStream = new FileOutputStream(localBufferFile);
            IOUtils.copy(hbaseDataStream, localFileStream);

            hbaseDataStream.close();
            localFileStream.close();

            localBufferFile.deleteOnExit();
        }
        tempDir.deleteOnExit();

        ISampleDataDeployer sampleDataDeployer = SourceManager.getSource(model.getRootFactTable().getTableDesc())
                .getSampleDataDeployer();
        
        // create hive tables
        sampleDataDeployer.createSampleDatabase("EDW");
        for (String tablename : TABLE_NAMES) {
            logger.info(String.format("get table desc %s", tablename));
            sampleDataDeployer.createSampleTable(metaMgr.getTableDesc(tablename, model.getProject()));
        }

        // load data to hive tables
        // LOAD DATA LOCAL INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
        for (String tablename : TABLE_NAMES) {
            logger.info(String.format("load data into %s", tablename));
            sampleDataDeployer.loadSampleData(tablename, tempDirAbsPath);
        }
        
        // create the view automatically here
        sampleDataDeployer.createWrapperView(TABLE_SELLER_TYPE_DIM_TABLE, VIEW_SELLER_TYPE_DIM);
    }
}
