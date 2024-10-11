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

package org.apache.kylin.tool;

import static org.apache.kylin.common.persistence.MetadataType.CC_MODEL_RELATION;
import static org.apache.kylin.common.persistence.MetadataType.COMPUTE_COLUMN;
import static org.apache.kylin.common.persistence.MetadataType.DATAFLOW;
import static org.apache.kylin.common.persistence.MetadataType.DATA_PARSER;
import static org.apache.kylin.common.persistence.MetadataType.FUSION_MODEL;
import static org.apache.kylin.common.persistence.MetadataType.INDEX_PLAN;
import static org.apache.kylin.common.persistence.MetadataType.JAR_INFO;
import static org.apache.kylin.common.persistence.MetadataType.KAFKA_CONFIG;
import static org.apache.kylin.common.persistence.MetadataType.LAYOUT;
import static org.apache.kylin.common.persistence.MetadataType.LAYOUT_DETAILS;
import static org.apache.kylin.common.persistence.MetadataType.MODEL;
import static org.apache.kylin.common.persistence.MetadataType.PROJECT;
import static org.apache.kylin.common.persistence.MetadataType.RESOURCE_GROUP;
import static org.apache.kylin.common.persistence.MetadataType.SEGMENT;
import static org.apache.kylin.common.persistence.MetadataType.STREAMING_JOB;
import static org.apache.kylin.common.persistence.MetadataType.SYSTEM;
import static org.apache.kylin.common.persistence.MetadataType.TABLE_EXD;
import static org.apache.kylin.common.persistence.MetadataType.TABLE_INFO;
import static org.apache.kylin.common.persistence.MetadataType.TABLE_MODEL_RELATION;
import static org.apache.kylin.common.persistence.ResourceStore.METASTORE_IMAGE;
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.cli.Option;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ImageDesc;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.RawResourceTool;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.apache.kylin.helper.MetadataToolHelper;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import lombok.val;
import lombok.var;

public class MetadataToolTest extends NLocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(MetadataToolTest.class);

    private static final Option OPERATE_BACKUP = OptionBuilder.getInstance()
            .withDescription("Backup metadata to local path or HDFS path").isRequired(false).create("backup");

    private static final Option OPERATE_RESTORE = OptionBuilder.getInstance()
            .withDescription("Restore metadata from local path or HDFS path").isRequired(false).create("restore");
    private static final String COMPRESSED_FILE = "metadata.zip";
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        createTestMetadata();
        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());
    }

    @After
    public void teardown() {
        try {
            val jdbcTemplate = getJdbcTemplate();
            jdbcTemplate.batchUpdate("SHUTDOWN;");
        } catch (Exception e) {
            logger.warn("SHUTDOWN; error.", e);
        }
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    private MetadataTool tool(String path) {
        KylinConfig kylinConfig = getTestConfig();
        return new MetadataTool(kylinConfig, new MetadataToolHelper() {
            @Override
            public String getMetadataUrl(String rootPath, boolean compressed, KylinConfig kylinConfig) {
                return "kylin_metadata@hdfs,zip=1,path=file://" + path;
            }
        });
    }

    /*@Test
    @Ignore
    public void testFetchTargetFile() throws IOException {
        val junitFolder = temporaryFolder.getRoot();
        val tool = new MetadataTool(getTestConfig());
        // test case for fetching a specific file
        tool.execute(new String[] { "-fetch", "-target", "default/table/DEFAULT.STREAMING_TABLE.json", "-dir",
                junitFolder.getAbsolutePath(), "-folder", "target_fetch" });
        //test case for fetching a folder
        tool.execute(new String[] { "-fetch", "-target", "_global", "-dir", junitFolder.getAbsolutePath(), "-folder",
                "target_fetch_global" });
    
        Assertions.assertThat(junitFolder.listFiles()).hasSize(2);
        File archiveFolder = null;
        File globalFolder = null;
        for (File folder : junitFolder.listFiles()) {
            if (folder.getName().equals("target_fetch_global")) {
                globalFolder = folder;
            }
            if (folder.getName().equals("target_fetch")) {
                archiveFolder = folder;
            }
        }
        Assertions.assertThat(archiveFolder).exists();
    
        Assertions.assertThat(archiveFolder.list()).isNotEmpty().containsOnly("default", "UUID");
    
        val projectFolder = findFile(archiveFolder.listFiles(), f -> f.getName().equals("default"));
        assertProjectFolder(projectFolder, globalFolder);
    }
    
    @Test
    public void testListFile() {
        val tool = new MetadataTool(getTestConfig());
        tool.execute(new String[] { "-list", "-target", "default" });
    }*/

    @Test
    public void testBackupProject() throws IOException {
        val junitFolder = temporaryFolder.getRoot();
        val tool = new MetadataTool(getTestConfig());
        tool.execute(new String[] { "-backup", "-project", "default", "-dir", junitFolder.getAbsolutePath(), "-folder",
                "prj_bak" });

        Assertions.assertThat(junitFolder.listFiles()).hasSize(1);
        val archiveFolder = junitFolder.listFiles()[0];
        Assertions.assertThat(archiveFolder).exists();

        val coreMetaFolder = findFile(archiveFolder.listFiles(), f -> f.getName().equals("core_meta"));
        val projectFolder = findFile(coreMetaFolder.listFiles(), f -> f.getName().equals(MetadataType.PROJECT.name()));

        Assertions.assertThat(projectFolder.list()).isNotEmpty().containsOnly("default.json");
        assertProjectMetadata("default", coreMetaFolder);
    }

    @Test
    public void testBackupProjectCompress() throws IOException {
        val junitFolder = temporaryFolder.getRoot();
        val tool = tool(junitFolder.getAbsolutePath() + "/prj_bak");
        tool.execute(new String[] { "-backup", "-project", "default", "-compress", "-dir", "ignored" });

        Assertions.assertThat(junitFolder.listFiles()).hasSize(1);
        val archiveFolder = junitFolder.listFiles()[0];
        Assertions.assertThat(archiveFolder).exists();

        Assertions.assertThat(archiveFolder.list()).isNotEmpty().contains(COMPRESSED_FILE);
        val files = getFilesFromCompressedFile(new File(archiveFolder, COMPRESSED_FILE));
        Assertions.assertThat(listFolder(files, "")).containsOnly(SYSTEM.name(), PROJECT.name(), TABLE_INFO.name(),
                MODEL.name(), DATAFLOW.name(), INDEX_PLAN.name(), SEGMENT.name(), LAYOUT.name(), COMPUTE_COLUMN.name(),
                CC_MODEL_RELATION.name(), TABLE_MODEL_RELATION.name());
        assertProjectFolder("default", files);
    }

    private Map<String, RawResource> getFilesFromCompressedFile(File file) {
        val res = Maps.<String, RawResource> newHashMap();
        FileInputStream in = null;
        ZipInputStream zipIn = null;
        try {
            in = new FileInputStream(file);
            zipIn = new ZipInputStream(in);
            ZipEntry zipEntry = null;
            while ((zipEntry = zipIn.getNextEntry()) != null) {
                val bs = ByteSource.wrap(IOUtils.toByteArray(zipIn));
                long t = zipEntry.getTime();
                val raw = new RawResource(zipEntry.getName(), bs, t, 0);
                res.put(zipEntry.getName(), raw);
            }
            return res;
        } catch (Exception ignored) {
        } finally {
            IOUtils.closeQuietly(zipIn);
            IOUtils.closeQuietly(in);
        }
        return Maps.newHashMap();
    }

    private Set<String> listFolder(Map<String, RawResource> files, String folder) {
        return files.keySet().stream().filter(name -> name.startsWith(folder))
                .map(name -> name.substring((folder).length()).split("/")[0]).collect(Collectors.toSet());
    }

    @Test
    public void testBackupAllCompress() throws IOException {
        val junitFolder = temporaryFolder.getRoot();
        val tool = tool(junitFolder.getAbsolutePath() + "/backup");
        tool.execute(new String[] { "-backup", "-compress", "-dir", "ignored" });
        Assertions.assertThat(junitFolder.listFiles()).hasSize(1);
        val archiveFolder = junitFolder.listFiles()[0];
        Assertions.assertThat(archiveFolder).exists();
        Assertions.assertThat(archiveFolder.list()).isNotEmpty().contains(COMPRESSED_FILE);
    }

    private boolean assertProjectMetadata(String project, File archiveFolder) {
        System.out.println("---------" + project);
        val tableFolder = findFile(archiveFolder.listFiles(), f -> f.getName().equals(MetadataType.TABLE_INFO.name()));
        Assertions.assertThat(tableFolder.list()).anyMatch(resourceName -> resourceName.startsWith(project));

        val projectFolder = findFile(archiveFolder.listFiles(), f -> f.getName().equals(MetadataType.PROJECT.name()));
        Assertions.assertThat(findFile(projectFolder.listFiles(), f -> f.getName().startsWith(project))).exists()
                .isFile();
        return true;
    }

    private boolean assertProjectFolder(String project, Map<String, RawResource> files) {
        Assertions.assertThat(listFolder(files, PROJECT.name() + "/")).containsOnly(project + ".json");
        return true;
    }

    @Test
    public void testBackupAll() throws IOException {
        val junitFolder = temporaryFolder.getRoot();
        val tool = new MetadataTool(getTestConfig());
        tool.execute(new String[] { "-backup", "-dir", junitFolder.getAbsolutePath() });

        Assertions.assertThat(junitFolder.listFiles()).hasSize(1);
        val archiveFolder = junitFolder.listFiles()[0];
        Assertions.assertThat(archiveFolder).exists();

        val coreMetaFolder = findFile(archiveFolder.listFiles(), f -> f.getName().equals("core_meta"));

        Assertions.assertThat(coreMetaFolder.list()).isNotEmpty().containsExactlyInAnyOrder(SYSTEM.name(),
                PROJECT.name(), TABLE_INFO.name(), MODEL.name(), DATAFLOW.name(), INDEX_PLAN.name(), SEGMENT.name(),
                LAYOUT.name(), LAYOUT_DETAILS.name(), COMPUTE_COLUMN.name(), CC_MODEL_RELATION.name(),
                TABLE_MODEL_RELATION.name(), DATA_PARSER.name(), RESOURCE_GROUP.name(), STREAMING_JOB.name(),
                JAR_INFO.name(), KAFKA_CONFIG.name(), FUSION_MODEL.name(), TABLE_EXD.name(), "kylin.properties");

        val projectDir = findFile(coreMetaFolder.listFiles(), f -> f.getName().startsWith(MetadataType.PROJECT.name()));
        Assertions.assertThat(projectDir.listFiles()).allMatch(
                projectFile -> assertProjectMetadata(projectFile.getName().replace(".json", ""), coreMetaFolder));
    }

    @Test
    public void testRestoreOverwriteAll() throws IOException {
        val junitFolder = temporaryFolder.getRoot();
        val junitCoreMetaFolder = new File(junitFolder.getAbsolutePath() + "/core_meta");
        junitCoreMetaFolder.mkdir();
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), junitCoreMetaFolder, MetadataType.ALL.name());

        //there is a project that destResourceStore contains and srcResourceStore doesn't contain
        val toDelete = ResourceStore.getKylinMetaStore(getTestConfig()).listResourcesRecursivelyByProject("demo");
        for (String resource : toDelete) {
            FileUtils.forceDelete(Paths.get(junitCoreMetaFolder.getAbsolutePath(), resource + ".json").toFile());
        }

        //there is a project that destResourceStore doesn't contain and srcResourceStore contains
        val destResourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val toDeleteDestResources = destResourceStore.listResourcesRecursivelyByProject("ssb");
        for (String res : toDeleteDestResources) {
            destResourceStore.deleteResource(res);
        }

        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNull();
        assertBeforeRestoreTest();
        val tool = new MetadataTool(getTestConfig());
        tool.execute(new String[] { "-restore", "-dir", junitFolder.getAbsolutePath(), "--after-truncate" });
        assertAfterRestoreTest();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNotNull();
    }

    @Test
    public void testRestoreUpdateAll() throws IOException {
        val junitFolder = temporaryFolder.getRoot();
        val junitCoreMetaFolder = new File(junitFolder.getAbsolutePath() + "/core_meta");
        junitCoreMetaFolder.mkdir();
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), junitCoreMetaFolder, MetadataType.ALL.name());

        val destResourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        String deletePath = MetadataType.mergeKeyWithType("3f8941de-d01c-42b8-91b5-44646390864b", MetadataType.MODEL);
        String modifyPath = MetadataType.mergeKeyWithType("039eef32-9691-4c88-93ba-d65c58a1ab7a", MetadataType.MODEL);
        String addPath = MetadataType.mergeKeyWithType("add", MetadataType.MODEL);

        val modelDesc = JsonUtil.readValue(destResourceStore.getResource(modifyPath).getByteSource().read(),
                NDataModel.class);
        String originDescription = modelDesc.getDescription();
        modelDesc.setDescription("after modify");

        destResourceStore.deleteResource(deletePath);
        destResourceStore.deleteResource(modifyPath);
        destResourceStore.putResourceWithoutCheck(modifyPath, ByteSource.wrap(JsonUtil.writeValueAsBytes(modelDesc)), 0,
                0);
        destResourceStore.putResourceWithoutCheck(addPath, RawResourceTool.createByteSource("test1"), 0, 0);
        Assert.assertNull(destResourceStore.getResource(deletePath));
        Assert.assertNotEquals(originDescription,
                JsonUtil.readValue(destResourceStore.getResource(modifyPath).getByteSource().read(), NDataModel.class)
                        .getDescription());
        Assert.assertNotNull(destResourceStore.getResource(addPath));

        val tool = new MetadataTool(getTestConfig());
        tool.execute(new String[] { "-restore", "-dir", junitFolder.getAbsolutePath() });

        Assert.assertNotNull(destResourceStore.getResource(deletePath));//delete path will restore
        Assert.assertEquals(originDescription,
                JsonUtil.readValue(destResourceStore.getResource(modifyPath).getByteSource().read(), NDataModel.class)
                        .getDescription());//modify path will restore
        Assert.assertNotNull(destResourceStore.getResource(addPath));//add path will not delete

        FileUtils.deleteDirectory(junitFolder.getAbsoluteFile());
    }

    @Test
    public void testRestoreUpdateProject() throws IOException {
        val junitFolder = temporaryFolder.getRoot();
        val junitCoreMetaFolder = new File(junitFolder.getAbsolutePath() + "/core_meta");
        junitCoreMetaFolder.mkdir();
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), junitCoreMetaFolder, MetadataType.ALL.name());

        val destResourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        String deletePath = MetadataType.mergeKeyWithType("f1bb4bbd-a638-442b-a276-e301fde0d7f6", MODEL);
        String modifyPath = MetadataType.mergeKeyWithType("039eef32-9691-4c88-93ba-d65c58a1ab7a", MODEL);
        String addPath = MetadataType.mergeKeyWithType("add", MODEL);

        val modelDesc = JsonUtil.readValue(destResourceStore.getResource(modifyPath).getByteSource().read(),
                NDataModel.class);
        String originDescription = modelDesc.getDescription();
        modelDesc.setDescription("after modify");

        destResourceStore.deleteResource(deletePath);
        destResourceStore.deleteResource(modifyPath);
        destResourceStore.putResourceWithoutCheck(modifyPath, ByteSource.wrap(JsonUtil.writeValueAsBytes(modelDesc)), 0,
                0);
        destResourceStore.putResourceWithoutCheck(addPath, RawResourceTool.createByteSource("test2"), 0, 0);

        Assert.assertNull(destResourceStore.getResource(deletePath));
        Assert.assertNotEquals(originDescription,
                JsonUtil.readValue(destResourceStore.getResource(modifyPath).getByteSource().read(), NDataModel.class)
                        .getDescription());
        Assert.assertNotNull(destResourceStore.getResource(addPath));

        val tool = new MetadataTool(getTestConfig());
        tool.execute(new String[] { "-restore", "-project", "broken_test", "-dir", junitFolder.getAbsolutePath() });

        Assert.assertNotNull(destResourceStore.getResource(deletePath));//delete path will restore
        Assert.assertEquals(originDescription,
                JsonUtil.readValue(destResourceStore.getResource(modifyPath).getByteSource().read(), NDataModel.class)
                        .getDescription());//modify path will restore
        Assert.assertNotNull(destResourceStore.getResource(addPath));//add path will not delete

        FileUtils.deleteDirectory(junitFolder.getAbsoluteFile());
    }

    private void prepareCompressedFile() throws Exception {
        val junitFolder = temporaryFolder.getRoot();

        //there is a project that destResourceStore doesn't contain and srcResourceStore contains
        val destResourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val destResources = destResourceStore.listResourcesRecursivelyByProject("demo");
        for (String res : destResources) {
            destResourceStore.deleteResource(res);
        }

        val tool = tool(junitFolder.getAbsolutePath());
        tool.execute(new String[] { "-backup", "-compress", "-dir", "ignored" });
        teardown();
        setup();
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }

    @Test
    public void testRestoreOverwriteAllCompress() throws Exception {
        prepareCompressedFile();

        val junitFolder = temporaryFolder.getRoot();
        MetadataToolTestFixture.fixtureRestoreTest();

        //there is a project that destResourceStore doesn't contain and srcResourceStore contains
        val destResourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val destResources = destResourceStore.listResourcesRecursivelyByProject("ssb");
        for (String res : destResources) {
            destResourceStore.deleteResource(res);
        }

        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNull();
        assertBeforeRestoreTest();
        val tool = tool(junitFolder.getAbsolutePath());
        tool.execute(new String[] { "-restore", "-compress", "-dir", "ignored", "--after-truncate", });
        assertAfterRestoreTest();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNotNull();
    }

    @Test
    public void testRestoreOverwriteAllWithSrcOrDestIsEmpty() throws Exception {
        val emptyFolder = temporaryFolder.newFolder();
        val restoreFolder = temporaryFolder.newFolder();
        val restoreCoreMetaFolder = new File(restoreFolder.getAbsolutePath() + "/core_meta");
        restoreCoreMetaFolder.mkdir();
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), restoreCoreMetaFolder, MetadataType.ALL.name());

        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("default")).isNotNull();
        val tool = new MetadataTool(getTestConfig());
        tool.execute(new String[] { "-restore", "-dir", emptyFolder.getAbsolutePath(), "--after-truncate" });
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).listAllProjects()).isEmpty();

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        tool.execute(new String[] { "-restore", "-dir", restoreFolder.getAbsolutePath(), "--after-truncate" });
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("default")).isNotNull();
    }

    @Test
    public void testRestoreDuplicateUuidModel() throws Exception {
        val project = "default";
        val backupPath = temporaryFolder.newFolder();
        String backFolder = "testRestoreDuplicateUuidMode_backup";
        val backupTool = new MetadataTool(getTestConfig());
        backupTool.execute(new String[] { "-backup", "-dir", backupPath.getAbsolutePath(), "-folder", backFolder });

        val tool = new MetadataTool(getTestConfig());
        val restorePath = backupPath.getAbsolutePath() + "/" + backFolder;
        tool.execute(new String[] { "-restore", "-dir", restorePath });
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), "default");

        String modelId = "82fa7671-a935-45f5-8779-85703601f49a";
        String modelId2 = "a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94";

        NDataModel dataModelDesc = dataModelManager.getDataModelDesc(modelId);
        NDataModel nDataModel = dataModelManager.copyBySerialization(dataModelDesc);
        nDataModel.setMvcc(-1);
        nDataModel.setUuid(RandomUtil.randomUUIDStr());

        NDataModel dataModelDesc2 = dataModelManager.getDataModelDesc(modelId2);
        NDataModel nDataModel2 = dataModelManager.copyBySerialization(dataModelDesc2);
        nDataModel2.setMvcc(-1);
        nDataModel2.setUuid(RandomUtil.randomUUIDStr());

        UnitOfWork.doInTransactionWithRetry(() -> {
            NDataModelManager.getInstance(getTestConfig(), project).dropModel(modelId);
            NDataModelManager.getInstance(getTestConfig(), project).dropModel(modelId2);
            return true;
        }, project);

        UnitOfWork.doInTransactionWithRetry(() -> {
            NDataModelManager.getInstance(getTestConfig(), project).createDataModelDesc(nDataModel,
                    nDataModel.getOwner());
            NDataModelManager.getInstance(getTestConfig(), project).createDataModelDesc(nDataModel2,
                    nDataModel2.getOwner());
            return true;
        }, project);

        try {
            tool.execute(new String[] { "-restore", "-dir", restorePath });
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertTrue(e.getCause() instanceof TransactionException);
            assertTrue(e.getCause().getCause() instanceof KylinException);
            assertEquals(
                    "KE-050041203: Please modify the model name and then restore:[project]:models: [default]:ut_inner_join_cube_partial,test_encoding",
                    e.getCause().getCause().toString());
        }
        long start = System.currentTimeMillis() / 1000;
        await().until(() -> start != (System.currentTimeMillis() / 1000));
        try {
            tool.execute(new String[] { "-restore", "-project", "default", "-dir", restorePath });
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertTrue(e.getCause() instanceof TransactionException);
            assertTrue(e.getCause().getCause() instanceof KylinException);
            assertEquals(
                    "KE-050041203: Please modify the model name and then restore:[project]:models: [default]:ut_inner_join_cube_partial,test_encoding",
                    e.getCause().getCause().toString());
        }

        Assertions.assertThat(dataModelManager.getDataModelDesc(modelId)).isNull();
        Assertions.assertThat(dataModelManager.getDataModelDesc(modelId2)).isNull();

        long start2 = System.currentTimeMillis() / 1000;
        await().until(() -> start2 != (System.currentTimeMillis() / 1000));
        tool.execute(new String[] { "-restore", "-dir", restorePath, "--after-truncate" });

        Assertions.assertThat(dataModelManager.getDataModelDesc(modelId)).isNotNull();
        Assertions.assertThat(dataModelManager.getDataModelDesc(modelId2)).isNotNull();
    }

    @Test
    public void testRestoreOverwriteAllCompressWithSrcOrDestIsEmpty() throws Exception {
        val emptyFolder = temporaryFolder.newFolder();
        createEmptyCompressedFile(emptyFolder);
        val restoreFolder = temporaryFolder.newFolder();
        createAllCompressedFile(restoreFolder);

        MetadataToolTestFixture.fixtureRestoreTest();

        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("default")).isNotNull();
        MetadataTool tool = tool(emptyFolder.getAbsolutePath());
        tool.execute(new String[] { "-restore", "-compress", "-dir", "ignored", "--after-truncate" });
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).listAllProjects()).isEmpty();

        tool = tool(restoreFolder.getAbsolutePath());
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        tool.execute(new String[] { "-restore", "-compress", "-dir", "ignored", "--after-truncate" });
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("default")).isNotNull();
    }

    private void createEmptyCompressedFile(File folder) throws IOException {
        FileOutputStream out = null;
        ZipOutputStream zipOut = null;
        try {
            out = new FileOutputStream(new File(folder, COMPRESSED_FILE));
            CheckedOutputStream cos = new CheckedOutputStream(out, new CRC32());
            zipOut = new ZipOutputStream(cos);
        } catch (Exception e) {
            throw new IOException("Put compressed resource fail", e);
        } finally {
            IOUtils.closeQuietly(zipOut);
            IOUtils.closeQuietly(out);
        }
    }

    private void createAllCompressedFile(File folder) throws IOException {
        val tool = tool(folder.getAbsolutePath());
        tool.execute(new String[] { "-backup", "-compress", "-dir", "ignored" });
    }

    @Test
    public void testRestoreOverwriteProject() throws Exception {
        val resourceStore = getStore();
        val jdbcTemplate = getJdbcTemplate();
        resourceStore.getMetadataStore().setAuditLogStore(new JdbcAuditLogStore(getTestConfig(), jdbcTemplate,
                new DataSourceTransactionManager(jdbcTemplate.getDataSource()), "test_audit_log"));
        val junitFolder = temporaryFolder.getRoot();
        val junitCoreMetaFolder = new File(junitFolder.getAbsolutePath() + "/core_meta");
        junitCoreMetaFolder.mkdir();
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), junitCoreMetaFolder, MetadataType.ALL.name());
        long oldOffset = resourceStore.getAuditLogStore().getMaxId();

        assertBeforeRestoreTest();
        val tool = new MetadataTool(getTestConfig());
        tool.execute(new String[] { "-restore", "-project", "default", "-dir", junitFolder.getAbsolutePath(),
                "--after-truncate" });
        assertAfterRestoreTest();

        val path = HadoopUtil.getBackupFolder(getTestConfig());
        val fs = HadoopUtil.getWorkingFileSystem();
        val rootPath = Stream.of(fs.listStatus(new Path(path)))
                .max(Comparator.comparing(FileStatus::getModificationTime)).map(FileStatus::getPath)
                .orElse(new Path(path + "/backup_0/"));
        try (val in = fs.open(new Path(rootPath + "/core_meta", METASTORE_IMAGE + ".json"))) {
            val image = JsonUtil.readValue(IOUtils.toByteArray(in), ImageDesc.class);
            // restore will delete 15 resources.
            Assert.assertEquals(resourceStore.listResourcesRecursivelyByProject("default").size() + 15,
                    image.getOffset() - oldOffset);
        }
        FileUtils.deleteDirectory(junitFolder.getAbsoluteFile());
    }

    @Test
    public void testRestoreOverwriteProjectCompress() throws Exception {
        val resourceStore = getStore();
        val jdbcTemplate = getJdbcTemplate();
        resourceStore.getMetadataStore().setAuditLogStore(new JdbcAuditLogStore(getTestConfig(), jdbcTemplate,
                new DataSourceTransactionManager(jdbcTemplate.getDataSource()), "test_audit_log"));
        val junitFolder = temporaryFolder.getRoot();
        createAllCompressedFile(junitFolder);
        MetadataToolTestFixture.fixtureRestoreTest();
        long oldOffset = resourceStore.getAuditLogStore().getMaxId();

        assertBeforeRestoreTest();
        val tool = tool(junitFolder.getAbsolutePath());
        tool.execute(
                new String[] { "-restore", "-project", "default", "-compress", "-dir", "ignored", "--after-truncate" });
        assertAfterRestoreTest();

        val path = HadoopUtil.getBackupFolder(getTestConfig());
        val fs = HadoopUtil.getWorkingFileSystem();
        val rootPath = Stream.of(fs.listStatus(new Path(path)))
                .max(Comparator.comparing(FileStatus::getModificationTime)).map(FileStatus::getPath)
                .orElse(new Path(path + "/backup_0/"));
        try (val in = fs.open(new Path(rootPath + "/core_meta", METASTORE_IMAGE + ".json"))) {
            val image = JsonUtil.readValue(IOUtils.toByteArray(in), ImageDesc.class);
            // restore will delete 15 resources.
            Assert.assertEquals(resourceStore.listResourcesRecursivelyByProject("default").size() + 15,
                    image.getOffset() - oldOffset);
        }
    }

    @Test
    public void testRestoreOverwriteProjectWithSrcOrDestIsEmpty() throws Exception {
        val junitFolder = temporaryFolder.getRoot();
        val junitCoreMetaFolder = new File(junitFolder.getAbsolutePath() + "/core_meta");
        junitCoreMetaFolder.mkdir();
        ResourceTool.copy(getTestConfig(), KylinConfig.createInstanceFromUri(junitCoreMetaFolder.getAbsolutePath()),
                MetadataType.ALL.name());
        val tool = new MetadataTool(getTestConfig());

        //there is a project metadata that destResourceStore contains and srcResourceStore doesn't contain
        val toDelete = ResourceStore.getKylinMetaStore(getTestConfig()).listResourcesRecursivelyByProject("demo");
        for (String resource : toDelete) {
            FileUtils.forceDelete(Paths.get(junitCoreMetaFolder.getAbsolutePath(), resource + ".json").toFile());
        }

        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNotNull();
        tool.execute(new String[] { "-restore", "-project", "demo", "-dir", junitFolder.getAbsolutePath(),
                "--after-truncate" });
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNull();

        //there is a project metadata that destResourceStore doesn't contain and srcResourceStore contains
        val destResourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val destResources = destResourceStore.listResourcesRecursivelyByProject("ssb");
        for (String res : destResources) {
            destResourceStore.deleteResource(res);
        }

        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNull();

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        tool.execute(new String[] { "-restore", "-project", "ssb", "-dir", junitFolder.getAbsolutePath(),
                "--after-truncate" });
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNotNull();
    }

    @Test
    public void testProjectOverwriteNameStartsWithSame() throws Exception {

        val resourceStore = getStore();

        val junitFolder = temporaryFolder.getRoot();
        val tool = new MetadataTool(getTestConfig());

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            val prj = projectMgr.getProject("default");
            projectMgr.createProject("default1", "", "", Maps.newLinkedHashMap());
            return 0;
        }, "default1", 1);

        tool.execute(new String[] { "-backup", "-project", "default", "-dir", junitFolder.getAbsolutePath(), "-folder",
                "prj_bak" });
        val tool1 = new MetadataTool(getTestConfig());
        tool1.execute(new String[] { "-restore", "-project", "default", "-dir",
                junitFolder.getAbsolutePath() + File.separator + "prj_bak", "--after-truncate" });

        Assert.assertTrue(resourceStore.exists("PROJECT/default1"));
    }

    @Test
    public void testRestoreOverwriteProjectCompressWithSrcOrDestIsEmpty() throws Exception {
        prepareCompressedFile();

        val junitFolder = temporaryFolder.getRoot();
        val tool = tool(junitFolder.getAbsolutePath());

        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNotNull();
        tool.execute(
                new String[] { "-restore", "-project", "demo", "-compress", "-dir", "ignored", "--after-truncate" });
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNull();

        //there is a project metadata that destResourceStore doesn't contain and srcResourceStore contains
        val destResourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val destResources = destResourceStore.listResourcesRecursivelyByProject("ssb");
        for (String res : destResources) {
            destResourceStore.deleteResource(res);
        }

        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNull();

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        tool.execute(
                new String[] { "-restore", "-project", "ssb", "-compress", "-dir", "ignored", "--after-truncate" });
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNotNull();
    }

    private void assertBeforeRestoreTest() {
        val dataModelMgr = NDataModelManager.getInstance(getTestConfig(), "default");

        val dataModel1 = dataModelMgr.getDataModelDescByAlias("nmodel_basic");
        Assertions.assertThat(dataModel1).isNotNull().hasFieldOrPropertyWithValue("owner", "who")
                .hasFieldOrPropertyWithValue("mvcc", 1L);

        val dataModel2 = dataModelMgr.getDataModelDescByAlias("nmodel_basic_inner");
        Assertions.assertThat(dataModel2).isNull();

        val dataModel3 = dataModelMgr.getDataModelDescByAlias("data_model_3");
        Assertions.assertThat(dataModel3).isNotNull().hasFieldOrPropertyWithValue("owner", "who")
                .hasFieldOrPropertyWithValue("mvcc", 0L);
    }

    private void assertAfterRestoreTest() {
        val dataModelMgr = NDataModelManager.getInstance(getTestConfig(), "default");

        val dataModel1 = dataModelMgr.getDataModelDescByAlias("nmodel_basic");
        Assertions.assertThat(dataModel1).isNotNull().hasFieldOrPropertyWithValue("owner", "ADMIN")
                .hasFieldOrPropertyWithValue("mvcc", 1L);

        val dataModel2 = dataModelMgr.getDataModelDescByAlias("nmodel_basic_inner");
        Assertions.assertThat(dataModel2).isNotNull().hasFieldOrPropertyWithValue("mvcc", 0L);

        val dataModel3 = dataModelMgr.getDataModelDescByAlias("data_model_3");
        Assertions.assertThat(dataModel3).isNull();
    }

    private File findFile(File[] files, Predicate<File> predicate) {
        Assertions.assertThat(files).anyMatch(predicate);
        return Arrays.stream(files).filter(predicate).findFirst().get();
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = StorageURL.valueOf(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1;MODE=MySQL,username=sa,password=");
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }

    @Test
    public void testNotIncludeTableExdDirectory() throws Exception {
        File junitFolder = temporaryFolder.getRoot();
        File test1 = new File(junitFolder, "include");
        File test2 = new File(junitFolder, "exclude");
        FileUtils.forceMkdir(test1);
        FileUtils.forceMkdir(test2);
        val tool = new MetadataTool(getTestConfig());
        tool.execute(new String[] { "-backup", "-project", "newten", "-dir", test1.getAbsolutePath() });
        var archiveFolder = test1.listFiles()[0];
        var coreMetaFolder = findFile(archiveFolder.listFiles(), f -> f.getName().equals("core_meta"));
        Assertions.assertThat(coreMetaFolder.list()).contains("TABLE_EXD");
        tool.execute(
                new String[] { "-backup", "-project", "newten", "-dir", test2.getAbsolutePath(), "-excludeTableExd" });
        archiveFolder = test2.listFiles()[0];
        coreMetaFolder = findFile(archiveFolder.listFiles(), f -> f.getName().equals("core_meta"));
        Assertions.assertThat(coreMetaFolder.list()).doesNotContain("TABLE_EXD");
    }

    @Test
    public void testGetMetadataUrl() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        MetadataToolHelper metadataToolHelper = new MetadataToolHelper();

        var hdfsPath = "hdfs://host/path/to/hdfs/dir";
        var hdfsMetadataUrl = metadataToolHelper.getMetadataUrl(hdfsPath, true, kylinConfig);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/hdfs/dir/,zip=1", hdfsMetadataUrl);
        hdfsMetadataUrl = metadataToolHelper.getMetadataUrl(hdfsPath, false, kylinConfig);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/hdfs/dir/", hdfsMetadataUrl);

        var maprfsPath = "maprfs://host/path/to/maprfs/dir";
        var maprfsMetadataUrl = metadataToolHelper.getMetadataUrl(maprfsPath, true, kylinConfig);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/maprfs/dir/,zip=1", maprfsMetadataUrl);
        maprfsMetadataUrl = metadataToolHelper.getMetadataUrl(maprfsPath, false, kylinConfig);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/maprfs/dir/", maprfsMetadataUrl);

        var s3Path = "s3://host/path/to/s3/dir";
        var s3MetadataUrl = metadataToolHelper.getMetadataUrl(s3Path, true, kylinConfig);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/s3/dir/,zip=1", s3MetadataUrl);
        s3MetadataUrl = metadataToolHelper.getMetadataUrl(s3Path, false, kylinConfig);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/s3/dir/", s3MetadataUrl);

        var s3aPath = "s3a://host/path/to/s3a/dir";
        var s3aMetadataUrl = metadataToolHelper.getMetadataUrl(s3aPath, true, kylinConfig);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/s3a/dir/,zip=1", s3aMetadataUrl);
        s3aMetadataUrl = metadataToolHelper.getMetadataUrl(s3aPath, false, kylinConfig);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/s3a/dir/", s3aMetadataUrl);

        var wasbPath = "wasb://host/path/to/wasb/dir";
        var wasbMetadataUrl = metadataToolHelper.getMetadataUrl(wasbPath, true, kylinConfig);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/wasb/dir/,zip=1", wasbMetadataUrl);
        wasbMetadataUrl = metadataToolHelper.getMetadataUrl(wasbPath, false, kylinConfig);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/wasb/dir/", wasbMetadataUrl);

        var filePath = "file:///path/to/file/dir";
        var fileMetadataUrl = metadataToolHelper.getMetadataUrl(filePath, true, kylinConfig);
        Assert.assertEquals("/path/to/file/dir/", fileMetadataUrl);
        fileMetadataUrl = metadataToolHelper.getMetadataUrl(filePath, false, kylinConfig);
        Assert.assertEquals("/path/to/file/dir/", fileMetadataUrl);

        var simplePath = "/just/a/path";
        var simpleMetadataUrl = metadataToolHelper.getMetadataUrl(simplePath, true, kylinConfig);
        Assert.assertEquals("/just/a/path/", simpleMetadataUrl);
        simpleMetadataUrl = metadataToolHelper.getMetadataUrl(simplePath, false, kylinConfig);
        Assert.assertEquals("/just/a/path/", simpleMetadataUrl);
    }

    @Test
    public void testExecute_throwsException() {
        MetadataTool metadataTool = new MetadataTool();
        OptionsHelper optionsHelper = mock(OptionsHelper.class);
        when(optionsHelper.hasOption(OPERATE_BACKUP)).thenReturn(false);
        when(optionsHelper.hasOption(OPERATE_RESTORE)).thenReturn(false);
        try {
            metadataTool.execute(optionsHelper);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-050040202: \"-restore\" is not specified.", e.toString());
        }
    }
}
