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

import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
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
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.OptionsHelper;
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
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.google.common.collect.Maps;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
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

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        try {
            val jdbcTemplate = getJdbcTemplate();
            jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        } catch (Exception e) {
            logger.warn("drop all objects error.", e);
        }
        cleanupTestMetadata();
    }

    private MetadataTool tool(String path) {
        val originTool = new MetadataTool(getTestConfig());
        val tool = Mockito.spy(originTool);
        Mockito.when(tool.getMetadataUrl(Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn("kylin_metadata@hdfs,zip=1,path=file://" + path);
        return tool;
    }

    @Test
    public void testBackupProject() throws IOException {
        val junitFolder = temporaryFolder.getRoot();
        val tool = new MetadataTool(getTestConfig());
        tool.execute(new String[] { "-backup", "-project", "default", "-dir", junitFolder.getAbsolutePath(), "-folder",
                "prj_bak" });

        Assertions.assertThat(junitFolder.listFiles()).hasSize(1);
        val archiveFolder = junitFolder.listFiles()[0];
        Assertions.assertThat(archiveFolder).exists();

        Assertions.assertThat(archiveFolder.list()).isNotEmpty().containsOnly("default", "UUID", "_global");

        val projectFolder = findFile(archiveFolder.listFiles(), f -> f.getName().equals("default"));
        assertProjectFolder(projectFolder, archiveFolder);
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
        Assertions.assertThat(listFolder(files, "")).containsOnly("default", "UUID", "_global");
        assertProjectFolder("/default", files);
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
                if (!zipEntry.getName().startsWith("/")) {
                    continue;
                }
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
        return files.keySet().stream().filter(name -> name.startsWith(folder + "/"))
                .map(name -> name.substring((folder + "/").length()).split("/")[0]).collect(Collectors.toSet());
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
        Assert.assertNotNull(tool.getBackupPath());
    }

    private boolean assertProjectFolder(File projectFolder, File archiveFolder) {
        if (projectFolder.getName().endsWith(".DS_Store") || archiveFolder.getName().endsWith(".DS_Store")) {
            return true;
        }
        Assertions.assertThat(projectFolder.list()).containsAnyOf("dataflow", "dataflow_details", "cube_plan",
                "model_desc", "table");
        Assertions.assertThat(projectFolder.listFiles()).filteredOn(f -> !f.getName().startsWith("."))
                .allMatch(f -> f.listFiles().length > 0);

        val projectName = projectFolder.toPath().getFileName().toString();
        val globalFolder = findFile(archiveFolder.listFiles(), f -> f.getName().equals("_global"));
        val projects = findFile(globalFolder.listFiles(), f -> f.getName().equals("project"));
        Assertions.assertThat(findFile(projects.listFiles(), f -> f.getName().startsWith(projectName))).exists()
                .isFile();
        return true;
    }

    private boolean assertProjectFolder(String projectFolder, Map<String, RawResource> files) {
        Assertions.assertThat(listFolder(files, projectFolder)).containsAnyOf("dataflow", "dataflow_details",
                "cube_plan", "model_desc", "table");
        Assert.assertTrue(files.containsKey("/_global/project" + projectFolder + ".json"));
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

        Assertions.assertThat(archiveFolder.list()).isNotEmpty().containsOnlyOnce("UUID").containsAnyOf("default",
                "ssb", "tdvt");
        Assertions.assertThat(archiveFolder.listFiles())
                .filteredOn(f -> !f.getName().equals("UUID") && !f.getName().startsWith("_"))
                .allMatch(projectFolder -> assertProjectFolder(projectFolder, archiveFolder));

    }

    @Test
    public void testRestoreOverwriteAll() throws IOException {
        val junitFolder = temporaryFolder.getRoot();
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), junitFolder, "/");

        //there is a project that destResourceStore contains and srcResourceStore doesn't contain
        FileUtils.forceDelete(Paths.get(junitFolder.getAbsolutePath(), "/demo").toFile());
        FileUtils.deleteQuietly(Paths.get(junitFolder.getAbsolutePath(), "_global", "project", "demo.json").toFile());

        //there is a project that destResourceStore doesn't contain and srcResourceStore contains
        val destResourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val destResources = destResourceStore.getMetadataStore().list("/ssb");
        for (String res : destResources) {
            destResourceStore.deleteResource("/ssb" + res);
        }
        destResourceStore.deleteResource("/_global/project/ssb.json");

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
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), junitFolder, "/");

        val destResourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        String deletePath = "/broken_test/model_desc/3f8941de-d01c-42b8-91b5-44646390864b.json";
        String modifyPath = "/broken_test/model_desc/039eef32-9691-4c88-93ba-d65c58a1ab7a.json";
        String addPath = "/broken_test/model_desc/add.json";

        val modelDesc = JsonUtil.readValue(destResourceStore.getResource(modifyPath).getByteSource().read(),
                NDataModel.class);
        String originDescription = modelDesc.getDescription();
        modelDesc.setDescription("after modify");

        destResourceStore.deleteResource(deletePath);
        destResourceStore.deleteResource(modifyPath);
        destResourceStore.putResourceWithoutCheck(modifyPath, ByteSource.wrap(JsonUtil.writeValueAsBytes(modelDesc)), 0,
                0);
        destResourceStore.putResourceWithoutCheck(addPath,
                ByteSource.wrap(("test1").getBytes(Charset.defaultCharset())), 0, 0);

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
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), junitFolder, "/");

        val destResourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        String deletePath = "/broken_test/model_desc/f1bb4bbd-a638-442b-a276-e301fde0d7f6.json";
        String modifyPath = "/broken_test/model_desc/039eef32-9691-4c88-93ba-d65c58a1ab7a.json";
        String addPath = "/broken_test/model_desc/add.json";

        val modelDesc = JsonUtil.readValue(destResourceStore.getResource(modifyPath).getByteSource().read(),
                NDataModel.class);
        String originDescription = modelDesc.getDescription();
        modelDesc.setDescription("after modify");

        destResourceStore.deleteResource(deletePath);
        destResourceStore.deleteResource(modifyPath);
        destResourceStore.putResourceWithoutCheck(modifyPath, ByteSource.wrap(JsonUtil.writeValueAsBytes(modelDesc)), 0,
                0);
        destResourceStore.putResourceWithoutCheck(addPath,
                ByteSource.wrap(("test2").getBytes(Charset.defaultCharset())), 0, 0);

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
        val destResources = destResourceStore.getMetadataStore().list("/demo");
        for (String res : destResources) {
            destResourceStore.deleteResource("/demo" + res);
        }
        destResourceStore.deleteResource("/_global/project/demo.json");

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
        val destResources = destResourceStore.getMetadataStore().list("/ssb");
        for (String res : destResources) {
            destResourceStore.deleteResource("/ssb" + res);
        }
        destResourceStore.deleteResource("/_global/project/ssb.json");

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
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), restoreFolder, "/");

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
    public void testRestoreOverwriteAllCompressWithSrcOrDestIsEmpty() throws Exception {
        val emptyFolder = temporaryFolder.newFolder();
        createEmptyCompressedFile(emptyFolder);
        val restoreFolder = temporaryFolder.newFolder();
        createAllCompressedFile(restoreFolder);

        MetadataToolTestFixture.fixtureRestoreTest();

        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("default")).isNotNull();
        val tool = tool(emptyFolder.getAbsolutePath());
        tool.execute(new String[] { "-restore", "-compress", "-dir", "ignored", "--after-truncate" });
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).listAllProjects()).isEmpty();

        Mockito.when(tool.getMetadataUrl(Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn("kylin_metadata@hdfs,zip=1,path=file://" + restoreFolder.getAbsolutePath());

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
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), junitFolder, "/");

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
        try (val in = fs.open(new Path(rootPath, "_image"))) {
            val image = JsonUtil.readValue(IOUtils.toByteArray(in), ImageDesc.class);
            Assert.assertEquals(resourceStore.listResourcesRecursively("/default").size() + 2,
                    image.getOffset().longValue());
        }
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
        try (val in = fs.open(new Path(rootPath, "_image"))) {
            val image = JsonUtil.readValue(IOUtils.toByteArray(in), ImageDesc.class);
            Assert.assertEquals(resourceStore.listResourcesRecursively("/default").size() + 2,
                    image.getOffset().longValue());
        }
    }

    @Test
    public void testRestoreOverwriteProjectWithSrcOrDestIsEmpty() throws Exception {
        val junitFolder = temporaryFolder.getRoot();
        ResourceTool.copy(getTestConfig(), KylinConfig.createInstanceFromUri(junitFolder.getAbsolutePath()), "/");
        val tool = new MetadataTool(getTestConfig());

        //there is a project metadata that destResourceStore contains and srcResourceStore doesn't contain
        FileUtils.forceDelete(Paths.get(junitFolder.getAbsolutePath(), "demo").toFile());
        FileUtils.deleteQuietly(Paths.get(junitFolder.getAbsolutePath(), "_global", "project", "demo.json").toFile());

        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNotNull();
        tool.execute(new String[] { "-restore", "-project", "demo", "-dir", junitFolder.getAbsolutePath(),
                "--after-truncate" });
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNull();

        //there is a project metadata that destResourceStore doesn't contain and srcResourceStore contains
        val destResourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val destResources = destResourceStore.getMetadataStore().list("/ssb");
        for (String res : destResources) {
            destResourceStore.deleteResource("/ssb" + res);
        }
        destResourceStore.deleteResource("/_global/project/ssb.json");

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

        Assert.assertTrue(resourceStore.exists("/_global/project/default1.json"));
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
        val destResources = destResourceStore.getMetadataStore().list("/ssb");
        for (String res : destResources) {
            destResourceStore.deleteResource("/ssb" + res);
        }
        destResourceStore.deleteResource("/_global/project/ssb.json");

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
                .hasFieldOrPropertyWithValue("mvcc", 2L);

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
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
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
        var projectFolder = findFile(archiveFolder.listFiles(), f -> f.getName().equals("newten"));
        Assertions.assertThat(projectFolder.list()).contains("table_exd");
        tool.execute(
                new String[] { "-backup", "-project", "newten", "-dir", test2.getAbsolutePath(), "-excludeTableExd" });
        archiveFolder = test2.listFiles()[0];
        projectFolder = findFile(archiveFolder.listFiles(), f -> f.getName().equals("newten"));
        Assertions.assertThat(projectFolder.list()).doesNotContain("table_exd");
    }

    @Test
    public void testGetMetadataUrl() {
        val tool = new MetadataTool();

        var hdfsPath = "hdfs://host/path/to/hdfs/dir";
        var hdfsMetadataUrl = tool.getMetadataUrl(hdfsPath, true);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/hdfs/dir/,zip=1", hdfsMetadataUrl);
        hdfsMetadataUrl = tool.getMetadataUrl(hdfsPath, false);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/hdfs/dir/", hdfsMetadataUrl);

        var maprfsPath = "maprfs://host/path/to/maprfs/dir";
        var maprfsMetadataUrl = tool.getMetadataUrl(maprfsPath, true);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/maprfs/dir/,zip=1", maprfsMetadataUrl);
        maprfsMetadataUrl = tool.getMetadataUrl(maprfsPath, false);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/maprfs/dir/", maprfsMetadataUrl);

        var s3Path = "s3://host/path/to/s3/dir";
        var s3MetadataUrl = tool.getMetadataUrl(s3Path, true);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/s3/dir/,zip=1", s3MetadataUrl);
        s3MetadataUrl = tool.getMetadataUrl(s3Path, false);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/s3/dir/", s3MetadataUrl);

        var s3aPath = "s3a://host/path/to/s3a/dir";
        var s3aMetadataUrl = tool.getMetadataUrl(s3aPath, true);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/s3a/dir/,zip=1", s3aMetadataUrl);
        s3aMetadataUrl = tool.getMetadataUrl(s3aPath, false);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/s3a/dir/", s3aMetadataUrl);

        var wasbPath = "wasb://host/path/to/wasb/dir";
        var wasbMetadataUrl = tool.getMetadataUrl(wasbPath, true);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/wasb/dir/,zip=1", wasbMetadataUrl);
        wasbMetadataUrl = tool.getMetadataUrl(wasbPath, false);
        Assert.assertEquals("kylin_metadata@hdfs,path=/path/to/wasb/dir/", wasbMetadataUrl);

        var filePath = "file:///path/to/file/dir";
        var fileMetadataUrl = tool.getMetadataUrl(filePath, true);
        Assert.assertEquals("/path/to/file/dir/", fileMetadataUrl);
        fileMetadataUrl = tool.getMetadataUrl(filePath, false);
        Assert.assertEquals("/path/to/file/dir/", fileMetadataUrl);

        var simplePath = "/just/a/path";
        var simpleMetadataUrl = tool.getMetadataUrl(simplePath, true);
        Assert.assertEquals("/just/a/path/", simpleMetadataUrl);
        simpleMetadataUrl = tool.getMetadataUrl(simplePath, false);
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
