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

import static org.apache.kylin.common.KylinConfigBase.WRITING_CLUSTER_WORKING_DIR;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.query.util.QueryHisStoreUtil;
import org.apache.kylin.tool.garbage.StorageCleaner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StorageCleanerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() throws IOException {
        createTestMetadata();

        JobContextUtil.cleanUp();
        JobContextUtil.getJobInfoDao(getTestConfig());

        prepare();
    }

    @After
    public void teardown() {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    @Ignore
    public void testCleanupGarbage() throws Exception {
        val cleaner = new StorageCleaner();

        val baseDir = new File(getTestConfig().getMetadataUrl().getIdentifier()).getParentFile();
        val files = FileUtils.listFiles(new File(baseDir, "working-dir"), null, true);
        val garbageFiles = files.stream().filter(f -> f.getAbsolutePath().contains("invalid"))
                .map(f -> FilenameUtils.normalize(f.getParentFile().getAbsolutePath())).collect(Collectors.toSet());

        cleaner.execute();

        val outdatedItems = normalizeGarbages(cleaner.getOutdatedItems());
        Assert.assertEquals(garbageFiles.size(), outdatedItems.size());
        for (String item : outdatedItems) {
            Assert.assertTrue(item + " not in garbageFiles", garbageFiles.contains(item));
        }
    }

    @Test
    public void testDeltaStorageCleaner() throws Exception {
        val cleaner = new StorageCleaner();
        val config = getTestConfig();
        FileUtils.copyDirectory(new File("src/test/resources/ut_storage/delta_storage_cleaner_test"),
                new File(config.getHdfsWorkingDirectory().replace("file://", "")));
        val baseDir = new File(getTestConfig().getMetadataUrl().getIdentifier()).getParentFile();
        val files = FileUtils.listFiles(new File(baseDir, "working-dir"), null, true);
        val garbageFiles = files.stream().filter(f -> f.getAbsolutePath().contains("invalid")
                        && f.getAbsolutePath().contains("storage_v3_test"))
                .map(f -> FilenameUtils.normalize(f.getParentFile().getAbsolutePath())).collect(Collectors.toSet());
        cleaner.execute();
        val outDateItem = cleaner.getOutdatedItems().stream()
                .filter(out -> out.getPath().contains("storage_v3_test"))
                .collect(Collectors.toSet());
        Assert.assertEquals(garbageFiles.size(), outDateItem.size());
        for (String outdatedItem : normalizeGarbages(outDateItem)) {
            Assert.assertTrue(outdatedItem + " not in garbageFiles", garbageFiles.contains(outdatedItem));
        }
    }

    @Test
    public void testEventLogClean() throws IOException {
        prepareForEventLogClean();
        String allSparderEventLogDir = (KapConfig.wrap(getTestConfig()).getSparkConf().get("spark.eventLog.dir"))
                .replace("file:", "");
        String currentSparderEventLogDir = (KapConfig.wrap(getTestConfig()).getSparkConf().get("spark.eventLog.dir")
                + "/" + AddressUtil.getLocalServerInfo() + "/eventlog_v2_application_1677899901295_4823#1690192675042")
                .replace("file:", "");
        String sparkEventLogDir = getTestConfig().getSparkConfigOverride().get("spark.eventLog.dir").replace("file:",
                "");

        try (MockedStatic<QueryHisStoreUtil> qhsuMocked = Mockito.mockStatic(QueryHisStoreUtil.class)) {
            qhsuMocked.when(QueryHisStoreUtil::getQueryHistoryMinQueryTime).thenReturn(null);

            int fileSize = new File(currentSparderEventLogDir).listFiles().length;
            Assert.assertEquals(3, fileSize);
            var cleaner = new StorageCleaner.EventLogCleaner();
            cleaner.cleanCurrentSparderEventLog();
            fileSize = new File(currentSparderEventLogDir).listFiles().length;
            Assert.assertEquals(2, fileSize);
            Assert.assertTrue(
                    new File(currentSparderEventLogDir + "/events_1_application_1677899901295_8490_1690953331329")
                            .exists());

            int sparkEventLogFileSize = new File(sparkEventLogDir).listFiles().length;
            Assert.assertEquals(5, sparkEventLogFileSize);

            cleaner.cleanSparkEventLogs("default");
            sparkEventLogFileSize = new File(sparkEventLogDir).listFiles().length;
            Assert.assertEquals(4, sparkEventLogFileSize);
            Assert.assertFalse(new File(sparkEventLogDir + "/application_1677899901295_8243").exists());

            File otherSparderEventLogFile = new File(
                    allSparderEventLogDir + "/localhost_7071/eventlog_v2_application_1677899901295_4824#1690192771380");
            Assert.assertTrue(otherSparderEventLogFile.exists());
            cleaner.cleanAllEventLog();
            otherSparderEventLogFile = new File(
                    allSparderEventLogDir + "/localhost_7071/eventlog_v2_application_1677899901295_4824#1690192771380");
            Assert.assertFalse(otherSparderEventLogFile.exists());
        }
    }

    @Test
    public void testCleanupAfterTruncate() throws Exception {
        val cleaner = new StorageCleaner();

        val projects = NProjectManager.getInstance(getTestConfig()).listAllProjects();
        for (ProjectInstance project : projects) {
            NProjectManager.getInstance(getTestConfig()).forceDropProject(project.getName());
        }

        cleaner.execute();
        val files = FileUtils.listFiles(new File(getTestConfig().getHdfsWorkingDirectory().replace("file://", "")),
                null, true);
        val existFiles = files.stream().filter(f -> !f.getName().startsWith("."))
                .map(f -> FilenameUtils.normalize(f.getParentFile().getAbsolutePath())).collect(Collectors.toSet());
        Assert.assertEquals(0, existFiles.size());
    }

    @Test
    public void testCleanupAfterRemoveTable() throws Exception {
        val cleaner = new StorageCleaner();

        NTableMetadataManager.getInstance(getTestConfig(), "default").removeSourceTable("DEFAULT.TEST_KYLIN_FACT");
        for (NDataflow dataflow : NDataflowManager.getInstance(getTestConfig(), "default").listAllDataflows()) {
            NDataflowManager.getInstance(getTestConfig(), "default").dropDataflow(dataflow.getId());
        }

        cleaner.execute();
        val files = FileUtils.listFiles(new File(getTestConfig().getHdfsWorkingDirectory().replace("file://", "")
                + "/default" + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT), null, true);
        Assert.assertEquals(0, files.size());

    }

    @Test
    public void testCleanup_WithRunningJobs() throws Exception {
        val jobMgr = ExecutableManager.getInstance(getTestConfig(), "default");
        val job1 = new DefaultExecutable();
        job1.setProject("default");
        job1.setJobType(JobTypeEnum.INC_BUILD);
        val task1 = new ShellExecutable();
        job1.addTask(task1);
        jobMgr.addJob(job1);
        Map<String, String> extra = Maps.newHashMap();
        val dependFiles = new String[] { "/default/dict/global_dict/invalid", "/default/parquet/invalid",
                "/default/parquet/abe3bf1a-c4bc-458d-8278-7ea8b00f5e96/invalid",
                "/default/table_snapshot/DEFAULT.TEST_COUNTRY",
                "/default/dict/global_dict/DEFAULT.TEST_KYLIN_FACT/invalid/keep" };

        extra.put(AbstractExecutable.DEPENDENT_FILES, StringUtils.join(dependFiles, ","));
        jobMgr.updateJobOutput(job1.getId(), ExecutableState.PENDING, extra, null, null);
        jobMgr.updateJobOutput(job1.getId(), ExecutableState.RUNNING, extra, null, null);

        val cleaner = new StorageCleaner();
        cleaner.execute();
        val garbagePaths = normalizeGarbages(cleaner.getOutdatedItems());
        for (String file : dependFiles) {
            Assert.assertFalse(garbagePaths.stream().anyMatch(p -> p.startsWith(file)));
            Assert.assertFalse(garbagePaths.stream().anyMatch(file::startsWith));
        }
    }

    @Test
    public void testCleanup_withProjects() throws Exception {
        val cleaner = new StorageCleaner(true, Collections.singletonList("default"));

        NTableMetadataManager.getInstance(getTestConfig(), "default").removeSourceTable("DEFAULT.TEST_KYLIN_FACT");
        for (NDataflow dataflow : NDataflowManager.getInstance(getTestConfig(), "default").listAllDataflows()) {
            NDataflowManager.getInstance(getTestConfig(), "default").dropDataflow(dataflow.getId());
        }

        cleaner.execute();
        Collection<File> files = FileUtils
                .listFiles(new File(getTestConfig().getHdfsWorkingDirectory().replace("file://", "") + "/default"
                        + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT), null, true);
        Assert.assertEquals(0, files.size());

        files = FileUtils.listFiles(new File(getTestConfig().getHdfsWorkingDirectory().replace("file://", "")
                + "/table_index" + HadoopUtil.GLOBAL_DICT_STORAGE_ROOT), null, true);

        Assert.assertNotEquals(0, files);
    }

    @Test
    public void testCleanupPreserveTableDescLastSnapshot() throws Exception {
        val cleaner = new StorageCleaner(true, Collections.singletonList("default"));

        for (NDataflow dataflow : NDataflowManager.getInstance(getTestConfig(), "default").listAllDataflows()) {
            NDataflowManager.getInstance(getTestConfig(), "default").dropDataflow(dataflow.getId());
        }

        val countryTableSnapshotPath = "default/table_snapshot/DEFAULT.TEST_COUNTRY/b4849638-4eb5-44f5-b776-0619813fb676";
        val countryTableDesc = NTableMetadataManager.getInstance(getTestConfig(), "default")
                .getTableDesc("DEFAULT.TEST_COUNTRY");
        countryTableDesc.setLastSnapshotPath(countryTableSnapshotPath);
        NTableMetadataManager.getInstance(getTestConfig(), "default").updateTableDesc(countryTableDesc);

        cleaner.execute();

        // test if table snapshot is preserved
        val snapshots = FileUtils.listFiles(new File(getTestConfig().getHdfsWorkingDirectory().replace("file://", "")
                + "/default" + HadoopUtil.SNAPSHOT_STORAGE_ROOT), null, true);
        Assert.assertEquals(1, snapshots.size());
        Assert.assertTrue(snapshots.iterator().next().getAbsolutePath().contains(countryTableSnapshotPath));
    }

    @Test
    public void testStorageCleanerWithRateLimiter() throws Exception {
        boolean cleanup = true;
        Collection<String> projects = Collections.emptyList();
        double requestFSRate = 10.0;
        int tRetryTimes = 10;

        val cleaner = new StorageCleaner(cleanup, projects, requestFSRate, tRetryTimes);
        val fsd = StorageCleaner.FileSystemDecorator.getInstance(HadoopUtil.getWorkingFileSystem());
        val filePath = new Path(getTestConfig().getHdfsWorkingDirectory());

        int totalRequestTimes = 30;
        long start = System.currentTimeMillis();
        for (int i = 0; i < totalRequestTimes; i++) {
            fsd.listStatus(filePath);
        }
        long duration = System.currentTimeMillis() - start;
        double expectTime = 1000 * (totalRequestTimes - 2 * requestFSRate) / requestFSRate;

        Assert.assertTrue(duration > expectTime);
    }

    @Test
    public void testStorageItem() {
        val storageItem1 = new StorageCleaner.StorageItem(
                StorageCleaner.FileSystemDecorator.getInstance(HadoopUtil.getWorkingFileSystem()),
                getTestConfig().getHdfsWorkingDirectory());
        val storageItem2 = new StorageCleaner.StorageItem(
                StorageCleaner.FileSystemDecorator.getInstance(HadoopUtil.getWorkingFileSystem()),
                getTestConfig().getHdfsWorkingDirectory());

        {
            boolean equals = storageItem1.equals(storageItem2);
            Assert.assertTrue(equals);
        }
        {
            boolean equals = storageItem1.equals(storageItem1);
            Assert.assertTrue(equals);
        }
        {
            boolean equals = storageItem1.equals(null);
            Assert.assertFalse(equals);
        }
        {
            boolean equals = storageItem1.equals("UT");
            Assert.assertFalse(equals);
        }
        Assert.assertEquals(storageItem1.hashCode(), storageItem2.hashCode());
    }

    @Test
    public void testCleanupAllFileSystemsWithWritingCluster() throws Exception {
        val cleaner = new StorageCleaner();
        KylinConfig testConfig = getTestConfig();
        val baseDir = new File(getTestConfig().getMetadataUrl().getIdentifier()).getParentFile();
        val writingClusterWorkingDir = new File(baseDir, "working-dir/working-dir1/project");

        testConfig.setProperty("kylin.engine.submit-hadoop-conf-dir", "/write_hadoop_conf");
        testConfig.setProperty(WRITING_CLUSTER_WORKING_DIR, writingClusterWorkingDir.toURI().toString());

        cleaner.execute();

        val outdatedItems = normalizeGarbages(cleaner.getOutdatedItems());
        Assert.assertTrue(outdatedItems.stream().anyMatch(file -> file.contains("working-dir/working-dir1/project")));
    }

    private void prepare() throws IOException {
        val config = getTestConfig();
        config.setProperty("kylin.garbage.storage.cuboid-layout-survival-time-threshold", "0s");
        String metaId = config.getMetadataUrlPrefix().replace(':', '-').replace('/', '-');
        val workingDir1 = new Path(config.getHdfsWorkingDirectory()).getParent().toString() + "/working-dir1";
        FileUtils.copyDirectory(new File("src/test/resources/ut_storage/working-dir"),
                new File(config.getHdfsWorkingDirectory().replace("file://", "")));
        FileUtils.copyDirectory(new File("src/test/resources/ut_storage/working-dir1"),
                new File(workingDir1.replace("file:", "") + "/" + metaId));
        FileUtils.copyDirectory(new File("src/test/resources/ut_storage/working-dir1"),
                new File(workingDir1.replace("file:", "") + "/project/" + metaId));

        val indexMgr = NIndexPlanManager.getInstance(config, "default");
        val inner = indexMgr.getIndexPlanByModelAlias("nmodel_basic_inner");
        indexMgr.updateIndexPlan(inner.getId(), copyForWrite -> {
            val map = Maps.<String, String> newLinkedHashMap();
            map.put("kylin.env.hdfs-working-dir", workingDir1);
            copyForWrite.setOverrideProps(map);
        });

        val dfMgr = NDataflowManager.getInstance(config, "default");
        val df = dfMgr.getDataflowByModelAlias("nmodel_basic_inner");
        val execMgr = ExecutableManager.getInstance(config, "default");
        val job1 = new DefaultExecutable();
        job1.setId("job1");
        job1.setJobType(JobTypeEnum.INC_BUILD);
        execMgr.addJob(job1);
        val job2 = new DefaultExecutable();
        job2.setJobType(JobTypeEnum.INC_BUILD);
        job2.setId("job2");
        execMgr.addJob(job2);
    }

    private void prepareForEventLogClean() throws IOException {
        KylinConfig config = getTestConfig();
        String currentSparderEventLogDir = (KapConfig.wrap(config).getSparkConf().get("spark.eventLog.dir") + "/"
                + AddressUtil.getLocalServerInfo()).replace("file:", "");
        Files.createDirectories(Paths.get(currentSparderEventLogDir));

        String allSparderEventLogDir = (KapConfig.wrap(config).getSparkConf().get("spark.eventLog.dir"))
                .replace("file:", "");
        String sparkEventLogDir = (config.getSparkConfigOverride().get("spark.eventLog.dir")).replace("file:", "");

        FileUtils.copyDirectory(new File("src/test/resources/ut_storage/working-dir2/sparder-history/localhost_7070"),
                new File(currentSparderEventLogDir));
        FileUtils.copyDirectoryToDirectory(
                new File("src/test/resources/ut_storage/working-dir2/sparder-history/localhost_7071"),
                new File(allSparderEventLogDir));
        FileUtils.copyFileToDirectory(
                new File("src/test/resources/ut_storage/working-dir2/spark-history/application_1677899901295_0989"),
                new File(sparkEventLogDir));
        FileUtils.copyFileToDirectory(
                new File("src/test/resources/ut_storage/working-dir2/spark-history/application_1677899901295_8243"),
                new File(sparkEventLogDir));

        long currentTime = System.currentTimeMillis();
        long twoMonth = 2 * 30 * 24 * 60 * 60 * 1000L;
        long oneDay = 24 * 60 * 60 * 1000L;
        long expired = currentTime - twoMonth;
        long notExpired = currentTime - oneDay;

        // Not expired
        updateLastModified(currentSparderEventLogDir + "/eventlog_v2_application_1677899901295_4823#1690192675042",
                notExpired);

        // Expired
        updateLastModified(currentSparderEventLogDir + "/eventlog_v2_application_1677899901295_4823#1690192675042/"
                + "appstatus_application_1677899901295_8490", expired);

        // Expired
        updateLastModified(currentSparderEventLogDir + "/eventlog_v2_application_1677899901295_4823#1690192675042/"
                + "events_1_application_1677899901295_8490_1690953331329", expired);

        // Not expired
        updateLastModified(currentSparderEventLogDir + "/eventlog_v2_application_1677899901295_4823#1690192675042/"
                + "events_2_application_1677899901295_8490_1690953331329", notExpired);

        // Expired
        updateLastModified(
                allSparderEventLogDir + "/localhost_7071/eventlog_v2_application_1677899901295_4824#1690192771380",
                expired);

        // Expired: Less than the creation time of the earliest executable 
        updateLastModified(sparkEventLogDir + "/application_1677899901295_8243", 974600451000L);

        // Not expired 
        updateLastModified(sparkEventLogDir + "/application_1677899901295_0989", notExpired);
        updateLastModified(sparkEventLogDir + "/application_1554187389076_9294", notExpired);
        updateLastModified(sparkEventLogDir + "/application_1554187389076_9295", notExpired);
        updateLastModified(sparkEventLogDir + "/application_1554187389076_9296", notExpired);
    }

    public void updateLastModified(String file, long timeStamp) throws IOException {
        java.nio.file.Path path = Paths.get(file);
        FileTime newLastModifiedTime = FileTime.fromMillis(timeStamp);
        Files.setLastModifiedTime(path, newLastModifiedTime);
    }

    private Set<String> normalizeGarbages(Set<StorageCleaner.StorageItem> items) {
        return items.stream().map(i -> i.getPath().replaceAll("file:", "").replaceAll("/keep", ""))
                .collect(Collectors.toSet());
    }

    @Test
    public void testCollectDropTemporaryTransactionTable() throws Exception {
        KylinConfig config = getTestConfig();
        String dir = config.getJobTmpTransactionalTableDir("default", "invalid");
        Path path = new Path(dir);
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
        if (!fileSystem.exists(path)) {
            fileSystem.mkdirs(path);
            fileSystem.setPermission(path, new FsPermission((short) 00777));
            path = new Path(dir + "/SSB.CUSTOMER_HIVE_TX_INTERMEDIATE5c5851ef8544");
            fileSystem.createNewFile(path);
        }
        new StorageCleaner(true, Collections.singletonList("default")).execute();
        new StorageCleaner(true, Collections.singletonList("default")).execute();
    }
}
