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

package org.apache.kylin.helper;

import static org.apache.kylin.common.exception.code.ErrorCodeTool.FILE_ALREADY_EXISTS;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.persistence.ImageDesc;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.AuditLogStore;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.MetadataChecker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.tool.HDFSMetadataTool;
import org.apache.kylin.tool.garbage.StorageCleaner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import lombok.val;
import lombok.var;

/*
* this class is only for removing dependency of kylin-tool module, and should be refactor later
*/
public class MetadataToolHelper {

    public static final DateTimeFormatter DATE_TIME_FORMATTER = HelperConstants.DATE_TIME_FORMATTER;
    private static final String GLOBAL = "global";
    private static final String HDFS_METADATA_URL_FORMATTER = "kylin_metadata@hdfs,path=%s";

    private static final Logger logger = LoggerFactory.getLogger(MetadataToolHelper.class);

    public void rotateAuditLog() {
        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        val auditLogStore = resourceStore.getAuditLogStore();
        auditLogStore.rotate();
    }

    public void backup(KylinConfig kylinConfig) throws Exception {
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        new MetadataToolHelper().backup(kylinConfig, null, HadoopUtil.getBackupFolder(kylinConfig), null, true, false);
    }

    public void backup(KylinConfig kylinConfig, String dir, String folder) throws Exception {
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        new MetadataToolHelper().backup(kylinConfig, null, dir, folder, true, false);
    }

    public Pair<String, String> backup(KylinConfig kylinConfig, String project, String path, String folder,
            boolean compress, boolean excludeTableExd) throws Exception {
        boolean isGlobal = null == project;
        long startAt = System.currentTimeMillis();
        try {
            return doBackup(kylinConfig, project, path, folder, compress, excludeTableExd);
        } catch (Exception be) {
            if (isGlobal) {
                MetricsGroup.hostTagCounterInc(MetricsName.METADATA_BACKUP_FAILED, MetricsCategory.GLOBAL, GLOBAL);
            } else {
                MetricsGroup.hostTagCounterInc(MetricsName.METADATA_BACKUP_FAILED, MetricsCategory.PROJECT, project);
            }
            throw be;
        } finally {
            if (isGlobal) {
                MetricsGroup.hostTagCounterInc(MetricsName.METADATA_BACKUP, MetricsCategory.GLOBAL, GLOBAL);
                MetricsGroup.hostTagCounterInc(MetricsName.METADATA_BACKUP_DURATION, MetricsCategory.GLOBAL, GLOBAL,
                        System.currentTimeMillis() - startAt);
            } else {
                MetricsGroup.hostTagCounterInc(MetricsName.METADATA_BACKUP, MetricsCategory.PROJECT, project);
                MetricsGroup.hostTagCounterInc(MetricsName.METADATA_BACKUP_DURATION, MetricsCategory.PROJECT, project,
                        System.currentTimeMillis() - startAt);
            }
        }
    }

    Pair<String, String> doBackup(KylinConfig kylinConfig, String project, String path, String folder, boolean compress,
            boolean excludeTableExd) throws Exception {
        ResourceStore resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        boolean isUTEnv = kylinConfig.isUTEnv();

        if (StringUtils.isBlank(path)) {
            path = KylinConfigBase.getKylinHome() + File.separator + "meta_backups";
        }
        if (StringUtils.isEmpty(folder)) {
            folder = LocalDateTime.now(Clock.systemDefaultZone()).format(MetadataToolHelper.DATE_TIME_FORMATTER)
                    + "_backup";
        }
        String backupPath = StringUtils.appendIfMissing(path, "/") + folder;
        //FIXME should replace printf with Logger while Logger MUST print this message to console, because test depends on it
        System.out.printf(Locale.ROOT, "The metadata backup path is %s.%n", backupPath);
        val backupMetadataUrl = getMetadataUrl(backupPath, compress, kylinConfig);
        val backupConfig = KylinConfig.createKylinConfig(kylinConfig);
        backupConfig.setMetadataUrl(backupMetadataUrl);
        abortIfAlreadyExists(backupPath);
        logger.info("The backup metadataUrl is {} and backup path is {}", backupMetadataUrl, backupPath);
        try (val backupResourceStore = ResourceStore.getKylinMetaStore(backupConfig)) {
            val backupMetadataStore = backupResourceStore.getMetadataStore();
            if (StringUtils.isBlank(project)) {
                logger.info("start to copy all projects from ResourceStore.");
                long finalOffset = getOffset(isUTEnv, resourceStore);
                backupResourceStore.putResourceWithoutCheck(ResourceStore.METASTORE_IMAGE,
                        ByteSource.wrap(JsonUtil.writeValueAsBytes(new ImageDesc(finalOffset))),
                        System.currentTimeMillis(), -1);
                var projectFolders = resourceStore.listResources("/");
                if (projectFolders == null) {
                    return Pair.newPair(backupPath, folder);
                }
                UnitOfWork.doInTransactionWithRetry(() -> {
                    backupProjects(projectFolders, resourceStore, backupResourceStore, excludeTableExd);
                    return null;
                }, UnitOfWork.GLOBAL_UNIT);

                val uuid = resourceStore.getResource(ResourceStore.METASTORE_UUID_TAG);
                if (uuid != null) {
                    backupResourceStore.putResourceWithoutCheck(uuid.getResPath(), uuid.getByteSource(),
                            uuid.getTimestamp(), -1);
                }
                logger.info("start to backup all projects");

            } else {
                logger.info("start to copy project {} from ResourceStore.", project);
                UnitOfWork.doInTransactionWithRetry(
                        UnitOfWorkParams.builder().readonly(true).unitName(project).processor(() -> {
                            copyResourceStore("/" + project, resourceStore, backupResourceStore, true, excludeTableExd);
                            val uuid = resourceStore.getResource(ResourceStore.METASTORE_UUID_TAG);
                            backupResourceStore.putResourceWithoutCheck(uuid.getResPath(), uuid.getByteSource(),
                                    uuid.getTimestamp(), -1);
                            return null;
                        }).build());
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("metadata task is interrupt");
                }
                logger.info("start to backup project {}", project);
            }
            backupResourceStore.deleteResource(ResourceStore.METASTORE_TRASH_RECORD);
            backupMetadataStore.dump(backupResourceStore);
            logger.info("backup successfully at {}", backupPath);
        }
        return Pair.newPair(backupPath, folder);
    }

    public String getMetadataUrl(String rootPath, boolean compressed, KylinConfig kylinConfig) {
        if (HadoopUtil.isHdfsCompatibleSchema(rootPath, kylinConfig)) {
            val url = String.format(Locale.ROOT, HDFS_METADATA_URL_FORMATTER,
                    Path.getPathWithoutSchemeAndAuthority(new Path(rootPath)).toString() + "/");
            return compressed ? url + ",zip=1" : url;
        } else if (rootPath.startsWith("file://")) {
            rootPath = rootPath.replace("file://", "");
            return StringUtils.appendIfMissing(rootPath, "/");

        } else {
            return StringUtils.appendIfMissing(rootPath, "/");
        }
    }

    private void backupProjects(NavigableSet<String> projectFolders, ResourceStore resourceStore,
            ResourceStore backupResourceStore, boolean excludeTableExd) throws InterruptedException {
        for (String projectPath : projectFolders) {
            if (projectPath.equals(ResourceStore.METASTORE_UUID_TAG)
                    || projectPath.equals(ResourceStore.METASTORE_IMAGE)) {
                continue;
            }
            // The "_global" directory is already included in the full backup
            copyResourceStore(projectPath, resourceStore, backupResourceStore, false, excludeTableExd);
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("metadata task is interrupt");
            }
        }
    }

    private void copyResourceStore(String projectPath, ResourceStore srcResourceStore,
            ResourceStore destResourceStore, boolean isProjectLevel, boolean excludeTableExd) {
        if (excludeTableExd) {
            String tableExdPath = projectPath + ResourceStore.TABLE_EXD_RESOURCE_ROOT;
            var projectItems = srcResourceStore.listResources(projectPath);
            for (String item : projectItems) {
                if (item.equals(tableExdPath)) {
                    continue;
                }
                srcResourceStore.copy(item, destResourceStore);
            }
        } else {
            srcResourceStore.copy(projectPath, destResourceStore);
        }
        if (isProjectLevel) {
            // The project-level backup needs to contain "/_global/project/*.json"
            val projectName = Paths.get(projectPath).getFileName().toString();
            srcResourceStore.copy(ProjectInstance.concatResourcePath(projectName), destResourceStore);
        }
    }

    private long getOffset(boolean isUTEnv, ResourceStore resourceStore) {
        AuditLogStore auditLogStore = resourceStore.getAuditLogStore();
        if (isUTEnv) {
            return auditLogStore.getMaxId();
        } else {
            return auditLogStore.getLogOffset() == 0 ? resourceStore.getOffset() : auditLogStore.getLogOffset();
        }
    }

    private void abortIfAlreadyExists(String path) throws IOException {
        URI uri = HadoopUtil.makeURI(path);
        if (!uri.isAbsolute()) {
            logger.info("no scheme specified for {}, try local file system file://", path);
            File localFile = new File(path);
            if (localFile.exists()) {
                logger.error("[UNEXPECTED_THINGS_HAPPENED] local file {} already exists ", path);
                throw new KylinException(FILE_ALREADY_EXISTS, path);
            }
            return;
        }
        val fs = HadoopUtil.getWorkingFileSystem();
        if (fs.exists(new Path(path))) {
            logger.error("[UNEXPECTED_THINGS_HAPPENED] specified file {} already exists ", path);
            throw new KylinException(FILE_ALREADY_EXISTS, path);
        }
    }

    public void restore(KylinConfig kylinConfig, String project, String path, boolean delete) throws Exception {
        logger.info("Restore metadata with delete : {}", delete);
        ResourceStore resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        val restoreMetadataUrl = getMetadataUrl(path, false, kylinConfig);
        val restoreConfig = KylinConfig.createKylinConfig(kylinConfig);
        restoreConfig.setMetadataUrl(restoreMetadataUrl);
        logger.info("The restore metadataUrl is {} and restore path is {} ", restoreMetadataUrl, path);

        val restoreResourceStore = ResourceStore.getKylinMetaStore(restoreConfig);
        val restoreMetadataStore = restoreResourceStore.getMetadataStore();
        MetadataChecker metadataChecker = new MetadataChecker(restoreMetadataStore);

        val verifyResult = metadataChecker.verify();
        Preconditions.checkState(verifyResult.isQualified(),
                verifyResult.getResultMessage() + "\n the metadata dir is not qualified");
        restore(resourceStore, restoreResourceStore, project, delete);
        backup(kylinConfig);

    }

    public void restore(ResourceStore currentResourceStore, ResourceStore restoreResourceStore, String project,
            boolean delete) {
        if (StringUtils.isBlank(project)) {
            logger.info("start to restore all projects");
            var srcProjectFolders = restoreResourceStore.listResources("/");
            var destProjectFolders = currentResourceStore.listResources("/");
            srcProjectFolders = srcProjectFolders == null ? Sets.newTreeSet() : srcProjectFolders;
            destProjectFolders = destProjectFolders == null ? Sets.newTreeSet() : destProjectFolders;
            val projectFolders = Sets.union(srcProjectFolders, destProjectFolders);

            for (String projectPath : projectFolders) {
                if (projectPath.equals(ResourceStore.METASTORE_UUID_TAG)
                        || projectPath.equals(ResourceStore.METASTORE_IMAGE)) {
                    continue;
                }
                val projectName = Paths.get(projectPath).getName(0).toString();
                val destResources = currentResourceStore.listResourcesRecursively(projectPath);
                val srcResources = restoreResourceStore.listResourcesRecursively(projectPath);
                UnitOfWork.doInTransactionWithRetry(() -> doRestore(currentResourceStore, restoreResourceStore,
                        destResources, srcResources, delete), projectName, 1);
            }

        } else {
            logger.info("start to restore project {}", project);
            val destGlobalProjectResources = currentResourceStore.listResourcesRecursively(ResourceStore.PROJECT_ROOT);

            Set<String> globalDestResources = null;
            if (Objects.nonNull(destGlobalProjectResources)) {
                globalDestResources = destGlobalProjectResources.stream().filter(x -> Paths.get(x).getFileName()
                        .toString().equals(String.format(Locale.ROOT, "%s.json", project))).collect(Collectors.toSet());
            }

            val globalSrcResources = restoreResourceStore
                    .listResourcesRecursively(ResourceStore.PROJECT_ROOT).stream().filter(x -> Paths.get(x)
                            .getFileName().toString().equals(String.format(Locale.ROOT, "%s.json", project)))
                    .collect(Collectors.toSet());

            Set<String> finalGlobalDestResources = globalDestResources;

            UnitOfWork.doInTransactionWithRetry(() -> doRestore(currentResourceStore, restoreResourceStore,
                    finalGlobalDestResources, globalSrcResources, delete), UnitOfWork.GLOBAL_UNIT, 1);

            val projectPath = FileSystems.getDefault().getSeparator() + project;
            val destResources = currentResourceStore.listResourcesRecursively(projectPath);
            val srcResources = restoreResourceStore.listResourcesRecursively(projectPath);

            UnitOfWork.doInTransactionWithRetry(
                    () -> doRestore(currentResourceStore, restoreResourceStore, destResources, srcResources, delete),
                    project, 1);
        }

        logger.info("restore successfully");
    }

    private int doRestore(ResourceStore currentResourceStore, ResourceStore restoreResourceStore,
            Set<String> destResources, Set<String> srcResources, boolean delete) throws IOException {
        val threadViewRS = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());

        //check destResources and srcResources are null,because  Sets.difference(srcResources, destResources) will report NullPointerException
        destResources = destResources == null ? Collections.emptySet() : destResources;
        srcResources = srcResources == null ? Collections.emptySet() : srcResources;

        logger.info("Start insert metadata resource...");
        val insertRes = Sets.difference(srcResources, destResources);
        for (val res : insertRes) {
            val metadataRaw = restoreResourceStore.getResource(res);
            threadViewRS.checkAndPutResource(res, metadataRaw.getByteSource(), -1L);
        }

        logger.info("Start update metadata resource...");
        val updateRes = Sets.intersection(destResources, srcResources);
        for (val res : updateRes) {
            val raw = currentResourceStore.getResource(res);
            val metadataRaw = restoreResourceStore.getResource(res);
            threadViewRS.checkAndPutResource(res, metadataRaw.getByteSource(), raw.getMvcc());
        }
        if (delete) {
            logger.info("Start delete metadata resource...");
            val deleteRes = Sets.difference(destResources, srcResources);
            for (val res : deleteRes) {
                threadViewRS.deleteResource(res);
            }
        }

        return 0;
    }

    public void cleanStorage(boolean storageCleanup, List<String> projects, double requestFSRate,
            int retryTimes) {
        try {
            StorageCleaner storageCleaner = new StorageCleaner(storageCleanup, projects, requestFSRate, retryTimes);
            System.out.println("Start to cleanup HDFS");
            storageCleaner.execute();
            System.out.println("cleanup HDFS finished");
        } catch (Exception e) {
            logger.error("cleanup HDFS failed", e);
            System.out.println(StorageCleaner.ANSI_RED
                    + "cleanup HDFS failed. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr"
                    + StorageCleaner.ANSI_RESET);
        }
    }

    public DataSource getDataSource(KylinConfig kylinConfig) throws Exception {
        val url = kylinConfig.getMetadataUrl();
        val props = JdbcUtil.datasourceParameters(url);
        return JdbcDataSource.getDataSource(props);
    }

    public void fetch(KylinConfig kylinConfig, String path, String folder, String target, boolean excludeTableExd) throws Exception {
        ResourceStore resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        if (StringUtils.isBlank(path)) {
            path = KylinConfigBase.getKylinHome() + File.separator + "meta_fetch";
        }
        if (StringUtils.isEmpty(folder)) {
            folder = LocalDateTime.now(Clock.systemDefaultZone()).format(DATE_TIME_FORMATTER) + "_fetch";
        }
        if (target == null) {
            System.out.println("target file must be set with fetch mode");
        } else {
            val fetchPath = StringUtils.appendIfMissing(path, "/") + folder;
            // currently do not support compress with fetch
            val fetchMetadataUrl = getMetadataUrl(fetchPath, false, kylinConfig);
            val fetchConfig = KylinConfig.createKylinConfig(kylinConfig);
            fetchConfig.setMetadataUrl(fetchMetadataUrl);
            abortIfAlreadyExists(fetchPath);
            logger.info("The fetch metadataUrl is {} and backup path is {}", fetchMetadataUrl, fetchPath);

            try (val fetchResourceStore = ResourceStore.getKylinMetaStore(fetchConfig)) {

                val fetchMetadataStore = fetchResourceStore.getMetadataStore();

                String targetPath = target.startsWith("/") ? target.substring(1) : target;

                logger.info("start to copy target file {} from ResourceStore.", target);
                UnitOfWork.doInTransactionWithRetry(
                        UnitOfWorkParams.builder().readonly(true).unitName(target).processor(() -> {
                            copyResourceStore("/" + targetPath, resourceStore, fetchResourceStore, true, excludeTableExd);
                            // uuid
                            val uuid = resourceStore.getResource(ResourceStore.METASTORE_UUID_TAG);
                            fetchResourceStore.putResourceWithoutCheck(uuid.getResPath(), uuid.getByteSource(),
                                    uuid.getTimestamp(), -1);
                            return null;
                        }).build());
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("metadata task is interrupt");
                }
                logger.info("start to fetch target file {}", target);

                // fetchResourceStore is read-only, currently we don't do any write operation on it.
                // fetchResourceStore.deleteResource(ResourceStore.METASTORE_TRASH_RECORD);
                fetchMetadataStore.dump(fetchResourceStore);
                logger.info("fetch successfully at {}", fetchPath);
            }
        }
    }

    public NavigableSet<String> list(KylinConfig kylinConfig, String target) throws Exception {
        ResourceStore resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        var res = resourceStore.listResources(target);
        if (res == null) {
            System.out.printf("%s is not exist%n", target);
        } else {
            System.out.println("" + res);
        }
        return res;
    }

}
