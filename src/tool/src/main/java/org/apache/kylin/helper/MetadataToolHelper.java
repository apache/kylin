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

import static org.apache.kylin.common.constant.Constants.CORE_META_DIR;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.FILE_ALREADY_EXISTS;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.MODEL_DUPLICATE_UUID_FAILED;
import static org.apache.kylin.common.persistence.ResourceStore.GLOBAL_PROJECT;
import static org.apache.kylin.common.persistence.ResourceStore.METASTORE_IMAGE;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.persistence.ImageDesc;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.AuditLogStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.MetadataChecker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.tool.CancelableTask;
import org.apache.kylin.tool.HDFSMetadataTool;
import org.apache.kylin.tool.constant.StringConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;
import lombok.var;

/*
* this class is only for removing dependency of kylin-tool module, and should be refactored later
*/
public class MetadataToolHelper extends CancelableTask {

    public static final DateTimeFormatter DATE_TIME_FORMATTER = StringConstant.DATE_TIME_FORMATTER;
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

    public void backupToDirectPath(KylinConfig kylinConfig, String backupPath) throws Exception {
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        new MetadataToolHelper().backup(kylinConfig, null, backupPath, true, false);
    }

    public void backup(KylinConfig kylinConfig, String dir, String folder, String project) throws Exception {
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        new MetadataToolHelper().backup(kylinConfig, project, dir, folder, true, false);
    }

    public void backupToDirectPath(KylinConfig kylinConfig, String backupPath, String project) throws Exception {
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        new MetadataToolHelper().backup(kylinConfig, project, backupPath, true, false);
    }

    public Pair<String, String> backup(KylinConfig kylinConfig, String project, String path, String folder,
            boolean compress, boolean excludeTableExd) throws Exception {
        Pair<String, String> pair = getBackupPath(path, folder);
        String coreMetadataBackupPath = StringUtils.appendIfMissing(pair.getFirst(), "/") + CORE_META_DIR;
        backup(kylinConfig, project, coreMetadataBackupPath, compress, excludeTableExd);
        return pair;
    }

    public void backup(KylinConfig kylinConfig, String project, String backupPath, boolean compress,
            boolean excludeTableExd) throws Exception {
        boolean isGlobal = null == project;
        long startAt = System.currentTimeMillis();
        try {
            doBackup(kylinConfig, project, backupPath, compress, excludeTableExd);
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

    private Pair<String, String> getBackupPath(String path, String folder) {
        if (StringUtils.isBlank(path)) {
            path = KylinConfigBase.getKylinHome() + File.separator + "meta_backups";
        }
        if (StringUtils.isEmpty(folder)) {
            folder = LocalDateTime.now(Clock.systemDefaultZone()).format(MetadataToolHelper.DATE_TIME_FORMATTER)
                    + "_backup";
        }
        String backupPath = StringUtils.appendIfMissing(path, "/") + folder;
        return Pair.newPair(backupPath, folder);
    }

    void doBackup(KylinConfig kylinConfig, String project, String backupPath, boolean compress, boolean excludeTableExd)
            throws Exception {
        ResourceStore resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        boolean isUTEnv = kylinConfig.isUTEnv();
        //FIXME should replace printf with Logger while Logger MUST print this message to console,
        // because test depends on it
        System.out.printf(Locale.ROOT, "The metadata backup path is %s.%n", backupPath);
        val backupMetadataUrl = getMetadataUrl(backupPath, compress, kylinConfig);
        val backupConfig = KylinConfig.createKylinConfig(kylinConfig);
        backupConfig.setMetadataUrl(backupMetadataUrl);
        abortIfAlreadyExists(backupPath);
        logger.info("The backup metadataUrl is {} and backup path is {}", backupMetadataUrl, backupPath);
        try (val backupResourceStore = ResourceStore.getKylinMetaStore(backupConfig)) {
            val backupMetadataStore = backupResourceStore.getMetadataStore();
            val projectMsg = StringUtils.isBlank(project) ? "all projects" : "project " + project;

            logger.info("start to copy {} from ResourceStore.", projectMsg);
            long finalOffset = getOffset(isUTEnv, resourceStore);
            backupResourceStore.putResourceWithoutCheck(ResourceStore.METASTORE_IMAGE,
                    ByteSource.wrap(JsonUtil.writeValueAsBytes(new ImageDesc(finalOffset))), System.currentTimeMillis(),
                    -1);
            NavigableSet<String> backupItems;
            if (StringUtils.isBlank(project)) {
                backupItems = resourceStore.listResourcesRecursively(MetadataType.ALL.name());
            } else {
                backupItems = resourceStore.listResourcesRecursivelyByProject(project);
            }
            if (backupItems == null || backupItems.isEmpty()) {
                return;
            }
            UnitOfWork.doInTransactionWithRetry(() -> {
                backupMetadata(backupItems, resourceStore, backupResourceStore, excludeTableExd);
                return null;
            }, UnitOfWork.GLOBAL_UNIT);

            if (!StringUtils.isBlank(project)) {
                val uuid = resourceStore.getResource(ResourceStore.METASTORE_UUID_TAG);
                if (uuid != null) {
                    backupResourceStore.putResourceWithoutCheck(ResourceStore.METASTORE_UUID_TAG, uuid.getByteSource(),
                            uuid.getTs(), -1);
                }
            }
            logger.info("start to backup {}", projectMsg);

            backupResourceStore.deleteResource(ResourceStore.METASTORE_TRASH_RECORD);
            backupMetadataStore.dump(backupResourceStore);
            logger.info("backup successfully at {}", backupPath);
        }
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

    private void backupMetadata(NavigableSet<String> items, ResourceStore resourceStore,
            ResourceStore backupResourceStore, boolean excludeTableExd) throws InterruptedException {
        for (String item : items) {
            if (excludeTableExd && item.startsWith(MetadataType.TABLE_EXD.name())) {
                continue;
            }
            resourceStore.copy(item, backupResourceStore);
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("metadata task is interrupt");
            }
            if (isCanceled()) {
                logger.info("core metadata backup was canceled.");
                return;
            }
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

    public void restore(KylinConfig kylinConfig, String project, String path, boolean delete, boolean backup)
            throws Exception {
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
        if (backup) {
            if (UnitOfWork.isAlreadyInTransaction()) {
                UnitOfWork.get().doAfterUnit(() -> backup(kylinConfig));
            } else {
                backup(kylinConfig);
            }
        }
    }

    public void restore(ResourceStore currentResourceStore, ResourceStore restoreResourceStore, String project,
            boolean delete) {
        checkDuplicateUuidModel(currentResourceStore, restoreResourceStore, project, delete);
        if (StringUtils.isBlank(project)) {
            logger.info("start to restore all projects");
            val destResources = currentResourceStore.listResourcesRecursively(MetadataType.ALL.name());
            val srcResources = restoreResourceStore.listResourcesRecursively(MetadataType.ALL.name());
            srcResources.remove(METASTORE_IMAGE);
            UnitOfWorkParams<Object> params = UnitOfWorkParams.builder().unitName(GLOBAL_PROJECT).maxRetry(1)
                    .useProjectLock(true)
                    .processor(() -> doRestore(restoreResourceStore, destResources, srcResources, delete)).build();
            UnitOfWork.doInTransactionWithRetry(params);

        } else {
            logger.info("start to restore project {}", project);
            val destResources = currentResourceStore.listResourcesRecursivelyByProject(project);
            val srcResources = restoreResourceStore.listResourcesRecursivelyByProject(project);

            UnitOfWorkParams<Object> params = UnitOfWorkParams.builder().unitName(project).maxRetry(1)
                    .useProjectLock(true)
                    .processor(() -> doRestore(restoreResourceStore, destResources, srcResources, delete)).build();
            UnitOfWork.doInTransactionWithRetry(params);
        }

        logger.info("restore successfully");
    }

    public void checkDuplicateUuidModel(ResourceStore currentResourceStore, ResourceStore restoreResourceStore,
            String project, boolean delete) {
        logger.info("start check duplicate uuid model");
        if (delete) {
            return;
        }

        Map<String, List<String>> duplicateUuidModelByProject = getDuplicateUuidModelByProject(currentResourceStore,
                restoreResourceStore, project);

        if (!duplicateUuidModelByProject.isEmpty()) {
            String errorMsg = duplicateUuidModelByProject.entrySet().stream()
                    .map(m -> "[" + m.getKey() + "]:" + String.join(",", m.getValue()))
                    .collect(Collectors.joining(";"));
            String info = String.format(Locale.ROOT,
                    "[UNEXPECTED_THINGS_HAPPENED] There will be models with the same name after recovery, please rename these models first:[project]:models: %s ",
                    errorMsg);
            logger.error(info);
            System.out.println(StringConstant.ANSI_RED + info + StringConstant.ANSI_RESET);
            throw new KylinException(MODEL_DUPLICATE_UUID_FAILED, errorMsg);
        }
        logger.info("end check duplicate uuid model");
    }

    private Map<String, List<String>> getDuplicateUuidModelByProject(ResourceStore currentResourceStore,
            ResourceStore restoreResourceStore, String project) {
        Map<String, List<String>> duplicateUuidModelByProject = Maps.newHashMap();
        if (StringUtils.isBlank(project)) {
            duplicateUuidModelByProject = getDuplicateUuidModelByAllProject(currentResourceStore, restoreResourceStore);
        } else {
            List<String> duplicateUuidModel = getDuplicateUuidModel(currentResourceStore, restoreResourceStore,
                    project);
            if (!duplicateUuidModel.isEmpty()) {
                duplicateUuidModelByProject.put(project, duplicateUuidModel);
            }
        }
        return duplicateUuidModelByProject;
    }

    private Map<String, List<String>> getDuplicateUuidModelByAllProject(ResourceStore currentResourceStore,
            ResourceStore restoreResourceStore) {
        var destProjectFolders = currentResourceStore.listResources(MetadataType.PROJECT.name());
        var srcProjectFolders = restoreResourceStore.listResources(MetadataType.PROJECT.name());
        destProjectFolders = destProjectFolders == null ? Sets.newTreeSet() : destProjectFolders;
        srcProjectFolders = srcProjectFolders == null ? Sets.newTreeSet() : srcProjectFolders;
        val projectFolders = Sets.union(srcProjectFolders, destProjectFolders);

        Map<String, List<String>> duplicateUuidModelByProject = Maps.newHashMap();
        for (String projectPath : projectFolders) {
            val projectName = MetadataType.splitKeyWithType(projectPath).getSecond();
            List<String> duplicateUuidModel = getDuplicateUuidModel(currentResourceStore, restoreResourceStore,
                    projectName);
            if (!duplicateUuidModel.isEmpty()) {
                duplicateUuidModelByProject.put(projectName, duplicateUuidModel);
            }
        }
        return duplicateUuidModelByProject;
    }

    private List<String> getDuplicateUuidModel(ResourceStore currentResourceStore, ResourceStore restoreResourceStore,
            String projectName) {
        RawResourceFilter filter = RawResourceFilter.equalFilter("project", projectName);
        Set<String> destModelResource = currentResourceStore.listResources(MetadataType.MODEL.name(), filter);
        Set<String> srcModelResource = restoreResourceStore.listResources(MetadataType.MODEL.name(), filter);
        destModelResource = destModelResource == null ? Collections.emptySet() : destModelResource;
        srcModelResource = srcModelResource == null ? Collections.emptySet() : srcModelResource;

        Sets.SetView<String> insertsModelResource = Sets.difference(srcModelResource, destModelResource);

        List<NDataModel> allModels = new ArrayList<>(
                getModelListFromResource(projectName, destModelResource, currentResourceStore));
        List<NDataModel> insertsModels = getModelListFromResource(projectName, new HashSet<>(insertsModelResource),
                restoreResourceStore);
        allModels.addAll(insertsModels);

        Map<String, Set<String>> nameUuids = Maps.newHashMap();
        for (NDataModel model : allModels) {
            String modelAlias = model.getAlias();
            nameUuids.putIfAbsent(modelAlias, Sets.newHashSet());
            nameUuids.get(modelAlias).add(model.getUuid());
        }
        return nameUuids.entrySet().stream().filter(m -> m.getValue().size() > 1).map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    public List<NDataModel> getModelListFromResource(String projectName, Set<String> modelResource,
            ResourceStore resourceStore) {

        if (modelResource == null) {
            return new ArrayList<>();
        }
        List<NDataModel> models = new ArrayList<>();
        for (String resource : modelResource) {
            try {
                NDataModel nDataModel = JsonUtil.readValue(resourceStore.getResource(resource).getByteSource().read(),
                        NDataModel.class);
                nDataModel.setProject(projectName);
                models.add(nDataModel);
            } catch (IOException e) {
                if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
                    throw new IllegalStateException(e);
                }
            }
        }
        return models;
    }

    private int doRestore(ResourceStore restoreResourceStore, Set<String> destResources, Set<String> srcResources,
            boolean delete) throws IOException {
        val transparentRS = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());

        // check destResources and srcResources are null,
        // because Sets.difference(srcResources, destResources) will report NullPointerException
        destResources = destResources == null ? Collections.emptySet() : destResources;
        srcResources = srcResources == null ? Collections.emptySet() : srcResources;

        logger.info("Start insert metadata resource...");
        val insertRes = Sets.difference(srcResources, destResources);
        for (val res : insertRes) {
            transparentRS.getResource(res, true); // return null, just to lock it.
            val metadataRaw = restoreResourceStore.getResource(res);
            UnitOfWork.get().getCopyForWriteItems().add(res);
            transparentRS.checkAndPutResource(res, metadataRaw.getByteSource(), -1L);
        }

        logger.info("Start update metadata resource...");
        val updateRes = Sets.intersection(destResources, srcResources);
        for (val res : updateRes) {
            val raw = transparentRS.getResource(res, true);
            val metadataRaw = restoreResourceStore.getResource(res);
            UnitOfWork.get().getCopyForWriteItems().add(res);
            transparentRS.checkAndPutResource(res, metadataRaw.getByteSource(), raw.getMvcc());
        }
        if (delete) {
            logger.info("Start delete metadata resource...");
            val deleteRes = Sets.difference(destResources, srcResources);
            for (val res : deleteRes) {
                UnitOfWork.get().getCopyForWriteItems().add(res);
                transparentRS.deleteResource(res);
            }
        }

        return 0;
    }

}
