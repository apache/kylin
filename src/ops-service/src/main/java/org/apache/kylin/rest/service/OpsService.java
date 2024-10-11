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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.constant.Constants.CORE_META_DIR;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.METADATA_BACKUP_IS_IN_PROGRESS;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.SYSTEM_IN_METADATA_RECOVER;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.helper.MetadataToolHelper;
import org.apache.kylin.metadata.asynctask.AbstractAsyncTask;
import org.apache.kylin.metadata.asynctask.MetadataRestoreTask;
import org.apache.kylin.metadata.favorite.AsyncTaskManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.reponse.MetadataBackupResponse;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.tool.CancelHook;
import org.apache.kylin.tool.FavoriteRuleTool;
import org.apache.kylin.tool.JobInfoTool;
import org.apache.kylin.tool.QueryHistoryOffsetTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import lombok.Getter;
import lombok.Setter;

@Service("opsService")
public class OpsService {

    public static final Logger log = LoggerFactory.getLogger(OpsService.class);

    public static final String META_BACKUP_DIR = "_metadata_backup";
    public static final String SYSTEM_LEVEL_METADATA_BACKUP_DIR_NAME = "_system_level_metadata_backup";
    public static final String PROJECT_LEVEL_METADATA_BACKUP_DIR_NAME = "_project_level_metadata_backup";
    public static final String BACKUP_STATUS = "_backup_status";
    private static final String PATH_SEP = "/";
    public static final String META_BACKUP_PATH = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()
            + META_BACKUP_DIR;
    public static final String GLOBAL_METADATA_BACKUP_PATH = META_BACKUP_PATH + PATH_SEP
            + SYSTEM_LEVEL_METADATA_BACKUP_DIR_NAME;
    public static final String PROJECT_METADATA_BACKUP_PATH = META_BACKUP_PATH + PATH_SEP
            + PROJECT_LEVEL_METADATA_BACKUP_DIR_NAME;
    private static int defaultStatusReadRetryTime = 5;

    public static void resetStatusReadRetryTime(int times) {
        defaultStatusReadRetryTime = times;
    }

    public static String getMetaBackupStoreDir(String project) {
        if (project == null || project.equals(UnitOfWork.GLOBAL_UNIT)) {
            return GLOBAL_METADATA_BACKUP_PATH;
        } else {
            return PROJECT_METADATA_BACKUP_PATH + PATH_SEP + project;
        }
    }

    public String backupMetadata(String project) {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        String username = AclPermissionUtil.getCurrentUsername();
        MetadataBackup metadataBackup = new MetadataBackup(getMetaBackupStoreDir(project), fs, username, project);
        return metadataBackup.startBackup();
    }

    public static List<MetadataBackupResponse> getMetadataBackupList(String project) throws IOException {
        String pathStr = getMetaBackupStoreDir(project);
        Path path = new Path(pathStr);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        List<MetadataBackupResponse> res = Lists.newArrayList();
        if (!fs.exists(path)) {
            return res;
        }
        for (FileStatus fileStatu : fs.listStatus(path)) {
            if (fileStatu.isDirectory()) {
                Path statusPath = new Path(fileStatu.getPath().toString() + PATH_SEP + BACKUP_STATUS);
                MetadataBackupResponse response = new MetadataBackupResponse();
                response.setPath(fileStatu.getPath().toUri().toString());
                if (fs.exists(statusPath)) {
                    MetadataBackupStatusInfo backupStatuInfo = getMetadataStatusInfoWithRetry(statusPath, fs,
                            defaultStatusReadRetryTime);
                    response.setStatus(backupStatuInfo.getStatus());
                    response.setSize(backupStatuInfo.getSize());
                    response.setOwner(backupStatuInfo.getOwner());
                }
                res.add(response);
            }
        }
        return res;
    }

    public static MetadataBackupStatusInfo getMetadataStatusInfoWithRetry(Path statusPath, FileSystem fs,
            int retryTime) {
        MetadataBackupStatusInfo metadataBackupStatusInfo = new MetadataBackupStatusInfo();
        do {
            try (FSDataInputStream fis = fs.open(statusPath)) {
                String value = fis.readUTF();
                metadataBackupStatusInfo = JsonUtil.readValue(value, MetadataBackupStatusInfo.class);
                return metadataBackupStatusInfo;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                metadataBackupStatusInfo.setStatus(MetadataBackupStatus.UNKNOWN);
                if (--retryTime >= 0) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ex) {
                        log.error(e.getMessage(), e);
                        Thread.currentThread().interrupt();
                    }
                }
            }
        } while (retryTime >= 0);
        return metadataBackupStatusInfo;
    }

    public static String deleteMetadataBackup(List<String> pathStrs, String project) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (String pathStr : pathStrs) {
            String response = deleteMetadataBackup(pathStr, project);
            if (sb.length() == 0) {
                sb.append(response);
            } else {
                sb.append(" : ");
                sb.append(response);
            }
        }
        return sb.toString();
    }

    public static String deleteMetadataBackup(String pathStr, String project) throws IOException {
        if (pathStr != null) {
            Path path = new Path(pathStr);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (fs.exists(path)) {
                if (!pathStr.startsWith(META_BACKUP_PATH)) {
                    return "can not delete path not in metadata backup dir " + META_BACKUP_PATH;
                }
                String rootPath = getMetaBackupStoreDir(project);
                if (!pathStr.startsWith(rootPath) || pathStr.equals(rootPath)) {
                    return "can not delete path not in metadata backup dir " + rootPath;
                }
                fs.delete(path, true);
                log.info("Delete metadata backup {} succeed.", path.toUri());
                if (fs.listStatus(path.getParent()).length == 0) {
                    fs.delete(path.getParent(), true);
                    log.info("Delete project path {} for no metadata backup exist.", path.getParent().toUri());
                }
                return path + " delete succeed.";
            }
        }
        return pathStr + " path not exist";
    }

    public void cancelAndDeleteMetadataBackup(String rootPath, String project) throws IOException {
        MetadataBackup.cancelAndAsyncDeleteBackup(rootPath, project);
    }

    public String restoreMetadata(String path, String project, boolean afterTruncate) {
        if (project == null) {
            project = UnitOfWork.GLOBAL_UNIT;
        }
        MetadataRestore metadataRestore = new MetadataRestore(path, project, afterTruncate);
        return MetadataRestore.submitMetadataRestore(metadataRestore);
    }

    public MetadataRestoreTask.MetadataRestoreStatus getMetadataRestoreStatus(String uuid, String project) {
        if (uuid == null) {
            return null;
        }
        AsyncTaskManager taskManager = AsyncTaskManager.getInstance(project);
        AbstractAsyncTask asyncTask = taskManager.get(AsyncTaskManager.METADATA_RECOVER_TASK, uuid);
        if (asyncTask == null) {
            return null;
        } else {
            return ((MetadataRestoreTask) asyncTask).getStatus();
        }
    }

    @Getter
    public static class MetadataBackup {
        public static final Logger log = LoggerFactory.getLogger(MetadataBackup.class);

        private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
        @Getter
        protected static final Map<String, Pair<Future, MetadataBackup>> runningTask = new ConcurrentHashMap<>();

        private final FileSystem fs;
        private final String rootPath;
        private final Path statusPath;
        private final MetadataBackupStatusInfo statusInfo;
        private final String project;
        private String projectMetadataBackupKey;

        private MetadataToolHelper coreMetadataTool;
        private JobInfoTool jobInfoTool;
        @Setter
        private QueryHistoryOffsetTool queryHistoryOffsetTool;
        private FavoriteRuleTool favoriteRuleTool;

        public MetadataBackup(String dir, FileSystem fs, String user, String project) {
            this.projectMetadataBackupKey = dir;
            this.rootPath = dir + PATH_SEP + System.currentTimeMillis();
            this.fs = fs;
            this.statusInfo = new MetadataBackupStatusInfo();
            this.statusPath = new Path(rootPath + PATH_SEP + BACKUP_STATUS);
            this.project = project;
            statusInfo.setOwner(user);
            statusInfo.setPath(rootPath);
            init();
        }

        public MetadataBackup(MetadataBackupResponse response, String project) {
            this.rootPath = response.getPath();
            this.project = project;
            this.fs = HadoopUtil.getWorkingFileSystem();
            this.statusPath = new Path(rootPath + PATH_SEP + BACKUP_STATUS);
            this.statusInfo = new MetadataBackupStatusInfo();
            statusInfo.setOwner(response.getOwner());
            statusInfo.setPath(rootPath);
            init();
        }

        private void init() {
            CancelHook hook = () -> runningTask.containsKey(projectMetadataBackupKey);
            this.coreMetadataTool = new MetadataToolHelper();
            coreMetadataTool.setHook(hook);
            this.queryHistoryOffsetTool = new QueryHistoryOffsetTool();
            queryHistoryOffsetTool.setHook(hook);
            this.favoriteRuleTool = new FavoriteRuleTool();
            favoriteRuleTool.setHook(hook);
            this.jobInfoTool = new JobInfoTool();
            jobInfoTool.setHook(hook);
            if (project == null || project.equals(UnitOfWork.GLOBAL_UNIT)) {
                List<String> projects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllProjects()
                        .stream().map(ProjectInstance::getName).collect(Collectors.toList());
                statusInfo.setProjects(projects);
            } else {
                statusInfo.setProjects(Lists.newArrayList(project));
            }
        }

        public static synchronized void cancelAndAsyncDeleteBackup(String rootPath, String project) throws IOException {
            String key = getProjectMetadataKeyFromRootPath(rootPath);
            if (runningTask.containsKey(key)) {
                Future future = runningTask.get(key).getFirst();
                cancelBackup(key);
                EXECUTOR.submit(() -> {
                    try {
                        future.get();
                        OpsService.deleteMetadataBackup(rootPath, project);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });
            }
        }

        public static void cancelBackup(String projectMetadataBackupKey) throws IOException {
            Pair<Future, MetadataBackup> pair = runningTask.remove(projectMetadataBackupKey);
            pair.getSecond().markCanceled();
        }

        public static String getProjectMetadataKeyFromRootPath(String rootPath) {
            return rootPath.substring(0, rootPath.lastIndexOf('/'));
        }

        public void doBackupMetadata() throws Exception {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            if (project == null || project.equals(UnitOfWork.GLOBAL_UNIT)) {
                List<ProjectInstance> projects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                        .listAllProjects();
                coreMetadataTool.backupToDirectPath(config, rootPath + PATH_SEP + CORE_META_DIR);
                for (ProjectInstance projectInstance : projects) {
                    queryHistoryOffsetTool.backup(rootPath, projectInstance.getName());
                    favoriteRuleTool.backup(rootPath, projectInstance.getName());
                    jobInfoTool.backup(rootPath, projectInstance.getName());
                }
            } else {
                coreMetadataTool.backupToDirectPath(config, rootPath + PATH_SEP + CORE_META_DIR, project);
                queryHistoryOffsetTool.backup(rootPath, project);
                favoriteRuleTool.backup(rootPath, project);
                jobInfoTool.backup(rootPath, project);
            }
        }

        public long getSize() throws IOException {
            long size = 0L;
            for (FileStatus status : fs.listStatus(new Path(rootPath))) {
                if (status.isDirectory()) {
                    for (FileStatus subStatus : fs.listStatus(status.getPath())) {
                        size += subStatus.getLen();
                    }
                }
                size += status.getLen();
            }
            return size;
        }

        public void updateStatus() throws IOException {
            try (FSDataOutputStream fos = fs.create(statusPath, true)) {
                String value = JsonUtil.writeValueAsString(statusInfo);
                fos.writeUTF(value);
            }
        }

        public void markCanceled() throws IOException {
            statusInfo.setStatus(MetadataBackupStatus.CANCELED);
            updateStatus();
        }

        public void markInProgress() throws IOException {
            statusInfo.setStatus(MetadataBackupStatus.IN_PROGRESS);
            updateStatus();
        }

        public void markSuccess(long size) throws IOException {
            statusInfo.setStatus(MetadataBackupStatus.SUCCEED);
            statusInfo.setSize(String.valueOf(size));
            updateStatus();
        }

        public void markFail() {
            try {
                statusInfo.setStatus(MetadataBackupStatus.FAILED);
                updateStatus();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                log.error("mark metadata backup {} failed failed.", statusPath);
            }
        }

        public String startBackup() {
            if (runningTask.containsKey(projectMetadataBackupKey)) {
                throw new KylinException(METADATA_BACKUP_IS_IN_PROGRESS);
            }
            Future<?> submit = EXECUTOR.submit(() -> {
                try {
                    markInProgress();
                    doBackupMetadata();
                    long size = getSize();
                    markSuccess(size);
                } catch (Exception e) {
                    log.error("Backup metadata to {} failed", rootPath);
                    log.error(e.getMessage(), e);
                    markFail();
                } finally {
                    runningTask.remove(projectMetadataBackupKey);
                }
            });
            runningTask.put(projectMetadataBackupKey, new Pair<>(submit, this));
            return rootPath;
        }

        public FileSystem getFs() {
            return fs;
        }

    }

    @Getter
    public static class MetadataRestore {

        public static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

        @Getter
        @Setter
        private static Future runningTask;

        private final boolean afterTruncate;
        private final String project;
        private final String path;
        private final String uuid;

        private QueryHistoryOffsetTool queryHistoryOffsetTool;
        private FavoriteRuleTool favoriteRuleTool;
        @Setter
        private JobInfoTool jobInfoTool;

        public MetadataRestore(String path, String project, boolean afterTruncate) {
            this.path = path;
            this.afterTruncate = afterTruncate;
            this.uuid = UUID.randomUUID().toString();
            this.project = project;
            init();
        }

        public static synchronized boolean hasMetadataRestoreRunning() {
            return runningTask != null;
        }

        public static String submitMetadataRestore(MetadataRestore metadataRestore) {
            if (hasMetadataRestoreRunning()) {
                throw new KylinException(SYSTEM_IN_METADATA_RECOVER);
            }
            runningTask = EXECUTOR_SERVICE.submit(metadataRestore::doRestore);
            return metadataRestore.uuid;
        }

        public void init() {
            this.queryHistoryOffsetTool = new QueryHistoryOffsetTool();
            this.favoriteRuleTool = new FavoriteRuleTool();
            this.jobInfoTool = new JobInfoTool();
        }

        public void doRestore() {
            synchronized (MetadataRestore.class) {
                AsyncTaskManager asyncTaskManager = AsyncTaskManager.getInstance(project);
                MetadataRestoreTask metadataRestoreTask = MetadataRestoreTask.newTask(uuid);
                metadataRestoreTask.setTaskAttributes(new MetadataRestoreTask.MetadataRestoreTaskAttributes());
                metadataRestoreTask.setStatus(MetadataRestoreTask.MetadataRestoreStatus.IN_PROGRESS);
                metadataRestoreTask.setProject(project);
                asyncTaskManager.save(metadataRestoreTask);
                try {
                    log.info("start to restore metadata from {}.", path);
                    KylinConfig config = KylinConfig.getInstanceFromEnv();
                    if (UnitOfWork.GLOBAL_UNIT.equals(project)) {
                        MetadataBackupStatusInfo backupStatusInfo = OpsService.getMetadataStatusInfoWithRetry(
                                new Path(path + PATH_SEP + BACKUP_STATUS), HadoopUtil.getWorkingFileSystem(), 1);
                        backupStatusInfo.projects.forEach(relProject -> this.restoreProject(relProject, config, false));
                        new MetadataToolHelper().restore(config, UnitOfWork.GLOBAL_UNIT,
                                path + PATH_SEP + CORE_META_DIR, afterTruncate, true);
                    } else {
                        this.restoreProject(this.project, config, true);
                    }
                    log.info("restore metadata from {} succeed.", path);
                    metadataRestoreTask.setStatus(MetadataRestoreTask.MetadataRestoreStatus.SUCCEED);
                } catch (Exception e) {
                    metadataRestoreTask.setStatus(MetadataRestoreTask.MetadataRestoreStatus.FAILED);
                    log.error("restore metadata {} failed.", path);
                    log.error(e.getMessage(), e);
                } finally {
                    asyncTaskManager.save(metadataRestoreTask);
                    MetadataRestore.setRunningTask(null);
                }
            }
        }

        public void restoreProject(String realProject, KylinConfig config, boolean backup) {
            log.info("start to restore metadata for {} project.", realProject);
            UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().processor(() -> {
                new MetadataToolHelper().restore(config, realProject, path + PATH_SEP + CORE_META_DIR, afterTruncate,
                        backup);
                UnitOfWork.get().doAfterUpdate(() -> {
                    queryHistoryOffsetTool.restoreProject(path, realProject, afterTruncate);
                    favoriteRuleTool.restoreProject(path, realProject, afterTruncate);
                    jobInfoTool.restoreProject(path, realProject, afterTruncate);
                });
                return null;
            }).useProjectLock(true).unitName(UnitOfWork.GLOBAL_UNIT).all(true).build());
            log.info("restore metadata for {} project succeed.", realProject);
        }
    }

    @Getter
    @Setter
    public static class MetadataBackupStatusInfo {
        private String path;
        private MetadataBackupStatus status;
        private String size;
        private String owner;
        private List<String> projects;
    }

    public enum MetadataBackupStatus {
        IN_PROGRESS, SUCCEED, FAILED, CANCELED, UNKNOWN
    }
}
