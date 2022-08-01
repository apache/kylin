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

import static org.apache.kylin.common.exception.code.ErrorCodeTool.FILE_ALREADY_EXISTS;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.PARAMETER_NOT_SPECIFY;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Locale;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.persistence.ImageDesc;
import org.apache.kylin.common.persistence.metadata.AuditLogStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.MetadataChecker;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.tool.util.ScreenPrintUtil;
import org.apache.kylin.tool.util.ToolMainWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.Getter;
import lombok.val;
import lombok.var;

public class MetadataTool extends ExecutableApplication {
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss",
            Locale.getDefault(Locale.Category.FORMAT));
    private static final Logger logger = LoggerFactory.getLogger("diag");
    private static final String HDFS_METADATA_URL_FORMATTER = "kylin_metadata@hdfs,path=%s";

    private static final String GLOBAL = "global";

    @SuppressWarnings("static-access")
    private static final Option OPERATE_BACKUP = OptionBuilder.getInstance()
            .withDescription("Backup metadata to local path or HDFS path").isRequired(false).create("backup");

    private static final Option OPERATE_COMPRESS = OptionBuilder.getInstance()
            .withDescription("Backup compressed metadata to HDFS path").isRequired(false).create("compress");

    private static final Option OPERATE_RESTORE = OptionBuilder.getInstance()
            .withDescription("Restore metadata from local path or HDFS path").isRequired(false).create("restore");

    private static final Option OPTION_AFTER_TRUNCATE = OptionBuilder.getInstance()
            .withDescription("Restore overwrite metadata from local path or HDFS path (optional)").isRequired(false)
            .withLongOpt("after-truncate").hasArg(false).create("d");

    private static final Option OPTION_DIR = OptionBuilder.getInstance().hasArg().withArgName("DIRECTORY_PATH")
            .withDescription("Specify the target directory for backup and restore").isRequired(false).create("dir");

    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().hasArg().withArgName("PROJECT_NAME")
            .withDescription("Specify project level backup and restore (optional)").isRequired(false).create("project");

    private static final Option FOLDER_NAME = OptionBuilder.getInstance().hasArg().withArgName("FOLDER_NAME")
            .withDescription("Specify the folder name for backup").isRequired(false).create("folder");

    private static final Option OPTION_EXCLUDE_TABLE_EXD = OptionBuilder.getInstance()
            .withDescription("Exclude metadata {project}/table_exd directory").isRequired(false)
            .create("excludeTableExd");

    private final Options options;

    private final KylinConfig kylinConfig;

    private ResourceStore resourceStore;

    @Getter
    private String backupPath;

    MetadataTool() {
        kylinConfig = KylinConfig.getInstanceFromEnv();
        this.options = new Options();
        initOptions();
    }

    public MetadataTool(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        this.options = new Options();
        initOptions();
    }

    public static void backup(KylinConfig kylinConfig) throws IOException {
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        String[] args = new String[] { "-backup", "-compress", "-dir", HadoopUtil.getBackupFolder(kylinConfig) };
        val backupTool = new MetadataTool(kylinConfig);
        backupTool.execute(args);
    }

    public static void backup(KylinConfig kylinConfig, String dir, String folder) throws IOException {
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        String[] args = new String[] { "-backup", "-compress", "-dir", dir, "-folder", folder };
        val backupTool = new MetadataTool(kylinConfig);
        backupTool.execute(args);
    }

    public static void restore(KylinConfig kylinConfig, String folder) throws IOException {
        val tool = new MetadataTool(kylinConfig);
        tool.execute(new String[] { "-restore", "-dir", folder, "--after-truncate" });
    }

    public static void main(String[] args) {
        ToolMainWrapper.wrap(args, () -> {
            val config = KylinConfig.getInstanceFromEnv();
            val tool = new MetadataTool(config);
            val optionsHelper = new OptionsHelper();
            optionsHelper.parseOptions(tool.getOptions(), args);
            boolean isBackup = optionsHelper.hasOption(OPERATE_BACKUP);
            if (isBackup && ScreenPrintUtil.isMainThread()) {
                config.setProperty("kylin.env.metadata.only-for-read", "true");
            }
            val resourceStore = ResourceStore.getKylinMetaStore(config);
            resourceStore.getAuditLogStore().setInstance(AddressUtil.getMockPortAddress());
            tool.execute(args);
            if (isBackup && StringUtils.isNotEmpty(tool.getBackupPath())) {
                System.out.printf(Locale.ROOT, "The metadata backup path is %s.%n", tool.getBackupPath());
            }
        });
        Unsafe.systemExit(0);
    }

    public static void restore(ResourceStore currentResourceStore, ResourceStore restoreResourceStore, String project,
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

            val projectPath = "/" + project;
            val destResources = currentResourceStore.listResourcesRecursively(projectPath);
            val srcResources = restoreResourceStore.listResourcesRecursively(projectPath);

            UnitOfWork.doInTransactionWithRetry(
                    () -> doRestore(currentResourceStore, restoreResourceStore, destResources, srcResources, delete),
                    project, 1);
        }

        logger.info("restore successfully");
    }

    private static int doRestore(ResourceStore currentResourceStore, ResourceStore restoreResourceStore,
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

    private void initOptions() {
        final OptionGroup optionGroup = new OptionGroup();
        optionGroup.setRequired(true);
        optionGroup.addOption(OPERATE_BACKUP);
        optionGroup.addOption(OPERATE_RESTORE);

        options.addOptionGroup(optionGroup);
        options.addOption(OPTION_DIR);
        options.addOption(OPTION_PROJECT);
        options.addOption(FOLDER_NAME);
        options.addOption(OPERATE_COMPRESS);
        options.addOption(OPTION_EXCLUDE_TABLE_EXD);
        options.addOption(OPTION_AFTER_TRUNCATE);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        logger.info("start to init ResourceStore");
        resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        if (optionsHelper.hasOption(OPERATE_BACKUP)) {
            boolean isGlobal = null == optionsHelper.getOptionValue(OPTION_PROJECT);
            long startAt = System.currentTimeMillis();

            try {
                backup(optionsHelper);
            } catch (Exception be) {
                if (isGlobal) {
                    MetricsGroup.hostTagCounterInc(MetricsName.METADATA_BACKUP_FAILED, MetricsCategory.GLOBAL, GLOBAL);
                } else {
                    MetricsGroup.hostTagCounterInc(MetricsName.METADATA_BACKUP_FAILED, MetricsCategory.PROJECT,
                            optionsHelper.getOptionValue(OPTION_PROJECT));
                }
                throw be;
            } finally {
                if (isGlobal) {
                    MetricsGroup.hostTagCounterInc(MetricsName.METADATA_BACKUP, MetricsCategory.GLOBAL, GLOBAL);
                    MetricsGroup.hostTagCounterInc(MetricsName.METADATA_BACKUP_DURATION, MetricsCategory.GLOBAL, GLOBAL,
                            System.currentTimeMillis() - startAt);
                } else {
                    MetricsGroup.hostTagCounterInc(MetricsName.METADATA_BACKUP, MetricsCategory.PROJECT,
                            optionsHelper.getOptionValue(OPTION_PROJECT));
                    MetricsGroup.hostTagCounterInc(MetricsName.METADATA_BACKUP_DURATION, MetricsCategory.PROJECT,
                            optionsHelper.getOptionValue(OPTION_PROJECT), System.currentTimeMillis() - startAt);
                }
            }

        } else if (optionsHelper.hasOption(OPERATE_RESTORE)) {
            restore(optionsHelper, optionsHelper.hasOption(OPTION_AFTER_TRUNCATE));
        } else {
            throw new KylinException(PARAMETER_NOT_SPECIFY, "-restore");
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

    private void backup(OptionsHelper optionsHelper) throws Exception {
        val project = optionsHelper.getOptionValue(OPTION_PROJECT);
        var path = optionsHelper.getOptionValue(OPTION_DIR);
        var folder = optionsHelper.getOptionValue(FOLDER_NAME);
        var compress = optionsHelper.hasOption(OPERATE_COMPRESS);
        val excludeTableExd = optionsHelper.hasOption(OPTION_EXCLUDE_TABLE_EXD);
        if (StringUtils.isBlank(path)) {
            path = KylinConfigBase.getKylinHome() + File.separator + "meta_backups";
        }
        if (StringUtils.isEmpty(folder)) {
            folder = LocalDateTime.now(Clock.systemDefaultZone()).format(DATE_TIME_FORMATTER) + "_backup";
        }
        backupPath = StringUtils.appendIfMissing(path, "/") + folder;
        val backupMetadataUrl = getMetadataUrl(backupPath, compress);
        val backupConfig = KylinConfig.createKylinConfig(kylinConfig);
        backupConfig.setMetadataUrl(backupMetadataUrl);
        abortIfAlreadyExists(backupPath);
        logger.info("The backup metadataUrl is {} and backup path is {}", backupMetadataUrl, backupPath);

        try (val backupResourceStore = ResourceStore.getKylinMetaStore(backupConfig)) {

            val backupMetadataStore = backupResourceStore.getMetadataStore();

            if (StringUtils.isBlank(project)) {
                logger.info("start to copy all projects from ResourceStore.");
                val auditLogStore = resourceStore.getAuditLogStore();
                long finalOffset = getOffset(auditLogStore);
                backupResourceStore.putResourceWithoutCheck(ResourceStore.METASTORE_IMAGE,
                        ByteSource.wrap(JsonUtil.writeValueAsBytes(new ImageDesc(finalOffset))),
                        System.currentTimeMillis(), -1);
                var projectFolders = resourceStore.listResources("/");
                if (projectFolders == null) {
                    return;
                }
                UnitOfWork.doInTransactionWithRetry(() -> {
                    backupProjects(projectFolders, backupResourceStore, excludeTableExd);
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
    }

    private long getOffset(AuditLogStore auditLogStore) {
        long offset = 0;
        if (kylinConfig.isUTEnv())
            offset = auditLogStore.getMaxId();
        else
            offset = auditLogStore.getLogOffset() == 0 ? resourceStore.getOffset() : auditLogStore.getLogOffset();
        return offset;
    }

    private void backupProjects(NavigableSet<String> projectFolders, ResourceStore backupResourceStore,
            boolean excludeTableExd) throws InterruptedException {
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

    private void copyResourceStore(String projectPath, ResourceStore srcResourceStore, ResourceStore destResourceStore,
            boolean isProjectLevel, boolean excludeTableExd) {
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

    private void restore(OptionsHelper optionsHelper, boolean delete) throws IOException {
        logger.info("Restore metadata with delete : {}", delete);
        val project = optionsHelper.getOptionValue(OPTION_PROJECT);
        val restorePath = optionsHelper.getOptionValue(OPTION_DIR);

        val restoreMetadataUrl = getMetadataUrl(restorePath, false);
        val restoreConfig = KylinConfig.createKylinConfig(kylinConfig);
        restoreConfig.setMetadataUrl(restoreMetadataUrl);
        logger.info("The restore metadataUrl is {} and restore path is {} ", restoreMetadataUrl, restorePath);

        val restoreResourceStore = ResourceStore.getKylinMetaStore(restoreConfig);
        val restoreMetadataStore = restoreResourceStore.getMetadataStore();
        MetadataChecker metadataChecker = new MetadataChecker(restoreMetadataStore);

        val verifyResult = metadataChecker.verify();
        if (!verifyResult.isQualified()) {
            throw new RuntimeException(verifyResult.getResultMessage() + "\n the metadata dir is not qualified");
        }
        restore(resourceStore, restoreResourceStore, project, delete);
        backup(kylinConfig);

    }

    String getMetadataUrl(String rootPath, boolean compressed) {
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
}
