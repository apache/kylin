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

import static org.apache.kylin.common.constant.Constants.HIDDEN_VALUE;
import static org.apache.kylin.common.constant.Constants.KYLIN_SOURCE_JDBC_CONNECTION_URL_KEY;
import static org.apache.kylin.common.constant.Constants.KYLIN_SOURCE_JDBC_DRIVER_KEY;
import static org.apache.kylin.common.constant.Constants.KYLIN_SOURCE_JDBC_PASS_KEY;
import static org.apache.kylin.common.constant.Constants.KYLIN_SOURCE_JDBC_SOURCE_ENABLE_KEY;
import static org.apache.kylin.common.constant.Constants.KYLIN_SOURCE_JDBC_SOURCE_NAME_KEY;
import static org.apache.kylin.common.constant.Constants.KYLIN_SOURCE_JDBC_USER_KEY;
import static org.apache.kylin.common.constant.Constants.KYLIN_JOB_MAX_CONCURRENT_JOBS;
import static org.apache.kylin.common.constant.NonCustomProjectLevelConfig.DATASOURCE_TYPE;
import static org.apache.kylin.common.exception.ServerErrorCode.DATABASE_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.DUPLICATE_PROJECT_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_EMAIL;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.FILE_TYPE_MISMATCH;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_JDBC_SOURCE_CONFIG;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.PROJECT_DROP_FAILED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CONFIG_NOT_SUPPORT_EDIT;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_INVALID_SUPPORT_LIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.event.ProjectCleanOldQueryResultEvent;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.scheduler.SourceUsageUpdateNotifier;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.common.util.JdbcUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.base.Strings;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.guava30.shaded.common.primitives.Ints;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cube.storage.ProjectStorageInfoCollector;
import org.apache.kylin.metadata.cube.storage.StorageInfoEnum;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.recommendation.candidate.RawRecManager;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.config.initialize.ProjectDropListener;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.ComputedColumnConfigRequest;
import org.apache.kylin.rest.request.GarbageCleanUpConfigRequest;
import org.apache.kylin.rest.request.JdbcRequest;
import org.apache.kylin.rest.request.JdbcSourceInfoRequest;
import org.apache.kylin.rest.request.JobNotificationConfigRequest;
import org.apache.kylin.rest.request.MultiPartitionConfigRequest;
import org.apache.kylin.rest.request.OwnerChangeRequest;
import org.apache.kylin.rest.request.ProjectExclusionRequest;
import org.apache.kylin.rest.request.ProjectGeneralInfoRequest;
import org.apache.kylin.rest.request.ProjectKerberosInfoRequest;
import org.apache.kylin.rest.request.PushDownConfigRequest;
import org.apache.kylin.rest.request.PushDownProjectConfigRequest;
import org.apache.kylin.rest.request.SCD2ConfigRequest;
import org.apache.kylin.rest.request.SegmentConfigRequest;
import org.apache.kylin.rest.request.ShardNumConfigRequest;
import org.apache.kylin.rest.request.SnapshotConfigRequest;
import org.apache.kylin.rest.response.FavoriteQueryThresholdResponse;
import org.apache.kylin.rest.response.ProjectConfigResponse;
import org.apache.kylin.rest.response.StorageVolumeInfoResponse;
import org.apache.kylin.rest.response.UserProjectPermissionResponse;
import org.apache.kylin.rest.security.AclManager;
import org.apache.kylin.rest.security.AclPermissionEnum;
import org.apache.kylin.rest.security.KerberosLoginManager;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.tool.garbage.GarbageCleaner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.SneakyThrows;
import lombok.val;

@Component("projectService")
public class ProjectService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(ProjectService.class);
    private static final String SNAPSHOT_AUTO_REFRESH_TIME_MODE = "snapshot_automatic_refresh_time_mode";
    private static final String SNAPSHOT_AUTO_REFRESH_TIME_MODE_DAY = "DAY";
    private static final String SNAPSHOT_AUTO_REFRESH_TIME_MODE_HOURS = "HOURS";
    private static final String SNAPSHOT_AUTO_REFRESH_TIME_MODE_MINUTE = "MINUTE";
    private static final String SNAPSHOT_AUTO_REFRESH_TIME_MODES = "DAY, HOURS, MINUTE";
    private static final String KYLIN_QUERY_PUSHDOWN_RUNNER_CLASS_NAME = "kylin.query.pushdown.runner-class-name";

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private MetadataBackupService metadataBackupService;

    @Autowired(required = false)
    @Qualifier("asyncTaskService")
    private AsyncTaskServiceSupporter asyncTaskService;

    @Autowired
    private AccessService accessService;

    @Autowired(required = false)
    private ProjectModelSupporter projectModelSupporter;

    @Autowired(required = false)
    private ProjectSmartServiceSupporter projectSmartService;

    @Autowired
    UserService userService;

    private static final String DEFAULT_VAL = "default";

    private static final String SPARK_YARN_QUEUE = "kylin.engine.spark-conf.spark.yarn.queue";

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    @Transaction(project = -1)
    public ProjectInstance createProject(String projectName, ProjectInstance newProject) {
        Message msg = MsgPicker.getMsg();
        String description = newProject.getDescription();
        LinkedHashMap<String, String> overrideProps = newProject.getOverrideKylinProps();
        if (overrideProps == null) {
            overrideProps = Maps.newLinkedHashMap();
        }

        overrideProps.put("kylin.metadata.semi-automatic-mode", String.valueOf(getConfig().isSemiAutoMode()));
        overrideProps.put(ProjectInstance.EXPOSE_COMPUTED_COLUMN_CONF, KylinConfigBase.TRUE);

        encryptJdbcPassInOverrideKylinProps(overrideProps);
        ProjectInstance currentProject = getManager(NProjectManager.class).getProject(projectName);
        if (currentProject != null) {
            throw new KylinException(DUPLICATE_PROJECT_NAME,
                    String.format(Locale.ROOT, msg.getProjectAlreadyExist(), projectName));
        }
        final String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        ProjectInstance createdProject = getManager(NProjectManager.class).createProject(projectName, owner,
                description, overrideProps);
        logger.debug("New project created.");
        return createdProject;
    }

    public List<ProjectInstance> getReadableProjects() {
        return getProjectsFilterByExactMatchAndPermission(null, false, AclPermissionEnum.READ);
    }

    public List<ProjectInstance> getAdminProjects() {
        return getProjectsFilterByExactMatchAndPermission(null, false, AclPermissionEnum.ADMINISTRATION);
    }

    public List<ProjectInstance> getReadableProjects(final String projectName, boolean exactMatch) {
        return getProjectsFilterByExactMatchAndPermission(projectName, exactMatch, AclPermissionEnum.READ);
    }

    public List<String> getOwnedProjects() {
        val config = KylinConfig.getInstanceFromEnv();
        val epochManager = EpochManager.getInstance();
        return NProjectManager.getInstance(config).listAllProjects().stream() //
                .map(ProjectInstance::getName) //
                .filter(epochManager::checkEpochOwner) // project owner
                .collect(Collectors.toList());
    }

    private Predicate<ProjectInstance> getRequestFilter(final String projectName, boolean exactMatch,
            AclPermissionEnum permission) {
        Predicate<ProjectInstance> filter;
        switch (permission) {
        case READ:
            filter = projectInstance -> aclEvaluate.hasProjectReadPermission(projectInstance);
            break;
        case OPERATION:
            filter = projectInstance -> aclEvaluate.hasProjectOperationPermission(projectInstance);
            break;
        case MANAGEMENT:
            filter = projectInstance -> aclEvaluate.hasProjectWritePermission(projectInstance);
            break;
        case ADMINISTRATION:
            filter = projectInstance -> aclEvaluate.hasProjectAdminPermission(projectInstance);
            break;
        default:
            throw new KylinException(PERMISSION_DENIED, "Operation failed, unknown permission:" + permission);
        }
        if (StringUtils.isNotBlank(projectName)) {
            Predicate<ProjectInstance> exactMatchFilter = projectInstance -> (exactMatch
                    && projectInstance.getName().equals(projectName))
                    || (!exactMatch && projectInstance.getName().toUpperCase(Locale.ROOT)
                            .contains(projectName.toUpperCase(Locale.ROOT)));
            filter = filter.and(exactMatchFilter);
        }

        return filter;
    }

    public List<ProjectInstance> getProjectsFilterByExactMatchAndPermission(final String projectName,
            boolean exactMatch, AclPermissionEnum permission) {
        Predicate<ProjectInstance> filter = getRequestFilter(projectName, exactMatch, permission);
        return getProjectsWithFilter(filter);
    }

    public List<UserProjectPermissionResponse> getProjectsFilterByExactMatchAndPermissionWrapperUserPermission(
            final String projectName, boolean exactMatch, AclPermissionEnum permission) throws IOException {
        Predicate<ProjectInstance> filter = getRequestFilter(projectName, exactMatch, permission);
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication == null) {
            return Collections.emptyList();
        }

        UserDetails user = null;

        final AtomicBoolean isGlobalAdmin = new AtomicBoolean(false);
        try {
            user = userService.loadUserByUsername(authentication.getName());
        } catch (Exception e) {
            logger.warn("Cat not load user by username {}", authentication.getName(), e);
            return Collections.emptyList();
        }

        if (userService.isGlobalAdmin(user)) {
            isGlobalAdmin.set(true);
        }

        UserDetails finalUser = user;
        return getProjectsWithFilter(filter).parallelStream().map(projectInstance -> {
            String userPermission;
            clearJdbcPassInOverrideKylinProps(projectInstance.getOverrideKylinProps());

            if (isGlobalAdmin.get()) {
                userPermission = AclPermissionEnum.ADMINISTRATION.name();
            } else {
                userPermission = AclPermissionEnum.convertToAclPermission(
                        accessService.getUserNormalPermission(projectInstance.getName(), finalUser).getFirst());
            }
            return new UserProjectPermissionResponse(projectInstance, userPermission);
        }).collect(Collectors.toList());
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateQueryAccelerateThresholdConfig(String project, Integer threshold, boolean tipsEnabled) {
        Map<String, String> overrideKylinProps = Maps.newHashMap();
        if (threshold != null) {
            if (threshold <= 0) {
                throw new KylinException(INVALID_PARAMETER,
                        "No valid value for 'threshold'. Please set an integer 'x' "
                                + "greater than 0 to 'threshold'. The system will notify you whenever there "
                                + "are more then 'x' queries waiting to accelerate.");
            }
            overrideKylinProps.put("kylin.favorite.query-accelerate-threshold", String.valueOf(threshold));
        }
        overrideKylinProps.put("kylin.favorite.query-accelerate-tips-enable", String.valueOf(tipsEnabled));
        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public FavoriteQueryThresholdResponse getQueryAccelerateThresholdConfig(String project) {
        val projectInstance = getManager(NProjectManager.class).getProject(project);
        val thresholdResponse = new FavoriteQueryThresholdResponse();
        val config = projectInstance.getConfig();
        thresholdResponse.setThreshold(config.getFavoriteQueryAccelerateThreshold());
        thresholdResponse.setTipsEnabled(config.getFavoriteQueryAccelerateTipsEnabled());
        return thresholdResponse;
    }

    public StorageVolumeInfoResponse getStorageVolumeInfoResponse(String project) {
        val response = new StorageVolumeInfoResponse();
        val collector = new ProjectStorageInfoCollector(Lists.newArrayList(StorageInfoEnum.GARBAGE_STORAGE,
                StorageInfoEnum.STORAGE_QUOTA, StorageInfoEnum.TOTAL_STORAGE));
        val storageVolumeInfo = collector.getStorageVolumeInfo(getConfig(), project);
        response.setGarbageStorageSize(storageVolumeInfo.getGarbageStorageSize());
        response.setStorageQuotaSize(storageVolumeInfo.getStorageQuotaSize());
        response.setTotalStorageSize(storageVolumeInfo.getTotalStorageSize());
        return response;
    }

    @SneakyThrows
    public void garbageCleanup(long remainingTime) {
        try (SetThreadName ignored = new SetThreadName("GarbageCleanupWorker")) {
            val config = KylinConfig.getInstanceFromEnv();
            val projectManager = NProjectManager.getInstance(config);
            val epochMgr = EpochManager.getInstance();
            for (ProjectInstance project : projectManager.listAllProjects()) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Thread is interrupted: " + Thread.currentThread().getName());
                }
                if (!config.isUTEnv() && !epochMgr.checkEpochOwner(project.getName()))
                    continue;
                logger.info("Start to cleanup garbage for project<{}>", project.getName());
                try {
                    projectSmartService.cleanupGarbage(project.getName(), remainingTime);
                    GarbageCleaner.cleanMetadata(project.getName());
                    EventBusFactory.getInstance().callService(new ProjectCleanOldQueryResultEvent(project.getName()));
                } catch (Exception e) {
                    logger.warn("clean project<" + project.getName() + "> failed", e);
                }
                logger.info("Garbage cleanup for project<{}> finished", project.getName());
            }
            cleanRawRecForDeletedProject(projectManager);
        }

    }

    private void cleanRawRecForDeletedProject(NProjectManager projectManager) {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
            return;
        }
        RawRecManager.getInstance(EpochManager.GLOBAL).cleanForDeletedProject(
                projectManager.listAllProjects().stream().map(ProjectInstance::getName).collect(Collectors.toList()));
    }

    public void cleanupAcl() {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val prjManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            Set<String> projects = prjManager.listAllProjects().stream().map(ProjectInstance::getUuid)
                    .collect(Collectors.toSet());
            val aclManager = AclManager.getInstance(KylinConfig.getInstanceFromEnv());
            for (val acl : aclManager.listAll()) {
                String id = acl.getDomainObjectInfo().getId();
                if (!projects.contains(id)) {
                    aclManager.delete(id);
                    continue;
                }
                val aceList = acl.getEntries();
                aceList.forEach(ace -> {
                    if (accessService.isPrincipalSidNotExists(ace.getSid())) {
                        accessService.revokeProjectPermission(AccessService.getName(ace.getSid()),
                                MetadataConstants.TYPE_USER);
                    }
                    if (accessService.isGrantedAuthoritySidNotExists(ace.getSid())) {
                        accessService.revokeProjectPermission(accessService.getName(ace.getSid()),
                                MetadataConstants.TYPE_GROUP);
                    }
                });
            }
            return 0;
        }, UnitOfWork.GLOBAL_UNIT);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public void cleanupGarbage(String project) throws Exception {
        projectSmartService.cleanupGarbage(project, 0);
        GarbageCleaner.cleanMetadata(project);
        asyncTaskService.cleanupStorage();
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateStorageQuotaConfig(String project, long storageQuotaSize) {
        if (storageQuotaSize < FileUtils.ONE_TB) {
            throw new KylinException(INVALID_PARAMETER,
                    "No valid storage quota size, Please set an integer greater than or equal to 1TB "
                            + "to 'storage_quota_size', unit byte.");
        }
        Map<String, String> overrideKylinProps = Maps.newHashMap();
        double storageQuotaSizeGB = 1.0 * storageQuotaSize / (FileUtils.ONE_GB);
        overrideKylinProps.put("kylin.storage.quota-in-giga-bytes", Double.toString(storageQuotaSizeGB));
        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    private void updateProjectOverrideKylinProps(String project, Map<String, String> overrideKylinProps) {
        val projectManager = getManager(NProjectManager.class);
        val projectInstance = projectManager.getProject(project);
        if (projectInstance == null) {
            throw new KylinException(PROJECT_NOT_EXIST, project);
        }
        if (overrideKylinProps.containsKey(KYLIN_JOB_MAX_CONCURRENT_JOBS)) {
            val maxConcurrentJobs = Ints.tryParse(overrideKylinProps.get(KYLIN_JOB_MAX_CONCURRENT_JOBS));
            if (Objects.isNull(maxConcurrentJobs) || maxConcurrentJobs < 0)
                throw new KylinException(INVALID_PARAMETER,
                        MsgPicker.getMsg().getIllegalNegative(KYLIN_JOB_MAX_CONCURRENT_JOBS));
        }
        encryptJdbcPassInOverrideKylinProps(overrideKylinProps);
        projectManager.updateProject(project, copyForWrite -> copyForWrite.getOverrideKylinProps()
                .putAll(KylinConfig.trimKVFromMap(overrideKylinProps)));
    }

    private void encryptJdbcPassInOverrideKylinProps(Map<String, String> overrideKylinProps) {
        if (overrideKylinProps.containsKey(KYLIN_SOURCE_JDBC_PASS_KEY)) {
            overrideKylinProps.computeIfPresent(KYLIN_SOURCE_JDBC_PASS_KEY,
                    (k, v) -> EncryptUtil.encryptWithPrefix(overrideKylinProps.get(KYLIN_SOURCE_JDBC_PASS_KEY)));
        }
    }

    private void clearJdbcPassInOverrideKylinProps(Map<String, String> overrideKylinProps) {
        overrideKylinProps.computeIfPresent(KYLIN_SOURCE_JDBC_PASS_KEY, (k, v) -> HIDDEN_VALUE);
    }

    @Transaction(project = 0)
    public void updateJobNotificationConfig(String project, JobNotificationConfigRequest request) {
        aclEvaluate.checkProjectAdminPermission(project);
        Map<String, String> overrideKylinProps = Maps.newHashMap();
        if (request.getJobStatesNotification() != null) {
            overrideKylinProps.put("kylin.job.notification-enable-states",
                    String.join(",", Sets.newHashSet(request.getJobStatesNotification())));
            overrideKylinProps.put("kylin.job.notification-on-job-error", "false");
        } else {
            overrideKylinProps.put("kylin.job.notification-on-job-error",
                    String.valueOf(request.getJobErrorNotificationEnabled()));
        }

        overrideKylinProps.put("kylin.job.notification-on-empty-data-load",
                String.valueOf(request.getDataLoadEmptyNotificationEnabled()));
        overrideKylinProps.put("kylin.job.notification-admin-emails",
                convertToString(request.getJobNotificationEmails()));
        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    @Transaction(project = 0)
    public void updateYarnQueue(String project, String queueName) {
        Map<String, String> overrideKylinProps = Maps.newHashMap();
        overrideKylinProps.put(SPARK_YARN_QUEUE, queueName);
        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    @Transaction(project = 0)
    public void updateJdbcInfo(String project, JdbcSourceInfoRequest jdbcSourceInfoRequest) {
        if (Boolean.TRUE.equals(jdbcSourceInfoRequest.getJdbcSourceEnable())) {
            validateJdbcConfig(project, jdbcSourceInfoRequest);
        }
        Map<String, String> overrideKylinProps = Maps.newHashMap();
        if (jdbcSourceInfoRequest.getJdbcSourceDriver() != null) {
            overrideKylinProps.put(KYLIN_SOURCE_JDBC_DRIVER_KEY, jdbcSourceInfoRequest.getJdbcSourceDriver());
        }
        if (jdbcSourceInfoRequest.getJdbcSourceEnable() != null) {
            overrideKylinProps.put(KYLIN_SOURCE_JDBC_SOURCE_ENABLE_KEY,
                    jdbcSourceInfoRequest.getJdbcSourceEnable().toString());
        }
        if (jdbcSourceInfoRequest.getJdbcSourceName() != null) {
            overrideKylinProps.put(KYLIN_SOURCE_JDBC_SOURCE_NAME_KEY, jdbcSourceInfoRequest.getJdbcSourceName());
        }
        if (jdbcSourceInfoRequest.getJdbcSourceConnectionUrl() != null) {
            overrideKylinProps.put(KYLIN_SOURCE_JDBC_CONNECTION_URL_KEY,
                    jdbcSourceInfoRequest.getJdbcSourceConnectionUrl());
        }
        if (jdbcSourceInfoRequest.getJdbcSourceUser() != null) {
            overrideKylinProps.put(KYLIN_SOURCE_JDBC_USER_KEY, jdbcSourceInfoRequest.getJdbcSourceUser());
        }
        if (jdbcSourceInfoRequest.getJdbcSourcePass() != null) {
            overrideKylinProps.put(KYLIN_SOURCE_JDBC_PASS_KEY, jdbcSourceInfoRequest.getJdbcSourcePass());
        }

        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    private void validateJdbcConfig(String project, JdbcSourceInfoRequest jdbcSourceInfoRequest) {
        val projectInstance = getManager(NProjectManager.class).getProject(project);
        val config = projectInstance.getConfig();
        String driver = jdbcSourceInfoRequest.getJdbcSourceDriver();
        if (driver == null) {
            driver = config.getJdbcDriver();
        }

        String url = jdbcSourceInfoRequest.getJdbcSourceConnectionUrl();
        if (url == null) {
            url = config.getJdbcConnectionUrl();
        }
        String username = jdbcSourceInfoRequest.getJdbcSourceUser();
        if (username == null) {
            username = config.getJdbcUser();
        }
        String password = jdbcSourceInfoRequest.getJdbcSourcePass();
        if (password == null) {
            password = config.getJdbcPass();
        }
        Preconditions.checkNotNull(driver, "driver can not be null");
        Preconditions.checkNotNull(url, "url can not be null");
        Preconditions.checkNotNull(username, "username can not be null");
        Preconditions.checkNotNull(password, "password can not be null");
        if (!JdbcUtils.checkConnectionParameter(driver, url, username, password)) {
            throw new KylinException(INVALID_JDBC_SOURCE_CONFIG, MsgPicker.getMsg().getJdbcConnectionInfoWrong());
        }
    }

    private String convertToString(List<String> stringList) {
        if (CollectionUtils.isEmpty(stringList)) {
            throw new KylinException(EMPTY_EMAIL, "Please enter at least one email address.");
        }
        Set<String> notEmails = Sets.newHashSet();
        for (String email : Sets.newHashSet(stringList)) {
            Pattern pattern = Pattern.compile("^[a-zA-Z0-9_.-]+@[a-zA-Z0-9-]+(\\.[a-zA-Z0-9-]+)*\\.[a-zA-Z0-9]{2,6}$");
            Matcher matcher = pattern.matcher(email);
            if (!matcher.find()) {
                notEmails.add(email);
            }
        }
        if (!notEmails.isEmpty()) {
            throw new KylinException(INVALID_PARAMETER,
                    "No valid value " + notEmails + " for 'job_notification_email'. Please enter valid email address.");
        }
        return String.join(",", Sets.newHashSet(stringList));
    }

    public ProjectConfigResponse getProjectConfig0(String project) {
        val response = new ProjectConfigResponse();
        val projectInstance = getManager(NProjectManager.class).getProject(project);
        val config = projectInstance.getConfig();

        response.setProject(project);
        response.setDescription(projectInstance.getDescription());
        response.setDefaultDatabase(projectInstance.getDefaultDatabase());
        response.setSemiAutomaticMode(config.isSemiAutoMode());
        response.setTableExclusionEnabled(config.isTableExclusionEnabled());

        response.setStorageQuotaSize(config.getStorageQuotaSize());

        response.setPushDownEnabled(config.isPushDownEnabled());
        response.setRunnerClassName(config.getPushDownRunnerClassName());
        response.setConverterClassNames(String.join(",", config.getPushDownConverterClassNames()));

        response.setAutoMergeEnabled(projectInstance.getSegmentConfig().getAutoMergeEnabled());
        response.setAutoMergeTimeRanges(projectInstance.getSegmentConfig().getAutoMergeTimeRanges());
        response.setVolatileRange(projectInstance.getSegmentConfig().getVolatileRange());
        response.setRetentionRange(projectInstance.getSegmentConfig().getRetentionRange());
        response.setCreateEmptySegmentEnabled(projectInstance.getSegmentConfig().getCreateEmptySegmentEnabled());

        response.setFavoriteQueryThreshold(config.getFavoriteQueryAccelerateThreshold());
        response.setFavoriteQueryTipsEnabled(config.getFavoriteQueryAccelerateTipsEnabled());

        response.setDataLoadEmptyNotificationEnabled(config.getJobDataLoadEmptyNotificationEnabled());
        response.setJobErrorNotificationEnabled(config.getJobErrorNotificationEnabled());

        List<String> jobStatesNotification = config.getJobNotificationStates() == null ? Lists.newArrayList()
                : Arrays.stream(config.getJobNotificationStates()).map(String::toLowerCase)
                        .collect(Collectors.toList());
        boolean jobErrorNotificationEnabled = config.getJobErrorNotificationEnabled();
        if (jobErrorNotificationEnabled && !jobStatesNotification.contains("error")) {
            jobStatesNotification.add("error");
        }
        response.setJobStatesNotification(jobStatesNotification);

        response.setJobNotificationEmails(Lists.newArrayList(config.getAdminDls()));

        response.setFrequencyTimeWindow(config.getFrequencyTimeWindowInDays());

        response.setLowFrequencyThreshold(config.getLowFrequencyThreshold());

        response.setYarnQueue(config.getOptional(SPARK_YARN_QUEUE, DEFAULT_VAL));

        response.setExposeComputedColumn(config.exposeComputedColumn());

        response.setKerberosProjectLevelEnabled(config.getKerberosProjectLevelEnable());

        response.setPrincipal(projectInstance.getPrincipal());
        // return favorite rules
        response.setFavoriteRules(projectSmartService != null ? projectSmartService.getFavoriteRules(project) : null);

        response.setScd2Enabled(config.isQueryNonEquiJoinModelEnabled());

        response.setSnapshotManualManagementEnabled(config.isSnapshotManualManagementEnabled());
        response.setSnapshotAutoRefreshEnabled(config.isSnapshotAutoRefreshEnabled());
        setSnapshotAutoRefreshParams(response, config.getSnapshotAutoRefreshCron());

        response.setMultiPartitionEnabled(config.isMultiPartitionEnabled());

        response.setQueryHistoryDownloadMaxSize(config.getQueryHistoryDownloadMaxSize());

        response.setQueryHistoryDownloadMaxSize(config.getQueryHistoryDownloadMaxSize());

        response.setJdbcSourceName(config.getJdbcSourceName());
        response.setJdbcSourceUser(config.getJdbcUser());
        response.setJdbcSourcePass(HIDDEN_VALUE);
        response.setJdbcSourceConnectionUrl(config.getJdbcConnectionUrl());
        response.setJdbcSourceEnable(config.getJdbcEnable());
        response.setJdbcSourceDriver(config.getJdbcDriver());

        Pair<String, String> infos = KylinVersion.getGitCommitInfo();
        response.setGitCommit(infos.getFirst());
        response.setPackageVersion(KylinVersion.getCurrentVersion().toString());
        response.setPackageTimestamp(infos.getSecond());

        if (SecondStorageUtil.isGlobalEnable()) {
            response.setSecondStorageEnabled(SecondStorageUtil.isProjectEnable(project));
            response.setSecondStorageNodes(SecondStorageUtil.listProjectNodes(project));
        }

        return response;
    }

    public void setSnapshotAutoRefreshParams(ProjectConfigResponse response, String cron) {
        if (!response.isSnapshotAutoRefreshEnabled()) {
            return;
        }
        val fields = org.springframework.util.StringUtils.tokenizeToStringArray(cron, " ");
        if (fields.length != 6) {
            throw new IllegalArgumentException(String
                    .format("Cron expression must consist of 6 fields (found %d in \"%s\")", fields.length, cron));
        }
        if (StringUtils.contains(fields[1], "*/")) {
            response.setSnapshotAutoRefreshTimeMode(SNAPSHOT_AUTO_REFRESH_TIME_MODE_MINUTE);
            response.setSnapshotAutoRefreshTimeInterval(StringUtils.substring(fields[1], 2));
            return;
        }
        if (StringUtils.contains(fields[2], "*/")) {
            response.setSnapshotAutoRefreshTimeMode(SNAPSHOT_AUTO_REFRESH_TIME_MODE_HOURS);
            response.setSnapshotAutoRefreshTimeInterval(StringUtils.substring(fields[2], 2));
            return;
        }
        if (StringUtils.contains(fields[3], "*/")) {
            response.setSnapshotAutoRefreshTimeMode(SNAPSHOT_AUTO_REFRESH_TIME_MODE_DAY);
            response.setSnapshotAutoRefreshTimeInterval(StringUtils.substring(fields[3], 2));
            response.setSnapshotAutoRefreshTriggerSecond(fields[0]);
            response.setSnapshotAutoRefreshTriggerMinute(fields[1]);
            response.setSnapshotAutoRefreshTriggerHours(fields[2]);
        }
    }

    public ProjectConfigResponse getProjectConfig(String project) {
        aclEvaluate.checkProjectReadPermission(project);
        return getProjectConfig0(project);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateShardNumConfig(String project, ShardNumConfigRequest req) {
        getManager(NProjectManager.class).updateProject(project, copyForWrite -> {
            try {
                copyForWrite.putOverrideKylinProps("kylin.engine.shard-num-json",
                        JsonUtil.writeValueAsString(req.getColToNum()));
            } catch (JsonProcessingException e) {
                logger.error("Can not write obj to json.", e);
            }
        });
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public String getShardNumConfig(String project) {
        return getManager(NProjectManager.class).getProject(project).getConfig().getExtendedOverrides()
                .getOrDefault("kylin.engine.shard-num-json", "");
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updatePushDownConfig(String project, PushDownConfigRequest pushDownConfigRequest) {
        getManager(NProjectManager.class).updateProject(project, copyForWrite -> {
            if (Boolean.TRUE.equals(pushDownConfigRequest.getPushDownEnabled())) {
                String runnerClassName = copyForWrite.getConfig().getPushDownRunnerClassName();
                if (StringUtils.isEmpty(runnerClassName)) {
                    val defaultPushDownRunner = getConfig().getPushDownRunnerClassNameWithDefaultValue();
                    copyForWrite.putOverrideKylinProps(KYLIN_QUERY_PUSHDOWN_RUNNER_CLASS_NAME, defaultPushDownRunner);
                }
                copyForWrite.putOverrideKylinProps("kylin.query.pushdown-enabled", KylinConfigBase.TRUE);
            } else {
                copyForWrite.putOverrideKylinProps("kylin.query.pushdown-enabled", KylinConfigBase.FALSE);
            }
        });
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateSnapshotConfig(String project, SnapshotConfigRequest snapshotConfigRequest) {
        checkSnapshotAutoRefreshConfig(snapshotConfigRequest);
        val projectManager = getManager(NProjectManager.class);
        projectManager.updateProject(project, copyForWrite -> {
            copyForWrite.putOverrideKylinProps("kylin.snapshot.manual-management-enabled",
                    snapshotConfigRequest.getSnapshotManualManagementEnabled().toString());
            if (Boolean.TRUE.equals(snapshotConfigRequest.getSnapshotManualManagementEnabled())) {
                copyForWrite.putOverrideKylinProps("kylin.snapshot.auto-refresh-enabled",
                        snapshotConfigRequest.getSnapshotAutoRefreshEnabled().toString());
                Optional.ofNullable(createSnapshotAutoRefreshCron(snapshotConfigRequest)).ifPresent(
                        cron -> copyForWrite.putOverrideKylinProps("kylin.snapshot.auto-refresh-cron", cron));
            }
        });
    }

    public String createSnapshotAutoRefreshCron(SnapshotConfigRequest snapshotConfigRequest) {
        if (Boolean.FALSE.equals(snapshotConfigRequest.getSnapshotAutoRefreshEnabled())) {
            return null;
        }
        String cron;
        switch (snapshotConfigRequest.getSnapshotAutoRefreshTimeMode()) {
        case SNAPSHOT_AUTO_REFRESH_TIME_MODE_DAY:
            cron = String.format(Locale.ROOT, "%s %s %s */%s * ?",
                    snapshotConfigRequest.getSnapshotAutoRefreshTriggerSecond(),
                    snapshotConfigRequest.getSnapshotAutoRefreshTriggerMinute(),
                    snapshotConfigRequest.getSnapshotAutoRefreshTriggerHours(),
                    snapshotConfigRequest.getSnapshotAutoRefreshTimeInterval());
            break;
        case SNAPSHOT_AUTO_REFRESH_TIME_MODE_HOURS:
            cron = String.format(Locale.ROOT, "0 0 */%s * * ?",
                    snapshotConfigRequest.getSnapshotAutoRefreshTimeInterval());
            break;
        case SNAPSHOT_AUTO_REFRESH_TIME_MODE_MINUTE:
            cron = String.format(Locale.ROOT, "0 */%s * * * ?",
                    snapshotConfigRequest.getSnapshotAutoRefreshTimeInterval());
            break;
        default:
            throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, SNAPSHOT_AUTO_REFRESH_TIME_MODE,
                    SNAPSHOT_AUTO_REFRESH_TIME_MODES);
        }
        if (!CronExpression.isValidExpression(cron)) {
            throw new KylinException(CONFIG_NOT_SUPPORT_EDIT, "kylin.snapshot.automatic_refresh_cron");
        }
        return cron;
    }

    public void checkSnapshotAutoRefreshConfig(SnapshotConfigRequest snapshotConfigRequest) {
        if (Boolean.FALSE.equals(snapshotConfigRequest.getSnapshotManualManagementEnabled())
                || Boolean.FALSE.equals(snapshotConfigRequest.getSnapshotAutoRefreshEnabled())) {
            return;
        }
        if (StringUtils.isBlank(snapshotConfigRequest.getSnapshotAutoRefreshTimeMode())) {
            throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, SNAPSHOT_AUTO_REFRESH_TIME_MODE,
                    SNAPSHOT_AUTO_REFRESH_TIME_MODES);
        }
        val autoRefreshTimeInterval = "snapshot_automatic_refresh_time_interval";
        val autoRefreshTimeIntervalValue = snapshotConfigRequest.getSnapshotAutoRefreshTimeInterval();
        switch (snapshotConfigRequest.getSnapshotAutoRefreshTimeMode()) {
        case SNAPSHOT_AUTO_REFRESH_TIME_MODE_DAY:
            checkSnapshotAutoParam(autoRefreshTimeInterval, autoRefreshTimeIntervalValue, 1, null, "> 1");
            checkSnapshotAutoTriggerTime(snapshotConfigRequest);
            break;
        case SNAPSHOT_AUTO_REFRESH_TIME_MODE_HOURS:
            checkSnapshotAutoParam(autoRefreshTimeInterval, autoRefreshTimeIntervalValue, 1, 23, "1 ~ 23");
            break;
        case SNAPSHOT_AUTO_REFRESH_TIME_MODE_MINUTE:
            checkSnapshotAutoParam(autoRefreshTimeInterval, autoRefreshTimeIntervalValue, 1, 59, "1 ~ 59");
            break;
        default:
            throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, SNAPSHOT_AUTO_REFRESH_TIME_MODE,
                    SNAPSHOT_AUTO_REFRESH_TIME_MODES);
        }
    }

    public void checkSnapshotAutoParam(String name, String value, Integer minValue, Integer maxValue, String message) {
        if (StringUtils.isBlank(value) || !StringUtils.isNumeric(value) || Integer.parseInt(value) < minValue) {
            throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, name, message);
        }
        if (null != maxValue && Integer.parseInt(value) > maxValue) {
            throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, name, message);
        }
    }

    public void checkSnapshotAutoTriggerTime(SnapshotConfigRequest snapshotConfigRequest) {
        checkSnapshotAutoParam("snapshot_automatic_refresh_trigger_hours",
                snapshotConfigRequest.getSnapshotAutoRefreshTriggerHours(), 0, 23, "0 ~ 23");
        checkSnapshotAutoParam("snapshot_automatic_refresh_trigger_minute",
                snapshotConfigRequest.getSnapshotAutoRefreshTriggerMinute(), 0, 59, "0 ~ 59");
        checkSnapshotAutoParam("snapshot_automatic_refresh_trigger_second",
                snapshotConfigRequest.getSnapshotAutoRefreshTriggerSecond(), 0, 59, "0 ~ 59");
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateSCD2Config(String project, SCD2ConfigRequest scd2ConfigRequest,
            ProjectModelSupporter modelService) {
        getManager(NProjectManager.class).updateProject(project,
                copyForWrite -> copyForWrite.putOverrideKylinProps("kylin.query.non-equi-join-model-enabled",
                        scd2ConfigRequest.getScd2Enabled().toString()));

        if (Boolean.TRUE.equals(scd2ConfigRequest.getScd2Enabled())) {
            modelService.onUpdateSCD2ModelStatus(project, RealizationStatusEnum.ONLINE);
        } else {
            modelService.onUpdateSCD2ModelStatus(project, RealizationStatusEnum.OFFLINE);
        }
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateMultiPartitionConfig(String project, MultiPartitionConfigRequest request,
            ProjectModelSupporter modelService) {
        getManager(NProjectManager.class).updateProject(project, copyForWrite -> {
            if (Boolean.TRUE.equals(request.getMultiPartitionEnabled())) {
                copyForWrite.getOverrideKylinProps().put("kylin.model.multi-partition-enabled", KylinConfigBase.TRUE);
            } else {
                copyForWrite.getOverrideKylinProps().put("kylin.model.multi-partition-enabled", KylinConfigBase.FALSE);
                modelService.onOfflineMultiPartitionModels(project);
            }
        });
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updatePushDownProjectConfig(String project, PushDownProjectConfigRequest pushDownProjectConfigRequest) {
        getManager(NProjectManager.class).updateProject(project, copyForWrite -> {
            copyForWrite.putOverrideKylinProps(KYLIN_QUERY_PUSHDOWN_RUNNER_CLASS_NAME,
                    pushDownProjectConfigRequest.getRunnerClassName());
            copyForWrite.putOverrideKylinProps("kylin.query.pushdown.converter-class-names",
                    pushDownProjectConfigRequest.getConverterClassNames());
        });
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateComputedColumnConfig(String project, ComputedColumnConfigRequest computedColumnConfigRequest) {
        getManager(NProjectManager.class).updateProject(project,
                copyForWrite -> copyForWrite.putOverrideKylinProps(ProjectInstance.EXPOSE_COMPUTED_COLUMN_CONF,
                        String.valueOf(computedColumnConfigRequest.getExposeComputedColumn())));
    }

    @Transaction(project = 0)
    public void updateSegmentConfig(String project, SegmentConfigRequest segmentConfigRequest) {
        aclEvaluate.checkProjectAdminPermission(project);
        //api send volatileRangeEnabled = false but finally it is reset to true
        segmentConfigRequest.getVolatileRange().setVolatileRangeEnabled(true);
        if (segmentConfigRequest.getVolatileRange().getVolatileRangeNumber() < 0) {
            throw new KylinException(INVALID_PARAMETER,
                    "No valid value. Please set an integer 'x' to "
                            + "'volatile_range_number'. The 'Auto-Merge' will not merge latest 'x' "
                            + "period(day/week/month/etc..) segments.");
        }
        if (segmentConfigRequest.getRetentionRange().getRetentionRangeNumber() < 0) {
            throw new KylinException(INVALID_PARAMETER, "No valid value for 'retention_range_number'."
                    + " Please set an integer 'x' to specify the retention threshold. The system will "
                    + "only retain the segments in the retention threshold (x years before the last data time). ");
        }
        if (segmentConfigRequest.getAutoMergeTimeRanges().isEmpty()) {
            throw new KylinException(INVALID_PARAMETER, "No valid value for 'auto_merge_time_ranges'. Please set "
                    + "{'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR'} to specify the period of auto-merge. ");
        }
        if (null == segmentConfigRequest.getRetentionRange().getRetentionRangeType()) {
            throw new KylinException(INVALID_PARAMETER,
                    "No valid value for 'retention_range_type', Please set {'DAY', 'MONTH', 'YEAR'} to specify the period of retention. ");
        }

        getManager(NProjectManager.class).updateProject(project, copyForWrite -> {
            copyForWrite.getSegmentConfig().setAutoMergeEnabled(segmentConfigRequest.getAutoMergeEnabled());
            copyForWrite.getSegmentConfig().setAutoMergeTimeRanges(segmentConfigRequest.getAutoMergeTimeRanges());
            copyForWrite.getSegmentConfig().setVolatileRange(segmentConfigRequest.getVolatileRange());
            copyForWrite.getSegmentConfig().setRetentionRange(segmentConfigRequest.getRetentionRange());
            copyForWrite.getSegmentConfig()
                    .setCreateEmptySegmentEnabled(segmentConfigRequest.getCreateEmptySegmentEnabled());
        });
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateProjectGeneralInfo(String project, ProjectGeneralInfoRequest projectGeneralInfoRequest) {
        getManager(NProjectManager.class).updateProject(project, copyForWrite -> {
            copyForWrite.setDescription(projectGeneralInfoRequest.getDescription());
            copyForWrite.putOverrideKylinProps("kylin.metadata.semi-automatic-mode",
                    String.valueOf(projectGeneralInfoRequest.isSemiAutoMode()));
        });
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateProjectKerberosInfo(String project, ProjectKerberosInfoRequest projectKerberosInfoRequest)
            throws Exception {
        KerberosLoginManager.getInstance().checkAndReplaceProjectKerberosInfo(project,
                projectKerberosInfoRequest.getPrincipal());
        getManager(NProjectManager.class).updateProject(project, copyForWrite -> {
            copyForWrite.setPrincipal(projectKerberosInfoRequest.getPrincipal());
            copyForWrite.setKeytab(projectKerberosInfoRequest.getKeytab());
        });

        backupAndDeleteKeytab(projectKerberosInfoRequest.getPrincipal());
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    @Transaction(project = 0)
    public void dropProject(String project) {
        if (SecondStorageUtil.isProjectEnable(project)) {
            throw new KylinException(PROJECT_DROP_FAILED,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getProjectDropFailedSecondStorageEnabled(), project));
        }

        val kylinConfig = KylinConfig.getInstanceFromEnv();
        NExecutableManager nExecutableManager = NExecutableManager.getInstance(kylinConfig, project);
        List<String> jobIds = nExecutableManager.getJobs().stream().map(nExecutableManager::getJob)
                .filter(Objects::nonNull)
                .filter(abstractExecutable -> (abstractExecutable.getStatus().toJobStatus() == JobStatusEnum.RUNNING)
                        || (abstractExecutable.getStatus().toJobStatus() == JobStatusEnum.PENDING)
                        || (abstractExecutable.getStatus().toJobStatus() == JobStatusEnum.STOPPED))
                .map(AbstractExecutable::getId).collect(Collectors.toList());
        val streamingJobStatusList = Arrays.asList(JobStatusEnum.STARTING, JobStatusEnum.RUNNING,
                JobStatusEnum.STOPPING);
        val streamingJobList = getManager(StreamingJobManager.class, project).listAllStreamingJobMeta().stream()
                .filter(meta -> streamingJobStatusList.contains(meta.getCurrentStatus()))
                .map(RootPersistentEntity::getUuid).collect(Collectors.toList());
        if (!jobIds.isEmpty() || !streamingJobList.isEmpty()) {
            logger.warn("The following jobs are in running or pending status and should be killed before dropping"
                    + " the project {} : {}", project, jobIds);
            throw new KylinException(PROJECT_DROP_FAILED,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getProjectDropFailedJobsNotKilled(), project));
        }

        NProjectManager prjManager = getManager(NProjectManager.class);
        prjManager.forceDropProject(project);
        UnitOfWork.get().doAfterUnit(() -> new ProjectDropListener().onDelete(project));
        EventBusFactory.getInstance().postAsync(new SourceUsageUpdateNotifier());
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateDefaultDatabase(String project, String defaultDatabase) {
        Preconditions.checkNotNull(project);
        Preconditions.checkNotNull(defaultDatabase);
        String uppderDB = defaultDatabase.toUpperCase(Locale.ROOT);

        val prjManager = getManager(NProjectManager.class);
        val tableManager = getManager(NTableMetadataManager.class, project);
        if (ProjectInstance.DEFAULT_DATABASE.equals(uppderDB)
                || tableManager.dbToTablesMap(getConfig().streamingEnabled()).containsKey(uppderDB)) {
            final ProjectInstance projectInstance = prjManager.getProject(project);
            if (uppderDB.equals(projectInstance.getDefaultDatabase())) {
                return;
            }
            projectInstance.setDefaultDatabase(uppderDB);
            prjManager.updateProject(projectInstance);
        } else {
            throw new KylinException(DATABASE_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getDatabaseNotExist(), defaultDatabase));
        }
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public String backupProject(String project) throws Exception {
        return metadataBackupService.backupProject(project);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public void clearManagerCache(String project) {
        val config = KylinConfig.getInstanceFromEnv();
        config.clearManagersByProject(project);
        config.clearManagersByClz(NProjectManager.class);
    }

    @Transaction(project = 0)
    public void setDataSourceType(String project, String sourceType) {
        getManager(NProjectManager.class).updateProject(project,
                copyForWrite -> copyForWrite.putOverrideKylinProps(DATASOURCE_TYPE.getValue(), sourceType));
    }

    public String getDataSourceType(String project) {
        return getManager(NProjectManager.class).getProject(project).getOverrideKylinProps()
                .get(DATASOURCE_TYPE.getValue());
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateGarbageCleanupConfig(String project, GarbageCleanUpConfigRequest garbageCleanUpConfigRequest) {
        if (garbageCleanUpConfigRequest.getLowFrequencyThreshold() < 0L) {
            throw new KylinException(INVALID_PARAMETER,
                    "No valid value for 'low_frequency_threshold'. Please "
                            + "set an integer 'x' greater than or equal to 0 to specify the low usage storage "
                            + "calculation time. When index usage is lower than 'x' times, it would be regarded "
                            + "as low usage storage.");
        }
        Map<String, String> overrideKylinProps = Maps.newHashMap();
        overrideKylinProps.put("kylin.cube.low-frequency-threshold",
                String.valueOf(garbageCleanUpConfigRequest.getLowFrequencyThreshold()));
        overrideKylinProps.put("kylin.cube.frequency-time-window",
                String.valueOf(garbageCleanUpConfigRequest.getFrequencyTimeWindow()));
        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public ProjectConfigResponse resetProjectConfig(String project, String resetItem) {
        Preconditions.checkNotNull(resetItem);
        switch (resetItem) {
        case "job_notification_config":
            resetJobNotificationConfig(project);
            break;
        case "query_accelerate_threshold":
            resetQueryAccelerateThreshold(project);
            break;
        case "garbage_cleanup_config":
            resetGarbageCleanupConfig(project);
            break;
        case "segment_config":
            resetSegmentConfig(project);
            break;
        case "kerberos_project_level_config":
            resetProjectKerberosConfig(project);
            break;
        case "storage_quota_config":
            resetProjectStorageQuotaConfig(project);
            break;
        case "favorite_rule_config":
            resetProjectRecommendationConfig(project);
            break;
        case "table_exclusion_config":
            resetTableExclusionConfig(project);
            break;
        default:
            throw new KylinException(INVALID_PARAMETER,
                    "No valid value for 'reset_item'. Please enter a project setting "
                            + "type which needs to be reset {'job_notification_config'，"
                            + "'query_accelerate_threshold'，'garbage_cleanup_config'，'segment_config', 'storage_quota_config'} to 'reset_item'.");
        }
        return getProjectConfig(project);
    }

    @Transaction(project = 0)
    public void updateProjectOwner(String project, OwnerChangeRequest ownerChangeRequest) {
        try {
            aclEvaluate.checkIsGlobalAdmin();
            checkTargetOwnerPermission(project, ownerChangeRequest.getOwner());
        } catch (AccessDeniedException e) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getProjectChangePermission());
        } catch (IOException e) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getOwnerChangeError());
        }

        getManager(NProjectManager.class).updateProject(project,
                copyForWrite -> copyForWrite.setOwner(ownerChangeRequest.getOwner()));
    }

    private void checkTargetOwnerPermission(String project, String owner) throws IOException {
        Set<String> projectAdminUsers = accessService.getProjectAdminUsers(project);
        projectAdminUsers.remove(getManager(NProjectManager.class).getProject(project).getOwner());
        if (CollectionUtils.isEmpty(projectAdminUsers) || !projectAdminUsers.contains(owner)) {
            Message msg = MsgPicker.getMsg();
            throw new KylinException(PERMISSION_DENIED, msg.getProjectOwnerChangeInvalidUser());
        }
    }

    private void resetJobNotificationConfig(String project) {
        Set<String> toBeRemovedProps = Sets.newHashSet();
        toBeRemovedProps.add("kylin.job.notification-on-empty-data-load");
        toBeRemovedProps.add("kylin.job.notification-on-job-error");
        toBeRemovedProps.add("kylin.job.notification-enable-states");
        toBeRemovedProps.add("kylin.job.notification-admin-emails");
        removeProjectOverrideProps(project, toBeRemovedProps);
    }

    private void resetQueryAccelerateThreshold(String project) {
        Set<String> toBeRemovedProps = Sets.newHashSet();
        toBeRemovedProps.add("kylin.favorite.query-accelerate-threshold");
        toBeRemovedProps.add("kylin.favorite.query-accelerate-tips-enable");
        removeProjectOverrideProps(project, toBeRemovedProps);
    }

    private void resetProjectRecommendationConfig(String project) {
        getManager(FavoriteRuleManager.class, project).resetRule();
        NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project).listAllModels()
                .forEach(model -> projectModelSupporter.onModelUpdate(project, model.getUuid()));
    }

    private void resetGarbageCleanupConfig(String project) {
        Set<String> toBeRemovedProps = Sets.newHashSet();
        toBeRemovedProps.add("kylin.cube.low-frequency-threshold");
        toBeRemovedProps.add("kylin.cube.frequency-time-window");
        removeProjectOverrideProps(project, toBeRemovedProps);
    }

    private void resetTableExclusionConfig(String project) {
        Set<String> toBeRemovedProps = Sets.newHashSet();
        toBeRemovedProps.add("kylin.metadata.table-exclusion-enabled");
        removeProjectOverrideProps(project, toBeRemovedProps);
    }

    private void resetSegmentConfig(String project) {
        getManager(NProjectManager.class).updateProject(project, copyForWrite -> {
            val projectInstance = new ProjectInstance();
            copyForWrite.getSegmentConfig()
                    .setAutoMergeEnabled(projectInstance.getSegmentConfig().getAutoMergeEnabled());
            copyForWrite.getSegmentConfig()
                    .setAutoMergeTimeRanges(projectInstance.getSegmentConfig().getAutoMergeTimeRanges());
            copyForWrite.getSegmentConfig().setVolatileRange(projectInstance.getSegmentConfig().getVolatileRange());
            copyForWrite.getSegmentConfig().setRetentionRange(projectInstance.getSegmentConfig().getRetentionRange());
        });
    }

    private void removeProjectOverrideProps(String project, Set<String> toBeRemovedProps) {
        val projectManager = getManager(NProjectManager.class);
        val projectInstance = projectManager.getProject(project);
        if (projectInstance == null) {
            throw new KylinException(PROJECT_NOT_EXIST, project);
        }
        projectManager.updateProject(project,
                copyForWrite -> toBeRemovedProps.forEach(copyForWrite.getOverrideKylinProps()::remove));
    }

    private void resetProjectKerberosConfig(String project) {
        val projectManager = getManager(NProjectManager.class);
        val projectInstance = projectManager.getProject(project);
        if (projectInstance == null) {
            throw new KylinException(PROJECT_NOT_EXIST, project);
        }
        getManager(NProjectManager.class).updateProject(project, copyForWrite -> {
            copyForWrite.setKeytab(null);
            copyForWrite.setPrincipal(null);
        });
    }

    private void resetProjectStorageQuotaConfig(String project) {
        Set<String> toBeRemovedProps = Sets.newHashSet();
        toBeRemovedProps.add("kylin.storage.quota-in-giga-bytes");
        removeProjectOverrideProps(project, toBeRemovedProps);
    }

    private List<ProjectInstance> getProjectsWithFilter(Predicate<ProjectInstance> filter) {
        val allProjects = getManager(NProjectManager.class).listAllProjects();
        return allProjects.stream().filter(filter).collect(Collectors.toList());
    }

    public File backupAndDeleteKeytab(String principal) throws IOException {
        String kylinConfHome = KapConfig.getKylinConfDirAtBestEffort();
        File kTempFile = new File(kylinConfHome, principal + KerberosLoginManager.TMP_KEYTAB_SUFFIX);
        File kFile = new File(kylinConfHome, principal + KerberosLoginManager.KEYTAB_SUFFIX);
        if (kTempFile.exists()) {
            FileUtils.copyFile(kTempFile, kFile);
            FileUtils.forceDelete(kTempFile);
        }
        return kFile;
    }

    public File generateTempKeytab(String principal, MultipartFile keytabFile) throws IOException {
        Message msg = MsgPicker.getMsg();
        if (null == principal || principal.isEmpty()) {
            throw new KylinException(EMPTY_PARAMETER, msg.getPrincipalEmpty());
        }
        val originalFilename = keytabFile.getOriginalFilename();
        if (originalFilename == null || !originalFilename.endsWith(".keytab")) {
            throw new KylinException(FILE_TYPE_MISMATCH, msg.getKeytabFileTypeMismatch());
        }
        String kylinConfHome = KapConfig.getKylinConfDirAtBestEffort();
        File kFile = new File(kylinConfHome, principal + KerberosLoginManager.TMP_KEYTAB_SUFFIX);
        FileUtils.copyInputStreamToFile(keytabFile.getInputStream(), kFile);
        return kFile;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateProjectConfig(String project, Map<String, String> overrides) {
        if (MapUtils.isEmpty(overrides)) {
            throw new KylinException(EMPTY_PARAMETER, "config map is required");
        }
        updateProjectOverrideKylinProps(project, overrides);
    }

    @Transaction(project = 0)
    public void deleteProjectConfig(String project, String configName) {
        aclEvaluate.checkProjectAdminPermission(project);
        val projectManager = getManager(NProjectManager.class);
        projectManager.updateProject(project, copyForWrite -> copyForWrite.getOverrideKylinProps().remove(configName));
    }

    @Transaction(project = 0)
    public void updateJdbcConfig(String project, JdbcRequest jdbcRequest) {
        Map<String, String> overrideKylinProps = Maps.newLinkedHashMap();
        overrideKylinProps.put("kylin.source.jdbc.connection-url", jdbcRequest.getUrl());
        overrideKylinProps.put("kylin.source.jdbc.driver", jdbcRequest.getDriver());
        overrideKylinProps.put("kylin.source.jdbc.user", jdbcRequest.getUser());
        overrideKylinProps.put("kylin.source.jdbc.pass", jdbcRequest.getPass());
        overrideKylinProps.put("kylin.source.jdbc.dialect", jdbcRequest.getDialect());
        overrideKylinProps.put("kylin.source.jdbc.adaptor", jdbcRequest.getAdaptor());
        if (!Strings.isNullOrEmpty(jdbcRequest.getPushdownClass())) {
            overrideKylinProps.put(KYLIN_QUERY_PUSHDOWN_RUNNER_CLASS_NAME, jdbcRequest.getPushdownClass());
            overrideKylinProps.put("kylin.query.pushdown.partition-check.runner-class-name",
                    jdbcRequest.getPushdownClass());
        }
        if (!Strings.isNullOrEmpty(jdbcRequest.getSourceConnector())) {
            overrideKylinProps.put("kylin.source.jdbc.connector-class-name", jdbcRequest.getSourceConnector());
        }
        // Use JDBC Source
        overrideKylinProps.put("kylin.source.default", String.valueOf(ISourceAware.ID_JDBC));
        updateProjectOverrideKylinProps(project, overrideKylinProps);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void updateTableExclusionRule(String project, ProjectExclusionRequest request) {
        getManager(NProjectManager.class).updateProject(project, copyForWrite -> {
            boolean exclusionEnabled = request.isTableExclusionEnabled();
            copyForWrite.putOverrideKylinProps("kylin.metadata.table-exclusion-enabled",
                    String.valueOf(exclusionEnabled));
        });
    }
}
