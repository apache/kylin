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

package org.apache.kylin.streaming.jobs.impl;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.kylin.common.persistence.ResourceStore.STREAMING_RESOURCE_ROOT;
import static org.apache.kylin.streaming.constants.StreamingConstants.REST_SERVER_IP;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_CORES_MAX;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_DRIVER_MEM;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_DRIVER_MEM_DEFAULT;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_DRIVER_OPTS;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_EXECUTOR_CORES;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_EXECUTOR_CORES_DEFAULT;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_EXECUTOR_INSTANCES;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_EXECUTOR_INSTANCES_DEFAULT;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_EXECUTOR_MEM;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_EXECUTOR_MEM_DEFAULT;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_EXECUTOR_OPTS;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_MASTER;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_MASTER_DEFAULT;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_SHUFFLE_PARTITIONS;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_SHUFFLE_PARTITIONS_DEFAULT;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_STREAMING_ENTRY;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_STREAMING_MERGE_ENTRY;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_YARN_AM_OPTS;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_YARN_DIST_JARS;
import static org.apache.kylin.streaming.constants.StreamingConstants.SPARK_YARN_TIMELINE_SERVICE;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_DURATION;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_DURATION_DEFAULT;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_META_URL;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_META_URL_DEFAULT;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_SEGMENT_MAX_SIZE;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_SEGMENT_MAX_SIZE_DEFAULT;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD_DEFAULT;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_WATERMARK;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_WATERMARK_DEFAULT;

import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.common.persistence.ImageDesc;
import org.apache.kylin.common.persistence.metadata.HDFSMetadataStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.streaming.app.StreamingEntry;
import org.apache.kylin.streaming.app.StreamingMergeEntry;
import org.apache.kylin.streaming.event.StreamingJobMetaCleanEvent;
import org.apache.kylin.streaming.jobs.AbstractSparkJobLauncher;
import org.apache.kylin.streaming.jobs.StreamingJobUtils;
import org.apache.kylin.streaming.util.MetaInfoUpdater;
import org.apache.spark.launcher.SparkLauncher;

import com.google.common.base.Preconditions;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingJobLauncher extends AbstractSparkJobLauncher {
    private static final String KRB5CONF_PROPS = "java.security.krb5.conf";
    private static final String JAASCONF_PROPS = "java.security.auth.login.config";
    private Map<String, String> jobParams;
    private String mainClazz;
    private String[] appArgs;
    private Long currentTimestamp;
    private StorageURL distMetaStorageUrl;

    // TODO: support yarn cluster
    private boolean isYarnCluster = false;
    @Getter
    private boolean initialized = false;

    @Override
    public void init(String project, String modelId, JobTypeEnum jobType) {
        super.init(project, modelId, jobType);
        jobParams = strmJob.getParams();
        currentTimestamp = System.currentTimeMillis();

        //reload configuration from job params
        this.config = StreamingJobUtils.getStreamingKylinConfig(this.config, jobParams, modelId, project);

        //dump metadata and init storage url
        initStorageUrl();

        switch (jobType) {
        case STREAMING_BUILD: {
            this.mainClazz = SPARK_STREAMING_ENTRY;
            this.appArgs = new String[] { project, modelId,
                    jobParams.getOrDefault(STREAMING_DURATION, STREAMING_DURATION_DEFAULT),
                    jobParams.getOrDefault(STREAMING_WATERMARK, STREAMING_WATERMARK_DEFAULT),
                    distMetaStorageUrl.toString() };
            break;
        }
        case STREAMING_MERGE: {
            this.mainClazz = SPARK_STREAMING_MERGE_ENTRY;
            this.appArgs = new String[] { project, modelId,
                    jobParams.getOrDefault(STREAMING_SEGMENT_MAX_SIZE, STREAMING_SEGMENT_MAX_SIZE_DEFAULT), jobParams
                            .getOrDefault(STREAMING_SEGMENT_MERGE_THRESHOLD, STREAMING_SEGMENT_MERGE_THRESHOLD_DEFAULT),
                    distMetaStorageUrl.toString() };
            break;
        }
        default:
            throw new IllegalArgumentException("The streaming job Type " + jobType.name() + " is not supported...");

        }
        initialized = true;
    }

    private String getDriverHDFSLogPath() {
        return String.format(Locale.ROOT, "%s/%s/%s/%s/driver.%s.log", config.getStreamingBaseJobsLocation(), project,
                jobId, currentTimestamp, currentTimestamp);
    }

    private String getJobTmpMetaStoreUrlPath() {
        return String.format(Locale.ROOT, "%s/%s/%s/meta", config.getStreamingBaseJobsLocation(), project, modelId);
    }

    private StorageURL getJobTmpHdfsMetaStorageUrl() {
        val params = new HashMap<String, String>();
        params.put("path", String.format(Locale.ROOT, "%s/meta_%d", getJobTmpMetaStoreUrlPath(), currentTimestamp));
        params.put("zip", "true");
        params.put("snapshot", "true");
        return new StorageURL(config.getMetadataUrlPrefix(), HDFSMetadataStore.HDFS_SCHEME, params);
    }

    protected Set<String> getMetadataDumpList() {
        val metaSet = NDataflowManager.getInstance(config, project).getDataflow(modelId)
                .collectPrecalculationResource();
        metaSet.add(ResourceStore.METASTORE_IMAGE);
        metaSet.add(String.format(Locale.ROOT, "/%s%s/%s", project, STREAMING_RESOURCE_ROOT, jobId));
        return metaSet;
    }

    @Nullable
    private String getAvailableLatestDumpPath() {
        val jobTmpMetaPath = getJobTmpMetaStoreUrlPath();
        HadoopUtil.mkdirIfNotExist(jobTmpMetaPath);
        val metaFileStatus = HadoopUtil.getFileStatusPathsFromHDFSDir(jobTmpMetaPath, false);
        val jobMetaRetainedTimeMills = config.getStreamingJobMetaRetainedTime();
        val outdatedMetaPathMap = metaFileStatus.stream()
                .collect(Collectors.groupingBy(locatedFileStatus -> currentTimestamp
                        - locatedFileStatus.getModificationTime() > jobMetaRetainedTimeMills));

        if (outdatedMetaPathMap.containsKey(TRUE)) {
            val deletedPath = outdatedMetaPathMap.get(TRUE).stream().map(FileStatus::getPath)
                    .collect(Collectors.toList());
            EventBusFactory.getInstance().postSync(new StreamingJobMetaCleanEvent(deletedPath));
            log.info("delete by {} streaming meta path size:{}", jobId, deletedPath.size());
        }

        if (!outdatedMetaPathMap.containsKey(FALSE)) {
            return null;
        }

        return outdatedMetaPathMap.get(FALSE).stream().max(Comparator.comparingLong(FileStatus::getModificationTime))
                .map(fileStatus -> fileStatus.getPath().toString()).orElse(null);
    }

    private void initStorageUrl() {
        // in local mode or ut env, or scheme is not hdfs
        // use origin config direct
        if (!StreamingUtils.isJobOnCluster(config) || !StringUtils.equals(HDFSMetadataStore.HDFS_SCHEME,
                jobParams.getOrDefault(STREAMING_META_URL, STREAMING_META_URL_DEFAULT))) {
            distMetaStorageUrl = config.getMetadataUrl();
            return;
        }

        StorageURL metaDumpUrl = getJobTmpHdfsMetaStorageUrl();

        Preconditions.checkState(StringUtils.isNotEmpty(metaDumpUrl.toString()), "Missing metaUrl!");

        val availableMetaPath = getAvailableLatestDumpPath();
        if (StringUtils.isNotEmpty(availableMetaPath)) {
            val maps = new HashMap<>(metaDumpUrl.getAllParameters());
            maps.put("path", availableMetaPath);
            distMetaStorageUrl = new StorageURL(metaDumpUrl.getIdentifier(), metaDumpUrl.getScheme(), maps);
            return;
        }

        val backupConfig = KylinConfig.createKylinConfig(config);
        backupConfig.setMetadataUrl(metaDumpUrl.toString());
        try (val backupResourceStore = ResourceStore.getKylinMetaStore(backupConfig)) {
            val backupMetadataStore = backupResourceStore.getMetadataStore();
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(
                    UnitOfWorkParams.builder().readonly(true).unitName(project).processor(() -> {
                        ResourceStore srcResourceStore = ResourceStore.getKylinMetaStore(config);
                        for (String resPath : getMetadataDumpList()) {
                            srcResourceStore.copy(resPath, backupResourceStore);
                        }
                        val auditLogStore = srcResourceStore.getAuditLogStore();
                        long finalOffset = auditLogStore.getLogOffset() == 0 ? srcResourceStore.getOffset()
                                : auditLogStore.getLogOffset();
                        backupResourceStore.putResourceWithoutCheck(ResourceStore.METASTORE_IMAGE,
                                ByteSource.wrap(JsonUtil.writeValueAsBytes(new ImageDesc(finalOffset))),
                                System.currentTimeMillis(), -1);
                        return null;
                    }).build());
            backupMetadataStore.dump(backupResourceStore);

            distMetaStorageUrl = metaDumpUrl;
            log.debug("dump meta success.{}", metaDumpUrl);
        } catch (Exception e) {
            log.error("dump meta error,{}", jobId, e);
        }
    }

    private String wrapDriverJavaOptions(Map<String, String> sparkConf) {
        val driverJavaOptsConfigStr = sparkConf.get(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS);

        Preconditions.checkNotNull(driverJavaOptsConfigStr, SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS + " is empty");
        StringBuilder driverJavaOptionsSB = new StringBuilder(driverJavaOptsConfigStr);
        val kapConfig = KapConfig.getInstanceFromEnv();
        if (kapConfig.isKerberosEnabled() && !driverJavaOptsConfigStr.contains(KRB5CONF_PROPS)) {
            val krb5conf = " -Djava.security.krb5.conf=" + kapConfig.getKerberosKrb5ConfPath();
            driverJavaOptionsSB.append(krb5conf);
        }
        if (kapConfig.isKafkaJaasEnabled() && !driverJavaOptsConfigStr.contains(JAASCONF_PROPS)) {
            val jaasConf = " -Djava.security.auth.login.config=" + kapConfig.getKafkaJaasConfPath();
            driverJavaOptionsSB.append(jaasConf);
        }
        driverJavaOptionsSB.append(javaPropertyFormatter(REST_SERVER_IP, AddressUtil.getLocalHostExactAddress()));
        driverJavaOptionsSB.append(javaPropertyFormatter("kylin.hdfs.working.dir", config.getHdfsWorkingDirectory()));
        driverJavaOptionsSB
                .append(javaPropertyFormatter("spark.driver.log4j.appender.hdfs.File", getDriverHDFSLogPath()));
        driverJavaOptionsSB.append(javaPropertyFormatter("user.timezone", config.getTimeZone()));

        final String driverLog4jXmlFile = config.getLogSparkStreamingDriverPropertiesFile();
        generateLog4jConfiguration(false, driverJavaOptionsSB, driverLog4jXmlFile);

        return driverJavaOptionsSB.toString();
    }

    private void generateLog4jConfiguration(boolean isExecutor, StringBuilder log4jJavaOptionsSB, String log4jXmlFile) {
        String log4jConfigStr = "file:" + log4jXmlFile;

        if (isExecutor || isYarnCluster || config.getSparkMaster().startsWith("k8s")) {
            // Direct file name.
            log4jConfigStr = Paths.get(log4jXmlFile).getFileName().toString();
        }

        log4jJavaOptionsSB.append(javaPropertyFormatter("log4j.configurationFile", log4jConfigStr));
    }

    private String wrapExecutorJavaOptions(Map<String, String> sparkConf) {
        val executorJavaOptsConfigStr = sparkConf.get(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS);

        Preconditions.checkNotNull(executorJavaOptsConfigStr, SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS + " is empty");

        StringBuilder executorJavaOptionsSB = new StringBuilder(executorJavaOptsConfigStr);
        val kapConfig = KapConfig.getInstanceFromEnv();
        if (kapConfig.isKerberosEnabled() && !executorJavaOptsConfigStr.contains(KRB5CONF_PROPS)) {
            val krb5Conf = " -Djava.security.krb5.conf=./__spark_conf__/__hadoop_conf__/"
                    + kapConfig.getKerberosKrb5Conf();
            executorJavaOptionsSB.append(krb5Conf);
        }
        if (kapConfig.isKafkaJaasEnabled() && !executorJavaOptsConfigStr.contains(JAASCONF_PROPS)) {
            val jaasConf = " -Djava.security.auth.login.config=./" + kapConfig.getKafkaJaasConf();
            executorJavaOptionsSB.append(jaasConf);
        }
        executorJavaOptionsSB.append(javaPropertyFormatter("kap.spark.identifier", jobId));
        executorJavaOptionsSB.append(javaPropertyFormatter("kap.spark.jobTimeStamp", currentTimestamp.toString()));
        executorJavaOptionsSB.append(javaPropertyFormatter("kap.spark.project", project));
        executorJavaOptionsSB.append(javaPropertyFormatter("user.timezone", config.getTimeZone()));
        if (StringUtils.isNotBlank(config.getMountSparkLogDir())) {
            executorJavaOptionsSB.append(javaPropertyFormatter("job.mountDir", config.getMountSparkLogDir()));
        }

        final String executorLog4jXmlFile = config.getLogSparkStreamingExecutorPropertiesFile();
        generateLog4jConfiguration(true, executorJavaOptionsSB, executorLog4jXmlFile);

        return executorJavaOptionsSB.toString();
    }

    private String wrapYarnAmJavaOptions(Map<String, String> sparkConf) {
        val yarnAmJavaOptsConfigStr = sparkConf.getOrDefault(SPARK_YARN_AM_OPTS, "");

        StringBuilder yarnAmJavaOptionsSB = new StringBuilder(yarnAmJavaOptsConfigStr);
        val kapConfig = KapConfig.getInstanceFromEnv();
        if (kapConfig.isKerberosEnabled() && !yarnAmJavaOptsConfigStr.contains(KRB5CONF_PROPS)) {
            val krb5Conf = " -Djava.security.krb5.conf=./__spark_conf__/__hadoop_conf__/"
                    + kapConfig.getKerberosKrb5Conf();
            yarnAmJavaOptionsSB.append(krb5Conf);
        }
        return yarnAmJavaOptionsSB.toString();
    }

    public void startYarnJob() throws Exception {
        Map<String, String> sparkConf = getStreamingSparkConfig(config);
        sparkConf.forEach((key, value) -> launcher.setConf(key, value));

        val numberOfExecutor = sparkConf.getOrDefault(SPARK_EXECUTOR_INSTANCES, SPARK_EXECUTOR_INSTANCES_DEFAULT);
        val numberOfCore = sparkConf.getOrDefault(SPARK_EXECUTOR_CORES, SPARK_EXECUTOR_CORES_DEFAULT);
        val sparkLauncher = launcher.setAppName(jobId).setSparkHome(KylinConfig.getSparkHome());
        val kapConfig = KapConfig.getInstanceFromEnv();
        if (kapConfig.isKerberosEnabled()) {
            sparkLauncher.setConf("spark.kerberos.keytab", kapConfig.getKerberosKeytabPath());
            sparkLauncher.setConf("spark.kerberos.principal", kapConfig.getKerberosPrincipal());
        }
        if (kapConfig.isKafkaJaasEnabled()) {
            sparkLauncher.addFile(kapConfig.getKafkaJaasConfPath());
        }
        sparkLauncher.setMaster(sparkConf.getOrDefault(SPARK_MASTER, SPARK_MASTER_DEFAULT))
                .setConf(SPARK_DRIVER_MEM, sparkConf.getOrDefault(SPARK_DRIVER_MEM, SPARK_DRIVER_MEM_DEFAULT))
                .setConf(SPARK_EXECUTOR_INSTANCES, numberOfExecutor).setConf(SPARK_EXECUTOR_CORES, numberOfCore)
                .setConf(SPARK_CORES_MAX, calcMaxCores(numberOfExecutor, numberOfCore))
                .setConf(SPARK_EXECUTOR_MEM, sparkConf.getOrDefault(SPARK_EXECUTOR_MEM, SPARK_EXECUTOR_MEM_DEFAULT))
                .setConf(SPARK_SHUFFLE_PARTITIONS,
                        sparkConf.getOrDefault(SPARK_SHUFFLE_PARTITIONS, SPARK_SHUFFLE_PARTITIONS_DEFAULT))
                .setConf(SPARK_YARN_DIST_JARS, kylinJobJar).setConf(SPARK_YARN_TIMELINE_SERVICE, "false")
                .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, kylinJobJar)
                .setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, Paths.get(kylinJobJar).getFileName().toString())
                .setConf(SPARK_DRIVER_OPTS, wrapDriverJavaOptions(sparkConf))
                .setConf(SPARK_EXECUTOR_OPTS, wrapExecutorJavaOptions(sparkConf))
                .setConf(SPARK_YARN_AM_OPTS, wrapYarnAmJavaOptions(sparkConf)).addJar(config.getKylinExtJarsPath())
                .addFile(config.getLogSparkStreamingExecutorPropertiesFile()).setAppResource(kylinJobJar)
                .setMainClass(mainClazz).addAppArgs(appArgs);
        handler = sparkLauncher.startApplication(listener);
    }

    @Override
    public void launch() {
        try {
            if (config.isUTEnv()) {
                log.info("{} -- {} {} streaming job starts to launch", project, modelId, jobType.name());
            } else if (StreamingUtils.isLocalMode()) {
                if (JobTypeEnum.STREAMING_BUILD == jobType) {
                    log.info("Starting streaming build job in local mode...");
                    StreamingEntry.main(appArgs);
                } else if (JobTypeEnum.STREAMING_MERGE == jobType) {
                    log.info("Starting streaming merge job in local mode...");
                    StreamingMergeEntry.main(appArgs);
                }
            } else {
                startYarnJob();
            }
        } catch (Exception e) {
            log.error("launch streaming application failed: " + e.getMessage(), e);
            MetaInfoUpdater.updateJobState(project, jobId, JobStatusEnum.LAUNCHING_ERROR);
            throw new KylinException(ServerErrorCode.JOB_START_FAILURE, e.getMessage());
        }
        log.info("Streaming job create success on model {}", modelId);
    }

    /**
     * Stop streaming job gracefully
     */
    @Override
    public void stop() {
        MetaInfoUpdater.markGracefulShutdown(project, StreamingUtils.getJobId(modelId, jobType.name()));
    }

    private String calcMaxCores(String executors, String cores) {
        return String.valueOf(Integer.parseInt(executors) * Integer.parseInt(cores));
    }
}
