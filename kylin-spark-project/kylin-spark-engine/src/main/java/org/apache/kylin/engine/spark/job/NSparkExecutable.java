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

package org.apache.kylin.engine.spark.job;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.Map.Entry;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.spark.utils.MetaDumpUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.ClassUtil;

import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.spark.deploy.SparkApplicationClient;
import org.apache.spark.utils.SparkVersionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

/**
 *
 */
public class NSparkExecutable extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkExecutable.class);

    protected static final String SPARK_MASTER = "spark.master";
    protected static final String DEPLOY_MODE = "spark.submit.deployMode";
    private static final String APP_JAR_NAME = "__app__.jar";

    private volatile boolean isYarnCluster = false;
    private volatile boolean isStandaloneCluster = false;

    protected void setSparkSubmitClassName(String className) {
        this.setParam(MetadataConstants.P_CLASS_NAME, className);
    }

    public String getSparkSubmitClassName() {
        return this.getParam(MetadataConstants.P_CLASS_NAME);
    }

    public String getJars() {
        return this.getParam(MetadataConstants.P_JARS);
    }

    protected void setDistMetaUrl(StorageURL storageURL) {
        HashMap<String, String> stringStringHashMap = Maps.newHashMap(storageURL.getAllParameters());
        StorageURL copy = storageURL.copy(stringStringHashMap);
        this.setParam(MetadataConstants.P_DIST_META_URL, copy.toString());
    }

    public String getDistMetaUrl() {
        return this.getParam(MetadataConstants.P_DIST_META_URL);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        //context.setLogPath(getSparkDriverLogHdfsPath(context.getConfig()));
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubeInstance cube = cubeMgr.getCube(this.getCubeName());
        KylinConfig config = cube.getConfig();
        this.setLogPath(getSparkDriverLogHdfsPath(context.getConfig()));
        config = wrapConfig(config);

        String sparkHome = KylinConfig.getSparkHome();
        if (StringUtils.isEmpty(sparkHome) && !config.isUTEnv()) {
            throw new RuntimeException("Missing spark home");
        }

        String kylinJobJar = config.getKylinParquetJobJarPath();
        if (!config.isUTEnv() && StringUtils.isEmpty(kylinJobJar) && !config.isUTEnv()) {
            throw new RuntimeException("Missing kylin parquet job jar");
        }
        String hadoopConf = System.getProperty("kylin.hadoop.conf.dir");
        logger.info("write hadoop conf is {} ", config.getBuildConf());
        if (!config.getBuildConf().isEmpty()) {
            logger.info("write hadoop conf is {} ", config.getBuildConf());
            hadoopConf = config.getBuildConf();
        }
        if (StringUtils.isEmpty(hadoopConf) && !config.isUTEnv() && !config.isZKLocal()) {
            throw new RuntimeException(
                    "kylin_hadoop_conf_dir is empty, check if there's error in the output of 'kylin.sh start'");
        }

        File hiveConfFile = new File(hadoopConf, "hive-site.xml");
        if (!hiveConfFile.exists() && !config.isUTEnv() && !config.isZKLocal()) {
            throw new RuntimeException("Cannot find hive-site.xml in kylin_hadoop_conf_dir: " + hadoopConf + //
                    ". In order to enable spark cubing, you must set kylin.env.hadoop-conf-dir to a dir which contains at least core-site.xml, hdfs-site.xml, hive-site.xml, mapred-site.xml, yarn-site.xml");
        }

        String jars = getJars();
        if (StringUtils.isEmpty(jars)) {
            jars = kylinJobJar;
        }

        deleteJobTmpDirectoryOnExists();
        onExecuteStart(context);

        try {
            attachMetadataAndKylinProps(config);
        } catch (IOException e) {
            throw new ExecuteException("meta dump failed", e);
        }
        String filePath = dumpArgs();
        if (config.isUTEnv() || config.isLocalEnv() || config.isZKLocal()) {
            return runLocalMode(filePath, config);
        } else {
            logger.info("Task id: {}", getId());
            if ("yarn".equals(config.getSparkEngineConfigOverrideWithSpecificName("spark.master"))) {
                logger.info("Try to kill orphan application on yarn.");
                killOrphanApplicationIfExists(config, getId());
            }
            return runSparkSubmit(config, hadoopConf, jars, kylinJobJar,
                    "-className " + getSparkSubmitClassName() + " " + filePath, getParent().getId());
        }
    }

    /**
     * Dump metadata from Kylin Metadata and persist to HDFS(getDistMetaUrl) for Spark Application
     */
    void attachMetadataAndKylinProps(KylinConfig config) throws IOException {
        // The way of Updating metadata is CopyOnWrite. So it is safe to use Reference in the value.
        Set<String> dumpList = getMetadataDumpList(config);
        MetaDumpUtil.dumpAndUploadKylinPropsAndMetadata(dumpList, config, getDistMetaUrl());
    }

    String dumpArgs() throws ExecuteException {
        File tmpDir = null;
        try {
            String pathName = getId() + "_" + MetadataConstants.P_JOB_ID;
            Path tgtPath = new Path(getConfig().getJobTmpDir(getParams().get("project")), pathName);
            FileSystem fileSystem = FileSystem.get(tgtPath.toUri(), HadoopUtil.getCurrentConfiguration());
            try (BufferedOutputStream outputStream = new BufferedOutputStream(fileSystem.create(tgtPath))) {
                outputStream.write(JsonUtil.writeValueAsBytes(getParams()));
            }

            logger.info("Spark job args json is : {}.", JsonUtil.writeValueAsString(getParams()));
            return tgtPath.toUri().toString();
        } catch (IOException e) {
            if (tmpDir != null && tmpDir.exists()) {
                try {
                    Files.delete(tmpDir.toPath());
                } catch (IOException e1) {
                    throw new ExecuteException(
                            "Write cuboidLayoutIds failed: Error for delete file " + tmpDir.getPath(), e1);
                }
            }
            throw new ExecuteException("Write cuboidLayoutIds failed: ", e);
        }
    }

    /**
     * generate the spark driver log hdfs path format, json path + timestamp + .log
     */
    public String getSparkDriverLogHdfsPath(KylinConfig config) {
        return String.format(Locale.ROOT, "%s.%s.log", config.getJobOutputStorePath(getParam(MetadataConstants.P_PROJECT_NAME), getId()),
                System.currentTimeMillis());
    }

    protected KylinConfig wrapConfig(ExecutableContext context) {
        return wrapConfig(context.getConfig());
    }

    protected KylinConfig wrapConfig(KylinConfig originalConfig) {
        String project = getParam(MetadataConstants.P_PROJECT_NAME);
        Preconditions.checkState(StringUtils.isNotBlank(project), "job " + getId() + " project info is empty");

        HashMap<String, String> jobOverrides = new HashMap<>();
        String parentId = getParentId();
        jobOverrides.put("job.id", StringUtils.defaultIfBlank(parentId, getId()));
        jobOverrides.put("job.project", project);
        if (StringUtils.isNotBlank(parentId)) {
            jobOverrides.put("job.stepId", getId());
        }
        jobOverrides.put("user.timezone", KylinConfig.getInstanceFromEnv().getTimeZone());
        jobOverrides.put("hdfs.working.dir", KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory());
        jobOverrides.put("spark.driver.log4j.appender.hdfs.File",
                Objects.isNull(this.getLogPath()) ? "null" : this.getLogPath());

        return KylinConfigExt.createInstance(originalConfig, jobOverrides);
    }

    private void killOrphanApplicationIfExists(KylinConfig config, String jobId) {
        PatternedLogger patternedLogger = new PatternedLogger(logger);
        String orphanApplicationId = null;

        try (YarnClient yarnClient = YarnClient.createYarnClient()) {
            Configuration yarnConfiguration = new YarnConfiguration();
            // bug of yarn : https://issues.apache.org/jira/browse/SPARK-15343
            yarnConfiguration.set("yarn.timeline-service.enabled", "false");
            yarnClient.init(yarnConfiguration);
            yarnClient.start();

            Set<String> types = Sets.newHashSet("SPARK");
            EnumSet<YarnApplicationState> states = EnumSet.of(YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
                    YarnApplicationState.SUBMITTED, YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING);
            List<ApplicationReport> applicationReports = yarnClient.getApplications(types, states);

            if (CollectionUtils.isEmpty(applicationReports))
                return;

            for (ApplicationReport report : applicationReports) {
                if (report.getName().equalsIgnoreCase("job_step_" + getId())) {
                    orphanApplicationId = report.getApplicationId().toString();
                    // kill orphan application by command line
                    String killApplicationCmd = "yarn application -kill " + orphanApplicationId;
                    config.getCliCommandExecutor().execute(killApplicationCmd, patternedLogger, jobId);
                }
            }
        } catch (YarnException | IOException ex2) {
            logger.error("get yarn application failed");
        }
    }

    private ExecuteResult runSparkSubmit(KylinConfig config, String hadoopConf, String jars,
                                         String kylinJobJar, String appArgs, String jobId) {
        PatternedLogger patternedLogger;
        if (config.isJobLogPrintEnabled()) {
            patternedLogger = new PatternedLogger(logger);
        } else {
            patternedLogger = new PatternedLogger(null);
        }
        try {
            String cmd = generateSparkCmd(config, hadoopConf, jars, kylinJobJar, appArgs);

            CliCommandExecutor exec = new CliCommandExecutor();
            exec.execute(cmd, patternedLogger, jobId);
            if (isStandaloneCluster) {
                SparkApplicationClient.awaitAndCheckAppState(SparkApplicationClient.STANDALONE_CLUSTER(), jobId);
            }
            updateMetaAfterOperation(config);
            //Add metrics information to execute result for JobMetricsFacade
            getManager().addJobInfo(getId(), getJobMetricsInfo(config));
            Map<String, String> extraInfo = makeExtraInfo(patternedLogger.getInfo());
            ExecuteResult ret = ExecuteResult.createSucceed(patternedLogger.getBufferedLog());
            ret.getExtraInfo().putAll(extraInfo);
            return ret;
        } catch (Exception e) {
            return ExecuteResult.createError(e);
        }
    }

    protected void updateMetaAfterOperation(KylinConfig config) throws IOException {
    }

    protected Map<String, String> getJobMetricsInfo(KylinConfig config) {
        return Maps.newHashMap();
    }

    protected Map<String, String> getSparkConfigOverride(KylinConfig config) {
        Map<String, String> sparkConfigOverride = config.getSparkConfigOverride();
        if ("yarn".equals(sparkConfigOverride.get(SPARK_MASTER))
                && "cluster".equals(sparkConfigOverride.get(DEPLOY_MODE)) && !(this instanceof NSparkLocalStep)) {
            this.isYarnCluster = true;
        }

        if (sparkConfigOverride.get(SPARK_MASTER).toLowerCase(Locale.ROOT).startsWith("spark")
                && "cluster".equals(sparkConfigOverride.get(DEPLOY_MODE)) && !(this instanceof NSparkLocalStep)) {
            this.isStandaloneCluster = true;
        }

        if (!sparkConfigOverride.containsKey("spark.driver.memory")) {
            sparkConfigOverride.put("spark.driver.memory", computeStepDriverMemory() + "m");
        }
        if (UserGroupInformation.isSecurityEnabled()) {
            sparkConfigOverride.put("spark.hadoop.hive.metastore.sasl.enabled", "true");
        }

        // With spark 2.X, when set 'spark.sql.adaptive.enabled' to true,
        // it will impact the actually partition number when doing repartition with spark,
        // which will lead to the wrong results for global dict generation and repartition by
        // shardby column.
        // For example, after writing a cuboid data, kylin will repartition the cuboid data with
        // a specified partition number if need, but if 'spark.sql.adaptive.enabled' is true,
        // spark will optimize the partition number according to the size of partitions,
        // which leads to the wrong results.
        if (SparkVersionUtils.isLessThanSparkVersion("2.4", true)) {
            sparkConfigOverride.put("spark.sql.adaptive.enabled", "false");
        }

        replaceSparkNodeJavaOpsConfIfNeeded(config, sparkConfigOverride);
        return sparkConfigOverride;
    }

    /**
     * Add property in spark.xxx.extraJavaOptions for AbstractHdfsLogAppender
     * Please check following for detail.
     * 1. conf/spark-driver-log4j.properties and conf/spark-executor-log4j.properties
     * 2. AbstractHdfsLogAppender
     */
    private void replaceSparkNodeJavaOpsConfIfNeeded(KylinConfig config,
                                                     Map<String, String> sparkConfigOverride) {
        String sparkDriverExtraJavaOptionsKey = "spark.driver.extraJavaOptions";
        StringBuilder sb = new StringBuilder();
        if (sparkConfigOverride.containsKey(sparkDriverExtraJavaOptionsKey)) {
            sb.append(sparkConfigOverride.get(sparkDriverExtraJavaOptionsKey));
        }
        String serverAddress = config.getServerRestAddress();
        String hdfsWorkingDir = config.getHdfsWorkingDirectory();

        String sparkDriverHdfsLogPath = null;
        if (config instanceof KylinConfigExt) {
            Map<String, String> extendedOverrides = ((KylinConfigExt) config).getExtendedOverrides();
            if (Objects.nonNull(extendedOverrides)) {
                sparkDriverHdfsLogPath = extendedOverrides.get("spark.driver.log4j.appender.hdfs.File");
            }
        }

        wrapLog4jConf(sb, config);
        sb.append(String.format(Locale.ROOT, " -Dkylin.kerberos.enabled=%s ", config.isKerberosEnabled()));
        if (config.isKerberosEnabled()) {
            sb.append(String.format(Locale.ROOT, " -Dkylin.kerberos.principal=%s ", config.getKerberosPrincipal()));
            sb.append(String.format(Locale.ROOT, " -Dkylin.kerberos.keytab=%s", config.getKerberosKeytabPath()));
            if (config.getPlatformZKEnable()) {
                sb.append(String.format(Locale.ROOT, " -Djava.security.auth.login.config=%s", config.getKerberosJaasConfPath()));
                sb.append(String.format(Locale.ROOT, " -Djava.security.krb5.conf=%s", config.getKerberosKrb5ConfPath()));
            }
        }
        sb.append(String.format(Locale.ROOT, " -Dkylin.hdfs.working.dir=%s ", hdfsWorkingDir));
        sb.append(String.format(Locale.ROOT, " -Dspark.driver.log4j.appender.hdfs.File=%s ", sparkDriverHdfsLogPath));
        sb.append(String.format(Locale.ROOT, " -Dlog4j.debug=%s ", "true"));
        sb.append(String.format(Locale.ROOT, " -Dspark.driver.rest.server.address=%s ", serverAddress));
        sb.append(String.format(Locale.ROOT, " -Dspark.driver.param.taskId=%s ", getId()));
        sb.append(String.format(Locale.ROOT, " -Dspark.driver.local.logDir=%s ", config.getKylinLogDir() + "/spark"));
        sparkConfigOverride.put(sparkDriverExtraJavaOptionsKey, sb.toString());
    }

    protected String generateSparkCmd(KylinConfig config, String hadoopConf, String jars, String kylinJobJar,
                                      String appArgs) {
        StringBuilder sb = new StringBuilder();

        String sparkSubmitCmd = config.getSparkSubmitCmd() != null ?
                config.getSparkSubmitCmd() : KylinConfig.getSparkHome() + "/bin/spark-submit";
        sb.append("export HADOOP_CONF_DIR=%s && %s --class org.apache.kylin.engine.spark.application.SparkEntry ");

        Map<String, String> sparkConfs = getSparkConfigOverride(config);
        for (Entry<String, String> entry : sparkConfs.entrySet()) {
            appendSparkConf(sb, entry.getKey(), entry.getValue());
        }
        if (!isLocalMaster(sparkConfs)) {
            appendSparkConf(sb, "spark.executor.extraClassPath", Paths.get(kylinJobJar).getFileName().toString());
        }
        // In yarn cluster mode, make sure class SparkDriverHdfsLogAppender will be in NM container's classpath.
        appendSparkConf(sb, "spark.driver.extraClassPath", isYarnCluster ? //
                String.format(Locale.ROOT, "%s:%s", APP_JAR_NAME,
                        Paths.get(kylinJobJar).getFileName().toString()) : kylinJobJar);

        String sparkUploadFiles = config.sparkUploadFiles(isLocalMaster(sparkConfs), isYarnCluster);
        if (StringUtils.isNotBlank(sparkUploadFiles)) {
            sb.append("--files ").append(sparkUploadFiles).append(" ");
        }
        if (config.isKerberosEnabled()) {
            sb.append("--principal ").append(config.getKerberosPrincipal()).append(" ");
            sb.append("--keytab ").append(config.getKerberosKeytabPath()).append(" ");
        }
        if (isYarnCluster) {
            final String aliasedJar = String.format(Locale.ROOT, "%s#%s", kylinJobJar, //
                    Paths.get(kylinJobJar).getFileName().toString());
            // Make sure class SparkDriverHdfsLogAppender will be in NM container's classpath.
            if (StringUtils.isBlank(jars) || jars.equals(kylinJobJar)) {
                jars = aliasedJar;
            } else if (jars.contains(kylinJobJar)) {
                jars = jars.replace(kylinJobJar, aliasedJar);
            } else {
                jars = String.format(Locale.ROOT, "%s,%s", jars, aliasedJar);
            }
        }

        sb.append("--name job_step_%s ");
        sb.append("--jars %s %s %s");
        String cmd = String.format(Locale.ROOT, sb.toString(), hadoopConf, sparkSubmitCmd, getId(), jars, kylinJobJar,
                appArgs);
        // SparkConf still have a change to be changed in CubeBuildJob.java (Spark Driver)
        logger.info("spark submit cmd: {}", cmd);
        return cmd;
    }

    private void wrapLog4jConf(StringBuilder sb, KylinConfig config) {
        final String localLog4j = config.getLogSparkDriverPropertiesFile();
        final String log4jName = Paths.get(localLog4j).getFileName().toString();
        if (isYarnCluster) {
            sb.append(String.format(Locale.ROOT, " -Dlog4j.configuration=%s ", log4jName));
        } else {
            sb.append(String.format(Locale.ROOT, " -Dlog4j.configuration=file:%s ", localLog4j));
        }
    }

    protected void appendSparkConf(StringBuilder sb, String key, String value) {
        // Multiple parameters in "--conf" need to be enclosed in single quotes
        sb.append(" --conf '").append(key).append("=").append(value.trim()).append("' ");
    }

    private ExecuteResult runLocalMode(String appArgs, KylinConfig config) {
        try {
            Class<? extends Object> appClz = ClassUtil.forName(getSparkSubmitClassName(), Object.class);
            appClz.getMethod("main", String[].class).invoke(null, (Object) new String[]{appArgs});
            updateMetaAfterOperation(config);

            //Add metrics information to execute result for JobMetricsFacade
            getManager().addJobInfo(getId(), getJobMetricsInfo(config));
            return ExecuteResult.createSucceed();
        } catch (Exception e) {
            return ExecuteResult.createError(e);
        }
    }

    protected Set<String> getMetadataDumpList(KylinConfig config) {
        return Collections.emptySet();
    }

    private void deleteJobTmpDirectoryOnExists() {
        StorageURL storageURL = StorageURL.valueOf(getDistMetaUrl());
        String metaPath = storageURL.getParameter("path");

        String[] directories = metaPath.split("/");
        String lastDirectory = directories[directories.length - 1];
        String taskPath = metaPath.substring(0, metaPath.length() - 1 - lastDirectory.length());
        try {
            Path path = new Path(taskPath);
            HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), path);
        } catch (Exception e) {
            logger.error("delete job tmp in path {} failed.", taskPath, e);
        }
    }

    protected boolean isLocalMaster(Map<String, String> sparkConfs) {
        String master = sparkConfs.getOrDefault("spark.master", "yarn");
        return (master.equalsIgnoreCase("local")) || (master.toLowerCase(Locale.ROOT)
                .startsWith("local["));
    }

    public boolean needMergeMetadata() {
        return false;
    }
}
