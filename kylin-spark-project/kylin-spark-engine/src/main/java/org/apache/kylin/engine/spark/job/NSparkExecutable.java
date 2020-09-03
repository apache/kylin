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

import java.io.File;
import java.io.IOException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Set;

import org.apache.kylin.engine.spark.utils.MetaDumpUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
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
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 *
 */
public class NSparkExecutable extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkExecutable.class);

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
        final KylinConfig config = wrapConfig(context);

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
            killOrphanApplicationIfExists(config, getId());
            return runSparkSubmit(config, hadoopConf, jars, kylinJobJar,
                    "-className " + getSparkSubmitClassName() + " " + filePath, getParent().getId());
        }
    }

    void attachMetadataAndKylinProps(KylinConfig config) throws IOException {
        // The way of Updating metadata is CopyOnWrite. So it is safe to use Reference in the value.
        Set<String> dumpList = getMetadataDumpList(config);
        MetaDumpUtil.dumpAndUploadKylinPropsAndMetadata(dumpList, KylinConfigExt.createInstance(config, new HashMap<>()), getDistMetaUrl());
    }

    String dumpArgs() throws ExecuteException {
        File tmpDir = null;
        try {
            tmpDir = File.createTempFile(MetadataConstants.P_SEGMENT_IDS, "");
            FileUtils.writeByteArrayToFile(tmpDir, JsonUtil.writeValueAsBytes(getParams()));

            logger.info("Spark job args json is : {}.", JsonUtil.writeValueAsString(getParams()));
            return tmpDir.getCanonicalPath();
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
     *
     * @param
     * @return
     */
    /*public String getSparkDriverLogHdfsPath(KylinConfig config) {
        return String.format("%s.%s.log", config.getJobTmpOutputStorePath(getProject(), getId()),
                System.currentTimeMillis());
    }*/

    protected KylinConfig wrapConfig(ExecutableContext context) {
        KylinConfig originalConfig = context.getConfig();
        String project = getParam(MetadataConstants.P_PROJECT_NAME);
        Preconditions.checkState(StringUtils.isNotBlank(project), "job " + getId() + " project info is empty");

        String parentId = getParentId();
        originalConfig.setProperty("job.id", StringUtils.defaultIfBlank(parentId, getId()));
        originalConfig.setProperty("job.project", project);
        if (StringUtils.isNotBlank(parentId)) {
            originalConfig.setProperty("job.stepId", getId());
        }
        originalConfig.setProperty("user.timezone", KylinConfig.getInstanceFromEnv().getTimeZone());

        return originalConfig;
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
            patternedLogger = new PatternedLogger(logger, new PatternedLogger.ILogListener() {
                @Override
                public void onLogEvent(String infoKey, Map<String, String> info) {
                    // only care three properties here
                    if (ExecutableConstants.SPARK_JOB_ID.equals(infoKey)
                            || ExecutableConstants.YARN_APP_ID.equals(infoKey)
                            || ExecutableConstants.YARN_APP_URL.equals(infoKey)) {
                        getManager().addJobInfo(getId(), info);
                    }
                }
            });
        } else {
            patternedLogger = new PatternedLogger(null);
        }
        try {
            String cmd = generateSparkCmd(config, hadoopConf, jars, kylinJobJar, appArgs);
            patternedLogger.log("cmd: ");
            patternedLogger.log(cmd);

            CliCommandExecutor exec = new CliCommandExecutor();
            exec.execute(cmd, patternedLogger, jobId);
            updateMetaAfterBuilding(config);
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

    protected void updateMetaAfterBuilding(KylinConfig config) throws IOException {
    }

    protected Map<String, String> getJobMetricsInfo(KylinConfig config) {
        return Maps.newHashMap();
    }

    protected Map<String, String> getSparkConfigOverride(KylinConfig config) {
        Map<String, String> sparkConfigOverride = config.getSparkConfigOverride();
        if (!sparkConfigOverride.containsKey("spark.driver.memory")) {
            sparkConfigOverride.put("spark.driver.memory", computeStepDriverMemory() + "m");
        }
        if (UserGroupInformation.isSecurityEnabled()) {
            sparkConfigOverride.put("spark.hadoop.hive.metastore.sasl.enabled", "true");
        }

        String serverIp = "127.0.0.1";
        try {
            serverIp = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.warn("use the InetAddress get local ip failed!", e);
        }

        String log4jConfiguration = "file:" + config.getLogSparkPropertiesFile();

        String sparkDriverExtraJavaOptionsKey = "spark.driver.extraJavaOptions";
        StringBuilder sb = new StringBuilder();
        if (sparkConfigOverride.containsKey(sparkDriverExtraJavaOptionsKey)) {
            sb.append(sparkConfigOverride.get(sparkDriverExtraJavaOptionsKey));
        }

        sb.append(String.format(Locale.ROOT, " -Dlog4j.configuration=%s ", log4jConfiguration));
        sb.append(String.format(Locale.ROOT, " -Dspark.driver.rest.server.ip=%s ", serverIp));
        sb.append(String.format(Locale.ROOT, " -Dspark.driver.param.taskId=%s ", getId()));

        sparkConfigOverride.put(sparkDriverExtraJavaOptionsKey, sb.toString());
        return sparkConfigOverride;
    }

    protected String generateSparkCmd(KylinConfig config, String hadoopConf, String jars, String kylinJobJar,
            String appArgs) {
        StringBuilder sb = new StringBuilder();
        sb.append("export HADOOP_CONF_DIR=%s && %s/bin/spark-submit --class org.apache.kylin.engine.spark.application.SparkEntry ");

        Map<String, String> sparkConfs = getSparkConfigOverride(config);
        for (Entry<String, String> entry : sparkConfs.entrySet()) {
            appendSparkConf(sb, entry.getKey(), entry.getValue());
        }
        appendSparkConf(sb, "spark.executor.extraClassPath", Paths.get(kylinJobJar).getFileName().toString());
        appendSparkConf(sb, "spark.driver.extraClassPath", kylinJobJar);

        if (sparkConfs.containsKey("spark.sql.hive.metastore.jars")) {
            jars = jars + "," + sparkConfs.get("spark.sql.hive.metastore.jars");
        }

        sb.append("--name job_step_%s ");
        sb.append("--jars %s %s %s");
        String cmd = String.format(Locale.ROOT, sb.toString(), hadoopConf, KylinConfig.getSparkHome(), getId(), jars, kylinJobJar,
                appArgs);
        logger.info("spark submit cmd: {}", cmd);
        return cmd;
    }

    protected void appendSparkConf(StringBuilder sb, String key, String value) {
        // Multiple parameters in "--conf" need to be enclosed in single quotes
        sb.append(" --conf '").append(key).append("=").append(value).append("' ");
    }

    private ExecuteResult runLocalMode(String appArgs, KylinConfig config) {
        try {
            Class<? extends Object> appClz = ClassUtil.forName(getSparkSubmitClassName(), Object.class);
            appClz.getMethod("main", String[].class).invoke(null, (Object) new String[] { appArgs });
            updateMetaAfterBuilding(config);

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

    public boolean needMergeMetadata() {
        return false;
    }
}
