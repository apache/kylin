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

package org.apache.kylin.engine.spark.application;

import com.google.common.collect.Maps;
import org.apache.kylin.engine.spark.job.BuildJobInfos;
import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.kylin.engine.spark.job.LogJobInfoUtils;
import org.apache.kylin.engine.spark.job.SparkJobConstants;
import org.apache.kylin.engine.spark.job.UdfManager;
import org.apache.kylin.engine.spark.utils.MetaDumpUtil;
import org.apache.kylin.engine.spark.utils.SparkConfHelper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.execution.KylinJoinSelection;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.execution.SparkStrategy;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.apache.spark.util.Utils;
import org.apache.spark.utils.ResourceUtils;
import org.apache.spark.utils.YarnInfoFetcherUtils;
import org.apache.kylin.engine.spark.common.util.TimeZoneUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public abstract class SparkApplication {
    private static final Logger logger = LoggerFactory.getLogger(SparkApplication.class);
    private Map<String, String> params = Maps.newHashMap();
    protected volatile KylinConfig config;
    protected volatile String jobId;
    protected SparkSession ss;
    protected String project;
    protected int layoutSize = -1;
    protected BuildJobInfos infos;

    public void execute(String[] args) {
        try {
            String argsLine = Files.readAllLines(Paths.get(args[0])).get(0);
            if (argsLine.isEmpty()) {
                throw new RuntimeException("Args file is empty");
            }
            params = JsonUtil.readValueAsMap(argsLine);
            checkArgs();
            logger.info("Executor task " + this.getClass().getName() + " with args : " + argsLine);
            execute();
        } catch (Exception e) {
            logger.error("The spark job execute failed!", e);
            throw new RuntimeException("Error execute " + this.getClass().getName(), e);
        }
    }

    public final Map<String, String> getParams() {
        return this.params;
    }

    public final String getParam(String key) {
        return this.params.get(key);
    }

    public final void setParam(String key, String value) {
        this.params.put(key, value);
    }

    public final boolean contains(String key) {
        return params.containsKey(key);
    }

    public void checkArgs() {
        // do nothing
    }


    /**
     * get tracking url by yarn app id
     *
     * @param yarnAppId
     * @return
     * @throws IOException
     * @throws YarnException
     */
    public String getTrackingUrl(String yarnAppId) throws IOException, YarnException {
        return YarnInfoFetcherUtils.getTrackingUrl(yarnAppId);
    }


    final protected void execute() throws Exception {
        String hdfsMetalUrl = getParam(MetadataConstants.P_DIST_META_URL);
        jobId = getParam(MetadataConstants.P_JOB_ID);
        project = getParam(MetadataConstants.P_PROJECT_NAME);
        if (getParam(MetadataConstants.P_CUBOID_NUMBER) != null) {
            layoutSize = Integer.valueOf(getParam(MetadataConstants.P_CUBOID_NUMBER));
        }
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoCloseConfig = KylinConfig
                .setAndUnsetThreadLocalConfig(MetaDumpUtil.loadKylinConfigFromHdfs(hdfsMetalUrl))) {
            config = autoCloseConfig.get();
            config.setProperty("kylin.source.provider.0", "org.apache.kylin.engine.spark.source.HiveSource");
            // init KylinBuildEnv
            KylinBuildEnv buildEnv = KylinBuildEnv.getOrCreate(config);
            infos = buildEnv.buildJobInfos();
            SparkConf sparkConf = buildEnv.sparkConf();
            if (config.isAutoSetSparkConf() && isJobOnCluster(sparkConf)) {
                try {
                    autoSetSparkConf(sparkConf);
                } catch (Exception e) {
                    logger.warn("Auto set spark conf failed. Load spark conf from system properties", e);
                }
                if (config.getSparkConfigOverride().size() > 0) {
                    for (Map.Entry<String, String> entry : config.getSparkConfigOverride().entrySet()) {
                        logger.info("Override user-defined spark conf, set {}={}.", entry.getKey(), entry.getValue());
                        sparkConf.set(entry.getKey(), entry.getValue());
                    }
                }
            } else if (!isJobOnCluster(sparkConf)) {
                sparkConf.set("spark.master", "local");
            }

            // for wrapping credential

            TimeZoneUtils.setDefaultTimeZone(config);

            if (isJobOnCluster(sparkConf)) {
                logger.info("Sleep for random seconds to avoid submitting too many spark job at the same time.");
                Thread.sleep((long) (Math.random() * 60 * 1000));
                try {
                    while (!ResourceUtils.checkResource(sparkConf, buildEnv.clusterInfoFetcher())) {
                        long waitTime = (long) (Math.random() * 10 * 60 * 1000L);
                        logger.info("Current available resource in cluster is not sufficient, wait for a period: {}.",
                                waitTime);
                        Thread.sleep(waitTime);
                    }
                } catch (Throwable throwable) {
                    logger.warn("Error occurred when check resource. Ignore it and try to submit this job. ",
                            throwable);
                }
            }

            ss = SparkSession.builder().withExtensions(new AbstractFunction1<SparkSessionExtensions, BoxedUnit>() {
                @Override
                public BoxedUnit apply(SparkSessionExtensions v1) {
                    v1.injectPlannerStrategy(new AbstractFunction1<SparkSession, SparkStrategy>() {
                        @Override
                        public SparkStrategy apply(SparkSession session) {
                            return new KylinJoinSelection(session);
                        }
                    });
                    return BoxedUnit.UNIT;
                }
            }).enableHiveSupport().config(sparkConf).config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                    .getOrCreate();

            // for spark metrics
            //JobMetricsUtils.registerListener(ss);

            UdfManager.create(ss);

            if (!config.isUTEnv()) {
                System.setProperty("kylin.env", config.getDeployEnv());
            }
            infos.startJob();
            doExecute();
        } finally {
            if (infos != null) {
                infos.jobEnd();
            }
            if (ss != null && !ss.conf().get("spark.master").startsWith("local")) {
                //JobMetricsUtils.unRegisterListener(ss);
                ss.stop();
            }
        }
    }

    public boolean isJobOnCluster(SparkConf conf) {
        return !Utils.isLocalMaster(conf) && !config.isUTEnv();
    }

    protected abstract void doExecute() throws Exception;

    protected String calculateRequiredCores() throws Exception {
        return SparkJobConstants.DEFAULT_REQUIRED_CORES;
    }

    private void autoSetSparkConf(SparkConf sparkConf) throws Exception {
        logger.info("Start set spark conf automatically.");
        SparkConfHelper helper = new SparkConfHelper();
        helper.setFetcher(KylinBuildEnv.get().clusterInfoFetcher());
        Path shareDir = config.getJobTmpShareDir(project, jobId);
        String contentSize = chooseContentSize(shareDir);

        // add content size with unit
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, contentSize);
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, Integer.toString(layoutSize));
        Map<String, String> configOverride = config.getSparkConfigOverride();
        helper.setConf(SparkConfHelper.DEFAULT_QUEUE, configOverride.get(SparkConfHelper.DEFAULT_QUEUE));
        helper.setOption(SparkConfHelper.REQUIRED_CORES, calculateRequiredCores());
        helper.setConf(SparkConfHelper.COUNT_DISTICT, hasCountDistinct().toString());
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
    }

    protected String chooseContentSize(Path shareDir) throws IOException {
        // return size with unit
        return ResourceDetectUtils.getMaxResourceSize(shareDir) + "b";
    }

    protected Boolean hasCountDistinct() throws IOException {
        Path countDistinct = new Path(config.getJobTmpShareDir(project, jobId),
                ResourceDetectUtils.countDistinctSuffix());
        FileSystem fileSystem = countDistinct.getFileSystem(HadoopUtil.getCurrentConfiguration());
        Boolean exist;
        if (fileSystem.exists(countDistinct)) {
            exist = ResourceDetectUtils.readResourcePathsAs(countDistinct);
        } else {
            exist = false;
            logger.info("File count_distinct.json doesn't exist, set hasCountDistinct to false.");
        }
        logger.info("Exist count distinct measure: {}", exist);
        return exist;
    }

    public void logJobInfo() {
        try {
            logger.info(generateInfo());
        } catch (Exception e) {
            logger.warn("Error occurred when generate job info.", e);
        }
    }

    protected String generateInfo() {
        return LogJobInfoUtils.sparkApplicationInfo();
    }
}
