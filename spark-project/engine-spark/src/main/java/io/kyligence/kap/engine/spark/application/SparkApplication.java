/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.application;

import static io.kyligence.kap.engine.spark.utils.SparkConfHelper.COUNT_DISTICT;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Application;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.TimeZoneUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.execution.JoinMemoryManager;
import org.apache.spark.sql.execution.KylinJoinSelection;
import org.apache.spark.sql.execution.SparkStrategy;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.apache.spark.util.Utils;
import org.apache.spark.utils.ResourceUtils;
import org.apache.spark.utils.YarnInfoFetcherUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.engine.spark.job.BuildJobInfos;
import io.kyligence.kap.engine.spark.job.KylinBuildEnv;
import io.kyligence.kap.engine.spark.job.LogJobInfoUtils;
import io.kyligence.kap.engine.spark.job.SparkJobConstants;
import io.kyligence.kap.engine.spark.job.UdfManager;
import io.kyligence.kap.engine.spark.utils.JobMetricsUtils;
import io.kyligence.kap.engine.spark.utils.SparkConfHelper;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.spark.common.CredentialUtils;
import lombok.val;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public abstract class SparkApplication implements Application, IKeep {
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
     * http request the spark job controller
     *
     * @param json
     */
    public Boolean updateSparkJobInfo(String json) {
        String serverIp = System.getProperty("spark.driver.rest.server.ip", "127.0.0.1");
        String port = System.getProperty("spark.driver.rest.server.port", "7070");
        String requestApi = String.format("http://%s:%s/kylin/api/jobs/spark", serverIp, port);

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPut httpPut = new HttpPut(requestApi);
            httpPut.addHeader("Content-Type", "application/vnd.apache.kylin-v2+json");
            httpPut.setEntity(new StringEntity(json, StandardCharsets.UTF_8));

            HttpResponse response = httpClient.execute(httpPut);
            int code = response.getStatusLine().getStatusCode();
            if (code == HttpStatus.SC_OK) {
                return true;
            } else {
                InputStream inputStream = response.getEntity().getContent();
                String responseContent = IOUtils.toString(inputStream);
                logger.warn("update spark job yarnAppid and yarnAppUrl failed, info: {}", responseContent);
            }
        } catch (IOException e) {
            logger.error("http request {} failed!", requestApi, e);
        }
        return false;
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

    /**
     * when
     * update spark job extra info, link yarn_application_tracking_url & yarn_application_id
     */
    public Boolean updateSparkJobExtraInfo(String project, String jobId, String yarnAppId) {
        Map<String, String> payload = new HashMap<>(5);
        payload.put("project", project);
        payload.put("jobId", jobId);
        payload.put("taskId", System.getProperty("spark.driver.param.taskId", jobId));
        payload.put("yarnAppId", yarnAppId);

        try {
            payload.put("yarnAppUrl", getTrackingUrl(yarnAppId));
        } catch (IOException | YarnException e) {
            logger.error("get yarn tracking url failed!", e);
        }

        try {
            String payloadJson = new ObjectMapper().writeValueAsString(payload);
            int retry = 3;
            for (int i = 0; i < retry; i++) {
                if (updateSparkJobInfo(payloadJson)) {
                    return Boolean.TRUE;
                }
                Thread.sleep(3000);
                logger.warn("retry request rest api update spark job yarnAppId and yarnAppUr!");
            }
        } catch (Exception e) {
            logger.error("update spark job extra info failed!", e);
        }

        return Boolean.FALSE;
    }

    final protected void execute() throws Exception {
        String hdfsMetalUrl = getParam(NBatchConstants.P_DIST_META_URL);
        jobId = getParam(NBatchConstants.P_JOB_ID);
        project = getParam(NBatchConstants.P_PROJECT_NAME);
        if (getParam(NBatchConstants.P_LAYOUT_IDS) != null) {
            layoutSize = StringUtils.split(getParam(NBatchConstants.P_LAYOUT_IDS), ",").length;
        }
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoCloseConfig = KylinConfig
                .setAndUnsetThreadLocalConfig(KylinConfig.loadKylinConfigFromHdfs(hdfsMetalUrl))) {
            config = autoCloseConfig.get();
            // init KylinBuildEnv
            KylinBuildEnv buildEnv = KylinBuildEnv.getOrCreate(config);
            infos = KylinBuildEnv.get().buildJobInfos();

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
            }

            // for wrapping credential
            CredentialUtils.wrap(sparkConf, project);

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

            if (isJobOnCluster(sparkConf)) {
                updateSparkJobExtraInfo(project, jobId, ss.sparkContext().applicationId());
            }

            JoinMemoryManager.releaseAllMemory();
            // for spark metrics
            JobMetricsUtils.registerListener(ss);

            //#8341
            SparderEnv.setSparkSession(ss);
            UdfManager.create(ss);

            if (!config.isUTEnv()) {
                System.setProperty("kylin.env", config.getDeployEnv());
            }
            infos.startJob();
            doExecute();
            // Output metadata to another folder
            val resourceStore = ResourceStore.getKylinMetaStore(config);
            val outputConfig = KylinConfig.createKylinConfig(config);
            outputConfig.setMetadataUrl(getParam(NBatchConstants.P_OUTPUT_META_URL));
            MetadataStore.createMetadataStore(outputConfig).dump(resourceStore);
        } finally {
            if (infos != null) {
                infos.jobEnd();
            }
            if (ss != null && !ss.conf().get("spark.master").startsWith("local")) {
                JobMetricsUtils.unRegisterListener(ss);
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
        helper.setConf(COUNT_DISTICT, hasCountDistinct().toString());
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
