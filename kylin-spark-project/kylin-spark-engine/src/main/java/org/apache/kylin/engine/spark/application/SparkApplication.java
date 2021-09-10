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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.engine.spark.job.BuildJobInfos;
import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.kylin.engine.spark.job.LogJobInfoUtils;
import org.apache.kylin.engine.spark.job.UdfManager;
import org.apache.kylin.engine.spark.utils.MetaDumpUtil;
import org.apache.kylin.engine.spark.utils.SparkConfHelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.StandaloneAppClient;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.apache.spark.util.Utils;
import org.apache.spark.utils.ResourceUtils;
import org.apache.spark.utils.SparkVersionUtils;
import org.apache.spark.utils.YarnInfoFetcherUtils;
import org.apache.kylin.engine.spark.common.util.TimeZoneUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        Path path = new Path(args[0]);
        FileSystem fs = HadoopUtil.getFileSystem(path);
        try (
                FSDataInputStream inputStream = fs.open(path);
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)
        ) {
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String argsLine = bufferedReader.readLine();
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

    /**
     * http request the spark job controller
     */
    public Boolean updateSparkJobInfo(String url, String json) {
        String serverAddress = System.getProperty("spark.driver.rest.server.address", "127.0.0.1:7070");
        String requestApi = String.format(Locale.ROOT, "http://%s%s", serverAddress, url);

        try {
            DefaultHttpClient httpClient = new DefaultHttpClient();
            HttpPut httpPut = new HttpPut(requestApi);
            httpPut.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
            httpPut.setEntity(new StringEntity(json, StandardCharsets.UTF_8));

            HttpResponse response = httpClient.execute(httpPut);
            int code = response.getStatusLine().getStatusCode();
            if (code == HttpStatus.SC_OK) {
                return true;
            } else {
                InputStream inputStream = response.getEntity().getContent();
                String responseContent = IOUtils.toString(inputStream);
                logger.warn("update spark job failed, info: {}", responseContent);
            }
        } catch (IOException e) {
            logger.error("http request {} failed!", requestApi, e);
        }
        return false;
    }

    /**
     * update spark job extra info, link yarn_application_tracking_url
     */
    public Boolean updateSparkJobExtraInfo(String url, String project, String jobId, Map<String, String> extraInfo) {
        Map<String, String> payload = new HashMap<>(5);
        payload.put("project", project);
        payload.put("taskId", System.getProperty("spark.driver.param.taskId", jobId));
        payload.putAll(extraInfo);

        try {
            String payloadJson = JsonUtil.writeValueAsString(payload);
            int retry = 3;
            for (int i = 0; i < retry; i++) {
                if (updateSparkJobInfo(url, payloadJson)) {
                    return Boolean.TRUE;
                }
                Thread.sleep(3000);
                logger.warn("retry request rest api update spark extra job info");
            }
        } catch (Exception e) {
            logger.error("update spark job extra info failed!", e);
        }

        return Boolean.FALSE;
    }

    private String tryReplaceHostAddress(String url) {
        String originHost = null;
        try {
            URI uri = URI.create(url);
            originHost = uri.getHost();
            String hostAddress = InetAddress.getByName(originHost).getHostAddress();
            return url.replace(originHost, hostAddress);
        } catch (UnknownHostException uhe) {
            logger.error("failed to get the ip address of " + originHost + ", step back to use the origin tracking url.", uhe);
            return url;
        }
    }

    private Map<String, String> getTrackingInfo(boolean ipAddressPreferred, String sparkMaster) {
        String applicationId = ss.sparkContext().applicationId();
        Map<String, String> extraInfo = new HashMap<>();
        try {
            String trackingUrl = getTrackingUrl(applicationId);
            if (sparkMaster.startsWith("spark")) {
                trackingUrl = StandaloneAppClient.getAppUrl(applicationId, sparkMaster);
            }
            if (StringUtils.isBlank(trackingUrl)) {
                logger.warn("Get tracking url of application {}, but empty url found.", applicationId);
                return extraInfo;
            }
            if (ipAddressPreferred) {
                trackingUrl = tryReplaceHostAddress(trackingUrl);
            }
            extraInfo.put("yarnAppUrl", trackingUrl);
        } catch (Exception e) {
            logger.error("get tracking url failed!", e);
        }
        return extraInfo;
    }


    final protected void execute() throws Exception {
        String hdfsMetalUrl = getParam(MetadataConstants.P_DIST_META_URL);
        jobId = getParam(MetadataConstants.P_JOB_ID);
        project = getParam(MetadataConstants.P_PROJECT_NAME);
        if (getParam(MetadataConstants.P_CUBOID_NUMBER) != null) {
            layoutSize = Integer.parseInt(getParam(MetadataConstants.P_CUBOID_NUMBER));
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
                if (!config.getSparkConfigOverride().isEmpty()) {
                    for (Map.Entry<String, String> entry : config.getSparkConfigOverride().entrySet()) {
                        logger.info("Override user-defined spark conf, set {}={}.", entry.getKey(), entry.getValue());
                        sparkConf.set(entry.getKey(), entry.getValue());
                    }
                }
            } else if (!isJobOnCluster(sparkConf)) {
                sparkConf.set("spark.master", "local");
                if (!config.getSparkConfigOverride().containsKey("spark.sql.shuffle.partitions")) {
                    sparkConf.set("spark.sql.shuffle.partitions", "1");
                }
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
                sparkConf.set("spark.sql.adaptive.enabled", "false");
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

            ss = SparkSession.builder()
                    .enableHiveSupport()
                    .config(sparkConf)
                    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                    .getOrCreate();

            if (isJobOnCluster(sparkConf)) {
                updateSparkJobExtraInfo("/kylin/api/jobs/spark", project, jobId,
                        getTrackingInfo(config.isTrackingUrlIpAddressEnabled(), sparkConf.get("spark.master")));
            }
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
        }
    }

    public boolean isJobOnCluster(SparkConf conf) {
        return !Utils.isLocalMaster(conf) && !config.isUTEnv();
    }

    protected abstract void doExecute() throws Exception;

    protected String calculateRequiredCores() throws Exception {
        return config.getSparkEngineRequiredTotalCores();
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
