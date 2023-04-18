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

import static org.apache.kylin.engine.spark.job.StageType.WAITE_FOR_RESOURCE;
import static org.apache.kylin.engine.spark.utils.SparkConfHelper.COUNT_DISTICT;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.kylin.cluster.IClusterManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.util.Application;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.TimeZoneUtils;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.engine.spark.job.BuildJobInfos;
import org.apache.kylin.engine.spark.job.EnviromentAdaptor;
import org.apache.kylin.engine.spark.job.IJobProgressReport;
import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.kylin.engine.spark.job.LogJobInfoUtils;
import org.apache.kylin.engine.spark.job.NSparkCubingUtil;
import org.apache.kylin.engine.spark.job.ParamsConstants;
import org.apache.kylin.engine.spark.job.ResourceDetect;
import org.apache.kylin.engine.spark.job.RestfulJobProgressReport;
import org.apache.kylin.engine.spark.job.SparkJobConstants;
import org.apache.kylin.engine.spark.job.UdfManager;
import org.apache.kylin.engine.spark.scheduler.ClusterMonitor;
import org.apache.kylin.engine.spark.utils.HDFSUtils;
import org.apache.kylin.engine.spark.utils.JobMetricsUtils;
import org.apache.kylin.engine.spark.utils.SparkConfHelper;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.view.LogicalView;
import org.apache.kylin.metadata.view.LogicalViewManager;
import org.apache.kylin.query.pushdown.SparkSubmitter;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.application.NoRetryException;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.sql.KylinSession;
import org.apache.spark.sql.KylinSession$;
import org.apache.spark.sql.LogicalViewLoader;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.datasource.AlignmentTableStats;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import org.apache.kylin.engine.spark.job.SegmentBuildJob;
import lombok.val;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public abstract class SparkApplication implements Application {
    private static final Logger logger = LoggerFactory.getLogger(SparkApplication.class);
    private Map<String, String> params = Maps.newHashMap();
    public static final String JOB_NAME_PREFIX = "job_step_";
    private IJobProgressReport report;

    protected volatile KylinConfig config;
    protected volatile String jobId;
    protected String project;
    protected int layoutSize = -1;
    protected BuildJobInfos infos;
    protected ConcurrentHashMap<String, Boolean> skipFollowingStagesMap = new ConcurrentHashMap<>();
    /**
     * path for spark app args on HDFS
     */
    protected String path;

    private ClusterMonitor clusterMonitor;
    private final AtomicLong atomicDisconnectSparkMasterTimes = new AtomicLong(0);
    private final AtomicBoolean atomicUnreachableSparkMaster = new AtomicBoolean(false);
    private final AtomicReference<SparkConf> atomicSparkConf = new AtomicReference<>(null);
    private final AtomicReference<SparkSession> atomicSparkSession = new AtomicReference<>(null);
    private final AtomicReference<KylinBuildEnv> atomicBuildEnv = new AtomicReference<>(null);

    public void execute(String[] args) {
        try {
            path = args[0];
            String argsLine = readArgsFromHDFS();
            params = JsonUtil.readValueAsMap(argsLine);
            logger.info("Execute {} with args : {}", this.getClass().getName(), argsLine);
            execute();
        } catch (Exception e) {
            throw new RuntimeException("Error execute " + this.getClass().getName(), e);
        }
    }

    public AtomicBoolean getAtomicUnreachableSparkMaster() {
        return atomicUnreachableSparkMaster;
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

    public String getJobId() {
        return jobId;
    }

    public String getProject() {
        return project;
    }

    public KylinConfig getConfig() {
        return config;
    }

    public IJobProgressReport getReport() {
        if (report == null)
            return new RestfulJobProgressReport();
        return report;
    }

    /// backwards compatibility, must have been initialized before invoking #doExecute.
    protected SparkSession ss;

    public SparkSession getSparkSession() throws NoRetryException {
        SparkSession sparkSession = atomicSparkSession.get();
        if (Objects.isNull(sparkSession)) {
            // shouldn't reach here
            throw new NoRetryException("spark session shouldn't be null");
        }
        return sparkSession;
    }

    public String readArgsFromHDFS() {
        val fs = HadoopUtil.getFileSystem(path);
        String argsLine = null;
        Path filePath = new Path(path);
        try (FSDataInputStream inputStream = fs.open(filePath)) {
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, Charset.defaultCharset()));
            argsLine = br.readLine();
        } catch (IOException e) {
            logger.error("Error occurred when reading args file: {}", path, e);
        }
        return argsLine;
    }

    /**
     * get tracking url by application id
     *
     * @param sparkSession build sparkSession
     * @return
     */
    public String getTrackingUrl(IClusterManager clusterManager, SparkSession sparkSession) {
        return clusterManager.getBuildTrackingUrl(sparkSession);
    }

    public void setSkipFollowingStages(String segmentId) {
        skipFollowingStagesMap.put(segmentId, true);
    }

    public boolean isSkipFollowingStages(String segmentId) {
        if (segmentId == null) {
            return false;
        }
        return Optional.ofNullable(skipFollowingStagesMap.get(segmentId)).orElse(false);
    }

    private String tryReplaceHostAddress(String url) {
        String originHost = null;
        try {
            URI uri = URI.create(url);
            originHost = uri.getHost();
            String hostAddress = InetAddress.getByName(originHost).getHostAddress();
            return url.replace(originHost, hostAddress);
        } catch (UnknownHostException uhe) {
            logger.error("failed to get the ip address of {}, step back to use the origin tracking url.", originHost,
                    uhe);
            return url;
        }
    }

    private Map<String, String> getTrackingInfo(SparkSession sparkSession, boolean ipAddressPreferred) {
        IClusterManager clusterManager = atomicBuildEnv.get().clusterManager();
        String applicationId = sparkSession.sparkContext().applicationId();
        Map<String, String> extraInfo = new HashMap<>();
        extraInfo.put("yarn_app_id", applicationId);
        try {
            String trackingUrl = getTrackingUrl(clusterManager, sparkSession);
            if (StringUtils.isBlank(trackingUrl)) {
                logger.warn("Get tracking url of application {}, but empty url found.", applicationId);
                return extraInfo;
            }
            if (ipAddressPreferred) {
                trackingUrl = tryReplaceHostAddress(trackingUrl);
            }
            extraInfo.put("yarn_app_url", trackingUrl);
        } catch (Exception e) {
            logger.error("get tracking url failed!", e);
        }
        return extraInfo;
    }

    protected void exchangeSparkSession() {
        exchangeSparkSession(atomicSparkConf.get());
    }

    protected final void execute() throws Exception {
        String hdfsMetalUrl = getParam(NBatchConstants.P_DIST_META_URL);
        jobId = getParam(NBatchConstants.P_JOB_ID);
        project = getParam(NBatchConstants.P_PROJECT_NAME);
        if (getParam(NBatchConstants.P_LAYOUT_IDS) != null) {
            layoutSize = StringUtils.split(getParam(NBatchConstants.P_LAYOUT_IDS), ",").length;
        }
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoCloseConfig = KylinConfig
                .setAndUnsetThreadLocalConfig(KylinConfig.loadKylinConfigFromHdfs(hdfsMetalUrl))) {
            config = autoCloseConfig.get();
            report = (IJobProgressReport) ClassUtil.newInstance(config.getBuildJobProgressReporter());
            report.initArgsParams(getParams());
            //// KylinBuildEnv
            final KylinBuildEnv buildEnv = KylinBuildEnv.getOrCreate(config);
            atomicBuildEnv.set(buildEnv);
            infos = buildEnv.buildJobInfos();
            infos.recordJobId(jobId);
            infos.recordProject(project);
            infos.recordJobStepId(System.getProperty("spark.driver.param.taskId", jobId));

            monitorSparkMaster();

            HadoopUtil.setCurrentConfiguration(new Configuration());
            ////////
            exchangeSparkConf(buildEnv.sparkConf());

            TimeZoneUtils.setDefaultTimeZone(config);

            /// wait until resource is enough
            waiteForResource(atomicSparkConf.get(), buildEnv);

            ///
            logger.info("Prepare job environment");
            prepareSparkSession();

            /// backwards compatibility
            ss = getSparkSession();
            val master = ss.conf().get(SparkLauncher.SPARK_MASTER, "");
            if (!master.equals("local")) {
                EnviromentAdaptor adaptor = (EnviromentAdaptor) ClassUtil
                        .newInstance(config.getBuildJobEnviromentAdaptor());
                adaptor.prepareEnviroment(ss, params);
            }

            if (config.useDynamicS3RoleCredentialInTable()) {
                val tableMetadataManager = NTableMetadataManager.getInstance(config, project);
                tableMetadataManager.listAllTables().forEach(tableDesc -> SparderEnv.addS3Credential(
                        tableMetadataManager.getOrCreateTableExt(tableDesc).getS3RoleCredentialInfo(), ss));
            }

            if (!config.isUTEnv()) {
                Unsafe.setProperty("kylin.env", config.getDeployEnv());
            }
            logger.info("Start job");
            infos.startJob();
            // should be invoked after method prepareSparkSession
            extraInit();

            waiteForResourceSuccess();
            doExecute();
            // Output metadata to another folder
            val resourceStore = ResourceStore.getKylinMetaStore(config);
            val outputConfig = KylinConfig.createKylinConfig(config);
            outputConfig.setMetadataUrl(getParam(NBatchConstants.P_OUTPUT_META_URL));
            MetadataStore.createMetadataStore(outputConfig).dump(resourceStore);
        } catch (Exception e) {
            handleException(e);
        } finally {
            if (infos != null) {
                infos.jobEnd();
            }
            destroySparkSession();
            extraDestroy();
            executeFinish();
        }
    }

    protected void handleException(Exception e) throws Exception {
        if (e instanceof AccessControlException) {
            interceptAccessControlException(e);
        }
        if (e instanceof RuntimeException && e.getCause() instanceof AccessControlException) {
            interceptAccessControlException(e.getCause());
        } else if (e instanceof RuntimeException && e.getCause() instanceof SparkException) {
            Throwable rootCause = extractRealRootCauseFromSparkException(e);
            if (rootCause instanceof AccessControlException) {
                interceptAccessControlException(e);
            }
        }
        throw e;
    }

    // Extract the real root exception that caused the spark job to fail.
    // For example. Intercepts Spark Job that fail due to  permissions exception to prevent unnecessary retry from wasting resources
    protected Throwable extractRealRootCauseFromSparkException(Exception e) {
        Throwable rootCause = e.getCause();
        while (rootCause instanceof SparkException) {
            rootCause = rootCause.getCause();
        }
        return rootCause;
    }

    // Permission exception will not be retried. Simply let the job fail.
    protected void interceptAccessControlException(Throwable e) throws NoRetryException {
        logger.error("Permission denied.", e);
        throw new NoRetryException("Permission denied.");
    }

    private SparkSession createSpark(SparkConf sparkConf) {
        SparkSession.Builder sessionBuilder = SparkSession.builder()
                .withExtensions(new AbstractFunction1<SparkSessionExtensions, BoxedUnit>() {
                    @Override
                    public BoxedUnit apply(SparkSessionExtensions v1) {
                        v1.injectPostHocResolutionRule(new AbstractFunction1<SparkSession, Rule<LogicalPlan>>() {
                            @Override
                            public Rule<LogicalPlan> apply(SparkSession session) {
                                return new AlignmentTableStats(session);
                            }
                        });
                        return BoxedUnit.UNIT;
                    }
                }).enableHiveSupport().config(sparkConf)
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        // If this is UT and SparkSession is already created, then use SparkSession.
        // Otherwise, we always use KylinSession
        boolean createWithSparkSession = !isJobOnCluster(sparkConf) && SparderEnv.isSparkAvailable();
        if (createWithSparkSession) {
            boolean isKylinSession = SparderEnv.getSparkSession() instanceof KylinSession;
            createWithSparkSession = !isKylinSession;
        }

        if (createWithSparkSession) {
            return sessionBuilder.getOrCreate();
        } else {
            return KylinSession$.MODULE$.KylinBuilder(sessionBuilder).buildCluster().getOrCreateKylinSession();
        }
    }

    public boolean isJobOnCluster(SparkConf conf) {
        return !Utils.isLocalMaster(conf) && !config.isUTEnv();
    }

    protected void extraInit() {
        loadLogicalView();
    }

    public void extraDestroy() {
        if (config != null && StringUtils.isNotEmpty(config.getKubernetesUploadPath())) {
            logger.info("uploadPath={}", config.getKubernetesUploadPath());
            try {
                HDFSUtils.deleteMarkFile(config.getKubernetesUploadPath());
            } catch (Exception e) {
                logger.warn("Failed to delete " + config.getKubernetesUploadPath(), e);
            }
        }
        if (clusterMonitor != null) {
            clusterMonitor.shutdown();
        }
    }

    protected abstract void doExecute() throws Exception;

    protected void onLayoutFinished(long layoutId) {
        //do nothing
    }

    protected void onExecuteFinished() {
        //do nothing
    }

    protected String calculateRequiredCores() throws Exception {
        return SparkJobConstants.DEFAULT_REQUIRED_CORES;
    }

    private void autoSetSparkConf(SparkConf sparkConf) throws Exception {
        SparkConfHelper helper = new SparkConfHelper();
        // copy user defined spark conf
        if (sparkConf.getAll() != null) {
            Arrays.stream(sparkConf.getAll()).forEach(config -> helper.setConf(config._1, config._2));
        }
        helper.setClusterManager(KylinBuildEnv.get().clusterManager());

        chooseContentSize(helper);

        helper.setOption(SparkConfHelper.LAYOUT_SIZE, Integer.toString(layoutSize));
        helper.setOption(SparkConfHelper.REQUIRED_CORES, calculateRequiredCores());
        helper.setConf(COUNT_DISTICT, hasCountDistinct().toString());
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
    }

    private void waiteForResource(SparkConf sparkConf, KylinBuildEnv buildEnv) throws Exception {
        val waiteForResource = WAITE_FOR_RESOURCE.create(this, null, null);
        infos.recordStageId(waiteForResource.getId());
        waiteForResource.execute();
    }

    protected void waiteForResourceSuccess() throws Exception {
        val waiteForResource = WAITE_FOR_RESOURCE.create(this, null, null);
        waiteForResource.onStageFinished(ExecutableState.SUCCEED);
        infos.recordStageId("");
    }

    protected void executeFinish() {
        try {
            getReport().executeFinish(getReportParams(), project, getJobId());
        } catch (Exception e) {
            logger.error("executeFinish failed", e);
        }
    }

    protected void chooseContentSize(SparkConfHelper helper) {
        Path shareDir = config.getJobTmpShareDir(project, jobId);
        // add content size with unit
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, chooseContentSize(shareDir));
    }

    protected boolean checkRangePartitionTableIsExist(NDataModel modelDesc) {
        return modelDesc.getAllTableRefs().stream().anyMatch(p -> p.getTableDesc().isRangePartition());
    }

    protected String chooseContentSize(Path shareDir) {
        // return size with unit
        return ResourceDetectUtils.getMaxResourceSize(shareDir) + "b";
    }

    protected Boolean hasCountDistinct() throws IOException {
        Path countDistinct = new Path(config.getJobTmpShareDir(project, jobId),
                ResourceDetectUtils.countDistinctSuffix());
        // Keep the same with ResourceDetectUtils#write
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
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
            if (KylinConfig.getInstanceFromEnv().skipRecordJobExecutionTime()) {
                logger.info("skip record job wait and run time");
                return;
            }
            Map<String, String> extraInfo = new HashMap<>();
            extraInfo.put("yarn_job_wait_time", ((Long) KylinBuildEnv.get().buildJobInfos().waitTime()).toString());
            extraInfo.put("yarn_job_run_time", ((Long) KylinBuildEnv.get().buildJobInfos().buildTime()).toString());

            getReport().updateSparkJobExtraInfo(getReportParams(), "/kylin/api/jobs/wait_and_run_time", project, jobId,
                    extraInfo);
        } catch (Exception e) {
            logger.warn("Error occurred when generate job info.", e);
        }
    }

    private Map<String, String> getReportParams() {
        val reportParams = new HashMap<String, String>();
        reportParams.put(ParamsConstants.TIME_OUT, String.valueOf(config.getUpdateJobInfoTimeout()));
        reportParams.put(ParamsConstants.JOB_TMP_DIR, config.getJobTmpDir(project, true));
        return reportParams;
    }

    protected String generateInfo() {
        return LogJobInfoUtils.sparkApplicationInfo();
    }

    public Set<String> getIgnoredSnapshotTables() {
        return NSparkCubingUtil.toIgnoredTableSet(getParam(NBatchConstants.P_IGNORED_SNAPSHOT_TABLES));
    }

    protected Map<String, String> getSparkConfigOverride(KylinConfig config) {
        return config.getSparkConfigOverride();
    }

    protected void checkDateFormatIfExist(String project, String modelId) throws Exception {
        if (config.isUTEnv()) {
            return;
        }
        val modelManager = NDataModelManager.getInstance(config, project);
        NDataModel model = modelManager.getDataModelDesc(modelId);
        if (checkRangePartitionTableIsExist(model)) {
            logger.info("Range partitioned tables do not support pushdown, so do not need to perform subsequent logic");
            return;
        }

        val partitionDesc = model.getPartitionDesc();
        if (PartitionDesc.isEmptyPartitionDesc(partitionDesc)
                || StringUtils.isEmpty(partitionDesc.getPartitionDateFormat()))
            return;

        if (CatalogTableType.VIEW().name().equals(model.getRootFactTable().getTableDesc().getTableType()))
            return;

        String partitionColumn = model.getPartitionDesc().getPartitionDateColumnRef().getBackTickExp();

        SparkSession sparkSession = atomicSparkSession.get();
        try (SparkSubmitter.OverriddenSparkSession ignored = SparkSubmitter.getInstance()
                .overrideSparkSession(sparkSession)) {
            String dateString = PushDownUtil.probeColFormat(model.getRootFactTableName(), partitionColumn, project);
            val sdf = new SimpleDateFormat(model.getPartitionDesc().getPartitionDateFormat(),
                    Locale.getDefault(Locale.Category.FORMAT));
            val date = sdf.parse(dateString);
            if (date == null || !dateString.equals(sdf.format(date))) {
                throw new NoRetryException("date format not match");
            }
        } catch (KylinException ignore) {
            // ignore it when pushdown return empty row
        } catch (ParseException | NoRetryException e) {
            throw new NoRetryException("date format not match");
        }
    }

    @VisibleForTesting
    void exchangeSparkConf(SparkConf sparkConf) throws Exception {
        if (isJobOnCluster(sparkConf) && !(this instanceof ResourceDetect)) {
            Map<String, String> baseSparkConf = getSparkConfigOverride(config);
            if (!baseSparkConf.isEmpty()) {
                baseSparkConf.forEach(sparkConf::set);
                String baseSparkConfStr = JsonUtil.writeValueAsString(baseSparkConf);
                logger.info("Override user-defined spark conf: {}", baseSparkConfStr);
            }
            if (config.isAutoSetSparkConf()) {
                logger.info("Set spark conf automatically.");
                try {
                    autoSetSparkConf(sparkConf);
                } catch (Exception e) {
                    logger.warn("Auto set spark conf failed. Load spark conf from system properties", e);
                }
            }

        }
        val eventLogEnabled = sparkConf.getBoolean("spark.eventLog.enabled", false);
        val logDir = sparkConf.get("spark.eventLog.dir", "");
        if (eventLogEnabled && !logDir.isEmpty()) {
            val logPath = new Path(new URI(logDir).getPath());
            val fs = HadoopUtil.getWorkingFileSystem();
            if (!fs.exists(logPath)) {
                fs.mkdirs(logPath);
            }
        }

        atomicSparkConf.set(sparkConf);
    }

    private void exchangeSparkSession(SparkConf sparkConf) {
        SparkSession sparkSession = atomicSparkSession.get();
        if (Objects.nonNull(sparkSession)) {
            // destroy previous spark session
            destroySparkSession();
        }

        sparkSession = createSpark(sparkConf);
        if (!config.isUTEnv() && !sparkConf.get("spark.master").startsWith("k8s")) {
            getReport().updateSparkJobExtraInfo(getReportParams(), "/kylin/api/jobs/spark", project, jobId,
                    getTrackingInfo(sparkSession, config.isTrackingUrlIpAddressEnabled()));
        }

        // for spark metrics
        JobMetricsUtils.registerListener(sparkSession);
        SparderEnv.registerListener(sparkSession.sparkContext());

        //#8341
        SparderEnv.setSparkSession(sparkSession);
        UdfManager.create(sparkSession);

        ///
        atomicSparkSession.set(sparkSession);
    }

    private void prepareSparkSession() throws NoRetryException {
        SparkConf sparkConf = atomicSparkConf.get();
        if (Objects.isNull(sparkConf)) {
            // shouldn't reach here
            throw new NoRetryException("spark conf shouldn't be null");
        }

        /// SegmentBuildJob only!!!
        if (config.isSnapshotSpecifiedSparkConf() && (this instanceof SegmentBuildJob)) {
            // snapshot specified spark conf, based on the exchanged spark conf.
            SparkConf clonedSparkConf = sparkConf.clone();
            Map<String, String> snapshotSparkConf = config.getSnapshotBuildingConfigOverride();
            snapshotSparkConf.forEach(clonedSparkConf::set);
            logger.info("exchange sparkSession using snapshot specified sparkConf");
            exchangeSparkSession(clonedSparkConf);
            return;
        }
        // normal logic
        exchangeSparkSession(sparkConf);
    }

    private void destroySparkSession() {
        SparkSession sparkSession = atomicSparkSession.get();
        if (Objects.isNull(sparkSession)) {
            logger.info("no initialized sparkSession instance");
            return;
        }
        if (sparkSession.conf().get("spark.master").startsWith("local")) {
            // for UT use? but very strange for resource detect mode (spark local).
            return;
        }
        JobMetricsUtils.unRegisterListener(sparkSession);
        sparkSession.stop();
    }

    private void monitorSparkMaster() {
        clusterMonitor = new ClusterMonitor();
        clusterMonitor.monitorSparkMaster(atomicBuildEnv, atomicSparkSession, atomicDisconnectSparkMasterTimes,
                atomicUnreachableSparkMaster);
    }

    @VisibleForTesting
    public void loadLogicalView() {
        if (!config.isDDLLogicalViewEnabled()) {
            return;
        }
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        String tableName = getParam(NBatchConstants.P_TABLE_NAME);
        LogicalViewManager viewManager = LogicalViewManager.getInstance(config);

        if (StringUtils.isNotBlank(dataflowId)) {
            viewManager.findLogicalViewsInModel(project, dataflowId)
                    .forEach(view -> LogicalViewLoader.loadView(view.getTableName(), true, ss));
        }
        if (StringUtils.isNotBlank(tableName)) {
            LogicalView view = viewManager.findLogicalViewInProject(getProject(), tableName);
            if (view != null) {
                LogicalViewLoader.loadView(view.getTableName(), true, ss);
            }
        }
    }
}
