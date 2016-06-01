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

package org.apache.kylin.common;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@SuppressWarnings("serial")
/**
 * An abstract class to encapsulate access to a set of 'properties'.
 * Subclass can override methods in this class to extend the content of the 'properties',
 * with some override values for example.
 */
abstract public class KylinConfigBase implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(KylinConfigBase.class);

    /*
     * DON'T DEFINE CONSTANTS FOR PROPERTY KEYS!
     * 
     * For 1), no external need to access property keys, all accesses are by public methods.
     * For 2), it's cumbersome to maintain constants at top and code at bottom.
     * For 3), key literals usually appear only once.
     */

    public static String getKylinHome() {
        String kylinHome = System.getenv("KYLIN_HOME");
        if (StringUtils.isEmpty(kylinHome)) {
            logger.warn("KYLIN_HOME was not set");
        }
        return kylinHome;
    }

    // ============================================================================

    private volatile Properties properties = new Properties();

    public KylinConfigBase() {
        this(new Properties());
    }

    public KylinConfigBase(Properties props) {
        this.properties = props;
    }

    final protected String getOptional(String prop) {
        return getOptional(prop, null);
    }

    protected String getOptional(String prop, String dft) {
        final String property = System.getProperty(prop);
        return property != null ? property : properties.getProperty(prop, dft);
    }

    protected Properties getAllProperties() {
        return properties;
    }
    
    final protected Map<String, String> getPropertiesByPrefix(String prefix) {
        Map<String, String> result = Maps.newLinkedHashMap();
        for (Entry<Object, Object> entry : getAllProperties().entrySet()) {
            String key = (String) entry.getKey();
            if (key.startsWith(prefix)) {
                result.put(key.substring(prefix.length()), (String) entry.getValue());
            }
        }
        return result;
    }

    final protected String[] getOptionalStringArray(String prop, String[] dft) {
        final String property = getOptional(prop);
        if (!StringUtils.isBlank(property)) {
            return property.split("\\s*,\\s*");
        } else {
            return dft;
        }
    }

    final public String getRequired(String prop) {
        String r = getOptional(prop);
        if (StringUtils.isEmpty(r)) {
            throw new IllegalArgumentException("missing '" + prop + "' in conf/kylin.properties");
        }
        return r;
    }

    /**
     * Use with care, properties should be read-only. This is for testing mostly.
     */
    final public void setProperty(String key, String value) {
        logger.info("Kylin Config was updated with " + key + " : " + value);
        properties.setProperty(key, value);
    }

    final protected void reloadKylinConfig(InputStream is) {
        Properties newProperties = new Properties();
        try {
            newProperties.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Cannot load kylin config.", e);
        } finally {
            IOUtils.closeQuietly(is);
        }
        this.properties = newProperties;
    }

    // ============================================================================

    public boolean isDevEnv() {
        return "DEV".equals(getOptional("deploy.env", "DEV"));
    }

    public String getMetadataUrl() {
        return getOptional("kylin.metadata.url");
    }

    public void setMetadataUrl(String metadataUrl) {
        setProperty("kylin.metadata.url", metadataUrl);
    }

    public String getMetadataUrlPrefix() {
        String hbaseMetadataUrl = getMetadataUrl();
        String defaultPrefix = "kylin_metadata";

        if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(hbaseMetadataUrl, "@hbase")) {
            int cut = hbaseMetadataUrl.indexOf('@');
            String tmp = cut < 0 ? defaultPrefix : hbaseMetadataUrl.substring(0, cut);
            return tmp;
        } else {
            return defaultPrefix;
        }
    }

    public String getServerMode() {
        return this.getOptional("kylin.server.mode", "all");
    }

    public String getStorageUrl() {
        return getOptional("kylin.storage.url");
    }

    public void setStorageUrl(String storageUrl) {
        setProperty("kylin.storage.url", storageUrl);
    }

    /** was for route to hive, not used any more */
    @Deprecated
    public String getHiveUrl() {
        return getOptional("hive.url", "");
    }

    /** was for route to hive, not used any more */
    @Deprecated
    public String getHiveUser() {
        return getOptional("hive.user", "");
    }

    /** was for route to hive, not used any more */
    @Deprecated
    public String getHivePassword() {
        return getOptional("hive.password", "");
    }

    public String getHdfsWorkingDirectory() {
        String root = getRequired("kylin.hdfs.working.dir");
        if (!root.endsWith("/")) {
            root += "/";
        }
        return new StringBuffer(root).append(StringUtils.replaceChars(getMetadataUrlPrefix(), ':', '-')).append("/").toString();
    }

    public CliCommandExecutor getCliCommandExecutor() throws IOException {
        CliCommandExecutor exec = new CliCommandExecutor();
        if (getRunAsRemoteCommand()) {
            exec.setRunAtRemote(getRemoteHadoopCliHostname(), getRemoteHadoopCliPort(), getRemoteHadoopCliUsername(), getRemoteHadoopCliPassword());
        }
        return exec;
    }

    public String getHBaseClusterFs() {
        return getOptional("kylin.hbase.cluster.fs", "");
    }

    public String getKylinJobLogDir() {
        return getOptional("kylin.job.log.dir", "/tmp/kylin/logs");
    }

    public String getKylinJobJarPath() {
        final String jobJar = getOptional("kylin.job.jar");
        if (StringUtils.isNotEmpty(jobJar)) {
            return jobJar;
        }
        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome)) {
            return "";
        }
        return getFileName(kylinHome + File.separator + "lib", JOB_JAR_NAME_PATTERN);
    }

    public void overrideMRJobJarPath(String path) {
        logger.info("override " + "kylin.job.jar" + " to " + path);
        System.setProperty("kylin.job.jar", path);
    }

    public String getKylinJobMRLibDir() {
        return getOptional("kylin.job.mr.lib.dir", "");
    }

    public Map<String, String> getMRConfigOverride() {
        return getPropertiesByPrefix("kylin.job.mr.config.override.");
    }

    public String getKylinSparkJobJarPath() {
        final String jobJar = getOptional("kylin.job.jar.spark");
        if (StringUtils.isNotEmpty(jobJar)) {
            return jobJar;
        }
        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome)) {
            return "";
        }
        return getFileName(kylinHome + File.separator + "lib", SPARK_JOB_JAR_NAME_PATTERN);
    }

    public void overrideSparkJobJarPath(String path) {
        logger.info("override " + "kylin.job.jar.spark" + " to " + path);
        System.setProperty("kylin.job.jar.spark", path);
    }

    private static final Pattern COPROCESSOR_JAR_NAME_PATTERN = Pattern.compile("kylin-coprocessor-(.+)\\.jar");
    private static final Pattern JOB_JAR_NAME_PATTERN = Pattern.compile("kylin-job-(.+)\\.jar");
    private static final Pattern SPARK_JOB_JAR_NAME_PATTERN = Pattern.compile("kylin-engine-spark-(.+)\\.jar");

    public String getCoprocessorLocalJar() {
        final String coprocessorJar = getOptional("kylin.coprocessor.local.jar");
        if (StringUtils.isNotEmpty(coprocessorJar)) {
            return coprocessorJar;
        }
        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome)) {
            throw new RuntimeException("getCoprocessorLocalJar needs KYLIN_HOME");
        }
        return getFileName(kylinHome + File.separator + "lib", COPROCESSOR_JAR_NAME_PATTERN);
    }

    public void overrideCoprocessorLocalJar(String path) {
        logger.info("override " + "kylin.coprocessor.local.jar" + " to " + path);
        System.setProperty("kylin.coprocessor.local.jar", path);
    }

    private static String getFileName(String homePath, Pattern pattern) {
        File home = new File(homePath);
        SortedSet<String> files = Sets.newTreeSet();
        if (home.exists() && home.isDirectory()) {
            for (File file : home.listFiles()) {
                final Matcher matcher = pattern.matcher(file.getName());
                if (matcher.matches()) {
                    files.add(file.getAbsolutePath());
                }
            }
        }
        if (files.isEmpty()) {
            throw new RuntimeException("cannot find " + pattern.toString() + " in " + homePath);
        } else {
            return files.last();
        }
    }

    public double getDefaultHadoopJobReducerInputMB() {
        return Double.parseDouble(getOptional("kylin.job.mapreduce.default.reduce.input.mb", "500"));
    }

    public double getDefaultHadoopJobReducerCountRatio() {
        return Double.parseDouble(getOptional("kylin.job.mapreduce.default.reduce.count.ratio", "1.0"));
    }

    public int getHadoopJobMinReducerNumber() {
        return Integer.parseInt(getOptional("kylin.job.mapreduce.min.reducer.number", "1"));
    }

    public int getHadoopJobMaxReducerNumber() {
        return Integer.parseInt(getOptional("kylin.job.mapreduce.max.reducer.number", "500"));
    }

    public int getHadoopJobMapperInputRows() {
        return Integer.parseInt(getOptional("kylin.job.mapreduce.mapper.input.rows", "500000"));
    }

    public boolean getRunAsRemoteCommand() {
        return Boolean.parseBoolean(getOptional("kylin.job.run.as.remote.cmd"));
    }

    public void setRunAsRemoteCommand(String v) {
        setProperty("kylin.job.run.as.remote.cmd", v);
    }

    public int getRemoteHadoopCliPort() {
        return Integer.parseInt(getOptional("kylin.job.remote.cli.port", "22"));
    }

    public String getRemoteHadoopCliHostname() {
        return getOptional("kylin.job.remote.cli.hostname");
    }

    public void setRemoteHadoopCliHostname(String v) {
        setProperty("kylin.job.remote.cli.hostname", v);
    }

    public String getRemoteHadoopCliUsername() {
        return getOptional("kylin.job.remote.cli.username");
    }

    public void setRemoteHadoopCliUsername(String v) {
        setProperty("kylin.job.remote.cli.username", v);
    }

    public String getRemoteHadoopCliPassword() {
        return getOptional("kylin.job.remote.cli.password");
    }

    public void setRemoteHadoopCliPassword(String v) {
        setProperty("kylin.job.remote.cli.password", v);
    }

    public String getCliWorkingDir() {
        return getOptional("kylin.job.remote.cli.working.dir");
    }

    public String getMapReduceCmdExtraArgs() {
        return getOptional("kylin.job.cmd.extra.args");
    }

    public String getOverrideHiveTableLocation(String table) {
        return getOptional("hive.table.location." + table.toUpperCase());
    }

    public String getYarnStatusCheckUrl() {
        return getOptional("kylin.job.yarn.app.rest.check.status.url", null);
    }

    public int getYarnStatusCheckIntervalSeconds() {
        return Integer.parseInt(getOptional("kylin.job.yarn.app.rest.check.interval.seconds", "60"));
    }

    public int getMaxConcurrentJobLimit() {
        return Integer.parseInt(getOptional("kylin.job.concurrent.max.limit", "10"));
    }

    public String getTimeZone() {
        return getOptional("kylin.rest.timezone", "PST");
    }

    public String[] getRestServers() {
        return getOptionalStringArray("kylin.rest.servers", new String[0]);
    }

    public String getClusterName() {
        return this.getOptional("kylin.cluster.name", getMetadataUrlPrefix());
    }

    public void setClusterName(String clusterName) {
        setProperty("kylin.cluster.name", clusterName);
    }


    public int getWorkersPerServer() {
        //for sequence sql use
        return Integer.parseInt(getOptional("kylin.rest.workers.per.server", "1"));
    }

    public String[] getAdminDls() {
        return getOptionalStringArray("kylin.job.admin.dls", null);
    }

    public long getJobStepTimeout() {
        return Long.parseLong(getOptional("kylin.job.step.timeout", String.valueOf(2 * 60 * 60)));
    }

    public double getJobCuboidSizeRatio(){
        return Double.parseDouble(getOptional("kylin.job.cuboid.size.ratio", "0.25"));
    }

    public double getJobCuboidSizeMemHungryRatio(){
        return Double.parseDouble(getOptional("kylin.job.cuboid.size.memhungry.ratio", "0.05"));
    }

    public String getCubeAlgorithm() {
        return getOptional("kylin.cube.algorithm", "auto");
    }

    public double getCubeAlgorithmAutoThreshold() {
        return Double.parseDouble(getOptional("kylin.cube.algorithm.auto.threshold", "8"));
    }

    @Deprecated
    public int getCubeAggrGroupMaxSize() {
        return Integer.parseInt(getOptional("kylin.cube.aggrgroup.max.size", "12"));
    }

    public int getCubeAggrGroupMaxCombination() {
        return Integer.parseInt(getOptional("kylin.cube.aggrgroup.max.combination", "4096"));
    }

    public boolean getCubeAggrGroupIsMandatoryOnlyValid() {
        return Boolean.parseBoolean(getOptional("kylin.cube.aggrgroup.isMandatoryOnlyValid", "false"));
    }

    public String[] getCubeDimensionCustomEncodingFactories() {
        return getOptionalStringArray("kylin.cube.dimension.customEncodingFactories", new String[0]);
    }

    public Map<String, String> getCubeCustomMeasureTypes() {
        return getPropertiesByPrefix("kylin.cube.measure.customMeasureType.");
    }

    public int getDictionaryMaxCardinality() {
        return Integer.parseInt(getOptional("kylin.dictionary.max.cardinality", "5000000"));
    }

    public int getTableSnapshotMaxMB() {
        return Integer.parseInt(getOptional("kylin.table.snapshot.max_mb", "300"));
    }

    public int getHBaseRegionCountMin() {
        return Integer.parseInt(getOptional("kylin.hbase.region.count.min", "1"));
    }

    public int getHBaseRegionCountMax() {
        return Integer.parseInt(getOptional("kylin.hbase.region.count.max", "500"));
    }

    public void setHBaseHFileSizeGB(float size) {
        setProperty("kylin.hbase.hfile.size.gb", String.valueOf(size));
    }

    public float getHBaseHFileSizeGB() {
        return Float.parseFloat(getOptional("kylin.hbase.hfile.size.gb", "2.0"));
    }

    public int getScanThreshold() {
        return Integer.parseInt(getOptional("kylin.query.scan.threshold", "10000000"));
    }

    public int getCubeVisitTimeoutTimes() {
        return Integer.parseInt(getOptional("kylin.query.cube.visit.timeout.times", "1"));
    }

    public int getBadQueryStackTraceDepth() {
        return Integer.parseInt(getOptional("kylin.query.badquery.stacktrace.depth", "10"));
    }

    public int getBadQueryHistoryNum() {
        return Integer.parseInt(getOptional("kylin.query.badquery.history.num", "10"));
    }

    public int getBadQueryDefaultAlertingSeconds() {
        return Integer.parseInt(getOptional("kylin.query.badquery.alerting.seconds", "90"));
    }

    public int getBadQueryDefaultDetectIntervalSeconds() {
        return Integer.parseInt(getOptional("kylin.query.badquery.detect.interval.seconds", "60"));
    }

    public boolean getBadQueryPersistentEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.badquery.persistent.enable", "true"));
    }

    public int getCachedDictMaxEntrySize() {
        return Integer.parseInt(getOptional("kylin.dict.cache.max.entry", "3000"));
    }

    public boolean getQueryRunLocalCoprocessor() {
        return Boolean.parseBoolean(getOptional("kylin.query.run.local.coprocessor", "false"));
    }

    public Long getQueryDurationCacheThreshold() {
        return Long.parseLong(this.getOptional("kylin.query.cache.threshold.duration", String.valueOf(2000)));
    }

    public Long getQueryScanCountCacheThreshold() {
        return Long.parseLong(this.getOptional("kylin.query.cache.threshold.scancount", String.valueOf(10 * 1024)));
    }

    public long getQueryMemBudget() {
        return Long.parseLong(this.getOptional("kylin.query.mem.budget", String.valueOf(3L * 1024 * 1024 * 1024)));
    }

    public double getQueryCoprocessorMemGB() {
        return Double.parseDouble(this.getOptional("kylin.query.coprocessor.mem.gb", "3.0"));
    }

    public boolean isQuerySecureEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.security.enabled", "true"));
    }

    public boolean isQueryCacheEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.cache.enabled", "true"));
    }

    public boolean isQueryIgnoreUnknownFunction() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.ignore_unknown_function", "false"));
    }

    public String getQueryStorageVisitPlanner() {
        return this.getOptional("kylin.query.storage.visit.planner", "org.apache.kylin.gridtable.GTScanRangePlanner");
    }

    public void setQueryStorageVisitPlanner(String v) {
        setProperty("kylin.query.storage.visit.planner", v);
    }

    public int getQueryScanFuzzyKeyMax() {
        return Integer.parseInt(this.getOptional("kylin.query.scan.fuzzykey.max", "200"));
    }

    public int getQueryStorageVisitScanRangeMax() {
        return Integer.valueOf(this.getOptional("kylin.query.storage.visit.scanrange.max", "1000000"));
    }

    public long getSequenceExpireTime() {
        return Long.valueOf(this.getOptional("kylin.query.sequence.expire.time", "86400000"));//default a day
    }

    public int getHBaseKeyValueSize() {
        return Integer.parseInt(this.getOptional("kylin.hbase.client.keyvalue.maxsize", "10485760"));
    }

    public String getDefaultIGTStorage() {
        return getOptional("kylin.query.storage.default.gtstorage", "org.apache.kylin.storage.hbase.cube.v2.CubeHBaseEndpointRPC");
    }

    public int getHBaseScanCacheRows() {
        return Integer.parseInt(this.getOptional("kylin.hbase.scan.cache_rows", "1024"));
    }

    public boolean isGrowingDictEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.dict.growing.enabled", "false"));
    }

    /**
     * HBase region cut size, in GB
     * @return
     */
    public float getKylinHBaseRegionCut() {
        return Float.valueOf(getOptional("kylin.hbase.region.cut", "5.0"));
    }

    public int getHBaseScanMaxResultSize() {
        return Integer.parseInt(this.getOptional("kylin.hbase.scan.max_result_size", "" + (5 * 1024 * 1024))); // 5 MB
    }

    public int getCubingInMemSamplingPercent() {
        int percent = Integer.parseInt(this.getOptional("kylin.job.cubing.inmem.sampling.percent", "100"));
        percent = Math.max(percent, 1);
        percent = Math.min(percent, 100);
        return percent;
    }

    public String getHbaseDefaultCompressionCodec() {
        return getOptional("kylin.hbase.default.compression.codec", "");
    }

    public String getHbaseDefaultEncoding() {
        return getOptional("kylin.hbase.default.encoding", "FAST_DIFF");
    }

    public int getHbaseDefaultBlockSize() {
        return Integer.valueOf(getOptional("kylin.hbase.default.block.size", "1048576"));
    }

    public int getHbaseSmallFamilyBlockSize() {
        return Integer.valueOf(getOptional("kylin.hbase.small.family.block.size", "65536"));
    }

    public boolean isHiveKeepFlatTable() {
        return Boolean.parseBoolean(this.getOptional("kylin.hive.keep.flat.table", "false"));
    }

    public String getHiveDatabaseForIntermediateTable() {
        return this.getOptional("kylin.job.hive.database.for.intermediatetable", "default");
    }

    public boolean isGetJobStatusWithKerberos() {
        return Boolean.valueOf(this.getOptional("kylin.job.status.with.kerberos", "false"));
    }

    public String getKylinOwner() {
        return this.getOptional("kylin.owner", "");
    }

    public String getSparkHome() {
        return getRequired("kylin.spark.home");
    }

    public String getSparkMaster() {
        return getRequired("kylin.spark.master");
    }

    public boolean isMailEnabled() {
        return Boolean.parseBoolean(getOptional("mail.enabled", "false"));
    }

    public void setMailEnabled(boolean enable) {
        setProperty("mail.enabled", "" + enable);
    }

    public String getMailHost() {
        return getOptional("mail.host", "");
    }

    public String getMailUsername() {
        return getOptional("mail.username", "");
    }

    public String getMailPassword() {
        return getOptional("mail.password", "");
    }

    public String getMailSender() {
        return getOptional("mail.sender", "");
    }

    public boolean isWebCrossDomainEnabled() {
        return Boolean.parseBoolean(getOptional("crossdomain.enable", "true"));
    }

    public int getJobRetry() {
        return Integer.parseInt(this.getOptional("kylin.job.retry", "0"));
    }

    public String toString() {
        return getMetadataUrl();
    }

    public String getHiveClientMode() {
        return getOptional("kylin.hive.client", "cli");
    }

    public String getHiveBeelineParams() {
        return getOptional("kylin.hive.beeline.params", "");
    }

    public String getDeployEnv() {
        return getOptional("deploy.env", "DEV");
    }

    public String getInitTasks() {
        return getOptional("kylin.init.tasks");
    }

    public int getDimCountDistinctMaxCardinality() {
        return Integer.parseInt(getOptional("kylin.query.dim.distinct.max", "5000000"));
    }

    public int getCubeStatsHLLPrecision() {
        return Integer.parseInt(getOptional("kylin.job.cubing.inmem.sampling.hll.precision", "14"));
    }

    public Map<Integer, String> getJobEngines() {
        Map<Integer, String> r = convertKeyToInteger(getPropertiesByPrefix("kylin.job.engine."));
        // ref constants in IEngineAware
        r.put(0, "org.apache.kylin.engine.mr.MRBatchCubingEngine");
        r.put(2, "org.apache.kylin.engine.mr.MRBatchCubingEngine2");
        return r;
    }

    public Map<Integer, String> getSourceEngines() {
        Map<Integer, String> r = convertKeyToInteger(getPropertiesByPrefix("kylin.source.engine."));
        // ref constants in ISourceAware
        r.put(0, "org.apache.kylin.source.hive.HiveSource");
        return r;
    }

    public Map<Integer, String> getStorageEngines() {
        Map<Integer, String> r = convertKeyToInteger(getPropertiesByPrefix("kylin.storage.engine."));
        // ref constants in IStorageAware
        r.put(0, "org.apache.kylin.storage.hbase.HBaseStorage");
        r.put(1, "org.apache.kylin.storage.hybrid.HybridStorage");
        r.put(2, "org.apache.kylin.storage.hbase.HBaseStorage");
        return r;
    }

    public Map<Integer, String> getSchedulers() {
        Map<Integer, String> r = convertKeyToInteger(getPropertiesByPrefix("kylin.scheduler."));
        r.put(0, "org.apache.kylin.job.impl.threadpool.DefaultScheduler");
        return r;
    }

    public Integer getSchedulerType() {
        return Integer.parseInt(getOptional("kylin.enable.scheduler", "0"));
    }


    public String getZookeeperAddress() {
        return this.getOptional("kylin.zookeeper.address");
    }

    public void setZookeeperAddress(String zkAddress) {
        setProperty("kylin.zookeeper.address", zkAddress);
    }

    public String getRestAddress() {
        return this.getOptional("kylin.rest.address", "localhost:7070");
    }
    public void setRestAddress(String restAddress) {
        setProperty("kylin.rest.address", restAddress);
    }

    private Map<Integer, String> convertKeyToInteger(Map<String, String> map) {
        Map<Integer, String> result = Maps.newLinkedHashMap();
        for (Entry<String, String> entry : map.entrySet()) {
            result.put(Integer.valueOf(entry.getKey()), entry.getValue());
        }
        return result;
    }

    public boolean getCompressionResult() {
        return Boolean.parseBoolean(getOptional("kylin.query.endpoint.compression.result", "true"));
    }

    public Map<String, String> getUDFs() {
        return getPropertiesByPrefix("kylin.query.udf.");
    }

    public int getHBaseMaxConnectionThreads() {
        return Integer.parseInt(getOptional("kylin.query.hbase.hconnection.threads.max", "3072"));
    }

    public long getStorageCleanupTimeThreshold() {
        return Long.valueOf(this.getOptional("kylin.storage.cleanup.time.threshold", "172800000"));//default two days
    }
}
