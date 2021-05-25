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

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.lock.DistributedLockFactory;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An abstract class to encapsulate access to a set of 'properties'.
 * Subclass can override methods in this class to extend the content of the 'properties',
 * with some override values for example.
 */
public abstract class KylinConfigBase implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(KylinConfigBase.class);
    private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();

    private static final String FALSE = "false";
    private static final String TRUE = "true";
    private static final String DEFAULT = "default";
    private static final String KYLIN_ENGINE_MR_JOB_JAR = "kylin.engine.mr.job-jar";
    private static final String KYLIN_STORAGE_HBASE_COPROCESSOR_LOCAL_JAR = "kylin.storage.hbase.coprocessor-local-jar";
    private static final String FILE_SCHEME = "file:";
    private static final String MAPRFS_SCHEME = "maprfs:";
    private static final Integer DEFAULT_MR_HIVE_GLOBAL_DICT_REDUCE_NUM_PER_COLUMN = 2;

    /*
     * DON'T DEFINE CONSTANTS FOR PROPERTY KEYS!
     *
     * For 1), no external need to access property keys, all accesses are by public methods.
     * For 2), it's cumbersome to maintain constants at top and code at bottom.
     * For 3), key literals usually appear only once.
     */

    public static String getKylinHome() {
        String kylinHome = getKylinHomeWithoutWarn();
        if (StringUtils.isEmpty(kylinHome)) {
            logger.warn("KYLIN_HOME was not set");
        }
        return kylinHome;
    }

    public static String getKylinHomeWithoutWarn() {
        String kylinHome = System.getenv("KYLIN_HOME");
        if (StringUtils.isEmpty(kylinHome)) {
            kylinHome = System.getProperty("KYLIN_HOME");
        }
        return kylinHome;
    }

    public static String getSparkHome() {
        String sparkHome = System.getenv("SPARK_HOME");
        if (StringUtils.isNotEmpty(sparkHome)) {
            logger.info("SPARK_HOME was set to {}", sparkHome);
            return sparkHome;
        }

        sparkHome = System.getProperty("SPARK_HOME");
        if (StringUtils.isNotEmpty(sparkHome)) {
            logger.info("SPARK_HOME was set to {}", sparkHome);
            return sparkHome;
        }

        return getKylinHome() + File.separator + "spark";
    }

    public static String getFlinkHome() {
        String flinkHome = System.getenv("FLINK_HOME");
        if (StringUtils.isNotEmpty(flinkHome)) {
            logger.info("FLINK_HOME was set to {}", flinkHome);
            return flinkHome;
        }

        flinkHome = System.getProperty("FLINK_HOME");
        if (StringUtils.isNotEmpty(flinkHome)) {
            logger.info("FLINK_HOME was set to {}", flinkHome);
            return flinkHome;
        }

        return getKylinHome() + File.separator + "flink";
    }

    public static String getTempDir() {
        return System.getProperty("java.io.tmpdir");
    }

    // backward compatibility check happens when properties is loaded or updated
    static BackwardCompatibilityConfig BCC = new BackwardCompatibilityConfig();

    // ============================================================================

    volatile Properties properties = new Properties();

    public KylinConfigBase() {
        this(new Properties());
    }

    public KylinConfigBase(Properties props) {
        this.properties = BCC.check(props);
    }

    protected KylinConfigBase(Properties props, boolean force) {
        this.properties = force ? props : BCC.check(props);
    }

    protected final String getOptional(String prop) {
        return getOptional(prop, null);
    }

    protected String getOptional(String prop, String dft) {

        final String property = System.getProperty(prop);
        return property != null ? StrSubstitutor.replace(property, System.getenv())
                : StrSubstitutor.replace(properties.getProperty(prop, dft), System.getenv());
    }

    protected Properties getAllProperties() {
        return getProperties(null);
    }

    /**
     * @param propertyKeys the collection of the properties; if null will return all properties
     * @return properties which contained in propertyKeys
     */
    protected Properties getProperties(Collection<String> propertyKeys) {
        Map<String, String> envMap = System.getenv();
        StrSubstitutor sub = new StrSubstitutor(envMap);

        Properties filteredProperties = new Properties();
        for (Entry<Object, Object> entry : this.properties.entrySet()) {
            if (propertyKeys == null || propertyKeys.contains(entry.getKey())) {
                filteredProperties.put(entry.getKey(), sub.replace((String) entry.getValue()));
            }
        }
        return filteredProperties;
    }

    protected Properties getRawAllProperties() {
        return properties;

    }

    protected final Map<String, String> getPropertiesByPrefix(String prefix) {
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

    final protected String[] getRawStringArray(String prop, String[] dft) {
        final String property = properties.getProperty(prop);
        if (!StringUtils.isBlank(property)) {
            return property.split("\\s*,\\s*");
        } else {
            return dft;
        }
    }

    final protected int[] getOptionalIntArray(String prop, String[] dft) {
        String[] strArray = getOptionalStringArray(prop, dft);
        int[] intArray = new int[strArray.length];
        for (int i = 0; i < strArray.length; i++) {
            intArray[i] = Integer.parseInt(strArray[i]);
        }
        return intArray;
    }

    final protected String getRequired(String prop) {
        String r = getOptional(prop);
        if (StringUtils.isEmpty(r)) {
            throw new IllegalArgumentException("missing '" + prop + "' in conf/kylin.properties");
        }
        return r;
    }

    /**
     * Use with care, properties should be read-only. This is for testing only.
     */
    final public void setProperty(String key, String value) {
        logger.info("Kylin Config was updated with {} : {}", key, value);
        properties.setProperty(BCC.check(key), value);
    }

    final protected void reloadKylinConfig(Properties properties) {
        this.properties = BCC.check(properties);
    }

    private Map<Integer, String> convertKeyToInteger(Map<String, String> map) {
        Map<Integer, String> result = Maps.newLinkedHashMap();
        for (Entry<String, String> entry : map.entrySet()) {
            result.put(Integer.valueOf(entry.getKey()), entry.getValue());
        }
        return result;
    }

    public String toString() {
        return getMetadataUrl().toString();
    }

    // ============================================================================
    // ENV
    // ============================================================================

    public boolean isDevEnv() {
        return "DEV".equals(getOptional("kylin.env", "DEV"));
    }

    public String getDeployEnv() {
        return getOptional("kylin.env", "DEV");
    }

    private String cachedHdfsWorkingDirectory;
    private String cachedBigCellDirectory;

    public String getHdfsWorkingDirectory() {
        if (cachedHdfsWorkingDirectory != null)
            return cachedHdfsWorkingDirectory;

        String root = getOptional("kylin.env.hdfs-working-dir", "/kylin");

        Path path = new Path(root);
        if (!path.isAbsolute())
            throw new IllegalArgumentException("kylin.env.hdfs-working-dir must be absolute, but got " + root);

        try {
            FileSystem fs = path.getFileSystem(HadoopUtil.getCurrentConfiguration());
            path = fs.makeQualified(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // append metadata-url prefix
        root = new Path(path, StringUtils.replaceChars(getMetadataUrlPrefix(), ':', '-')).toString();

        if (!root.endsWith("/"))
            root += "/";

        cachedHdfsWorkingDirectory = root;
        if (cachedHdfsWorkingDirectory.startsWith(FILE_SCHEME)) {
            cachedHdfsWorkingDirectory = cachedHdfsWorkingDirectory.replace(FILE_SCHEME, "file://");
        } else if (cachedHdfsWorkingDirectory.startsWith(MAPRFS_SCHEME)) {
            cachedHdfsWorkingDirectory = cachedHdfsWorkingDirectory.replace(MAPRFS_SCHEME, "maprfs://");
        }
        return cachedHdfsWorkingDirectory;
    }

    public String getMetastoreBigCellHdfsDirectory() {

        if (cachedBigCellDirectory != null)
            return cachedBigCellDirectory;

        String root = getOptional("kylin.env.hdfs-metastore-bigcell-dir");

        if (root == null) {
            return getJdbcHdfsWorkingDirectory();
        }

        Path path = new Path(root);
        if (!path.isAbsolute())
            throw new IllegalArgumentException(
                    "kylin.env.hdfs-metastore-bigcell-dir must be absolute, but got " + root);

        // make sure path is qualified
        try {
            FileSystem fs = HadoopUtil.getReadFileSystem();
            path = fs.makeQualified(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        root = new Path(path, StringUtils.replaceChars(getMetadataUrlPrefix(), ':', '-')).toString();

        if (!root.endsWith("/"))
            root += "/";

        cachedBigCellDirectory = root;
        if (cachedBigCellDirectory.startsWith(FILE_SCHEME)) {
            cachedBigCellDirectory = cachedBigCellDirectory.replace(FILE_SCHEME, "file://");
        } else if (cachedBigCellDirectory.startsWith(MAPRFS_SCHEME)) {
            cachedBigCellDirectory = cachedBigCellDirectory.replace(MAPRFS_SCHEME, "maprfs://");
        }

        return cachedBigCellDirectory;
    }

    public String getReadHdfsWorkingDirectory() {
        if (StringUtils.isNotEmpty(getHBaseClusterFs())) {
            Path workingDir = new Path(getHdfsWorkingDirectory());
            return new Path(getHBaseClusterFs(), Path.getPathWithoutSchemeAndAuthority(workingDir)).toString() + "/";
        }

        return getHdfsWorkingDirectory();
    }

    private String getJdbcHdfsWorkingDirectory() {
        if (StringUtils.isNotEmpty(getJdbcFileSystem())) {
            Path workingDir = new Path(getReadHdfsWorkingDirectory());
            return new Path(getJdbcFileSystem(), Path.getPathWithoutSchemeAndAuthority(workingDir)).toString() + "/";
        }

        return getReadHdfsWorkingDirectory();
    }

    private String getJdbcFileSystem() {
        return getOptional("kylin.storage.columnar.jdbc.file-system", "");
    }

    public String getHdfsWorkingDirectory(String project) {
        if (isProjectIsolationEnabled() && project != null) {
            return new Path(getHdfsWorkingDirectory(), project).toString() + "/";
        } else {
            return getHdfsWorkingDirectory();
        }
    }

    public String getZookeeperBasePath() {
        return getOptional("kylin.env.zookeeper-base-path", "/kylin");
    }

    /**
     * A comma separated list of host:port pairs, each corresponding to a ZooKeeper server
     */
    public String getZookeeperConnectString() {
        return getOptional("kylin.env.zookeeper-connect-string");
    }

    public int getZKBaseSleepTimeMs() {
        return Integer.parseInt(getOptional("kylin.env.zookeeper-base-sleep-time", "3000"));
    }

    public int getZKMaxRetries() {
        return Integer.parseInt(getOptional("kylin.env.zookeeper-max-retries", "3"));
    }

    public int getZKMonitorInterval() {
        return Integer.parseInt(getOptional("kylin.job.zookeeper-monitor-interval", "30"));
    }

    public boolean isZookeeperAclEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.env.zookeeper-acl-enabled", FALSE));
    }

    public String getZKAuths() {
        return getOptional("kylin.env.zookeeper.zk-auth", "digest:ADMIN:KYLIN");
    }

    public String getZKAcls() {
        return getOptional("kylin.env.zookeeper.zk-acl", "world:anyone:rwcda");
    }

    public String[] getRestServersWithMode() {
        return getOptionalStringArray("kylin.server.cluster-servers-with-mode", new String[0]);
    }
    // ============================================================================
    // METADATA
    // ============================================================================

    public StorageURL getMetadataUrl() {
        return StorageURL.valueOf(getOptional("kylin.metadata.url", "kylin_metadata@hbase"));
    }

    public int getCacheSyncRetrys() {
        return Integer.parseInt(getOptional("kylin.metadata.sync-retries", "3"));
    }

    public String getCacheSyncErrorHandler() {
        return getOptional("kylin.metadata.sync-error-handler");
    }

    // for test only
    public void setMetadataUrl(String metadataUrl) {
        setProperty("kylin.metadata.url", metadataUrl);
    }

    public String getMetadataUrlPrefix() {
        return getMetadataUrl().getIdentifier();
    }

    public Map<String, String> getResourceStoreImpls() {
        Map<String, String> r = Maps.newLinkedHashMap();
        // ref constants in ISourceAware
        r.put("", "org.apache.kylin.common.persistence.FileResourceStore");
        r.put("hbase", "org.apache.kylin.storage.hbase.HBaseResourceStore");
        r.put("hdfs", "org.apache.kylin.common.persistence.HDFSResourceStore");
        r.put("ifile", "org.apache.kylin.common.persistence.IdentifierFileResourceStore");
        r.put("jdbc", "org.apache.kylin.common.persistence.JDBCResourceStore");
        r.putAll(getPropertiesByPrefix("kylin.metadata.resource-store-provider.")); // note the naming convention -- http://kylin.apache.org/development/coding_naming_convention.html
        return r;
    }

    public boolean isResourceStoreReconnectEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.resourcestore.reconnect-enabled", FALSE));
    }

    public int getResourceStoreReconnectBaseMs() {
        return Integer.parseInt(getOptional("kylin.resourcestore.reconnect-base-ms", "1000"));
    }

    public int getResourceStoreReconnectMaxMs() {
        return Integer.parseInt(getOptional("kylin.resourcestore.reconnect-max-ms", "60000"));
    }

    public int getResourceStoreReconnectTimeoutMs() {
        return Integer.parseInt(getOptional("kylin.resourcestore.reconnect-timeout-ms", "3600000"));
    }

    public String getResourceStoreConnectionExceptions() {
        return getOptional("kylin.resourcestore.connection-exceptions", "");
    }

    public String getDataModelImpl() {
        return getOptional("kylin.metadata.data-model-impl", null);
    }

    public String getDataModelManagerImpl() {
        return getOptional("kylin.metadata.data-model-manager-impl", null);
    }

    public String[] getRealizationProviders() {
        return getOptionalStringArray("kylin.metadata.realization-providers", //
                new String[] { "org.apache.kylin.cube.CubeManager", "org.apache.kylin.storage.hybrid.HybridManager" });
    }

    public String[] getCubeDimensionCustomEncodingFactories() {
        return getOptionalStringArray("kylin.metadata.custom-dimension-encodings", new String[0]);
    }

    public Map<String, String> getCubeCustomMeasureTypes() {
        return getPropertiesByPrefix("kylin.metadata.custom-measure-types.");
    }

    public DistributedLockFactory getDistributedLockFactory() {
        String clsName = getOptional("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.zookeeper.ZookeeperDistributedLock$Factory");
        return (DistributedLockFactory) ClassUtil.newInstance(clsName);
    }

    public String getHBaseMappingAdapter() {
        return getOptional("kylin.metadata.hbasemapping-adapter");
    }

    public boolean isCheckCopyOnWrite() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.check-copy-on-write", FALSE));
    }

    public String getHbaseClientScannerTimeoutPeriod() {
        return getOptional("kylin.metadata.hbase-client-scanner-timeout-period", "10000");
    }

    public String getHbaseRpcTimeout() {
        return getOptional("kylin.metadata.hbase-rpc-timeout", "5000");
    }

    public String getHbaseClientRetriesNumber() {
        return getOptional("kylin.metadata.hbase-client-retries-number", "3");
    }

    public boolean isModelSchemaUpdaterCheckerEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.model-schema-updater-checker-enabled", "false"));
    }

    public boolean isAbleChangeStringToDateTime() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.able-change-string-to-datetime", "false"));
    }

    // ============================================================================
    // DICTIONARY & SNAPSHOT
    // ============================================================================

    public boolean isUseForestTrieDictionary() {
        return Boolean.parseBoolean(getOptional("kylin.dictionary.use-forest-trie", TRUE));
    }

    public long getTrieDictionaryForestMaxTrieSizeMB() {
        return Integer.parseInt(getOptional("kylin.dictionary.forest-trie-max-mb", "500"));
    }

    public long getCachedDictMaxEntrySize() {
        return Long.parseLong(getOptional("kylin.dictionary.max-cache-entry", "3000"));
    }

    public int getCachedDictMaxSize() {
        return Integer.parseInt(getOptional("kylin.dictionary.max-cache-size", "-1"));
    }

    public boolean isGrowingDictEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.dictionary.growing-enabled", FALSE));
    }

    public boolean isDictResuable() {
        return Boolean.parseBoolean(this.getOptional("kylin.dictionary.resuable", FALSE));
    }

    public long getCachedDictionaryMaxEntrySize() {
        return Long.parseLong(getOptional("kylin.dictionary.cached-dict-max-cache-entry", "50000"));
    }

    public int getAppendDictEntrySize() {
        return Integer.parseInt(getOptional("kylin.dictionary.append-entry-size", "10000000"));
    }

    public int getAppendDictMaxVersions() {
        return Integer.parseInt(getOptional("kylin.dictionary.append-max-versions", "3"));
    }

    public int getAppendDictVersionTTL() {
        return Integer.parseInt(getOptional("kylin.dictionary.append-version-ttl", "259200000"));
    }

    public int getCachedSnapshotMaxEntrySize() {
        return Integer.parseInt(getOptional("kylin.snapshot.max-cache-entry", "500"));
    }

    public int getTableSnapshotMaxMB() {
        return Integer.parseInt(getOptional("kylin.snapshot.max-mb", "300"));
    }

    public int getExtTableSnapshotShardingMB() {
        return Integer.parseInt(getOptional("kylin.snapshot.ext.shard-mb", "500"));
    }

    public String getExtTableSnapshotLocalCachePath() {
        return getOptional("kylin.snapshot.ext.local.cache.path", "lookup_cache");
    }

    public double getExtTableSnapshotLocalCacheMaxSizeGB() {
        return Double.parseDouble(getOptional("kylin.snapshot.ext.local.cache.max-size-gb", "200"));
    }

    public long getExtTableSnapshotLocalCacheCheckVolatileRange() {
        return Long.parseLong(getOptional("kylin.snapshot.ext.local.cache.check.volatile", "3600000"));
    }

    public boolean isShrunkenDictFromGlobalEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.dictionary.shrunken-from-global-enabled", TRUE));
    }

    public int getDictionarySliceEvicationThreshold() {
        return Integer.parseInt(getOptional("kylin.dictionary.slice.eviction.threshold", "5"));
    }


    // ============================================================================
    // Hive Global dictionary by mr/hive
    // ============================================================================
    public String[] getMrHiveDictColumns() {
        String columnStr = getMrHiveDictColumnsStr();
        if (!StringUtils.isEmpty(columnStr)) {
            return columnStr.split(",");
        }
        return new String[0];
    }

    public String[] getMrHiveDictColumnsExcludeRefColumns() {
        String[] excludeRefCols = null;
        String[] hiveDictColumns = getMrHiveDictColumns();
        Map<String, String> refCols = getMrHiveDictRefColumns();
        if (Objects.nonNull(hiveDictColumns) && hiveDictColumns.length > 0) {
            excludeRefCols = Arrays.stream(hiveDictColumns).filter(x -> !refCols.containsKey(x)).toArray(String[]::new);
        }
        return excludeRefCols;
    }

    /**
     * set kylin.dictionary.mr-hive.columns in Cube level config , value are the columns which want to use MR/Hive to build global dict ,
     * Format, tableAliasName_ColumnName, multiple columns separated by comma,eg KYLIN_SALES_BUYER_ID,KYLIN_SALES_SELLER_ID
     *
     * @return if mr-hive dict not enabled, return "";
     * else return {TABLE_NAME}_{COLUMN_NAME1},{TABLE_NAME}_{COLUMN_NAME2}"
     */
    private String getMrHiveDictColumnsStr() {
        return getOptional("kylin.dictionary.mr-hive.columns", "");
    }

    /**
     * @return The global dic reduce num per column. Default 2  per column.
     */
    public Integer[] getMrHiveDictColumnsReduceNumExcludeRefCols() {
        String[] excludeRefCols = getMrHiveDictColumnsExcludeRefColumns();

        if (Objects.nonNull(excludeRefCols) && excludeRefCols.length > 0) {
            String[] arr = null;
            Map<String, Integer> colNum = new HashMap<>();
            Integer[] reduceNumArr = new Integer[excludeRefCols.length];
            String[] columnReduceNum = getMrHiveDictColumnsReduceNumStr().split(",");

            try {
                for (int i = 0; i < columnReduceNum.length; i++) {
                    if (!StringUtils.isBlank(columnReduceNum[i])) {
                        arr = columnReduceNum[i].split(":");
                        colNum.put(arr[0], Integer.parseInt(arr[1]));
                    }
                }
            } catch (Exception e) {
                logger.error("set kylin.dictionary.mr-hive.columns.reduce.num error {} , the value should like colAilasName:reduceNum,colAilasName:reduceNum", getMrHiveDictColumnsReduceNumStr());
            }

            for (int i = 0; i < excludeRefCols.length; i++) {
                reduceNumArr[i] = colNum.containsKey(excludeRefCols[i]) ? colNum.get(excludeRefCols[i]) : DEFAULT_MR_HIVE_GLOBAL_DICT_REDUCE_NUM_PER_COLUMN;
            }

            Arrays.asList(reduceNumArr).stream().forEach(x -> {
                if (x < 1) {
                    throw new RuntimeException("kylin.dictionary.mr-hive.columns.reduce.num set error ,every column's reduce num should greater than 0");
                }
            });

            return reduceNumArr;
        } else {
            return null;
        }
    }

    /**
     * Set kylin.dictionary.mr-hive.columns.reduce.num in Cube level config , value are the reduce number for global dict columns which are set in kylin.dictionary.mr-hive.columns.
     * Format, tableAliasName_ColumnName:number, multiple columns separated by commas,eg KYLIN_SALES_BUYER_ID:5,KYLIN_SALES_SELLER_ID:3
     */
    private String getMrHiveDictColumnsReduceNumStr() {
        return getOptional("kylin.dictionary.mr-hive.columns.reduce.num", "");
    }

    /**
     * MR/Hive global domain dictionary (reuse dict from other cube's MR/Hive global dic column)
     */
    public Map<String, String> getMrHiveDictRefColumns() {
        Map<String, String> result = new HashMap<>();
        String columnStr = getOptional("kylin.dictionary.mr-hive.ref.columns", "");
        if (!StringUtils.isEmpty(columnStr)) {
            String[] pairs = columnStr.split(",");
            for (String pair : pairs) {
                String[] infos = pair.split(":");
                result.put(infos[0], infos[1]);
            }
        }
        return result;
    }

    public String getMrHiveDictDB() {
        return getOptional("kylin.dictionary.mr-hive.database", getHiveDatabaseForIntermediateTable());
    }

    public String getMrHiveDictTableSuffix() {
        return getOptional("kylin.dictionary.mr-hive.table.suffix", "_global_dict");
    }

    public String getMrHiveDistinctValueTableSuffix() {
        return getOptional("kylin.dictionary.mr-hive.intermediate.table.suffix", "_distinct_value");
    }

    // ============================================================================
    // CUBE
    // ============================================================================

    public String getCuboidScheduler() {
        return getOptional("kylin.cube.cuboid-scheduler", "org.apache.kylin.cube.cuboid.DefaultCuboidScheduler");
    }

    public boolean isRowKeyEncodingAutoConvert() {
        return Boolean.parseBoolean(getOptional("kylin.cube.rowkey-encoding-auto-convert", "true"));
    }

    public String getSegmentAdvisor() {
        return getOptional("kylin.cube.segment-advisor", "org.apache.kylin.cube.CubeSegmentAdvisor");
    }

    public boolean enableJobCuboidSizeOptimize() {
        return Boolean.parseBoolean(getOptional("kylin.cube.size-estimate-enable-optimize", "false"));
    }

    public double getJobCuboidSizeRatio() {
        return Double.parseDouble(getOptional("kylin.cube.size-estimate-ratio", "0.25"));
    }

    @Deprecated
    public double getJobCuboidSizeMemHungryRatio() {
        return Double.parseDouble(getOptional("kylin.cube.size-estimate-memhungry-ratio", "0.05"));
    }

    public double getJobCuboidSizeCountDistinctRatio() {
        return Double.parseDouble(getOptional("kylin.cube.size-estimate-countdistinct-ratio", "0.5"));
    }

    public double getJobCuboidSizeTopNRatio() {
        return Double.parseDouble(getOptional("kylin.cube.size-estimate-topn-ratio", "0.5"));
    }

    public String getCubeAlgorithm() {
        return getOptional("kylin.cube.algorithm", "auto");
    }

    public double getCubeAlgorithmAutoThreshold() {
        return Double.parseDouble(getOptional("kylin.cube.algorithm.layer-or-inmem-threshold", "7"));
    }

    public boolean isAutoInmemToOptimize() {
        return Boolean.parseBoolean(getOptional("kylin.cube.algorithm.inmem-auto-optimize", TRUE));
    }

    public int getCubeAlgorithmAutoMapperLimit() {
        return Integer.parseInt(getOptional("kylin.cube.algorithm.inmem-split-limit", "500"));
    }

    public int getCubeAlgorithmInMemConcurrentThreads() {
        return Integer.parseInt(getOptional("kylin.cube.algorithm.inmem-concurrent-threads", "1"));
    }

    public boolean isIgnoreCubeSignatureInconsistency() {
        return Boolean.parseBoolean(getOptional("kylin.cube.ignore-signature-inconsistency", FALSE));
    }

    public long getCubeAggrGroupMaxCombination() {
        return Long.parseLong(getOptional("kylin.cube.aggrgroup.max-combination", "32768"));
    }

    public boolean getCubeAggrGroupIsMandatoryOnlyValid() {
        return Boolean.parseBoolean(getOptional("kylin.cube.aggrgroup.is-mandatory-only-valid", FALSE));
    }

    public int getCubeRowkeyMaxSize() {
        return Integer.parseInt(getOptional("kylin.cube.rowkey.max-size", "63"));
    }

    public int getDimensionEncodingMaxLength() {
        return Integer.parseInt(getOptional("kylin.metadata.dimension-encoding-max-length", "256"));
    }

    public int getMaxBuildingSegments() {
        return Integer.parseInt(getOptional("kylin.cube.max-building-segments", "10"));
    }

    public long getMaxSegmentMergeSpan() {
        return Long.parseLong(getOptional("kylin.cube.max-segment-merge.span", "-1"));
    }

    public boolean allowCubeAppearInMultipleProjects() {
        return Boolean.parseBoolean(getOptional("kylin.cube.allow-appear-in-multiple-projects", FALSE));
    }

    public int getGTScanRequestSerializationLevel() {
        return Integer.parseInt(getOptional("kylin.cube.gtscanrequest-serialization-level", "1"));
    }

    public boolean isAutoMergeEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.cube.is-automerge-enabled", TRUE));
    }

    public String[] getCubeMetadataExtraValidators() {
        return getOptionalStringArray("kylin.cube.metadata-extra-validators", new String[0]);
    }

    // ============================================================================
    // Cube Planner
    // ============================================================================

    public boolean isCubePlannerEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.cube.cubeplanner.enabled", TRUE));
    }

    public boolean isCubePlannerEnabledForExistingCube() {
        return Boolean.parseBoolean(getOptional("kylin.cube.cubeplanner.enabled-for-existing-cube", TRUE));
    }

    public double getCubePlannerExpansionRateThreshold() {
        return Double.parseDouble(getOptional("kylin.cube.cubeplanner.expansion-threshold", "15.0"));
    }

    public int getCubePlannerRecommendCuboidCacheMaxSize() {
        return Integer.parseInt(getOptional("kylin.cube.cubeplanner.recommend-cache-max-size", "200"));
    }

    public double getCubePlannerQueryUncertaintyRatio() {
        return Double.parseDouble(getOptional("kylin.cube.cubeplanner.query-uncertainty-ratio", "0.1"));
    }

    public double getCubePlannerBPUSMinBenefitRatio() {
        return Double.parseDouble(getOptional("kylin.cube.cubeplanner.bpus-min-benefit-ratio", "0.01"));
    }

    public int getCubePlannerAgreedyAlgorithmAutoThreshold() {
        return Integer.parseInt(getOptional("kylin.cube.cubeplanner.algorithm-threshold-greedy", "8"));
    }

    public int getCubePlannerGeneticAlgorithmAutoThreshold() {
        return Integer.parseInt(getOptional("kylin.cube.cubeplanner.algorithm-threshold-genetic", "23"));
    }

    /**
     * get assigned server array, which a empty string array in default
     * @return
     */
    public String[] getAssignedServers() {
        return getOptionalStringArray("kylin.cube.schedule.assigned-servers", new String[] {});
    }

    /**
     * Determine if the target node is in the assigned node
     * @param targetServers target task servers
     * @return
     */
    public boolean isOnAssignedServer(String... targetServers) {

        String[] servers = this.getAssignedServers();
        if (null == servers || servers.length == 0) {
            return true;
        }

        for (String s : servers) {
            for (String ts : targetServers) {
                if (s.equalsIgnoreCase(ts)) {
                    return true;
                }
            }
        }
        return false;
    }

    // ============================================================================
    // JOB
    // ============================================================================

    public CliCommandExecutor getCliCommandExecutor() {
        CliCommandExecutor exec = new CliCommandExecutor();
        if (getRunAsRemoteCommand()) {
            exec.setRunAtRemote(getRemoteHadoopCliHostname(), getRemoteHadoopCliPort(), getRemoteHadoopCliUsername(),
                    getRemoteHadoopCliPassword());
        }
        return exec;
    }

    public String getKylinJobLogDir() {
        return getOptional("kylin.job.log-dir", "/tmp/kylin/logs");
    }

    public boolean getRunAsRemoteCommand() {
        return Boolean.parseBoolean(getOptional("kylin.job.use-remote-cli"));
    }

    public int getRemoteHadoopCliPort() {
        return Integer.parseInt(getOptional("kylin.job.remote-cli-port", "22"));
    }

    public String getRemoteHadoopCliHostname() {
        return getOptional("kylin.job.remote-cli-hostname");
    }

    public String getRemoteHadoopCliUsername() {
        return getOptional("kylin.job.remote-cli-username");
    }

    public String getRemoteHadoopCliPassword() {
        return getOptional("kylin.job.remote-cli-password");
    }

    public String getCliWorkingDir() {
        return getOptional("kylin.job.remote-cli-working-dir");
    }

    public boolean isEmptySegmentAllowed() {
        return Boolean.parseBoolean(getOptional("kylin.job.allow-empty-segment", TRUE));
    }

    public int getMaxConcurrentJobLimit() {
        return Integer.parseInt(getOptional("kylin.job.max-concurrent-jobs", "10"));
    }

    public int getCubingInMemSamplingPercent() {
        int percent = Integer.parseInt(this.getOptional("kylin.job.sampling-percentage", "100"));
        percent = Math.max(percent, 1);
        percent = Math.min(percent, 100);
        return percent;
    }

    public String getHiveDependencyFilterList() {
        return this.getOptional("kylin.job.dependency-filter-list", "[^,]*hive-exec[^,]*?\\.jar" + "|"
                + "[^,]*hive-metastore[^,]*?\\.jar" + "|" + "[^,]*hive-hcatalog-core[^,]*?\\.jar");
    }

    public boolean isNotificationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-enabled", FALSE));
    }

    public boolean isStarttlsEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-mail-enable-starttls", FALSE));
    }

    public String getSmtpPort() {
        return getOptional("kylin.job.notification-mail-port", "25");
    }

    public String getMailHost() {
        return getOptional("kylin.job.notification-mail-host", "");
    }

    public String getMailUsername() {
        return getOptional("kylin.job.notification-mail-username", "");
    }

    public String getMailPassword() {
        return getOptional("kylin.job.notification-mail-password", "");
    }

    public String getMailSender() {
        return getOptional("kylin.job.notification-mail-sender", "");
    }

    public String[] getAdminDls() {
        return getOptionalStringArray("kylin.job.notification-admin-emails", null);
    }

    public int getJobRetry() {
        return Integer.parseInt(getOptional("kylin.job.retry", "0"));
    }

    // retry interval in milliseconds
    public int getJobRetryInterval() {
        return Integer.parseInt(getOptional("kylin.job.retry-interval", "30000"));
    }

    public String[] getJobRetryExceptions() {
        return getOptionalStringArray("kylin.job.retry-exception-classes", new String[0]);
    }

    public int getCubeStatsHLLPrecision() {
        return Integer.parseInt(getOptional("kylin.job.sampling-hll-precision", "14"));
    }

    public Map<Integer, String> getSchedulers() {
        Map<Integer, String> r = Maps.newLinkedHashMap();
        r.put(0, "org.apache.kylin.job.impl.threadpool.DefaultScheduler");
        r.put(2, "org.apache.kylin.job.impl.threadpool.DistributedScheduler");
        r.put(77, "org.apache.kylin.job.impl.threadpool.NoopScheduler");
        r.putAll(convertKeyToInteger(getPropertiesByPrefix("kylin.job.scheduler.provider.")));
        return r;
    }

    public Integer getSchedulerType() {
        return Integer.parseInt(getOptional("kylin.job.scheduler.default", "0"));
    }

    public boolean getSchedulerPriorityConsidered() {
        return Boolean.parseBoolean(getOptional("kylin.job.scheduler.priority-considered", FALSE));
    }

    public Integer getSchedulerPriorityBarFetchFromQueue() {
        return Integer.parseInt(getOptional("kylin.job.scheduler.priority-bar-fetch-from-queue", "20"));
    }

    public Integer getSchedulerPollIntervalSecond() {
        return Integer.parseInt(getOptional("kylin.job.scheduler.poll-interval-second", "30"));
    }

    public boolean isSchedulerSafeMode() {
        return Boolean.parseBoolean(getOptional("kylin.job.scheduler.safemode", "false"));
    }

    public List<String> getSafeModeRunnableProjects() {
        return Arrays.asList(getOptionalStringArray("kylin.job.scheduler.safemode.runnable-projects", new String[0]));
    }

    public Integer getErrorRecordThreshold() {
        return Integer.parseInt(getOptional("kylin.job.error-record-threshold", "0"));
    }

    public boolean isAdvancedFlatTableUsed() {
        return Boolean.parseBoolean(getOptional("kylin.job.use-advanced-flat-table", FALSE));
    }

    public String getAdvancedFlatTableClass() {
        return getOptional("kylin.job.advanced-flat-table.class");
    }

    public String getJobTrackingURLPattern() {
        return getOptional("kylin.job.tracking-url-pattern", "");
    }

    public int getJobMetadataPersistRetry() {
        return Integer.parseInt(this.getOptional("kylin.job.metadata-persist-retry", "5"));
    }

    public boolean isJobAutoReadyCubeEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.cube-auto-ready-enabled", TRUE));
    }

    public String getCubeInMemBuilderClass() {
        return getOptional("kylin.job.cube-inmem-builder-class", "org.apache.kylin.cube.inmemcubing.DoggedCubeBuilder");
    }

    public int getJobOutputMaxSize() {
        return Integer.parseInt(getOptional("kylin.job.execute-output.max-size", "10485760"));
    }

    // ============================================================================
    // SOURCE.HIVE
    // ============================================================================

    public String getHiveDatabaseDir(String databaseName) {
        String dbDir = getOptional("kylin.source.hive.warehouse-dir", "");
        if (!StringUtil.isEmpty(dbDir)) {
            logger.info("kylin.source.hive.warehouse-dir is {}", dbDir);
        } else {
            logger.warn("kylin.source.hive.warehouse-dir is null");
        }
        if (!StringUtil.isEmpty(databaseName) && !databaseName.equalsIgnoreCase(DEFAULT)) {
            if (!dbDir.endsWith("/")) {
                dbDir += "/";
            }
            dbDir += databaseName + ".db";
        }
        return dbDir;
    }

    public int getDefaultSource() {
        return Integer.parseInt(getOptional("kylin.source.default", "0"));
    }

    public Map<Integer, String> getSourceEngines() {
        Map<Integer, String> r = Maps.newLinkedHashMap();
        // ref constants in ISourceAware
        r.put(0, "org.apache.kylin.source.hive.HiveSource");
        r.put(1, "org.apache.kylin.source.kafka.KafkaSource");
        r.put(8, "org.apache.kylin.source.jdbc.JdbcSource");
        r.put(16, "org.apache.kylin.source.jdbc.extensible.JdbcSource");
        r.put(20, "org.apache.kylin.stream.source.kafka.KafkaBatchSourceAdaptor");
        r.put(21, "org.apache.kylin.stream.source.kafka.KafkaBatchSourceAdaptor");
        r.putAll(convertKeyToInteger(getPropertiesByPrefix("kylin.source.provider.")));
        return r;
    }

    /**
     * whether to enable quote identifier in create flat table in Kylin, how to quote is chosen by
     * @see KylinConfigBase#getFactTableDialect
     */
    public boolean enableHiveDdlQuote() {
        return Boolean.parseBoolean(getOptional("kylin.source.hive.quote-enabled", TRUE));
    }

    public String getFactTableDialect() {
        return getOptional("kylin.fact.table.dialect", "hive");
    }

    /**
     * was for route to hive, not used any more
     */
    @Deprecated
    public String getHiveUrl() {
        return getOptional("kylin.source.hive.connection-url", "");
    }

    /**
     * was for route to hive, not used any more
     */
    @Deprecated
    public String getHiveUser() {
        return getOptional("kylin.source.hive.connection-user", "");
    }

    /**
     * was for route to hive, not used any more
     */
    @Deprecated
    public String getHivePassword() {
        return getOptional("kylin.source.hive.connection-password", "");
    }

    public Map<String, String> getHiveConfigOverride() {
        return getPropertiesByPrefix("kylin.source.hive.config-override.");
    }

    public String getOverrideHiveTableLocation(String table) {
        return getOptional("kylin.source.hive.table-location." + table.toUpperCase(Locale.ROOT));
    }

    public boolean isHiveKeepFlatTable() {
        return Boolean.parseBoolean(this.getOptional("kylin.source.hive.keep-flat-table", FALSE));
    }

    public String getHiveDatabaseForIntermediateTable() {
        return CliCommandExecutor.checkHiveProperty(this.getOptional("kylin.source.hive.database-for-flat-table", DEFAULT));
    }

    public String getIntermediateTableDatabaseDir() {
        return getHiveDatabaseDir(getHiveDatabaseForIntermediateTable());
    }

    public String getFlatTableStorageFormat() {
        return this.getOptional("kylin.source.hive.flat-table-storage-format", "SEQUENCEFILE");
    }

    public String getFlatTableFieldDelimiter() {
        return this.getOptional("kylin.source.hive.flat-table-field-delimiter", "\u001F");
    }

    public boolean isHiveRedistributeEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.source.hive.redistribute-flat-table", TRUE));
    }

    public String getHiveClientMode() {
        return getOptional("kylin.source.hive.client", "cli");
    }

    public String getHiveBeelineShell() {
        return getOptional("kylin.source.hive.beeline-shell", "beeline");
    }

    public String getHiveBeelineParams() {
        return getOptional("kylin.source.hive.beeline-params", "");
    }

    public boolean getEnableSparkSqlForTableOps() {
        return Boolean.parseBoolean(getOptional("kylin.source.hive.enable-sparksql-for-table-ops", FALSE));
    }

    public String getSparkSqlBeelineShell() {
        return getOptional("kylin.source.hive.sparksql-beeline-shell", "");
    }

    public String getSparkSqlBeelineParams() {
        return getOptional("kylin.source.hive.sparksql-beeline-params", "");
    }

    public boolean getHiveTableDirCreateFirst() {
        return Boolean.parseBoolean(getOptional("kylin.source.hive.table-dir-create-first", FALSE));
    }

    public String getFlatHiveTableClusterByDictColumn() {
        return getOptional("kylin.source.hive.flat-table-cluster-by-dict-column");
    }

    public int getHiveRedistributeColumnCount() {
        return Integer.parseInt(getOptional("kylin.source.hive.redistribute-column-count", "3"));
    }

    public int getDefaultVarcharPrecision() {
        int v = Integer.parseInt(getOptional("kylin.source.hive.default-varchar-precision", "256"));
        if (v < 1) {
            return 256;
        } else if (v > 65535) {
            return 65535;
        } else {
            return v;
        }
    }

    public int getDefaultCharPrecision() {
        //at most 255 according to https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-CharcharChar
        int v = Integer.parseInt(getOptional("kylin.source.hive.default-char-precision", "255"));
        if (v < 1) {
            return 255;
        } else if (v > 255) {
            return 255;
        } else {
            return v;
        }
    }

    public int getDefaultDecimalPrecision() {
        int v = Integer.parseInt(getOptional("kylin.source.hive.default-decimal-precision", "19"));
        if (v < 1) {
            return 19;
        } else {
            return v;
        }
    }

    public int getDefaultDecimalScale() {
        int v = Integer.parseInt(getOptional("kylin.source.hive.default-decimal-scale", "4"));
        if (v < 1) {
            return 4;
        } else {
            return v;
        }
    }

    public String getHiveIntermediateTablePrefix() {
        return getOptional("kylin.source.hive.intermediate-table-prefix", "kylin_intermediate_");
    }

    public String getHiveMetaDataType() {
        return getOptional("kylin.source.hive.metadata-type", "hcatalog");
    }

    // ============================================================================
    // SOURCE.KAFKA
    // ============================================================================

    public Map<String, String> getKafkaConfigOverride() {
        return getPropertiesByPrefix("kylin.source.kafka.config-override.");
    }

    // ============================================================================
    // SOURCE.JDBC
    // ============================================================================

    public String getJdbcSourceConnectionUrl() {
        return getOptional("kylin.source.jdbc.connection-url");
    }

    public String getJdbcSourceDriver() {
        return getOptional("kylin.source.jdbc.driver");
    }

    public String getJdbcSourceDialect() {
        return getOptional("kylin.source.jdbc.dialect", DEFAULT);
    }

    public String getJdbcSourceUser() {
        return getOptional("kylin.source.jdbc.user");
    }

    public String getJdbcSourcePass() {
        return getOptional("kylin.source.jdbc.pass");
    }

    public String getSqoopHome() {
        return getOptional("kylin.source.jdbc.sqoop-home");
    }

    public int getSqoopMapperNum() {
        return Integer.parseInt(getOptional("kylin.source.jdbc.sqoop-mapper-num", "4"));
    }

    public String getSqoopNullString() {
        return getOptional("kylin.source.jdbc.sqoop-null-string", "\\\\N");
    }

    public String getSqoopNullNonString() {
        return getOptional("kylin.source.jdbc.sqoop-null-non-string", "\\\\N");
    }

    public Map<String, String> getSqoopConfigOverride() {
        return getPropertiesByPrefix("kylin.source.jdbc.sqoop-config-override.");
    }

    public String getJdbcSourceFieldDelimiter() {
        return getOptional("kylin.source.jdbc.field-delimiter", "|");
    }

    // ============================================================================
    // STORAGE.HBASE
    // ============================================================================

    public Map<Integer, String> getStorageEngines() {
        Map<Integer, String> r = Maps.newLinkedHashMap();
        // ref constants in IStorageAware
        r.put(0, "org.apache.kylin.storage.hbase.HBaseStorage");
        r.put(1, "org.apache.kylin.storage.hybrid.HybridStorage");
        r.put(2, "org.apache.kylin.storage.hbase.HBaseStorage");
        r.put(3, "org.apache.kylin.storage.stream.StreamStorage");
        r.putAll(convertKeyToInteger(getPropertiesByPrefix("kylin.storage.provider.")));
        return r;
    }

    public int getDefaultStorageEngine() {
        return Integer.parseInt(getOptional("kylin.storage.default", "2"));
    }

    public StorageURL getStorageUrl() {
        String url = getOptional("kylin.storage.url", "default@hbase");

        // for backward compatibility
        if ("hbase".equals(url))
            url = "default@hbase";

        return StorageURL.valueOf(url);
    }

    public StorageURL getSecondaryStorageUrl() {
        String url = getOptional("kylin.secondary.storage.url", "");
        if (StringUtils.isEmpty(url)) {
            return null;
        }
        return StorageURL.valueOf(url);
    }

    public String getHBaseTableNamePrefix() {
        return getOptional("kylin.storage.hbase.table-name-prefix", "KYLIN_");
    }

    public String getHBaseStorageNameSpace() {
        return getOptional("kylin.storage.hbase.namespace", DEFAULT);
    }

    public String getHBaseClusterFs() {
        return getOptional("kylin.storage.hbase.cluster-fs", "");
    }

    public String getHBaseClusterHDFSConfigFile() {
        return getOptional("kylin.storage.hbase.cluster-hdfs-config-file", "");
    }

    private static final Pattern COPROCESSOR_JAR_NAME_PATTERN = Pattern.compile("kylin-coprocessor-(.+)\\.jar");
    private static final Pattern JOB_JAR_NAME_PATTERN = Pattern.compile("kylin-job-(.+)\\.jar");

    public String getCoprocessorLocalJar() {
        final String coprocessorJar = getOptional(KYLIN_STORAGE_HBASE_COPROCESSOR_LOCAL_JAR);
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
        logger.info("override {} to {}", KYLIN_STORAGE_HBASE_COPROCESSOR_LOCAL_JAR, path);
        System.setProperty(KYLIN_STORAGE_HBASE_COPROCESSOR_LOCAL_JAR, path);
    }

    private static String getFileName(String homePath, Pattern pattern) {
        File home = new File(homePath);
        SortedSet<String> files = Sets.newTreeSet();
        if (home.exists() && home.isDirectory()) {
            File[] listFiles = home.listFiles();
            if (listFiles != null) {
                for (File file : listFiles) {
                    final Matcher matcher = pattern.matcher(file.getName());
                    if (matcher.matches()) {
                        files.add(file.getAbsolutePath());
                    }
                }
            }
        }
        if (files.isEmpty()) {
            throw new RuntimeException("cannot find " + pattern.toString() + " in " + homePath);
        } else {
            return files.last();
        }
    }

    public int getHBaseHTableAvailableRetry() {
        return Integer.parseInt(getOptional("kylin.storage.hbase.htable-available-retry", "3"));
    }

    public int getHBaseRegionCompactionThreshold() {
        return Integer.parseInt(getOptional("kylin.storage.hbase.region-compaction-threshold", "3"));
    }

    public int getHBaseRegionCountMin() {
        return Integer.parseInt(getOptional("kylin.storage.hbase.min-region-count", "1"));
    }

    public int getHBaseRegionCountMax() {
        return Integer.parseInt(getOptional("kylin.storage.hbase.max-region-count", "500"));
    }

    public float getHBaseHFileSizeGB() {
        return Float.parseFloat(getOptional("kylin.storage.hbase.hfile-size-gb", "2.0"));
    }

    public boolean getQueryRunLocalCoprocessor() {
        return Boolean.parseBoolean(getOptional("kylin.storage.hbase.run-local-coprocessor", FALSE));
    }

    public double getQueryCoprocessorMemGB() {
        return Double.parseDouble(this.getOptional("kylin.storage.hbase.coprocessor-mem-gb", "3.0"));
    }

    public boolean getQueryCoprocessorSpillEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.storage.partition.aggr-spill-enabled", TRUE));
    }

    public long getPartitionMaxScanBytes() {
        long value = Long.parseLong(
                this.getOptional("kylin.storage.partition.max-scan-bytes", String.valueOf(3L * 1024 * 1024 * 1024)));
        return value > 0 ? value : Long.MAX_VALUE;
    }

    public int getQueryCoprocessorTimeoutSeconds() {
        return Integer.parseInt(this.getOptional("kylin.storage.hbase.coprocessor-timeout-seconds", "0"));
    }

    public int getQueryScanFuzzyKeyMax() {
        return Integer.parseInt(this.getOptional("kylin.storage.hbase.max-fuzzykey-scan", "200"));
    }

    public int getQueryScanFuzzyKeySplitMax() {
        return Integer.parseInt(this.getOptional("kylin.storage.hbase.max-fuzzykey-scan-split", "1"));
    }

    public int getQueryStorageVisitScanRangeMax() {
        return Integer.parseInt(this.getOptional("kylin.storage.hbase.max-visit-scanrange", "1000000"));
    }

    public String getDefaultIGTStorage() {
        return getOptional("kylin.storage.hbase.gtstorage",
                "org.apache.kylin.storage.hbase.cube.v2.CubeHBaseEndpointRPC");
    }

    public int getHBaseScanCacheRows() {
        return Integer.parseInt(this.getOptional("kylin.storage.hbase.scan-cache-rows", "1024"));
    }

    public float getKylinHBaseRegionCut() {
        return Float.parseFloat(getOptional("kylin.storage.hbase.region-cut-gb", "5.0"));
    }

    public int getHBaseScanMaxResultSize() {
        return Integer.parseInt(this.getOptional("kylin.storage.hbase.max-scan-result-bytes", "" + (5 * 1024 * 1024))); // 5 MB
    }

    public String getHbaseDefaultCompressionCodec() {
        return getOptional("kylin.storage.hbase.compression-codec", "none");
    }

    public String getHbaseDefaultEncoding() {
        return getOptional("kylin.storage.hbase.rowkey-encoding", "FAST_DIFF");
    }

    public int getHbaseDefaultBlockSize() {
        return Integer.parseInt(getOptional("kylin.storage.hbase.block-size-bytes", "1048576"));
    }

    public int getHbaseSmallFamilyBlockSize() {
        return Integer.parseInt(getOptional("kylin.storage.hbase.small-family-block-size-bytes", "65536"));
    }

    public String getKylinOwner() {
        return this.getOptional("kylin.storage.hbase.owner-tag", "");
    }

    public boolean getCompressionResult() {
        return Boolean.parseBoolean(getOptional("kylin.storage.hbase.endpoint-compress-result", TRUE));
    }

    public int getHBaseMaxConnectionThreads() {
        return Integer.parseInt(getOptional("kylin.storage.hbase.max-hconnection-threads", "2048"));
    }

    public int getHBaseCoreConnectionThreads() {
        return Integer.parseInt(getOptional("kylin.storage.hbase.core-hconnection-threads", "2048"));
    }

    public long getHBaseConnectionThreadPoolAliveSeconds() {
        return Long.parseLong(getOptional("kylin.storage.hbase.hconnection-threads-alive-seconds", "60"));
    }

    public int getHBaseReplicationScope() {
        return Integer.parseInt(getOptional("kylin.storage.hbase.replication-scope", "0"));
    }

    public boolean cleanStorageAfterDelOperation() {
        return Boolean.parseBoolean(getOptional("kylin.storage.clean-after-delete-operation", FALSE));
    }

    public int getHBaseMaxConnectionThreadsPerQuery() {
        return Integer.parseInt(getOptional("kylin.storage.hbase.max-hconnection-threads-per-query", "400"));
    }

    public int getHBaseMaxConnectionThreadsPerProject() {
        return Integer.parseInt(getOptional("kylin.storage.hbase.max-hconnection-threads-per-project", "800"));
    }

    // ============================================================================
    // ENGINE.MR
    // ============================================================================

    public Map<Integer, String> getJobEngines() {
        Map<Integer, String> r = Maps.newLinkedHashMap();
        // ref constants in IEngineAware
        r.put(0, "org.apache.kylin.engine.mr.MRBatchCubingEngine"); //IEngineAware.ID_MR_V1
        r.put(2, "org.apache.kylin.engine.mr.MRBatchCubingEngine2"); //IEngineAware.ID_MR_V2
        r.put(4, "org.apache.kylin.engine.spark.SparkBatchCubingEngine2"); //IEngineAware.ID_SPARK
        r.put(5, "org.apache.kylin.engine.flink.FlinkBatchCubingEngine2"); //IEngineAware.ID_FLINK

        r.putAll(convertKeyToInteger(getPropertiesByPrefix("kylin.engine.provider.")));
        return r;
    }

    public int getDefaultCubeEngine() {
        return Integer.parseInt(getOptional("kylin.engine.default", "2"));
    }

    public String getKylinJobJarPath() {
        final String jobJar = getOptional(KYLIN_ENGINE_MR_JOB_JAR);
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
        logger.info("override {} to {}", KYLIN_ENGINE_MR_JOB_JAR, path);
        System.setProperty(KYLIN_ENGINE_MR_JOB_JAR, path);
    }

    public String getKylinJobMRLibDir() {
        return getOptional("kylin.engine.mr.lib-dir", "");
    }

    public Map<String, String> getMRConfigOverride() {
        return getPropertiesByPrefix("kylin.engine.mr.config-override.");
    }

    // used for some mem-hungry step
    public Map<String, String> getMemHungryConfigOverride() {
        return getPropertiesByPrefix("kylin.engine.mr.mem-hungry-config-override.");
    }

    public Map<String, String> getUHCMRConfigOverride() {
        return getPropertiesByPrefix("kylin.engine.mr.uhc-config-override.");
    }

    public Map<String, String> getBaseCuboidMRConfigOverride() {
        return getPropertiesByPrefix("kylin.engine.mr.base-cuboid-config-override.");
    }

    public String getCuboidDfsReplication() {
        return getOptional("kylin.engine.cuboid.dfs.replication", "2");
    }

    public Map<String, String> getSparkConfigOverride() {
        return getPropertiesByPrefix("kylin.engine.spark-conf.");
    }

    public Map<String, String> getFlinkConfigOverride() {
        return getPropertiesByPrefix("kylin.engine.flink-conf.");
    }

    public Map<String, String> getSparkConfigOverrideWithSpecificName(String configName) {
        return getPropertiesByPrefix("kylin.engine.spark-conf-" + configName + ".");
    }

    public Map<String, String> getFlinkConfigOverrideWithSpecificName(String configName) {
        return getPropertiesByPrefix("kylin.engine.flink-conf-" + configName + ".");
    }

    public double getDefaultHadoopJobReducerInputMB() {
        return Double.parseDouble(getOptional("kylin.engine.mr.reduce-input-mb", "500"));
    }

    public double getDefaultHadoopJobReducerCountRatio() {
        return Double.parseDouble(getOptional("kylin.engine.mr.reduce-count-ratio", "1.0"));
    }

    public int getHadoopJobMinReducerNumber() {
        return Integer.parseInt(getOptional("kylin.engine.mr.min-reducer-number", "1"));
    }

    public int getHadoopJobMaxReducerNumber() {
        return Integer.parseInt(getOptional("kylin.engine.mr.max-reducer-number", "500"));
    }

    public int getHadoopJobMapperInputRows() {
        return Integer.parseInt(getOptional("kylin.engine.mr.mapper-input-rows", "1000000"));
    }

    public int getCuboidStatsCalculatorMaxNumber() {
        // set 1 to disable multi-thread statistics calculation
        return Integer.parseInt(getOptional("kylin.engine.mr.max-cuboid-stats-calculator-number", "1"));
    }

    public int getCuboidNumberPerStatsCalculator() {
        return Integer.parseInt(getOptional("kylin.engine.mr.cuboid-number-per-stats-calculator", "100"));
    }

    public int getHadoopJobPerReducerHLLCuboidNumber() {
        return Integer.parseInt(getOptional("kylin.engine.mr.per-reducer-hll-cuboid-number", "100"));
    }

    public int getHadoopJobHLLMaxReducerNumber() {
        // by default multi-reducer hll calculation is disabled
        return Integer.parseInt(getOptional("kylin.engine.mr.hll-max-reducer-number", "1"));
    }

    //UHC: ultra high cardinality columns, contain the ShardByColumns and the GlobalDictionaryColumns
    public int getUHCReducerCount() {
        return Integer.parseInt(getOptional("kylin.engine.mr.uhc-reducer-count", "3"));
    }

    public boolean isBuildUHCDictWithMREnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.mr.build-uhc-dict-in-additional-step", FALSE));
    }

    public boolean isBuildDictInReducerEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.mr.build-dict-in-reducer", TRUE));
    }

    public String getYarnStatusCheckUrl() {
        return getOptional("kylin.engine.mr.yarn-check-status-url", null);
    }

    public int getYarnStatusCheckIntervalSeconds() {
        return Integer.parseInt(getOptional("kylin.engine.mr.yarn-check-interval-seconds", "10"));
    }

    public boolean isUseLocalClasspathEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.mr.use-local-classpath", TRUE));
    }

    /**
     * different version hive use different UNION style
     * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Union
     */
    public String getHiveUnionStyle() {
        return getOptional("kylin.hive.union.style", "UNION");
    }

    // ============================================================================
    // ENGINE.SPARK
    // ============================================================================

    public String getHadoopConfDir() {
        return getOptional("kylin.env.hadoop-conf-dir", "");
    }

    public String getSparkAdditionalJars() {
        return getOptional("kylin.engine.spark.additional-jars", "");
    }

    public String getFlinkAdditionalJars() {
        return getOptional("kylin.engine.flink.additional-jars", "");
    }

    public float getSparkRDDPartitionCutMB() {
        return Float.parseFloat(getOptional("kylin.engine.spark.rdd-partition-cut-mb", "10.0"));
    }

    public float getFlinkPartitionCutMB() {
        return Float.parseFloat(getOptional("kylin.engine.flink.partition-cut-mb", "10.0"));
    }

    public int getSparkMinPartition() {
        return Integer.parseInt(getOptional("kylin.engine.spark.min-partition", "1"));
    }

    public int getFlinkMinPartition() {
        return Integer.parseInt(getOptional("kylin.engine.flink.min-partition", "1"));
    }

    public int getSparkMaxPartition() {
        return Integer.parseInt(getOptional("kylin.engine.spark.max-partition", "5000"));
    }

    public int getFlinkMaxPartition() {
        return Integer.parseInt(getOptional("kylin.engine.flink.max-partition", "5000"));
    }

    public String getSparkStorageLevel() {
        return getOptional("kylin.engine.spark.storage-level", "MEMORY_AND_DISK_SER");
    }

    public boolean isSparkSanityCheckEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.spark.sanity-check-enabled", FALSE));
    }

    public boolean isSparkFactDistinctEnable() {
        return Boolean.parseBoolean(getOptional("kylin.engine.spark-fact-distinct", FALSE));
    }

    public boolean isSparkUHCDictionaryEnable() {
        return Boolean.parseBoolean(getOptional("kylin.engine.spark-udc-dictionary", FALSE));
    }

    public boolean isSparkCardinalityEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.spark-cardinality", FALSE));
    }

    public int getSparkOutputMaxSize() {
        return Integer.valueOf(getOptional("kylin.engine.spark.output.max-size", "10485760"));
    }

    public boolean isSparkDimensionDictionaryEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.spark-dimension-dictionary", FALSE));
    }

    public boolean isSparCreateHiveTableViaSparkEnable() {
        return Boolean.parseBoolean(getOptional("kylin.engine.spark-create-table-enabled", FALSE));
    }

    public boolean isFlinkSanityCheckEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.flink.sanity-check-enabled", FALSE));
    }

    public boolean isFlinkFactDistinctEnable() {
        return Boolean.parseBoolean(getOptional("kylin.engine.flink-fact-distinct", FALSE));
    }

    public boolean isFlinkCubeHFileEnable() {
        return Boolean.parseBoolean(getOptional("kylin.engine.flink-cube-hfile", FALSE));
    }

    // ============================================================================
    // ENGINE.LIVY
    // ============================================================================

    public boolean isLivyEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.livy-conf.livy-enabled", FALSE));
    }

    public String getLivyRestApiBacktick() {
        return getOptional("kylin.engine.livy.backtick.quote", "\\\\`");
    }

    public String getLivyUrl() {
        return getOptional("kylin.engine.livy-conf.livy-url");
    }

    public Map<String, String> getLivyKey() {
        return getPropertiesByPrefix("kylin.engine.livy-conf.livy-key.");
    }

    public Map<String, String> getLivyArr() {
        return getPropertiesByPrefix("kylin.engine.livy-conf.livy-arr.");
    }

    public Map<String, String> getLivyMap() {
        return getPropertiesByPrefix("kylin.engine.livy-conf.livy-map.");
    }

    // ============================================================================
    // QUERY
    // ============================================================================

    public boolean isDictionaryEnumeratorEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.enable-dict-enumerator", FALSE));
    }

    public boolean isEnumerableRulesEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.calcite.enumerable-rules-enabled", FALSE));
    }

    public boolean isReduceExpressionsRulesEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.calcite.reduce-rules-enabled", TRUE));
    }

    public boolean isConvertCreateTableToWith() {
        return Boolean.parseBoolean(getOptional("kylin.query.convert-create-table-to-with", FALSE));
    }

    /**
     * Extras calcite properties to config Calcite connection
     */
    public Properties getCalciteExtrasProperties() {
        Properties properties = new Properties();
        Map<String, String> map = getPropertiesByPrefix("kylin.query.calcite.extras-props.");
        properties.putAll(map);
        return properties;
    }

    /**
     * Rule is usually singleton as static field, the configuration of this property is like:
     * RuleClassName1#FieldName1,RuleClassName2#FieldName2,...
     */
    public List<String> getCalciteAddRule() {
        String rules = getOptional("kylin.query.calcite.add-rule");
        if (rules == null) {
            return Lists.newArrayList();
        }
        return Lists.newArrayList(rules.split(","));
    }

    /**
     * Rule is usually singleton as static field, the configuration of this property is like:
     * RuleClassName1#FieldName1,RuleClassName2#FieldName2,...
     */
    public List<String> getCalciteRemoveRule() {
        String rules = getOptional("kylin.query.calcite.remove-rule");
        if (rules == null) {
            return Lists.newArrayList();
        }
        return Lists.newArrayList(rules.split(","));
    }

    // check KYLIN-3358, need deploy coprocessor if enabled
    // finally should be deprecated
    public boolean isDynamicColumnEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.enable-dynamic-column", FALSE));
    }

    //check KYLIN-1684, in most cases keep the default value
    public boolean isSkippingEmptySegments() {
        return Boolean.parseBoolean(getOptional("kylin.query.skip-empty-segments", TRUE));
    }

    public boolean isDisableCubeNoAggSQL() {
        return Boolean.parseBoolean(getOptional("kylin.query.disable-cube-noagg-sql", FALSE));
    }

    public boolean isStreamAggregateEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.stream-aggregate-enabled", TRUE));
    }

    public boolean isProjectIsolationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.storage.project-isolation-enable", TRUE));
    }

    @Deprecated //Limit is good even it's large. This config is meaning less since we already have scan threshold
    public int getStoragePushDownLimitMax() {
        return Integer.parseInt(getOptional("kylin.query.max-limit-pushdown", "10000"));
    }

    // Select star on large table is too slow for BI, add limit by default if missing
    // https://issues.apache.org/jira/browse/KYLIN-2649
    public int getForceLimit() {
        return Integer.parseInt(getOptional("kylin.query.force-limit", "-1"));
    }

    @Deprecated
    public int getScanThreshold() {
        return Integer.parseInt(getOptional("kylin.query.scan-threshold", "10000000"));
    }

    public boolean isLazyQueryEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.lazy-query-enabled", FALSE));
    }

    public long getLazyQueryWaitingTimeoutMilliSeconds() {
        return Long.parseLong(getOptional("kylin.query.lazy-query-waiting-timeout-milliseconds", "60000"));
    }

    public int getQueryConcurrentRunningThresholdForProject() {
        // by default there's no limitation
        return Integer.parseInt(getOptional("kylin.query.project-concurrent-running-threshold", "0"));
    }

    public long getQueryMaxScanBytes() {
        long value = Long.parseLong(getOptional("kylin.query.max-scan-bytes", "0"));
        return value > 0 ? value : Long.MAX_VALUE;
    }

    public long getQueryMaxReturnBytes() {
        long value = Long
                .parseLong(this.getOptional("kylin.query.max-return-bytes", String.valueOf(3L * 1024 * 1024 * 1024)));
        return value > 0 ? value : Long.MAX_VALUE;
    }

    public int getTranslatedInClauseMaxSize() {
        return Integer.parseInt(getOptional("kylin.query.translated-in-clause-max-size", String.valueOf(1024 * 1024)));
    }

    public int getLargeQueryThreshold() {
        return Integer.parseInt(getOptional("kylin.query.large-query-threshold", String.valueOf(1000000)));
    }

    public int getDerivedInThreshold() {
        return Integer.parseInt(getOptional("kylin.query.derived-filter-translation-threshold", "20"));
    }

    public int getBadQueryStackTraceDepth() {
        return Integer.parseInt(getOptional("kylin.query.badquery-stacktrace-depth", "10"));
    }

    public int getBadQueryHistoryNum() {
        return Integer.parseInt(getOptional("kylin.query.badquery-history-number", "50"));
    }

    public int getBadQueryDefaultAlertingSeconds() {
        return Integer.parseInt(getOptional("kylin.query.badquery-alerting-seconds", "90"));
    }

    public double getBadQueryDefaultAlertingCoefficient() {
        return Double.parseDouble(getOptional("kylin.query.timeout-seconds-coefficient", "0.5"));
    }

    public int getBadQueryDefaultDetectIntervalSeconds() {
        int time = (int) (getQueryTimeoutSeconds() * getBadQueryDefaultAlertingCoefficient()); // half of query timeout
        if (time == 0) {
            time = 60; // 60 sec
        }
        return time;
    }

    public boolean getBadQueryPersistentEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.badquery-persistent-enabled", TRUE));
    }

    public String[] getQueryTransformers() {
        return getOptionalStringArray("kylin.query.transformers", new String[0]);
    }

    public String[] getQueryInterceptors() {
        return getOptionalStringArray("kylin.query.interceptors", new String[0]);
    }

    public long getQueryDurationCacheThreshold() {
        return Long.parseLong(this.getOptional("kylin.query.cache-threshold-duration", String.valueOf(2000)));
    }

    public long getQueryScanCountCacheThreshold() {
        return Long.parseLong(this.getOptional("kylin.query.cache-threshold-scan-count", String.valueOf(10 * 1024)));
    }

    public long getQueryScanBytesCacheThreshold() {
        return Long.parseLong(this.getOptional("kylin.query.cache-threshold-scan-bytes", String.valueOf(1024 * 1024)));
    }

    public boolean isQuerySecureEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.security-enabled", TRUE));
    }

    public boolean isQueryCacheEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.cache-enabled", TRUE));
    }

    public boolean isQueryIgnoreUnknownFunction() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.ignore-unknown-function", FALSE));
    }

    public boolean isMemcachedEnabled() {
        return !StringUtil.isEmpty(getMemCachedHosts());
    }

    public String getMemCachedHosts() {
        return getOptional("kylin.cache.memcached.hosts", null);
    }

    public boolean isQuerySegmentCacheEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.segment-cache-enabled", FALSE));
    }

    public int getQuerySegmentCacheTimeout() {
        return Integer.parseInt(getOptional("kylin.query.segment-cache-timeout", "2000"));
    }

    // define the maximum size for each segment in one query that can be cached, in megabytes
    public int getQuerySegmentCacheMaxSize() {
        return Integer.parseInt(getOptional("kylin.query.segment-cache-max-size", "200"));
    }

    public String getQueryAccessController() {
        return getOptional("kylin.query.access-controller", null);
    }

    public int getQueryMaxCacheStatementNum() {
        return Integer.parseInt(this.getOptional("kylin.query.statement-cache-max-num", String.valueOf(50000)));
    }

    public int getQueryMaxCacheStatementInstancePerKey() {
        return Integer.parseInt(this.getOptional("kylin.query.statement-cache-max-num-per-key", String.valueOf(50)));
    }

    public boolean isQueryPreparedStatementCacheEnable() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.statement-cache-enabled", TRUE));
    }

    public int getDimCountDistinctMaxCardinality() {
        return Integer.parseInt(getOptional("kylin.query.max-dimension-count-distinct", "5000000"));
    }

    public Map<String, String> getUDFs() {
        Map<String, String> udfMap = Maps.newLinkedHashMap();
        udfMap.put("version", "org.apache.kylin.query.udf.VersionUDF");
        udfMap.put("concat", "org.apache.kylin.query.udf.ConcatUDF");
        udfMap.put("massin", "org.apache.kylin.query.udf.MassInUDF");
        Map<String, String> overrideUdfMap = getPropertiesByPrefix("kylin.query.udf.");
        udfMap.putAll(overrideUdfMap);
        return udfMap;
    }

    public int getQueryTimeoutSeconds() {
        int time = Integer.parseInt(this.getOptional("kylin.query.timeout-seconds", "0"));
        if (time != 0 && time <= 60) {
            logger.warn("query timeout seconds less than 60 sec, set to 60 sec.");
            time = 60;
        }
        return time;
    }

    public boolean isPushDownEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.pushdown.enabled", FALSE))
               || StringUtils.isNotEmpty(getPushDownRunnerClassName());
    }

    public boolean isPushDownUpdateEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.pushdown.update-enabled", FALSE));
    }

    public String getSchemaFactory() {
        return this.getOptional("kylin.query.schema-factory", "org.apache.kylin.query.schema.OLAPSchemaFactory");
    }

    public String getPushDownRunnerClassName() {
        return getOptional("kylin.query.pushdown.runner-class-name", "");
    }

    public List<String> getPushDownRunnerIds() {
        List<String> ids = Lists.newArrayList();
        String idsStr = getOptional("kylin.query.pushdown.runner.ids", "");
        if (StringUtils.isNotEmpty(idsStr)) {
            for (String id: idsStr.split(",")) {
                ids.add(id);
            }
        }
        return ids;
    }

    public String[] getPushDownConverterClassNames() {
        return getOptionalStringArray("kylin.query.pushdown.converter-class-names",
                new String[] { "org.apache.kylin.source.adhocquery.HivePushDownConverter" });
    }

    public boolean isPushdownQueryCacheEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.pushdown.cache-enabled", FALSE));
    }

    public String getJdbcUrl(String id) {
        if (null == id) {
            return getOptional("kylin.query.pushdown.jdbc.url", "");
        } else {
            return getOptional("kylin.query.pushdown." + id + ".jdbc.url", "");
        }
    }

    public String getJdbcDriverClass(String id) {
        if (null == id) {
            return getOptional("kylin.query.pushdown.jdbc.driver", "");
        } else {
            return getOptional("kylin.query.pushdown." + id + ".jdbc.driver", "");
        }
    }

    public String getJdbcUsername(String id) {
        if (null == id) {
            return getOptional("kylin.query.pushdown.jdbc.username", "");
        } else {
            return getOptional("kylin.query.pushdown." + id + ".jdbc.username", "");
        }
    }

    public String getJdbcPassword(String id) {
        if (null == id) {
            return getOptional("kylin.query.pushdown.jdbc.password", "");
        } else {
            return getOptional("kylin.query.pushdown." + id + ".jdbc.password", "");
        }
    }

    public int getPoolMaxTotal(String id) {
        if (null == id) {
            return Integer.parseInt(
                    this.getOptional("kylin.query.pushdown.jdbc.pool-max-total", "8")
            );
        } else {
            return Integer.parseInt(
                    this.getOptional("kylin.query.pushdown." + id + ".jdbc.pool-max-total", "8")
            );
        }
    }

    public int getPoolMaxIdle(String id) {
        if (null == id) {
            return Integer.parseInt(
                    this.getOptional("kylin.query.pushdown.jdbc.pool-max-idle", "8")
            );
        } else {
            return Integer.parseInt(
                    this.getOptional("kylin.query.pushdown." + id + ".jdbc.pool-max-idle", "8")
            );
        }
    }

    public int getPoolMinIdle(String id) {
        if (null == id) {
            return Integer.parseInt(
                    this.getOptional("kylin.query.pushdown.jdbc.pool-min-idle", "0")
            );
        } else {
            return Integer.parseInt(
                    this.getOptional("kylin.query.pushdown." + id + ".jdbc.pool-min-idle", "0")
            );
        }
    }

    public boolean isTableACLEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.security.table-acl-enabled", TRUE));
    }

    public boolean isEscapeDefaultKeywordEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.escape-default-keyword", FALSE));
    }

    public String getQueryRealizationFilter() {
        return getOptional("kylin.query.realization-filter", null);
    }

    public String getSQLResponseSignatureClass() {
        return this.getOptional("kylin.query.signature-class",
                "org.apache.kylin.rest.signature.FactTableRealizationSetCalculator");
    }

    public boolean isQueryCacheSignatureEnabled() {
        return Boolean.parseBoolean(
                this.getOptional("kylin.query.cache-signature-enabled", String.valueOf(isMemcachedEnabled())));
    }

    public int getFlatFilterMaxChildrenSize() {
        return Integer.parseInt(this.getOptional("kylin.query.flat-filter-max-children", "500000"));
    }

    // ============================================================================
    // SERVER
    // ============================================================================

    public String getServerMode() {
        return this.getOptional("kylin.server.mode", "all");
    }

    public String[] getRestServers() {
        return getOptionalStringArray("kylin.server.cluster-servers", new String[0]);
    }

    public String[] getRawRestServers() {
        return getRawStringArray("kylin.server.cluster-servers", new String[0]);
    }

    public String getServerRestAddress() {
        return getOptional("kylin.server.host-address", "localhost:7070");
    }

    public boolean getServerSelfDiscoveryEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.server.self-discovery-enabled", FALSE));
    }

    public String getClusterName() {
        String key = "kylin.server.cluster-name";
        String clusterName = this.getOptional(key, getMetadataUrlPrefix());
        setProperty(key, clusterName);
        return clusterName;
    }

    public String getInitTasks() {
        return getOptional("kylin.server.init-tasks");
    }

    public int getWorkersPerServer() {
        //for sequence sql use
        return Integer.parseInt(getOptional("kylin.server.sequence-sql.workers-per-server", "1"));
    }

    public long getSequenceExpireTime() {
        return Long.parseLong(this.getOptional("kylin.server.sequence-sql.expire-time", "86400000"));//default a day
    }

    public boolean getQueryMetricsEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.server.query-metrics-enabled", FALSE));
    }

    public boolean getQueryMetrics2Enabled() {
        return Boolean.parseBoolean(getOptional("kylin.server.query-metrics2-enabled", FALSE));
    }

    public int[] getQueryMetricsPercentilesIntervals() {
        String[] dft = { "60", "300", "3600" };
        return getOptionalIntArray("kylin.server.query-metrics-percentiles-intervals", dft);
    }

    public int getServerUserCacheExpireSeconds() {
        return Integer.parseInt(this.getOptional("kylin.server.auth-user-cache.expire-seconds", "300"));
    }

    public int getServerUserCacheMaxEntries() {
        return Integer.parseInt(this.getOptional("kylin.server.auth-user-cache.max-entries", "100"));
    }

    public String getSecurityProfile() {
        return getOptional("kylin.security.profile", "testing");
    }

    public String getExternalAclProvider() {
        return getOptional("kylin.server.external-acl-provider", "");
    }

    public String getLDAPUserSearchBase() {
        return getOptional("kylin.security.ldap.user-search-base", "");
    }

    public String getLDAPGroupSearchBase() {
        return getOptional("kylin.security.ldap.user-group-search-base", "");
    }

    public String getLDAPAdminRole() {
        return getOptional("kylin.security.acl.admin-role", "");
    }

    public boolean createAdminWhenAbsent() {
        return Boolean.parseBoolean(getOptional("kylin.security.create-admin-when-absent", FALSE));
    }

    // ============================================================================
    // WEB
    // ============================================================================

    public String getTimeZone() {
        String timezone = getOptional("kylin.web.timezone");
        if (StringUtils.isBlank(timezone)) {
            timezone = TimeZone.getDefault().getID();
        }
        return timezone;
    }

    public boolean isWebCrossDomainEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.web.cross-domain-enabled", TRUE));
    }

    public boolean isAdminUserExportAllowed() {
        return Boolean.parseBoolean(getOptional("kylin.web.export-allow-admin", TRUE));
    }

    public boolean isNoneAdminUserExportAllowed() {
        return Boolean.parseBoolean(getOptional("kylin.web.export-allow-other", TRUE));
    }

    public boolean isWebDashboardEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.web.dashboard-enabled", FALSE));
    }

    public boolean isWebConfigEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.web.set-config-enable", FALSE));
    }

    /**
     * @see #isWebConfigEnabled
     */
    public String getPropertiesWhiteListForModification() {
        return getOptional("kylin.web.properties.whitelist", "kylin.query.cache-enabled");
    }

    public String getPropertiesWhiteList() {
        return getOptional("kylin.web.properties.whitelist", "kylin.web.timezone,kylin.query.cache-enabled,kylin.env,"
                + "kylin.cube.migration.enabled,"
                + "kylin.web.hive-limit,kylin.storage.default,"
                + "kylin.engine.default,kylin.web.link-hadoop,kylin.web.link-diagnostic,"
                + "kylin.web.contact-mail,kylin.web.help.length,kylin.web.help.0,kylin.web.help.1,kylin.web.help.2,"
                + "kylin.web.help.3,"
                + "kylin.web.help,kylin.web.hide-measures,kylin.web.link-streaming-guide,kylin.server.external-acl-provider,"
                + "kylin.security.profile,kylin.security.additional-profiles,"
                + "kylin.htrace.show-gui-trace-toggle,kylin.web.export-allow-admin,kylin.web.export-allow-other,"
                + "kylin.cube.cubeplanner.enabled,kylin.web.dashboard-enabled,"
                + "kylin.job.scheduler.default,kylin.web.default-time-filter");
    }

    // ============================================================================
    // RESTCLIENT
    // ============================================================================

    public int getRestClientDefaultMaxPerRoute() {
        return Integer.parseInt(this.getOptional("kylin.restclient.connection.default-max-per-route", "20"));
    }

    public int getRestClientMaxTotal() {
        return Integer.parseInt(this.getOptional("kylin.restclient.connection.max-total", "200"));
    }

    // ============================================================================
    // Metrics
    // ============================================================================

    public String getCoadhaleMetricsReportClassesNames() {
        return getOptional("kylin.metrics.reporter-classes",
                "org.apache.kylin.common.metrics.metrics2.JsonFileMetricsReporter,org.apache.kylin.common.metrics.metrics2.JmxMetricsReporter");
    }

    public String getMetricsFileLocation() {
        return getOptional("kylin.metrics.file-location", "/tmp/report.json");
    }

    public Long getMetricsReporterFrequency() {
        return Long.parseLong(getOptional("kylin.metrics.file-frequency", "5000"));
    }

    public String getPerfLoggerClassName() {
        return getOptional("kylin.metrics.perflogger-class", "org.apache.kylin.common.metrics.perflog.PerfLogger");
    }

    public boolean isShowingGuiTraceToggle() {
        return Boolean.parseBoolean(getOptional("kylin.htrace.show-gui-trace-toggle", FALSE));
    }

    public boolean isHtraceTracingEveryQuery() {
        return Boolean.parseBoolean(getOptional("kylin.htrace.trace-every-query", FALSE));
    }

    public String getKylinMetricsEventTimeZone() {
        return getOptional("kylin.metrics.event-time-zone", getTimeZone()).toUpperCase(Locale.ROOT);
    }

    public boolean isKylinMetricsMonitorEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metrics.monitor-enabled", FALSE));
    }

    public boolean isKylinMetricsReporterForQueryEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metrics.reporter-query-enabled", FALSE));
    }

    public boolean isKylinMetricsReporterForJobEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metrics.reporter-job-enabled", FALSE));
    }

    public String getKylinMetricsPrefix() {
        return getOptional("kylin.metrics.prefix", "KYLIN").toUpperCase(Locale.ROOT);
    }

    public String getKylinMetricsActiveReservoirDefaultClass() {
        return getOptional("kylin.metrics.active-reservoir-default-class",
                "org.apache.kylin.metrics.lib.impl.StubReservoir");
    }

    public String getKylinSystemCubeSinkDefaultClass() {
        return getOptional("kylin.metrics.system-cube-sink-default-class",
                "org.apache.kylin.metrics.lib.impl.hive.HiveSink");
    }

    public String getKylinMetricsSubjectSuffix() {
        return getOptional("kylin.metrics.subject-suffix", getDeployEnv());
    }

    public String getKylinMetricsSubjectJob() {
        return getOptional("kylin.metrics.subject-job", "METRICS_JOB") + "_" + getKylinMetricsSubjectSuffix();
    }

    public String getKylinMetricsSubjectJobException() {
        return getOptional("kylin.metrics.subject-job-exception", "METRICS_JOB_EXCEPTION") + "_"
                + getKylinMetricsSubjectSuffix();
    }

    public String getKylinMetricsSubjectQuery() {
        return getOptional("kylin.metrics.subject-query", "METRICS_QUERY") + "_" + getKylinMetricsSubjectSuffix();
    }

    public String getKylinMetricsSubjectQueryCube() {
        return getOptional("kylin.metrics.subject-query-cube", "METRICS_QUERY_CUBE") + "_"
                + getKylinMetricsSubjectSuffix();
    }

    public String getKylinMetricsSubjectQueryRpcCall() {
        return getOptional("kylin.metrics.subject-query-rpc", "METRICS_QUERY_RPC") + "_"
                + getKylinMetricsSubjectSuffix();
    }

    public Map<String, String> getKylinMetricsConf() {
        return getPropertiesByPrefix("kylin.metrics.");
    }

    public int printSampleEventRatio() {
        String val = getOptional("kylin.metrics.kafka-sample-ratio", "10000");
        return Integer.parseInt(val);
    }

    // ============================================================================
    // CUBE MIGRATION
    // ============================================================================
    public boolean isMigrationCubeEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.cube.migration.enabled", FALSE));
    }

    public int getMigrationRuleExpansionRateThreshold() {
        return Integer.parseInt(getOptional("kylin.cube.migration.expansion-rate", "5"));
    }

    public int getMigrationRuleQueryGeneratorMaxDimensions() {
        return Integer.parseInt(getOptional("kylin.cube.migration.query-generator-max-dimension-number", "3"));
    }

    public int getMigrationRuleQueryLatency() {
        return 1000 * Integer.parseInt(getOptional("kylin.cube.migration.query-latency-seconds", "2"));
    }

    public int getMigrationRuleQueryLatencyMaxThreads() {
        return Integer.parseInt(getOptional("kylin.cube.migration.query-latency-max-threads", "5"));
    }

    public int getMigrationRuleQueryEvaluationIteration() {
        return Integer.parseInt(getOptional("kylin.cube.migration.query-latency-iteration", "5"));
    }

    public String getMigrationLocalAddress() {
        return getOptional("kylin.cube.migration.source-address", "localhost:80");
    }

    public String getMigrationTargetAddress() {
        return getOptional("kylin.cube.migration.target-address", "sandbox:80");
    }

    public String getNotificationMailSuffix() {
        return getOptional("kylin.cube.notification-mail-suffix", "@mail.com");
    }

    public boolean isMigrationApplyQueryLatencyRule() {
        return Boolean.parseBoolean(getOptional("kylin.cube.migration.rule-query-latency-enabled", TRUE));
    }

    // ============================================================================
    // jdbc metadata resource store
    // ============================================================================

    public String getMetadataDialect() {
        return getOptional("kylin.metadata.jdbc.dialect", "mysql");
    }

    public boolean isJsonAlwaysSmallCell() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.jdbc.json-always-small-cell", TRUE));
    }

    public int getSmallCellMetadataWarningThreshold() {
        return Integer.parseInt(
                getOptional("kylin.metadata.jdbc.small-cell-meta-size-warning-threshold", String.valueOf(100 << 20))); //100mb
    }

    public int getSmallCellMetadataErrorThreshold() {
        return Integer.parseInt(
                getOptional("kylin.metadata.jdbc.small-cell-meta-size-error-threshold", String.valueOf(1 << 30))); // 1gb
    }

    public int getJdbcResourceStoreMaxCellSize() {
        return Integer.parseInt(getOptional("kylin.metadata.jdbc.max-cell-size", "1048576")); // 1mb
    }

    public String getJdbcSourceAdaptor() {
        return getOptional("kylin.source.jdbc.adaptor");
    }

    public boolean isLimitPushDownEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.storage.limit-push-down-enabled", TRUE));
    }

    // ============================================================================
    // Realtime streaming
    // ============================================================================
    public String getStreamingStoreClass() {
        return getOptional("kylin.stream.store.class",
                "org.apache.kylin.stream.core.storage.columnar.ColumnarSegmentStore");
    }

    public String getStreamingBasicCuboidJobDFSBlockSize() {
        return getOptional("kylin.stream.job.dfs.block.size", String.valueOf(16 * 1024 * 1024));
    }

    public String getStreamingIndexPath() {
        return getOptional("kylin.stream.index.path", "stream_index");
    }

    public int getStreamingCubeConsumerTasksNum() {
        return Integer.parseInt(getOptional("kylin.stream.cube-num-of-consumer-tasks", "3"));
    }

    public int getStreamingCubeWindowInSecs() {
        return Integer.parseInt(getOptional("kylin.stream.cube.window", "3600"));
    }

    public int getStreamingCubeDurationInSecs() {
        return Integer.parseInt(getOptional("kylin.stream.cube.duration", "7200"));
    }

    public int getStreamingCubeMaxDurationInSecs() {
        return Integer.parseInt(getOptional("kylin.stream.cube.duration.max", "43200"));
    }

    public int getStreamingCheckPointFileMaxNum() {
        return Integer.parseInt(getOptional("kylin.stream.checkpoint.file.max.num", "5"));
    }

    public int getStreamingCheckPointIntervalsInSecs() {
        return Integer.parseInt(getOptional("kylin.stream.index.checkpoint.intervals", "300"));
    }

    public int getStreamingIndexMaxRows() {
        return Integer.parseInt(getOptional("kylin.stream.index.maxrows", "50000"));
    }

    public int getStreamingMaxImmutableSegments() {
        return Integer.parseInt(getOptional("kylin.stream.immutable.segments.max.num", "100"));
    }

    public boolean isStreamingConsumeFromLatestOffsets() {
        return Boolean.parseBoolean(getOptional("kylin.stream.consume.offsets.latest", "true"));
    }

    public String getStreamingNode() {
        return getOptional("kylin.stream.node", null);
    }

    public Map<String, String> getStreamingNodeProperties() {
        return getPropertiesByPrefix("kylin.stream.node");
    }

    public String getStreamingMetadataStoreType() {
        return getOptional("kylin.stream.metadata.store.type", "zk");
    }

    public String getStreamingSegmentRetentionPolicy() {
        return getOptional("kylin.stream.segment.retention.policy", "fullBuild");
    }

    public String getStreamingAssigner() {
        return getOptional("kylin.stream.assigner", "DefaultAssigner");
    }

    public int getCoordinatorHttpClientTimeout() {
        return Integer.parseInt(getOptional("kylin.stream.coordinator.client.timeout.millsecond", "5000"));
    }

    public int getReceiverHttpClientTimeout() {
        return Integer.parseInt(getOptional("kylin.stream.receiver.client.timeout.millsecond", "5000"));
    }

    public int getStreamingReceiverHttpMaxThreads() {
        return Integer.parseInt(getOptional("kylin.stream.receiver.http.max.threads", "200"));
    }

    public int getStreamingReceiverHttpMinThreads() {
        return Integer.parseInt(getOptional("kylin.stream.receiver.http.min.threads", "10"));
    }

    public int getStreamingReceiverQueryCoreThreads() {
        int def = Math.max(2, AVAILABLE_PROCESSORS - 1);
        return Integer.parseInt(getOptional("kylin.stream.receiver.query-core-threads", def + ""));
    }

    public int getStreamingReceiverQueryMaxThreads() {
        int def = Math.max(2, AVAILABLE_PROCESSORS - 1);
        return Integer.parseInt(getOptional("kylin.stream.receiver.query-max-threads", def + ""));
    }

    public int getStreamingReceiverUseThreadsPerQuery() {
        return Integer.parseInt(getOptional("kylin.stream.receiver.use-threads-per-query", "8"));
    }

    public int getStreamingRPCHttpConnTimeout() {
        return Integer.parseInt(getOptional("kylin.stream.rpc.http.connect.timeout", "10000"));
    }

    public int getStreamingRPCHttpReadTimeout() {
        return Integer.parseInt(getOptional("kylin.stream.rpc.http.read.timeout", "60000"));
    }

    public boolean isStreamingBuildAdditionalCuboids() {
        return Boolean.parseBoolean(getOptional("kylin.stream.build.additional.cuboids", "false"));
    }

    public Map<String, String> getStreamingSegmentRetentionPolicyProperties(String policyName) {
        return getPropertiesByPrefix("kylin.stream.segment.retention.policy." + policyName + ".");
    }

    public int getStreamingMaxFragmentsInSegment() {
        return Integer.parseInt(getOptional("kylin.stream.segment-max-fragments", "50"));
    }

    public int getStreamingMinFragmentsInSegment() {
        return Integer.parseInt(getOptional("kylin.stream.segment-min-fragments", "15"));
    }

    public int getStreamingMaxFragmentSizeInMb() {
        return Integer.parseInt(getOptional("kylin.stream.max-fragment-size-mb", "300"));
    }

    public boolean isStreamingFragmentsAutoMergeEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.stream.fragments-auto-merge-enable", "true"));
    }

    public boolean isStreamingConcurrentScanEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.stream.segment.concurrent.scan", "false"));
    }

    public boolean isStreamingStandAloneMode() {
        return Boolean.parseBoolean(getOptional("kylin.stream.stand-alone.mode", "false"));
    }

    public boolean isNewCoordinatorEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.stream.new.coordinator-enabled", "true"));
    }

    public String getLocalStorageImpl() {
        return getOptional("kylin.stream.settled.storage", null);
    }

    public String getStreamMetrics() {
        return getOptional("kylin.stream.metrics.option", "");
    }

    /**
     * whether to print encode integer value for count distinct string value, only for debug/test purpose
     */
    public boolean isPrintRealtimeDictEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.stream.print-realtime-dict-enabled", "false"));
    }

    public long getStreamMetricsInterval() {
        return Long.parseLong(getOptional("kylin.stream.metrics.interval", "5"));
    }

    /**
     * whether realtime query should add timezone offset by kylin's web-timezone, please refer to KYLIN-4010 for detail
     */
    public String getStreamingDerivedTimeTimezone() {
        return (getOptional("kylin.stream.event.timezone", ""));
    }

    public boolean isAutoResubmitDiscardJob() {
        return Boolean.parseBoolean(getOptional("kylin.stream.auto-resubmit-after-discard-enabled", "true"));
    }

    public String getHiveDatabaseLambdaCube() {
        return this.getOptional("kylin.stream.hive.database-for-lambda-cube", DEFAULT);
    }

    // ============================================================================
    // Health Check CLI
    // ============================================================================

    public int getWarningSegmentNum() {
        return Integer.parseInt(getOptional("kylin.tool.health-check.warning-segment-num", "-1"));
    }

    public int getWarningCubeExpansionRate() {
        return Integer.parseInt(getOptional("kylin.tool.health-check.warning-cube-expansion-rate", "5"));
    }

    public int getExpansionCheckMinCubeSizeInGb() {
        return Integer.parseInt(getOptional("kylin.tool.health-check.expansion-check.min-cube-size-gb", "500"));
    }

    public int getStaleCubeThresholdInDays() {
        return Integer.parseInt(getOptional("kylin.tool.health-check.stale-cube-threshold-days", "100"));
    }

    public int getStaleJobThresholdInDays() {
        return Integer.parseInt(getOptional("kylin.tool.health-check.stale-job-threshold-days", "30"));
    }

    public String getIntersectFilterOrSeparator() {
        return getOptional("kylin.query.intersect.separator", "|");
    }

    public int getDefaultTimeFilter() {
        return Integer.parseInt(getOptional("kylin.web.default-time-filter", "2"));
    }

    public boolean jobStatusWriteKafka() {
        return Boolean.parseBoolean(getOptional("kylin.engine.job-status.write.kafka", FALSE));
    }

    public Map<String, String> getJobStatusKafkaConfig() {
        return getPropertiesByPrefix("kylin.engine.job-status.kafka.");
    }

    public boolean isHFileDistCP() {
        return Boolean.parseBoolean(getOptional("kylin.storage.hfile-distcp-enable", "false"));
    }

    public int getDistCPMapBandWidth(){
        return Integer.valueOf(getOptional("kylin.storage.distcp-map-bandwidth", "20"));
    }

    public int getDistCPMaxMapNum(){
        return Integer.valueOf(getOptional("kylin.storage.distcp-max-map-num", "50"));
    }

    public String getKylinDictCacheStrength(){
        return getOptional("kylin.dict.cache.strength", "soft");
    };
}
