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

import static org.apache.kylin.common.KylinExternalConfigLoader.KYLIN_CONF_PROPERTIES_FILE;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.annotation.ThirdPartyDependencies;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OrderedProperties;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.config.core.loader.IExternalConfigLoader;
import lombok.Setter;

/**
 */
public class KylinConfig extends KylinConfigBase {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(KylinConfig.class);

    @Setter
    private static IExternalConfigLoader configLoader = null;

    final transient ReentrantLock lock = new ReentrantLock();
    /**
     * Kylin properties file name
     */
    public static final String KYLIN_CONF = "KYLIN_CONF";

    // static cached instances
    private static volatile KylinConfig SYS_ENV_INSTANCE = null;

    // thread-local instances, will override SYS_ENV_INSTANCE
    private static transient ThreadLocal<KylinConfig> THREAD_ENV_INSTANCE = new ThreadLocal<>();

    public static final String MODEL_OFFLINE_FLAG = "kylin.model.offline";

    public static final String USE_LEGACY_CONFIG = "KYLIN_LEGACY_CONFIG";

    static {
        /*
         * Make Calcite to work with Unicode.
         *
         * Sets default char set for string literals in SQL and row types of
         * RelNode. This is more a label used to compare row type equality. For
         * both SQL string and row record, they are passed to Calcite in String
         * object and does not require additional codec.
         *
         * Ref SaffronProperties.defaultCharset
         * Ref SqlUtil.translateCharacterSetName()
         * Ref NlsString constructor()
         */
        // copied from org.apache.calcite.util.ConversionUtil.NATIVE_UTF16_CHARSET_NAME
        String NATIVE_UTF16_CHARSET_NAME = (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) ? "UTF-16BE" : "UTF-16LE";
        Unsafe.setProperty("saffron.default.charset", NATIVE_UTF16_CHARSET_NAME);
        Unsafe.setProperty("saffron.default.nationalcharset", NATIVE_UTF16_CHARSET_NAME);
        Unsafe.setProperty("saffron.default.collation.name", NATIVE_UTF16_CHARSET_NAME + "$en_US");
    }

    @ThirdPartyDependencies({
            @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager", classes = {
                    "StaticAuthenticationProvider", "StaticUserGroupService" }) })
    public static KylinConfig getInstanceFromEnv() {
        KylinConfig config = THREAD_ENV_INSTANCE.get();
        if (config != null) {
            logger.trace("Using thread local KylinConfig");
            return config;
        }
        if (SYS_ENV_INSTANCE != null) {
            return SYS_ENV_INSTANCE;
        }
        synchronized (KylinConfig.class) {
            if (SYS_ENV_INSTANCE == null) {
                SYS_ENV_INSTANCE = newKylinConfig(configLoader != null && !useLegacyConfig() ? configLoader
                        : ShellKylinExternalConfigLoaderFactory.getConfigLoader());
                logger.trace("Created a new KylinConfig by getInstanceFromEnv, KylinConfig Id: {}",
                        System.identityHashCode(SYS_ENV_INSTANCE));
            }

            return SYS_ENV_INSTANCE;
        }
    }

    /**
     * Developer can only use this method for read system KylinConfig.
     */
    public static KylinConfig readSystemKylinConfig() {
        if (SYS_ENV_INSTANCE == null) {
            return KylinConfig.getInstanceFromEnv();
        }
        return SYS_ENV_INSTANCE;
    }

    /**
     * only for xxTool and UT
     * @return
     */
    public static KylinConfig newKylinConfig() {
        return newKylinConfig(ShellKylinExternalConfigLoaderFactory.getConfigLoader());
    }

    public static KylinConfig newKylinConfig(IExternalConfigLoader configLoader) {
        try {
            KylinConfig config = new KylinConfig(configLoader);
            config.reloadKylinConfig(new Properties());
            logger.trace("Created a new KylinConfig by newKylinConfig, KylinConfig Id: {}",
                    System.identityHashCode(config));
            return config;
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Failed to find KylinConfig ", e);
        }
    }

    // Only used in test cases!!!
    public static void setKylinConfigForLocalTest(String localMetaDir) {
        synchronized (KylinConfig.class) {
            if (!new File(localMetaDir, "kylin.properties").exists()) {
                throw new IllegalArgumentException(localMetaDir + " is not a valid local meta dir");
            }

            destroyInstance();
            logger.info("Setting KylinConfig to " + localMetaDir);
            Unsafe.setProperty(KylinConfig.KYLIN_CONF, localMetaDir);

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            config.setMetadataUrl(localMetaDir + "/metadata");

            // make sure a local working directory
            File workingDir = new File(localMetaDir, "working-dir");
            workingDir.mkdirs();
            String path = workingDir.getAbsolutePath();
            if (!path.startsWith("/")) {
                path = "/" + path;
            }
            if (!path.endsWith("/")) {
                path = path + "/";
            }
            path = path.replace("\\", "/");
            config.setProperty("kylin.env.hdfs-working-dir", "file:" + path);
        }
    }

    @VisibleForTesting
    public static void destroyInstance() {
        synchronized (KylinConfig.class) {
            logger.info("Destroy KylinConfig");
            SYS_ENV_INSTANCE = null;
            THREAD_ENV_INSTANCE = new ThreadLocal<>();
        }
    }

    public enum UriType {
        PROPERTIES_FILE, REST_ADDR, LOCAL_FOLDER, HDFS_FILE
    }

    public static UriType decideUriType(String metaUri) {

        try {
            File file = new File(metaUri);
            if (file.exists() || metaUri.contains("/")) {
                if (!file.exists()) {
                    file.mkdirs();
                }
                if (file.isDirectory()) {
                    return UriType.LOCAL_FOLDER;
                } else if (file.isFile()) {
                    if (file.getName().equalsIgnoreCase(KYLIN_CONF_PROPERTIES_FILE)) {
                        return UriType.PROPERTIES_FILE;
                    } else {
                        throw new IllegalStateException(
                                "Metadata uri : " + metaUri + " is a local file but not kylin.properties");
                    }
                } else {
                    throw new IllegalStateException(
                            "Metadata uri : " + metaUri + " looks like a file but it's neither a file nor a directory");
                }
            }
            throw new IllegalStateException("Metadata uri : " + metaUri + " is not recognized");
        } catch (Exception e) {
            throw new IllegalStateException("Metadata uri : " + metaUri + " is not recognized", e);
        }
    }

    public static KylinConfig createInstanceFromUri(String uri) {
        /**
         * --hbase:
         *
         * 1. PROPERTIES_FILE: path to kylin.properties
         * --local:
         *
         * 1.  LOCAL_FOLDER: path to resource folder
         */
        UriType uriType = decideUriType(uri);

        if (uriType == UriType.LOCAL_FOLDER) {
            KylinConfig config = new KylinConfig();
            config.setMetadataUrl(uri);
            return config;
        } else if (uriType == UriType.PROPERTIES_FILE) {
            KylinConfig config;
            try {
                config = new KylinConfig();
                InputStream is = new FileInputStream(uri);
                Properties prop = streamToTrimProps(is);
                config.reloadKylinConfig(prop);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return config;
        }
        throw new RuntimeException("not implement");
    }

    public static Properties streamToTrimProps(InputStream is) throws IOException {
        Properties originProps = streamToProps(is);
        Properties trimProps = new Properties();

        originProps.forEach(
                (k, v) -> trimProps.put(StringUtils.trim(String.valueOf(k)), StringUtils.trim(String.valueOf(v))));
        return originProps;
    }

    public static Properties streamToProps(InputStream is) throws IOException {
        Properties prop = new Properties();
        prop.load(is);
        IOUtils.closeQuietly(is);
        return prop;
    }

    /**
     * trim all kv from map
     * @param originMap
     * @return linkedHashMap
     */
    public static LinkedHashMap<String, String> trimKVFromMap(@Nullable Map<String, String> originMap) {
        LinkedHashMap<String, String> newMap = Maps.newLinkedHashMap();
        if (MapUtils.isEmpty(originMap) || originMap == null) {
            return newMap;
        }
        originMap.forEach((k, v) -> newMap.put(StringUtils.trim(k), StringUtils.trim(v)));

        return newMap;
    }

    public static void setKylinConfigInEnvIfMissing(Properties prop) {
        synchronized (KylinConfig.class) {
            if (SYS_ENV_INSTANCE == null) {
                try {
                    KylinConfig config = new KylinConfig();
                    config.reloadKylinConfig(prop);
                    logger.info("Resetting SYS_ENV_INSTANCE by a input stream: " + System.identityHashCode(config));
                    SYS_ENV_INSTANCE = config;
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("Failed to find KylinConfig ", e);
                }
            }
        }
    }

    public static void setKylinConfigInEnvIfMissing(String propsInStr) throws IOException {
        Properties props = new Properties();
        props.load(new StringReader(propsInStr));
        setKylinConfigInEnvIfMissing(props);
    }

    /**
     * @deprecated use SetAndUnsetThreadLocalConfig instead.
     */
    @Deprecated
    public static void setKylinConfigThreadLocal(KylinConfig config) {
        if (THREAD_ENV_INSTANCE.get() != null) {
            logger.warn("current thread already has a thread local KylinConfig, existing: {}, new: {}",
                    THREAD_ENV_INSTANCE.get(), config);
        } else {
            logger.info("current thread local KylinConfig is set to: {}", config);
        }

        THREAD_ENV_INSTANCE.set(config);
    }

    public static boolean isKylinConfigThreadLocal() {
        return THREAD_ENV_INSTANCE.get() != null;
    }

    public static KylinConfig createKylinConfig(String propsInStr) throws IOException {
        Properties props = new Properties();
        props.load(new StringReader(propsInStr));
        return createKylinConfig(props);
    }

    public static KylinConfig createKylinConfig(KylinConfig another) {
        return createKylinConfig(another.getRawAllProperties());
    }

    /** If use createKylinConfig to create a new config,remember to release this config */
    public static KylinConfig createKylinConfig(Properties prop) {
        KylinConfig kylinConfig = new KylinConfig();
        kylinConfig.reloadKylinConfig(prop);
        return kylinConfig;
    }

    public static File getKylinConfDir() {
        return getSitePropertiesFile().getParentFile();
    }

    // should be private; package visible for test only
    static File getSitePropertiesFile() {
        String kylinConfHome = getKylinConfHome();
        if (!StringUtils.isEmpty(kylinConfHome)) {
            logger.info("Use KYLIN_CONF=" + kylinConfHome);
            return existFile(kylinConfHome);
        }

        logger.debug("KYLIN_CONF property was not set, will seek KYLIN_HOME env variable");

        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome)) {
            throw new KylinConfigCannotInitException("Didn't find KYLIN_CONF or KYLIN_HOME, please set one of them");
        }

        String path = kylinHome + File.separator + "conf";
        return existFile(path);
    }

    /**
     * Return a File only if it exists
     */
    private static File existFile(String path) {
        if (path == null) {
            return null;
        }

        return new File(path, KYLIN_CONF_PROPERTIES_FILE);
    }

    // build kylin properties from site deployment, a.k.a KYLIN_HOME/conf/kylin.properties
    public static Properties buildSiteProperties() {
        Properties conf = new Properties();

        OrderedProperties orderedProperties = buildSiteOrderedProps();
        for (Map.Entry<String, String> each : orderedProperties.entrySet()) {
            conf.put(each.getKey(), each.getValue());
        }

        return conf;
    }

    // build kylin properties from site deployment, a.k.a KYLIN_HOME/conf/kylin.properties
    private static OrderedProperties buildSiteOrderedProps() {
        String config;
        if (configLoader == null) {
            config = ShellKylinExternalConfigLoaderFactory.getConfigLoader().getConfig();
        } else {
            config = configLoader.getConfig();
        }
        StringReader reader = new StringReader(config);
        OrderedProperties orderedProperties = new OrderedProperties();
        try {
            orderedProperties.load(reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return orderedProperties;
    }

    public static KylinConfig loadKylinConfigFromHdfs(String uri) {
        if (uri == null)
            throw new IllegalArgumentException("StorageUrl should not be null");
        if (!uri.contains("@hdfs"))
            throw new IllegalArgumentException("StorageUrl should like @hdfs schema");
        logger.info("Ready to load KylinConfig from uri: {}", uri);
        StorageURL url = StorageURL.valueOf(uri);
        String metaDir = url.getParameter("path") + "/" + KYLIN_CONF_PROPERTIES_FILE;
        Path path = new Path(metaDir);
        try (InputStream is = path.getFileSystem(HadoopUtil.getCurrentConfiguration()).open(new Path(metaDir))) {
            Properties prop = KylinConfig.streamToProps(is);
            return KylinConfig.createKylinConfig(prop);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // ============================================================================

    private final Singletons singletons;

    private KylinConfig() {
        this(null);
    }

    private KylinConfig(IExternalConfigLoader configLoader) {
        super(configLoader);
        this.singletons = new Singletons();
        logger.trace("a new KylinConfig is created with id: {}", System.identityHashCode(this));
    }

    protected KylinConfig(Properties props, boolean force) {
        super(props, force, null);
        this.singletons = new Singletons();
        logger.trace("a new KylinConfig is created with id: {}", System.identityHashCode(this));
    }

    public <T> T getManager(Class<T> clz) {
        return getManager(null, clz, null);
    }

    public <T1, T2> T1 getManager(Class<T1> managerClass, Class<T2> entityClass) {
        return getManager(null, managerClass, entityClass);
    }

    public <T> T getManager(String project, Class<T> clz) {
        return getManager(project, clz, null);
    }

    public <T1, T2> T1 getManager(@Nullable String project, Class<T1> managerClass, @Nullable Class<T2> entityClass) {
        KylinConfig base = base();
        if (base != this)
            return base.getManager(project, managerClass, entityClass);
        Singletons.Creator<T1> creator = clazz -> {
            if (entityClass == null) {
                if (StringUtils.isEmpty(project)) {
                    Method method = clazz.getDeclaredMethod("newInstance", KylinConfig.class);
                    Unsafe.changeAccessibleObject(method, true);// override accessibility
                    return (T1) method.invoke(null, this);
                } else {
                    Method method = clazz.getDeclaredMethod("newInstance", KylinConfig.class, String.class);
                    Unsafe.changeAccessibleObject(method, true);// override accessibility
                    return (T1) method.invoke(null, this, project);
                }
            } else {
                Method method = clazz.getDeclaredMethod("newInstance", Class.class, KylinConfig.class, String.class);
                Unsafe.changeAccessibleObject(method, true);// override accessibility
                return (T1) method.invoke(null, entityClass, this, project);
            }
        };
        if (StringUtils.isEmpty(project)) {
            return singletons.getInstance0(managerClass, entityClass, creator);
        } else {
            return singletons.getInstance0(project, managerClass, entityClass, creator);
        }
    }

    public void clearManagers() {
        KylinConfig base = base();
        if (base != this) {
            base.clearManagers();
            return;
        }
        singletons.clear();
    }

    public void clearManagersByProject(String project) {
        KylinConfig base = base();
        if (base != this) {
            base.clearManagersByProject(project);
            return;
        }
        singletons.clearByProject(project);
    }

    public void clearManagersByClz(Class clz) {
        KylinConfig base = base();
        if (base != this) {
            base.clearManagersByClz(clz);
            return;
        }
        singletons.clearByType(clz);
    }

    public Properties exportToProperties() {
        Properties all = getAllProperties();
        Properties copy = new Properties();
        copy.putAll(all);
        return copy;
    }

    public String exportAllToString() throws IOException {
        final Properties allProps = getProperties(null);
        final OrderedProperties orderedProperties = KylinConfig.buildSiteOrderedProps();

        for (Map.Entry<Object, Object> entry : allProps.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            if (!orderedProperties.containsProperty(key)) {
                orderedProperties.setProperty(key, value);
            } else if (!orderedProperties.getProperty(key).equalsIgnoreCase(value)) {
                orderedProperties.setProperty(key, value);
            }
        }

        final StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : orderedProperties.entrySet()) {
            sb.append(entry.getKey() + "=" + entry.getValue()).append('\n');
        }
        return sb.toString();

    }

    public String exportToString() throws IOException {
        return exportToString(null); // null means to export all
    }

    public String exportToString(Collection<String> propertyKeys) throws IOException {
        Properties filteredProps = getProperties(propertyKeys);
        OrderedProperties orderedProperties = KylinConfig.buildSiteOrderedProps();

        if (propertyKeys != null) {
            for (String key : propertyKeys) {
                if (!filteredProps.containsKey(key)) {
                    filteredProps.put(key, orderedProperties.getProperty(key, ""));
                }
            }
        }

        final StringBuilder sb = new StringBuilder();
        for (Map.Entry<Object, Object> entry : filteredProps.entrySet()) {
            sb.append(entry.getKey() + "=" + entry.getValue()).append('\n');
        }
        return sb.toString();
    }

    public void exportToFile(File file) throws IOException {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            getAllProperties().store(fos, file.getAbsolutePath());
        } finally {
            IOUtils.closeQuietly(fos);
        }
    }

    public boolean isSystemConfig() {
        return SYS_ENV_INSTANCE == this;
    }

    public void reloadKylinConfigPropertiesFromSiteProperties() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            final Properties newProperties = reloadKylinConfig2Properties(buildSiteProperties());
            this.cachedHdfsWorkingDirectory = null;
            this.properties.reloadProperties(newProperties);
        } finally {
            lock.unlock();
        }
    }

    public Properties reloadKylinConfig2Properties(Properties properties) {
        Properties result = BCC.check(properties);
        result.setProperty(BCC.check("kylin.metadata.url.identifier"), getMetadataUrlPrefixFromProperties(properties));
        result.setProperty(BCC.check("kylin.metadata.url.unique-id"), getMetadataUrlUniqueIdFromProperties(properties));
        result.setProperty(BCC.check("kylin.log.spark-executor-properties-file"), getLogSparkExecutorPropertiesFile());
        result.setProperty(BCC.check("kylin.log.spark-driver-properties-file"), getLogSparkDriverPropertiesFile());
        result.setProperty(BCC.check("kylin.log.spark-appmaster-properties-file"),
                getLogSparkAppMasterPropertiesFile());

        result.put(WORKING_DIR_PROP,
                makeQualified(new Path(result.getProperty(WORKING_DIR_PROP, KYLIN_ROOT))).toString());
        if (result.getProperty(DATA_WORKING_DIR_PROP) != null) {
            result.put(DATA_WORKING_DIR_PROP,
                    makeQualified(new Path(result.getProperty(DATA_WORKING_DIR_PROP))).toString());
        }
        if (result.getProperty(WRITING_CLUSTER_WORKING_DIR) != null) {
            result.put(WRITING_CLUSTER_WORKING_DIR,
                    makeQualified(new Path(result.getProperty(WRITING_CLUSTER_WORKING_DIR))).toString());
        }
        return result;
    }

    public String getMetadataUrlPrefixFromProperties(Properties properties) {
        return getMetadataUrlFromProperties(properties).getIdentifier();
    }

    public StorageURL getMetadataUrlFromProperties(Properties properties) {
        return StorageURL.valueOf(getOptionalFromProperties("kylin.metadata.url", "kylin_metadata@jdbc", properties));
    }

    public String getOptionalFromProperties(String prop, String dft, Properties properties) {
        final String property = SystemPropertiesCache.getProperty(prop);
        return property != null ? getSubstitutor().replace(property)
                : getSubstitutor().replace(properties.getProperty(prop, dft));
    }

    public String getChannelFromProperties(Properties properties) {
        return getOptionalFromProperties("kylin.env.channel", "on-premises", properties);
    }

    public String getMetadataUrlUniqueIdFromProperties(Properties properties) {
        if (KapConfig.CHANNEL_CLOUD.equalsIgnoreCase(getChannelFromProperties(properties))) {
            return getMetadataUrlPrefixFromProperties(properties);
        }
        StorageURL url = getMetadataUrlFromProperties(properties);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(url.getIdentifier());
        Optional.ofNullable(url.getParameter("url")).map(value -> value.split("\\?")[0]).map(value -> "_" + value)
                .ifPresent(stringBuilder::append);
        String instanceId = stringBuilder.toString().replaceAll("\\W", "_");
        return instanceId;
    }

    public String getHdfsWorkingDirectoryFromProperties(Properties properties) {
        String root = getOptionalFromProperties(DATA_WORKING_DIR_PROP, null, properties);
        boolean compriseMetaId = false;

        if (root == null) {
            root = getOptionalFromProperties(WORKING_DIR_PROP, KYLIN_ROOT, properties);
            compriseMetaId = true;
        }

        Path path = new Path(root);
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("kylin.env.hdfs-working-dir must be absolute, but got " + root);
        }

        // make sure path is qualified
        path = makeQualified(path);

        if (compriseMetaId) {
            // if configuration WORKING_DIR_PROP_V2 dose not exist, append metadata-url prefix
            String metaId = getMetadataUrlPrefixFromProperties(properties).replace(':', '-').replace('/', '-');
            path = new Path(path, metaId);
        }

        root = path.toString();
        if (!root.endsWith("/")) {
            root += "/";
        }

        String hdfsWorkingDirectory = root;
        if (hdfsWorkingDirectory.startsWith("file:")) {
            hdfsWorkingDirectory = hdfsWorkingDirectory.replace("file:", "file://");
        } else if (hdfsWorkingDirectory.startsWith("maprfs:")) {
            hdfsWorkingDirectory = hdfsWorkingDirectory.replace("maprfs:", "maprfs://");
        }
        logger.info("Hdfs data working dir is {} in properties", hdfsWorkingDirectory);
        return hdfsWorkingDirectory;
    }

    public KylinConfig base() {
        return this;
    }

    private int superHashCode() {
        return super.hashCode();
    }

    @Override
    public int hashCode() {
        return base().superHashCode();
    }

    @Override
    public boolean equals(Object another) {
        if (!(another instanceof KylinConfig))
            return false;
        else
            return this.base() == ((KylinConfig) another).base();
    }

    public String toString() {
        return "KylinConfig " + System.identityHashCode(this) + " with base id: " + System.identityHashCode(base());
    }

    public static SetAndUnsetThreadLocalConfig setAndUnsetThreadLocalConfig(KylinConfig config) {
        return new SetAndUnsetThreadLocalConfig(config);
    }

    public static class SetAndUnsetThreadLocalConfig implements AutoCloseable {
        private KylinConfig originThreadLocalConfig = null;

        private SetAndUnsetThreadLocalConfig(KylinConfig config) {
            originThreadLocalConfig = THREAD_ENV_INSTANCE.get();
            if (originThreadLocalConfig != null) {
                logger.warn("KylinConfig already hosts thread local instance {}, will be overwritten by {}",
                        originThreadLocalConfig, config);
            }
            THREAD_ENV_INSTANCE.set(config);
        }

        public KylinConfig get() {
            Preconditions.checkNotNull(THREAD_ENV_INSTANCE.get(),
                    "KylinConfig thread local instance is already closed");
            return THREAD_ENV_INSTANCE.get();
        }

        public KylinConfig getOriginConfig() {
            return originThreadLocalConfig == null ? SYS_ENV_INSTANCE : originThreadLocalConfig;
        }

        @Override
        public void close() {
            THREAD_ENV_INSTANCE.remove();
            if (originThreadLocalConfig != null) {
                THREAD_ENV_INSTANCE.set(originThreadLocalConfig);
            }
        }
    }

    public static boolean useLegacyConfig() {
        return Objects.equals(TRUE, System.getenv(USE_LEGACY_CONFIG));
    }

}
