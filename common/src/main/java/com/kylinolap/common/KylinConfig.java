/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.common;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.restclient.RestClient;
import com.kylinolap.common.util.CliCommandExecutor;

/**
 * @author yangli9
 */
public class KylinConfig {

    public static final String KYLIN_STORAGE_URL = "kylin.storage.url";

    public static final String PROP_SCAN_THRESHOLD = "PROP_SCAN_THRESHOLD";

    public static final String KYLIN_METADATA_URL = "kylin.metadata.url";

    public static final String KYLIN_REST_SERVERS = "kylin.rest.servers";

    public static final String KYLIN_REST_TIMEZONE = "kylin.rest.timezone";
    /**
     * The dir containing scripts for kylin. For example: /usr/lib/kylin/bin
     */
    public static final String KYLIN_SCRIPT_DIR = "kylin.script.dir";
    /**
     * The script file name for generating table metadat from hive. For example:
     * generateTable.sh
     */
    public static final String KYLIN_SCRIPT_GEN_TABLE_META = "kylin.script.genTableMeta";

    public static final String KYLIN_JOB_CONCURRENT_MAX_LIMIT = "kylin.job.concurrent.max.limit";

    public static final String KYLIN_JOB_YARN_APP_REST_CHECK_STATUS_URL = "kylin.job.yarn.app.rest.check.status.url";

    public static final String KYLIN_JOB_YARN_APP_REST_CHECK_INTERVAL_SECONDS = "kylin.job.yarn.app.rest.check.interval.seconds";

    public static final String KYLIN_TMP_HDFS_DIR = "kylin.tmp.hdfs.dir";

    public static final String HIVE_TABLE_LOCATION_PREFIX = "hive.table.location.";

    public static final String KYLIN_JOB_REMOTE_CLI_PASSWORD = "kylin.job.remote.cli.password";

    public static final String KYLIN_JOB_REMOTE_CLI_USERNAME = "kylin.job.remote.cli.username";

    public static final String KYLIN_JOB_REMOTE_CLI_HOSTNAME = "kylin.job.remote.cli.hostname";

    public static final String KYLIN_JOB_REMOTE_CLI_WORKING_DIR = "kylin.job.remote.cli.working.dir";
    
    public static final String KYLIN_JOB_CMD_EXTRA_ARGS = "kylin.job.cmd.extra.args";
    /**
     * Toggle to indicate whether to use hive for table flattening. Default
     * true.
     */
    public static final String KYLIN_JOB_HIVE_FLATTEN = "kylin.job.hive.flatten";

    public static final String KYLIN_JOB_RUN_AS_REMOTE_CMD = "kylin.job.run.as.remote.cmd";

    public static final String KYLIN_JOB_MAPREDUCE_DEFAULT_REDUCE_COUNT_RATIO = "kylin.job.mapreduce.default.reduce.count.ratio";

    public static final String KYLIN_JOB_MAPREDUCE_DEFAULT_REDUCE_INPUT_MB = "kylin.job.mapreduce.default.reduce.input.mb";

    public static final String KYLIN_JOB_MAPREDUCE_MAX_REDUCER_NUMBER = "kylin.job.mapreduce.max.reducer.number";

    public static final String KYLIN_JOB_JAR = "kylin.job.jar";

    public static final String COPROCESSOR_LOCAL_JAR = "kylin.coprocessor.local.jar";
    public static final String COPROCESSOR_SCAN_BITS_THRESHOLD = "kylin.coprocessor.scan.bits.threshold";

    public static final String KYLIN_JOB_JAR_LOCAL = "kylin.job.jar.local";

    public static final String KYLIN_JOB_LOG_DIR = "kylin.job.log.dir";

    public static final String KYLIN_HDFS_WORKING_DIR = "kylin.hdfs.working.dir";

    public static final String HIVE_PASSWORD = "hive.password";

    public static final String HIVE_USER = "hive.user";

    public static final String HIVE_URL = "hive.url";
    /**
     * Key string to point to the kylin conf directory
     */
    public static final String KYLIN_CONF = "KYLIN_CONF";
    /**
     * Key string to specify the kylin evn: prod, dev, qa
     */
    public static final String KYLIN_ENV = "KYLIN_ENV";
    /**
     * Default Kylin conf path
     */
    public static final String KYLIN_CONF_DEFAULT = "/etc/kylin";
    /**
     * Kylin properties file
     */
    public static final String KYLIN_CONF_PROPERTIES_FILE = "kylin.properties";

    public static final String MAIL_ENABLED = "mail.enabled";
    
    public static final String MAIL_HOST = "mail.host";
    
    public static final String MAIL_USERNAME = "mail.username";
    
    public static final String MAIL_PASSWORD = "mail.password";
    
    public static final String MAIL_SENDER = "mail.sender";

    private static final Logger logger = LoggerFactory.getLogger(KylinConfig.class);

    // static cached instances
    private static KylinConfig ENV_INSTANCE = null;

    public static KylinConfig getInstanceFromEnv() {
        if (ENV_INSTANCE == null) {
            try {
                KylinConfig config = loadKylinConfig();
                ENV_INSTANCE = config;
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("Failed to find KylinConfig ", e);
            }
        }
        return ENV_INSTANCE;
    }

    public static void destoryInstance() {
        ENV_INSTANCE = null;
    }


    /**
     * This method only for test case. You can get a KylinConfig instance by
     * path "/a/b/c", where "/a/b/c/kylin.properties" exists. By default, the
     * getInstanceFromEnv() should be called.
     *
     * @param confPath
     * @return
     * @deprecated
     */
    public static KylinConfig getInstanceForTest(String confPath) {
        File file = new File(confPath);
        if (!file.exists() || !file.isDirectory()) {
            throw new IllegalArgumentException(confPath + " is not a valid path");
        }

        String env = System.getProperty(KYLIN_CONF);
        System.setProperty(KYLIN_CONF, confPath);
        KylinConfig config = getInstanceFromEnv();
        if (env == null) {
            System.clearProperty(KYLIN_CONF);
        } else {
            System.setProperty(KYLIN_CONF, env);
        }
        return config;
    }

    public static enum UriType {
        PROPERTIES_FILE, REST_ADDR, LOCAL_FOLDER
    }

    private static UriType decideUriType(String metaUri) {

        try {
            File file = new File(metaUri);
            if (file.exists() || metaUri.contains("/")) {
                if (file.exists() == false) {
                    file.mkdirs();
                }
                if (file.isDirectory()) {
                    return UriType.LOCAL_FOLDER;
                } else if (file.isFile()) {
                    if (file.getName().equalsIgnoreCase(KYLIN_CONF_PROPERTIES_FILE)) {
                        return UriType.PROPERTIES_FILE;
                    } else {
                        throw new IllegalStateException("Metadata uri : " + metaUri + " is a local file but not kylin.properties");
                    }
                } else {
                    throw new IllegalStateException("Metadata uri : " + metaUri + " looks like a file but it's neither a file nor a directory");
                }
            } else {
                if (RestClient.matchFullRestPattern(metaUri))
                    return UriType.REST_ADDR;
                else
                    throw new IllegalStateException("Metadata uri : " + metaUri + " is not a valid REST URI address");
            }
        } catch (Exception e) {
            logger.info(e.getLocalizedMessage());
            throw new IllegalStateException("Metadata uri : " + metaUri + " is not recognized");
        }
    }

    public static KylinConfig createInstanceFromUri(String uri) {
        /**
         * --hbase: 1. PROPERTIES_FILE: path to kylin.properties 2. REST_ADDR:
         * rest service resource, format: user:password@host:port --local: 1.
         * LOCAL_FOLDER: path to resource folder
         */
        UriType uriType = decideUriType(uri);
        logger.info("The URI " + uri + " is recognized as " + uriType);

        if (uriType == UriType.LOCAL_FOLDER) {
            KylinConfig config = new KylinConfig();
            config.setMetadataUrl(uri);
            return config;
        } else if (uriType == UriType.PROPERTIES_FILE) {
            KylinConfig config;
            try {
                config = new KylinConfig();
                InputStream is = new FileInputStream(uri);
                config.reloadKylinConfig(is);
                is.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return config;
        } else {// rest_addr
            try {
                KylinConfig config = new KylinConfig();
                RestClient client = new RestClient(uri);
                String propertyText = client.getKylinProperties();
                InputStream is = IOUtils.toInputStream(propertyText);
                config.reloadKylinConfig(is);
                is.close();
                return config;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static KylinConfig getKylinConfigFromInputStream(InputStream is) {
        KylinConfig config = new KylinConfig();
        config.reloadKylinConfig(is);
        return config;
    }

    // ============================================================================

    /**
     * Find config from environment. The Search process: 1. Check the
     * $KYLIN_CONF/kylin.properties 2. Check the /etc/kylin/kylin.properties 3.
     * Check the kylin.properties in classpath
     *
     * @return
     */
    private static KylinConfig loadKylinConfig() {
        InputStream is = getKylinPropertiesAsInputSteam();
        if (is == null) {
            throw new IllegalArgumentException("Failed to load kylin config");
        }
        KylinConfig config = new KylinConfig();
        config.reloadKylinConfig(is);
        return config;
    }

    private PropertiesConfiguration kylinConfig = new PropertiesConfiguration();

    public CliCommandExecutor getCliCommandExecutor() throws IOException {
        CliCommandExecutor exec = new CliCommandExecutor();
        if (getRunAsRemoteCommand()) {
            exec.setRunAtRemote(getRemoteHadoopCliHostname(), getRemoteHadoopCliUsername(), getRemoteHadoopCliPassword());
        }
        return exec;
    }

    // ============================================================================

    public String getStorageUrl() {
        return getOptional(KYLIN_STORAGE_URL);
    }

    public String getHiveUrl() {
        return getOptional(HIVE_URL, "");
    }

    public String getHiveUser() {
        return getOptional(HIVE_USER, "");
    }

    public String getHivePassword() {
        return getOptional(HIVE_PASSWORD, "");
    }

    public String getHdfsWorkingDirectory() {
        return getRequired(KYLIN_HDFS_WORKING_DIR);
    }

    public String getKylinJobLogDir() {
        return getOptional(KYLIN_JOB_LOG_DIR, "/tmp/kylin/logs");
    }

    public String getKylinJobJarPath() {
        return getRequired(KYLIN_JOB_JAR);
    }
    
    public void overrideKylinJobJarPath(String path) {
        kylinConfig.setProperty(KYLIN_JOB_JAR, path);
    }

    public String getCoprocessorLocalJar() {
        return getRequired(COPROCESSOR_LOCAL_JAR);
    }

    public void overrideCoprocessorLocalJar(String path) {
        kylinConfig.setProperty(COPROCESSOR_LOCAL_JAR, path);
    }

    public int getCoprocessorScanBitsThreshold() {
        return Integer.parseInt(getOptional(COPROCESSOR_SCAN_BITS_THRESHOLD, "32"));
    }

    public double getDefaultHadoopJobReducerInputMB() {
        return Double.parseDouble(getOptional(KYLIN_JOB_MAPREDUCE_DEFAULT_REDUCE_INPUT_MB, "500"));
    }

    public double getDefaultHadoopJobReducerCountRatio() {
        return Double.parseDouble(getOptional(KYLIN_JOB_MAPREDUCE_DEFAULT_REDUCE_COUNT_RATIO, "1.0"));
    }

    public int getHadoopJobMaxReducerNumber() {
        return Integer.parseInt(getOptional(KYLIN_JOB_MAPREDUCE_MAX_REDUCER_NUMBER, "5000"));
    }

    public boolean getRunAsRemoteCommand() {
        return Boolean.parseBoolean(getOptional(KYLIN_JOB_RUN_AS_REMOTE_CMD));
    }

    public String getRemoteHadoopCliHostname() {
        return getOptional(KYLIN_JOB_REMOTE_CLI_HOSTNAME);
    }

    public String getRemoteHadoopCliUsername() {
        return getOptional(KYLIN_JOB_REMOTE_CLI_USERNAME);
    }

    public String getRemoteHadoopCliPassword() {
        return getOptional(KYLIN_JOB_REMOTE_CLI_PASSWORD);
    }

    public String getCliWorkingDir() {
        return getOptional(KYLIN_JOB_REMOTE_CLI_WORKING_DIR);
    }

    public String getMapReduceCmdExtraArgs() {
        return getOptional(KYLIN_JOB_CMD_EXTRA_ARGS);
    }

    public boolean getFlatTableByHive() {
        return Boolean.parseBoolean(getOptional(KYLIN_JOB_HIVE_FLATTEN, "true"));
    }

    public String getOverrideHiveTableLocation(String table) {
        return getOptional(HIVE_TABLE_LOCATION_PREFIX + table.toUpperCase());
    }

    public String getTempHDFSDir() {
        return getOptional(KYLIN_TMP_HDFS_DIR, "/tmp/kylin");
    }

    public String getYarnStatusServiceUrl() {
        return getOptional(KYLIN_JOB_YARN_APP_REST_CHECK_STATUS_URL, null);
    }

    public int getYarnStatusCheckIntervalSeconds() {
        return Integer.parseInt(getOptional(KYLIN_JOB_YARN_APP_REST_CHECK_INTERVAL_SECONDS, "60"));
    }

    public int getMaxConcurrentJobLimit() {
        return Integer.parseInt(getOptional(KYLIN_JOB_CONCURRENT_MAX_LIMIT, "10"));
    }

    public String getTimeZone() {
        return getOptional(KYLIN_REST_TIMEZONE, "PST");
    }

    public String[] getRestServers() {
        String nodes = getOptional(KYLIN_REST_SERVERS);
        if (nodes == null)
            return null;
        return nodes.split("\\s*,\\s*");
    }

    public String getAdminDls() {
        return getOptional("kylin.job.admin.dls", null);
    }

    public long getJobStepTimeout() {
        return Long.parseLong(getOptional("kylin.job.step.timeout", String.valueOf(2 * 60 * 60)));
    }

    public String getServerMode() {
        return this.getOptional("kylin.server.mode", "all");
    }
    
    public int getDictionaryMaxCardinality() {
        return Integer.parseInt(getOptional("kylin.dictionary.max.cardinality", "5000000"));
    }

    public int getScanThreshold() {
        return Integer.parseInt(getOptional("kylin.query.scan.threshold", "10000000"));
    }

    public Long getQueryDurationCacheThreshold() {
        return Long.parseLong(this.getOptional("kylin.query.cache.threshold.duration", String.valueOf(2000)));
    }

    public Long getQueryScanCountCacheThreshold() {
        return Long.parseLong(this.getOptional("kylin.query.cache.threshold.scancount", String.valueOf(10 * 1024)));
    }

    public boolean isQuerySecureEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.security.enabled", "false"));
    }

    public int getConcurrentScanThreadCount() {
        return Integer.parseInt(this.getOptional("kylin.query.scan.thread.count", "40"));
    }

    public boolean isQueryCacheEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.cache.enabled", "true"));
    }

    public int getHBaseKeyValueSize() {
        return Integer.parseInt(this.getOptional("kylin.hbase.client.keyvalue.maxsize", "10485760"));
    }

    private String getOptional(String prop) {
        return kylinConfig.getString(prop);
    }

    private String getOptional(String prop, String dft) {
        return kylinConfig.getString(prop, dft);
    }

    private String getRequired(String prop) {
        String r = kylinConfig.getString(prop);
        if (StringUtils.isEmpty(r))
            throw new IllegalArgumentException("missing '" + prop + "' in conf/kylin_instance.properties");
        return r;
    }

    void reloadKylinConfig(InputStream is) {
        PropertiesConfiguration config = new PropertiesConfiguration();
        try {
            config.load(is);
        } catch (ConfigurationException e) {
            throw new RuntimeException("Cannot load kylin config.", e);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                logger.error("Failed to close inputstream.", e);
            }
        }
        this.kylinConfig = config;
    }

    public void writeProperties(File file) throws IOException {
        try {
            kylinConfig.save(file);
        } catch (ConfigurationException ex) {
            throw new IOException("Error writing KylinConfig to " + file, ex);
        }
    }

    public static InputStream getKylinPropertiesAsInputSteam() {
        File propFile = null;
        
        // 1st, find conf path from env
        String path = System.getProperty(KYLIN_CONF);
        if (path == null) {
            path = System.getenv(KYLIN_CONF);
        }
        propFile = getKylinPropertiesFile(path);
        
        // 2nd, find /etc/kylin
        if (propFile == null) {
            propFile = getKylinPropertiesFile(KYLIN_CONF_DEFAULT);
        }
        if (propFile != null) {
            logger.debug("Loading property file " + propFile.getAbsolutePath());
            try {
                return new FileInputStream(propFile);
            } catch (FileNotFoundException e) {
                logger.warn("Failed to read properties " + propFile.getAbsolutePath() + " and skip");
            }
        }
        
        // 3rd, find classpath
        logger.info("Search " + KYLIN_CONF_PROPERTIES_FILE + " from classpath ...");
        InputStream is = KylinConfig.class.getClassLoader().getResourceAsStream("kylin.properties");
        if (is == null) {
            logger.info("Did not find properties file " + KYLIN_CONF_PROPERTIES_FILE + " from classpath");
        }
        return is;
    }

    /**
     * Check if there is kylin.properties exist
     *
     *
     * @param path
     * @param env
     * @return the properties file
     */
    private static File getKylinPropertiesFile(String path) {
        if (path == null)
            return null;
        
        File propFile = new File(path, KYLIN_CONF_PROPERTIES_FILE);
        if (propFile.exists()) {
            logger.info(KYLIN_CONF_PROPERTIES_FILE + " was found at " + propFile.getAbsolutePath());
            return propFile;
        }
        
        logger.info(KYLIN_CONF_PROPERTIES_FILE + " was NOT found at " + propFile.getAbsolutePath());
        return null;
    }

    public String getMetadataUrl() {
        return getOptional(KYLIN_METADATA_URL);
    }

    public String getMetadataUrlPrefix() {
        String hbaseMetadataUrl = getMetadataUrl();
        String defaultPrefix = "kylin_metadata";

        if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(hbaseMetadataUrl, "hbase:")) {
            int cut = hbaseMetadataUrl.indexOf('@');
            String tmp = cut < 0 ? defaultPrefix : hbaseMetadataUrl.substring(0, cut);
            return tmp;
        } else {
            return defaultPrefix;
        }
    }

    public void setMetadataUrl(String metadataUrl) {
        kylinConfig.setProperty(KYLIN_METADATA_URL, metadataUrl);
    }

    /**
     * return -1 if there is no setting
     *
     * @return
     */
    public int getPropScanThreshold() {
        return kylinConfig.getInt(PROP_SCAN_THRESHOLD, -1);
    }

    public String getProperty(String key, String defaultValue) {
        return kylinConfig.getString(key, defaultValue);
    }

    /**
     * Set a new key:value into the kylin config.
     *
     * @param key
     * @param value
     */
    public void setProperty(String key, String value) {
        logger.info("Kylin Config was updated with " + key + " : " + value);
        kylinConfig.setProperty(key, value);
    }

    public String getConfigAsString() throws IOException {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            kylinConfig.save(baos);
            String content = baos.toString();
            return content;
        } catch (ConfigurationException ex) {
            throw new IOException("Error writing KylinConfig to String", ex);
        }
    }

    public String toString() {
        return getMetadataUrl();
    }

}
