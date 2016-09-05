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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.ClassUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class KylinConfig extends KylinConfigBase {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(KylinConfig.class);


    /** Kylin properties file name */
    public static final String KYLIN_CONF_PROPERTIES_FILE = "kylin.properties";
    public static final String KYLIN_ACCOUNT_CONF_PROPERTIES_FILE = "kylin_account.properties";
    public static final String KYLIN_CONF = "KYLIN_CONF";

    // static cached instances
    private static KylinConfig ENV_INSTANCE = null;

    public static KylinConfig getInstanceFromEnv() {
        synchronized (KylinConfig.class) {
            if (ENV_INSTANCE == null) {
                try {
                    KylinConfig config = new KylinConfig();
                    config.reloadKylinConfig(getKylinProperties());

                    logger.info("Initialized a new KylinConfig from getInstanceFromEnv : " + System.identityHashCode(config));
                    ENV_INSTANCE = config;
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("Failed to find KylinConfig ", e);
                }
            }
            return ENV_INSTANCE;
        }
    }

    //Only used in test cases!!! 
    public static void destroyInstance() {
        logger.info("Destory KylinConfig");
        dumpStackTrace();
        ENV_INSTANCE = null;
    }

    public enum UriType {
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
            throw new IllegalStateException("Metadata uri : " + metaUri + " is not recognized", e);
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
                Properties prop = streamToProps(is);
                config.reloadKylinConfig(prop);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return config;
        } else {// rest_addr
            try {
                KylinConfig config = new KylinConfig();
                RestClient client = new RestClient(uri);
                String propertyText = client.getKylinProperties();
                InputStream is = IOUtils.toInputStream(propertyText, Charset.defaultCharset());
                Properties prop = streamToProps(is);
                config.reloadKylinConfig(prop);
                return config;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static Properties streamToProps(InputStream is) throws IOException {
        Properties prop = new Properties();
        prop.load(is);
        IOUtils.closeQuietly(is);
        return prop;
    }

    public static void setKylinConfigInEnvIfMissing(Properties prop) {
        synchronized (KylinConfig.class) {
            if (ENV_INSTANCE == null) {
                try {
                    KylinConfig config = new KylinConfig();
                    config.reloadKylinConfig(prop);
                    logger.info("Resetting ENV_INSTANCE by a input stream: " + System.identityHashCode(config));
                    ENV_INSTANCE = config;
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("Failed to find KylinConfig ", e);
                }
            }
        }
    }

    public static KylinConfig createKylinConfig(String propsInStr) throws IOException {
        Properties props = new Properties();
        props.load(new StringReader(propsInStr));
        return createKylinConfig(props);
    }
    
    public static KylinConfig createKylinConfig(KylinConfig another) {
        return createKylinConfig(another.getAllProperties());
    }
    
    public static KylinConfig createKylinConfig(Properties prop) {
        KylinConfig kylinConfig = new KylinConfig();
        kylinConfig.reloadKylinConfig(prop);
        return kylinConfig;
    }

    static File getKylinPropertiesFile() {
        String kylinConfHome = System.getProperty(KYLIN_CONF);
        if (!StringUtils.isEmpty(kylinConfHome)) {
            logger.info("Use KYLIN_CONF=" + kylinConfHome);
            return getKylinPropertiesFile(kylinConfHome);
        }

        logger.warn("KYLIN_CONF property was not set, will seek KYLIN_HOME env variable");

        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome))
            throw new KylinConfigCannotInitException("Didn't find KYLIN_CONF or KYLIN_HOME, please set one of them");

        String path = kylinHome + File.separator + "conf";
        return getKylinPropertiesFile(path);
    }

    static File getKylinAccountPropertiesFile() {
        String kylinConfHome = System.getProperty(KYLIN_CONF);
        if (!StringUtils.isEmpty(kylinConfHome)) {
            logger.info("Use KYLIN_CONF=" + kylinConfHome);
            return getKylinAccountPropertiesFile(kylinConfHome);
        }

        logger.warn("KYLIN_CONF property was not set, will seek KYLIN_HOME env variable");

        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome))
            throw new KylinConfigCannotInitException("Didn't find KYLIN_CONF or KYLIN_HOME, please set one of them");

        String path = kylinHome + File.separator + "conf";
        return getKylinAccountPropertiesFile(path);
    }

    public static Properties getKylinProperties() {
        File propFile = getKylinPropertiesFile();
        if (propFile == null || !propFile.exists()) {
            logger.error("fail to locate " + KYLIN_CONF_PROPERTIES_FILE);
            throw new RuntimeException("fail to locate " + KYLIN_CONF_PROPERTIES_FILE);
        }
        Properties conf = new Properties();
        try {
            FileInputStream is = new FileInputStream(propFile);
            conf.load(is);
            IOUtils.closeQuietly(is);

            File propOverrideFile = new File(propFile.getParentFile(), propFile.getName() + ".override");
            if (propOverrideFile.exists()) {
                FileInputStream ois = new FileInputStream(propOverrideFile);
                Properties propOverride = new Properties();
                propOverride.load(ois);
                IOUtils.closeQuietly(ois);
                conf.putAll(propOverride);
            }

            File accountPropFile = getKylinAccountPropertiesFile();
            if (accountPropFile.exists()) {
                FileInputStream ois = new FileInputStream(accountPropFile);
                Properties propAccount = new Properties();
                propAccount.load(ois);
                IOUtils.closeQuietly(ois);
                conf.putAll(propAccount);
            }

            File accountPropOverrideFile = new File(accountPropFile.getParentFile(), accountPropFile.getName() + ".override");
            if (accountPropOverrideFile.exists()) {
                FileInputStream ois = new FileInputStream(accountPropOverrideFile);
                Properties propAccountOverride = new Properties();
                propAccountOverride.load(ois);
                IOUtils.closeQuietly(ois);
                conf.putAll(propAccountOverride);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return conf;
    }

    /**
     * Check if there is kylin.properties exist
     *
     * @param path
     * @return the properties file
     */
    private static File getKylinPropertiesFile(String path) {
        if (path == null) {
            return null;
        }

        return new File(path, KYLIN_CONF_PROPERTIES_FILE);
    }

    private static File getKylinAccountPropertiesFile(String path) {
        if (path == null) {
            return null;
        }

        return new File(path, KYLIN_ACCOUNT_CONF_PROPERTIES_FILE);
    }

    public static void setSandboxEnvIfPossible() {
        File dir1 = new File("../examples/test_case_data/sandbox");
        File dir2 = new File("../../kylin/examples/test_case_data/sandbox");

        if (dir1.exists()) {
            logger.info("Setting sandbox env, KYLIN_CONF=" + dir1.getAbsolutePath());
            ClassUtil.addClasspath(dir1.getAbsolutePath());
            System.setProperty(KylinConfig.KYLIN_CONF, dir1.getAbsolutePath());
        } else if (dir2.exists()) {
            logger.info("Setting sandbox env, KYLIN_CONF=" + dir2.getAbsolutePath());
            ClassUtil.addClasspath(dir2.getAbsolutePath());
            System.setProperty(KylinConfig.KYLIN_CONF, dir2.getAbsolutePath());
        }
    }

    // ============================================================================

    private KylinConfig() {
        super();
        logger.info("New KylinConfig " + System.identityHashCode(this));
        KylinConfig.dumpStackTrace();
    }

    protected KylinConfig(Properties props) {
        super(props);
    }

    public void writeProperties(File file) throws IOException {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            getAllProperties().store(fos, file.getAbsolutePath());
        } finally {
            IOUtils.closeQuietly(fos);
        }
    }

    public String getConfigAsString() throws IOException {
        final StringWriter stringWriter = new StringWriter();
        list(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }

    private void list(PrintWriter out) {
        Properties props = getAllProperties();
        for (Enumeration<?> e = props.keys(); e.hasMoreElements();) {
            String key = (String) e.nextElement();
            String val = (String) props.get(key);
            out.println(key + "=" + val);
        }
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

    public static void writeOverrideProperties(Properties properties) throws IOException {
        File propFile = getKylinPropertiesFile();
        File overrideFile = new File(propFile.getParentFile(), propFile.getName() + ".override");
        overrideFile.createNewFile();
        FileInputStream fis2 = null;
        Properties override = new Properties();
        try {
            fis2 = new FileInputStream(overrideFile);
            override.load(fis2);
            for (Map.Entry<Object, Object> entries : properties.entrySet()) {
                override.setProperty(entries.getKey().toString(), entries.getValue().toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(fis2);
        }

        PrintWriter pw = null;
        try {
            pw = new PrintWriter(overrideFile);
            Enumeration<?> e = override.propertyNames();
            while (e.hasMoreElements()) {
                String key = (String) e.nextElement();
                pw.println(key + "=" + override.getProperty(key));
            }
            pw.close();
        } finally {
            IOUtils.closeQuietly(pw);
        }

    }

    private static void dumpStackTrace() {

        //uncomment below to start debugging

        //        Thread t = Thread.currentThread();
        //        int maxStackTraceDepth = 20;
        //        int current = 0;
        //
        //        StackTraceElement[] stackTrace = t.getStackTrace();
        //        StringBuilder buf = new StringBuilder("This is not a exception, just for diagnose purpose:");
        //        buf.append("\n");
        //        for (StackTraceElement e : stackTrace) {
        //            if (++current > maxStackTraceDepth) {
        //                break;
        //            }
        //            buf.append("\t").append("at ").append(e.toString()).append("\n");
        //        }
        //        logger.info(buf.toString());
    }
}
