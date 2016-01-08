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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.Log4jConfigurer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
@SuppressWarnings("serial")
public class KylinConfig extends KylinConfigBase {

    private static final Logger logger = LoggerFactory.getLogger(KylinConfig.class);

    /** Kylin properties file name */
    public static final String KYLIN_CONF_PROPERTIES_FILE = "kylin.properties";
    public static final String KYLIN_CONF = "KYLIN_CONF";

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

    private static File getKylinProperties() {
        String kylinConfHome = System.getProperty(KYLIN_CONF);
        if (!StringUtils.isEmpty(kylinConfHome)) {
            logger.info("Use KYLIN_CONF=" + kylinConfHome);
            return getKylinPropertiesFile(kylinConfHome);
        }

        logger.warn("KYLIN_CONF property was not set, will seek KYLIN_HOME env variable");

        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome))
            throw new RuntimeException("Didn't find KYLIN_CONF or KYLIN_HOME, please set one of them");

        String path = kylinHome + File.separator + "conf";
        return getKylinPropertiesFile(path);

    }

    public static InputStream getKylinPropertiesAsInputSteam() {
        File propFile = getKylinProperties();
        if (propFile == null || !propFile.exists()) {
            logger.error("fail to locate kylin.properties");
            throw new RuntimeException("fail to locate kylin.properties");
        }

        File overrideFile = new File(propFile.getParentFile(), propFile.getName() + ".override");
        if (overrideFile.exists()) {
            FileInputStream fis = null;
            FileInputStream fis2 = null;
            try {
                fis = new FileInputStream(propFile);
                fis2 = new FileInputStream(overrideFile);
                Properties conf = new Properties();
                conf.load(fis);
                Properties override = new Properties();
                override.load(fis2);
                for (Map.Entry<Object, Object> entries : override.entrySet()) {
                    conf.setProperty(entries.getKey().toString(), entries.getValue().toString());
                }
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                conf.store(bout, "output");
                return new ByteArrayInputStream(bout.toByteArray());
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                IOUtils.closeQuietly(fis);
                IOUtils.closeQuietly(fis2);
            }
        } else {
            try {
                return new FileInputStream(propFile);
            } catch (FileNotFoundException e) {
                logger.error("this should not happen");
                throw new RuntimeException(e);
            }
        }

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

    /**
     * Find config from environment. The Search process: 1. Check the
     * $KYLIN_CONF/kylin.properties 2. Check the $KYLIN_HOME/conf/kylin.properties
     */
    private static KylinConfig loadKylinConfig() {
        Log4jConfigurer.initLogger();

        InputStream is = getKylinPropertiesAsInputSteam();
        if (is == null) {
            throw new IllegalArgumentException("Failed to load kylin config");
        }
        KylinConfig config = new KylinConfig();
        config.reloadKylinConfig(is);

        return config;
    }

    // ============================================================================

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

}
