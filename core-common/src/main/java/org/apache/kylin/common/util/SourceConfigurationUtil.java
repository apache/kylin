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

package org.apache.kylin.common.util;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

/**
 * @author ycq
 * @since 2018-03-05
 */
public class SourceConfigurationUtil {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SourceConfigurationUtil.class);
    private static final String HIVE_CONF_PREFIX = "hiveconf:";

    private SourceConfigurationUtil() {
        throw new IllegalStateException("Class SourceConfigurationUtil is an utility class !");
    }

    public static final String HIVE_CONF_FILENAME = "kylin_hive_conf";
    public static final String SQOOP_CONF_FILENAME = "kylin_sqoop_conf";

    public static Properties loadHiveJDBCProperties() {
        Map<String, String> hiveConfiguration = loadHiveConfiguration();
        Properties ret = new Properties();
        for (Map.Entry<String, String> entry : hiveConfiguration.entrySet()) {
            ret.put(HIVE_CONF_PREFIX + entry.getKey(), entry.getValue());
        }
        Map<String, String> overrideConf = KylinConfig.getInstanceFromEnv().getHiveConfigOverride();
        for (Map.Entry<String, String> entry : overrideConf.entrySet()) {
            ret.put(HIVE_CONF_PREFIX + entry.getKey(), entry.getValue());
        }
        return ret;
    }

    public static Map<String, String> loadHiveConfiguration() {
        return loadXmlConfiguration(HIVE_CONF_FILENAME, true);
    }

    public static Map<String, String> loadSqoopConfiguration() {
        return loadXmlConfiguration(SQOOP_CONF_FILENAME, false);
    }

    private static Map<String, String> loadXmlConfiguration(String filename, boolean checkExist) {
        Map<String, String> confProps = new HashMap<>();
        File confFile = null;
        String xmlFileName = filename + ".xml";
        String path = System.getProperty(KylinConfig.KYLIN_CONF);

        if (StringUtils.isNotEmpty(path)) {
            confFile = new File(path, xmlFileName);
            if (!confFile.exists() && path.contains("meta")) {
                confFile = null;
            }
        }

        if (confFile == null) {
            path = KylinConfig.getKylinHome();
            if (StringUtils.isEmpty(path)) {
                logger.error("KYLIN_HOME is not set, can not locate conf: {}", xmlFileName);
                return confProps;
            }
            confFile = new File(path + File.separator + "conf", xmlFileName);
            System.setProperty(KylinConfig.KYLIN_CONF, path + File.separator + "conf");
        }

        if (!confFile.exists()) {
            if (checkExist)
                throw new RuntimeException(
                        "Failed to read " + xmlFileName + " at '" + confFile.getAbsolutePath() + "'");
            else
                return confProps;
        }

        String fileUrl = OptionsHelper.convertToFileURL(confFile.getAbsolutePath());

        try {
            File file = new File(fileUrl);
            if (file.exists()) {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document doc = builder.parse(file);
                NodeList nl = doc.getElementsByTagName("property");
                confProps.clear();
                for (int i = 0; i < nl.getLength(); i++) {
                    String key = doc.getElementsByTagName("name").item(i).getFirstChild().getNodeValue();
                    String value = doc.getElementsByTagName("value").item(i).getFirstChild().getNodeValue();
                    if (!key.equals("tmpjars")) {
                        confProps.put(key, value);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse hive conf file ", e);
        }
        return confProps;
    }

}
