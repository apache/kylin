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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import com.google.common.collect.Lists;

public class HiveCmdBuilder {
    private static final Logger logger = LoggerFactory.getLogger(HiveCmdBuilder.class);

    public static final String HIVE_CONF_FILENAME = "kylin_hive_conf";

    public enum HiveClientMode {
        CLI, BEELINE
    }

    private HiveClientMode clientMode;
    private KylinConfig kylinConfig;
    final private Map<String, String> hiveConfProps = new HashMap<>();
    final private ArrayList<String> statements = Lists.newArrayList();

    public HiveCmdBuilder() {
        kylinConfig = KylinConfig.getInstanceFromEnv();
        clientMode = HiveClientMode.valueOf(kylinConfig.getHiveClientMode().toUpperCase());
        loadHiveConfiguration();
    }

    public String build() {
        StringBuffer buf = new StringBuffer();

        switch (clientMode) {
        case CLI:
            buf.append("hive -e \"");
            for (String statement : statements) {
                buf.append(statement).append("\n");
            }
            buf.append("\"");
            buf.append(parseProps());
            break;
        case BEELINE:
            BufferedWriter bw = null;
            File tmpHql = null;
            try {
                tmpHql = File.createTempFile("beeline_", ".hql");
                bw = new BufferedWriter(new FileWriter(tmpHql));
                for (String statement : statements) {
                    bw.write(statement);
                    bw.newLine();
                }
                buf.append("beeline ");
                buf.append(kylinConfig.getHiveBeelineParams());
                buf.append(parseProps());
                buf.append(" -f ");
                buf.append(tmpHql.getAbsolutePath());
                buf.append(";ret_code=$?;rm -f ");
                buf.append(tmpHql.getAbsolutePath());
                buf.append(";exit $ret_code");

            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                IOUtils.closeQuietly(bw);

                if (tmpHql != null && logger.isDebugEnabled()) {
                    String hql = null;
                    try {
                        hql = FileUtils.readFileToString(tmpHql, Charset.defaultCharset());
                    } catch (IOException e) {
                        // ignore
                    }
                    logger.debug("The SQL to execute in beeline: \n" + hql);
                }
            }
            break;
        default:
            throw new RuntimeException("Hive client cannot be recognized: " + clientMode);
        }

        return buf.toString();
    }

    private String parseProps() {
        StringBuilder s = new StringBuilder();
        for (Map.Entry<String, String> prop : hiveConfProps.entrySet()) {
            s.append(" --hiveconf ");
            s.append(prop.getKey());
            s.append("=");
            s.append(prop.getValue());
        }
        return s.toString();
    }

    public void reset() {
        statements.clear();
        hiveConfProps.clear();
    }

    public void setHiveConfProps(Map<String, String> hiveConfProps) {
        this.hiveConfProps.clear();
        this.hiveConfProps.putAll(hiveConfProps);
    }

    public void overwriteHiveProps(Map<String, String> overwrites) {
        this.hiveConfProps.putAll(overwrites);
    }

    public void addStatement(String statement) {
        statements.add(statement);
    }

    public void addStatementWithRedistributeBy(String statement) {
        StringBuilder builder = new StringBuilder();
        builder.append(statement);
        addStatementWithRedistributeBy(builder);
    }

    public void addStatementWithRedistributeBy(StringBuilder statement) {
        /**
         * When hive.execution.engine is tez and table is a view of union-all struct, it generates
         * subdirectories in output, which causes file not found exception.
         * Use "DISTRIBUTE BY RAND()" to workaround this issue.
         */
        statement.append("DISTRIBUTE BY RAND()").append(";\n");
        statements.add(statement.toString());
    }

    public void addStatements(String[] stats) {
        for (String s : stats) {
            statements.add(s);
        }
    }

    @Override
    public String toString() {
        return build();
    }

    private void loadHiveConfiguration() {

        File hiveConfFile;
        String hiveConfFileName = (HIVE_CONF_FILENAME + ".xml");
        String path = System.getProperty(KylinConfig.KYLIN_CONF);

        if (StringUtils.isNotEmpty(path)) {
            hiveConfFile = new File(path, hiveConfFileName);
        } else {
            path = KylinConfig.getKylinHome();
            if (StringUtils.isEmpty(path)) {
                logger.error("KYLIN_HOME is not set, can not locate hive conf: {}.xml", HIVE_CONF_FILENAME);
                return;
            }
            hiveConfFile = new File(path + File.separator + "conf", hiveConfFileName);
        }

        if (hiveConfFile == null || !hiveConfFile.exists()) {
            throw new RuntimeException("Failed to read " + HIVE_CONF_FILENAME + ".xml");
        }

        String fileUrl = OptionsHelper.convertToFileURL(hiveConfFile.getAbsolutePath());

        try {
            File file = new File(fileUrl);
            if (file.exists()) {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document doc = builder.parse(file);
                NodeList nl = doc.getElementsByTagName("property");
                hiveConfProps.clear();
                for (int i = 0; i < nl.getLength(); i++) {
                    String key = doc.getElementsByTagName("name").item(i).getFirstChild().getNodeValue();
                    String value = doc.getElementsByTagName("value").item(i).getFirstChild().getNodeValue();
                    if (!key.equals("tmpjars")) {
                        hiveConfProps.put(key, value);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse hive conf file ", e);
        }
    }
}