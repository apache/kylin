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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import com.google.common.collect.Lists;

public class HiveCmdBuilder {
    public static final Logger logger = LoggerFactory.getLogger(HiveCmdBuilder.class);

    public static final String HIVE_CONF_FILENAME = "kylin_hive_conf";
    static final String CREATE_HQL_TMP_FILE_TEMPLATE = "cat >%s<<EOL\n%sEOL";

    public enum HiveClientMode {
        CLI, BEELINE
    }

    private KylinConfig kylinConfig;
    final private Map<String, String> hiveConfProps = new HashMap<>();
    final private ArrayList<String> statements = Lists.newArrayList();

    public HiveCmdBuilder() {
        kylinConfig = KylinConfig.getInstanceFromEnv();
        loadHiveConfiguration();
    }

    public String build() {
        HiveClientMode clientMode = HiveClientMode.valueOf(kylinConfig.getHiveClientMode().toUpperCase());
        String beelineShell = kylinConfig.getHiveBeelineShell();
        String beelineParams = kylinConfig.getHiveBeelineParams();
        if (kylinConfig.getEnableSparkSqlForTableOps()) {
            clientMode = HiveClientMode.BEELINE;
            beelineShell = kylinConfig.getSparkSqlBeelineShell();
            beelineParams = kylinConfig.getSparkSqlBeelineParams();
            if (StringUtils.isBlank(beelineShell)) {
                throw new IllegalStateException(
                        "Missing config 'kylin.source.hive.sparksql-beeline-shell', please check kylin.properties");
            }
        }

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
            String tmpHqlPath = null;
            StringBuilder hql = new StringBuilder();
            try {
                tmpHqlPath = "/tmp/" + System.currentTimeMillis() + ".hql";
                for (String statement : statements) {
                    hql.append(statement);
                    hql.append("\n");
                }
                String createFileCmd = String.format(CREATE_HQL_TMP_FILE_TEMPLATE, tmpHqlPath, hql);
                buf.append(createFileCmd);
                buf.append("\n");
                buf.append(beelineShell);
                buf.append(" ");
                buf.append(beelineParams);
                buf.append(parseProps());
                buf.append(" -f ");
                buf.append(tmpHqlPath);
                buf.append(";ret_code=$?;rm -f ");
                buf.append(tmpHqlPath);
                buf.append(";exit $ret_code");
            } finally {
                if (tmpHqlPath != null && logger.isDebugEnabled()) {
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

        if (!hiveConfFile.exists()) {
            throw new RuntimeException("Missing config file: " + hiveConfFile.getAbsolutePath());
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