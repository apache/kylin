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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class HiveCmdBuilder {
    public static final Logger logger = LoggerFactory.getLogger(HiveCmdBuilder.class);

    static final String CREATE_HQL_TMP_FILE_TEMPLATE = "cat >%s<<EOL\n%sEOL";

    public enum HiveClientMode {
        CLI, BEELINE
    }

    private KylinConfig kylinConfig;
    private final Map<String, String> hiveConfProps;
    private final List<String> statements = Lists.newArrayList();

    public HiveCmdBuilder() {
        this("");
    }

    public HiveCmdBuilder(String jobName) {
        kylinConfig = KylinConfig.getInstanceFromEnv();
        hiveConfProps = SourceConfigurationUtil.loadHiveConfiguration();
        hiveConfProps.putAll(kylinConfig.getHiveConfigOverride());
        if (StringUtils.isNotEmpty(jobName)) {
            addStatement("set mapred.job.name='" + jobName + "';");
        }
    }

    public String build() {
        HiveClientMode clientMode = HiveClientMode.valueOf(kylinConfig.getHiveClientMode().toUpperCase(Locale.ROOT));
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

        StringBuilder buf = new StringBuilder();

        switch (clientMode) {
        case CLI:
            buf.append("hive -e \"");
            for (String statement : statements) {
                //in bash need escape " and ` by using \
                buf.append(statement.replaceAll("`", "\\\\`")).append("\n");
            }
            buf.append("\"");
            buf.append(parseProps());
            break;
        case BEELINE:
            String tmpHqlPath = null;
            StringBuilder hql = new StringBuilder();
            try {
                tmpHqlPath = "/tmp/" + UUID.randomUUID().toString() + ".hql";
                for (String statement : statements) {
                    hql.append(statement.replaceAll("`", "\\\\`"));
                    hql.append("\n");
                }
                String createFileCmd = String.format(Locale.ROOT, CREATE_HQL_TMP_FILE_TEMPLATE, tmpHqlPath, hql);
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
                    logger.debug("The SQL to execute in beeline: {} \n", hql);
                }
            }
            break;
        default:
            throw new IllegalArgumentException("Hive client cannot be recognized: " + clientMode);
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

    public List<String> getStatements() {
        return statements;
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

}
