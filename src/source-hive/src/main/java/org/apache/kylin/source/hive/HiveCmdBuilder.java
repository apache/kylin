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

package org.apache.kylin.source.hive;

import java.util.ArrayList;
import java.util.Locale;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class HiveCmdBuilder {
    public static final Logger logger = LoggerFactory.getLogger(HiveCmdBuilder.class);

    static final String CREATE_HQL_TMP_FILE_TEMPLATE = "cat >%s<<EOL\n%sEOL";
    private final ArrayList<String> statements = Lists.newArrayList();
    private KylinConfig kylinConfig;

    public HiveCmdBuilder(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        if (null == this.kylinConfig) {
            logger.warn("KylinConfig can not be null, will try KylinConfig.getInstanceFromEnv().");
            this.kylinConfig = KylinConfig.getInstanceFromEnv();
        }
    }

    public String build() {
        String beelineParams = kylinConfig.getHiveBeelineParams();
        StringBuilder buf = new StringBuilder();
        String tmpBeelineHqlPath = null;
        StringBuilder beelineHql = new StringBuilder();
        try {
            tmpBeelineHqlPath = "/tmp/" + UUID.randomUUID().toString() + ".hql";
            for (String statement : statements) {
                beelineHql.append(statement.replace("`", "\\`"));
                beelineHql.append("\n");
            }
            String createFileCmd = String.format(Locale.ROOT, CREATE_HQL_TMP_FILE_TEMPLATE, tmpBeelineHqlPath,
                    beelineHql);
            buf.append(createFileCmd);
            buf.append("\n");
            buf.append("beeline");
            buf.append(" ");
            buf.append(beelineParams);
            buf.append(" -f ");
            buf.append(tmpBeelineHqlPath);
            buf.append(";ret_code=$?;rm -f ");
            buf.append(tmpBeelineHqlPath);
            buf.append(";exit $ret_code");
        } finally {
            if (tmpBeelineHqlPath != null) {
                logger.debug("The SQL to execute in beeline: \n {}", beelineHql);
            }
        }

        return buf.toString();
    }

    public void addStatement(String statement) {
        statements.add(statement);
    }

    @Override
    public String toString() {
        return build();
    }

}
