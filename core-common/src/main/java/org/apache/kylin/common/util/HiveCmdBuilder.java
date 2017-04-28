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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class HiveCmdBuilder {
    private static final Logger logger = LoggerFactory.getLogger(HiveCmdBuilder.class);

    public enum HiveClientMode {
        CLI, BEELINE
    }

    private HiveClientMode clientMode;
    private KylinConfig kylinConfig;
    final private ArrayList<String> statements = Lists.newArrayList();

    public HiveCmdBuilder() {
        kylinConfig = KylinConfig.getInstanceFromEnv();
        clientMode = HiveClientMode.valueOf(kylinConfig.getHiveClientMode().toUpperCase());
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

    public void reset() {
        statements.clear();
    }

    public void addStatement(String statement) {
        statements.add(statement);
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