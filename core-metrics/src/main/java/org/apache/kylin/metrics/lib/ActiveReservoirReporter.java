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

package org.apache.kylin.metrics.lib;

import java.io.Closeable;
import java.util.regex.Pattern;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;

import org.apache.kylin.shaded.com.google.common.base.Strings;

/**
 * ActiveReservoirReporter report metrics event via listener from ActiveReservoir
 */
public abstract class ActiveReservoirReporter implements Closeable {

    public static final String KYLIN_PREFIX = KylinConfig.getInstanceFromEnv().getKylinMetricsPrefix();

    public static Pair<String, String> getTableNameSplits(String tableName) {
        if (Strings.isNullOrEmpty(tableName)) {
            return null;
        }

        String[] splits = tableName.split(Pattern.quote("."));
        int i = 0;
        String database = splits.length == 1 ? KYLIN_PREFIX : splits[i++];
        String tableNameOnly = splits[i];
        return new Pair<>(database, tableNameOnly);
    }

    public static String getTableName(Pair<String, String> tableNameSplits) {
        return tableNameSplits.getFirst() + "." + tableNameSplits.getSecond();
    }

    public abstract void start();

    public abstract void stop();
}
