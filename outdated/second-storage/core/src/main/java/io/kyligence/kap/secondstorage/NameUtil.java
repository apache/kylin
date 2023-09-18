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
package io.kyligence.kap.secondstorage;

import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;

import org.apache.kylin.metadata.cube.model.NDataflow;

public class NameUtil {

    private static final int UUID_LENGTH = RandomUtil.randomUUIDStr().length();
    public static final String TEMP_TABLE_FLAG = "temp";
    public static final String TEMP_SRC_TABLE_FLAG = "src";

    private NameUtil() {
    }

    public static String getDatabase(NDataflow df) {
        return getDatabase(df.getConfig(), df.getProject());
    }

    private static String databasePrefix(KylinConfig config) {
        return config.isUTEnv() ? "UT" : config.getMetadataUrlPrefix();
    }

    public static String getDatabase(KylinConfig config, String project) {
        return String.format(Locale.ROOT, "%s_%s", databasePrefix(config), project);
    }

    public static String getTable(NDataflow df, long layoutId) {
        return getTable(df.getUuid(), layoutId);
    }

    public static String getTable(String modelId, long layoutId){
        return String.format(Locale.ROOT, "%s_%d", tablePrefix(modelId), layoutId);
    }

    public static String tablePrefix(String modelId) {
        return modelId.replace("-", "_");
    }

    // reverse
    public static String recoverProject(String database, KylinConfig config) {
        return database.substring(databasePrefix(config).length() + 1);
    }

    public static Pair<String, Long> recoverLayout(String table) {
        String model = table.substring(0, UUID_LENGTH).replace("_", "-");
        Long layout = Long.parseLong(table.substring(UUID_LENGTH + 1));
        return Pair.newPair(model, layout);
    }

    public static boolean isTempTable(String table) {
        return table.contains(TEMP_TABLE_FLAG) || table.contains(TEMP_SRC_TABLE_FLAG);
    }
}
