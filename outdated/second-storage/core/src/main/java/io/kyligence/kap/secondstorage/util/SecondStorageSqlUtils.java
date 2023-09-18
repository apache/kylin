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
package io.kyligence.kap.secondstorage.util;

import org.apache.commons.lang3.StringUtils;

public class SecondStorageSqlUtils {
    private static final String IF_NOT_EXISTS = "IF NOT EXISTS";
    public static String addIfNotExists(String sql, String type) {
        if (StringUtils.isBlank(sql)) {
            return sql;
        }
        int index = StringUtils.indexOfIgnoreCase(sql, type);
        if (index < 0) {
            return sql;
        }

        return sql.substring(0, index + type.length()) + " "
                + IF_NOT_EXISTS + sql.substring(index + type.length());
    }
}
