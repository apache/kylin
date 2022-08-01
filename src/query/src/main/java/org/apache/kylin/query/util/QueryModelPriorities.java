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

package org.apache.kylin.query.util;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryModelPriorities {

    private QueryModelPriorities() {
    }

    private static final Pattern MODEL_PRIORITY_PATTERN = Pattern.compile("SELECT\\W+/\\*\\+\\W*([^*/]+)\\*/");

    private static String getHint(String sql) {
        Matcher matcher = MODEL_PRIORITY_PATTERN.matcher(sql.toUpperCase(Locale.ROOT));
        if (matcher.find()) {
            return matcher.group(1).trim();
        } else {
            return "";
        }
    }

    public static String[] getModelPrioritiesFromComment(String sql) {
        String[] models = doGetModelPrioritiesFromComment(sql);
        if (models.length > 0) {
            return models;
        }
        // for backward compatibility with KE3
        return loadCubePriorityFromComment(sql);
    }

    static String[] doGetModelPrioritiesFromComment(String sql) {
        String hint = getHint(sql).toUpperCase(Locale.ROOT);
        if (hint.isEmpty() || hint.indexOf("MODEL_PRIORITY(") != 0) {
            return new String[0];
        }

        String[] modelHints = hint.replace("MODEL_PRIORITY(", "").replace(")", "").split(",");
        for (int i = 0; i < modelHints.length; i++) {
            modelHints[i] = modelHints[i].trim();
        }
        return modelHints;
    }

    // for backward compatibility with KE3
    private static final Pattern CUBE_PRIORITY_PATTERN = Pattern
            .compile("(?<=--(\\s){0,2}CubePriority\\().*(?=\\)(\\s)*[\r\n])");

    static String[] loadCubePriorityFromComment(String sql) {
        // get CubePriority From Comment
        Matcher matcher = CUBE_PRIORITY_PATTERN.matcher(sql + "\n");
        if (matcher.find()) {
            String cubeNames = matcher.group().trim().toUpperCase(Locale.ROOT);
            return cubeNames.split(",");
        } else {
            return new String[0];
        }
    }
}
