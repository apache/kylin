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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryModelPriorities {

    private static final Logger log = LoggerFactory.getLogger(QueryModelPriorities.class);
    private static final Pattern MODEL_PRIORITY_PATTERN = Pattern.compile("MODEL_PRIORITY\\([^()]*\\)");

    private QueryModelPriorities() {
    }

    public static String[] getModelPrioritiesFromComment(String sql) {
        String[] models = new String[0];
        try {
            RawSql rawSql = new RawSqlParser(sql).parse();
            models = getModelPrioritiesFromHintStr(rawSql.getFirstHintString());
        } catch (Throwable t) {
            log.error("Error on parse sql when invoking getModelPrioritiesFromComment", t);
        }

        if (models.length == 0) {
            // for backward compatibility with KE3
            models = loadCubePriorityFromComment(sql);
        }

        return models;
    }

    public static String[] getModelPrioritiesFromHintStrOrComment(String hintStr, String sql) {
        String[] models = getModelPrioritiesFromHintStr(hintStr);
        if (models.length == 0) {
            // for backward compatibility with KE3
            models = loadCubePriorityFromComment(sql);
        }
        return models;
    }

    public static String[] getModelPrioritiesFromHintStr(String hintStr) {
        if (StringUtils.isNotBlank(hintStr)) {
            Matcher matcher = MODEL_PRIORITY_PATTERN.matcher(hintStr);
            if (matcher.find(0)) {
                String modelPriorityHint = matcher.group().toUpperCase(Locale.ROOT);
                return extractModelPriorityModelNames(modelPriorityHint);
            }
        }
        return new String[0];
    }

    private static String[] extractModelPriorityModelNames(String modelPriorityHint) {
        if (StringUtils.isBlank(modelPriorityHint) || modelPriorityHint.indexOf("MODEL_PRIORITY(") != 0) {
            return new String[0];
        }
        String[] modelNames = modelPriorityHint.replace("MODEL_PRIORITY(", "").replace(")", "").split(",");
        for (int i = 0; i < modelNames.length; i++) {
            modelNames[i] = modelNames[i].trim();
        }
        return modelNames;
    }

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
