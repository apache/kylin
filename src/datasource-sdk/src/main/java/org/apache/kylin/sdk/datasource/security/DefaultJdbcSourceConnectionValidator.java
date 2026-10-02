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
package org.apache.kylin.sdk.datasource.security;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

public class DefaultJdbcSourceConnectionValidator extends AbstractJdbcSourceConnectionValidator {

    private static final String JDBC_PREFIX = "jdbc:";

    @Override
    public boolean isValid() {
        if (StringUtils.isBlank(url) || !url.startsWith(JDBC_PREFIX)) {
            return false;
        }

        try {
            String schemeSpecificUrl = url.substring(JDBC_PREFIX.length());
            int schemeEnd = schemeSpecificUrl.indexOf(':');
            if (schemeEnd <= 0 || StringUtils.isBlank(schemeSpecificUrl.substring(0, schemeEnd))) {
                return false;
            }

            int queryIndex = url.indexOf('?');
            String urlWithoutQuery = queryIndex < 0 ? url : url.substring(0, queryIndex);
            String query = queryIndex < 0 ? "" : url.substring(queryIndex + 1);
            if (queryIndex >= 0 && StringUtils.isBlank(query)) {
                return false;
            }
            if (query.contains(";") || url.indexOf('#') >= 0) {
                return false;
            }

            Set<String> queryKeys = parseKeys(query, '&');
            Set<String> semicolonKeys = parseSemicolonKeys(urlWithoutQuery);
            Set<String> parenthesisKeys = parseParenthesisKeys(urlWithoutQuery);
            return settings.getValidUrlParamKeys().containsAll(queryKeys)
                    && settings.getValidSemicolonParamKeys().containsAll(semicolonKeys)
                    && settings.getValidParenthesisParamKeys().containsAll(parenthesisKeys);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private Set<String> parseSemicolonKeys(String urlWithoutQuery) {
        int semicolonIndex = urlWithoutQuery.indexOf(';');
        if (semicolonIndex < 0) {
            return Collections.emptySet();
        }
        String semicolonContent = urlWithoutQuery.substring(semicolonIndex + 1);
        if (StringUtils.isBlank(semicolonContent)) {
            throw new IllegalArgumentException("empty semicolon parameters");
        }
        return parseKeys(semicolonContent, ';');
    }

    private Set<String> parseParenthesisKeys(String urlWithoutQuery) {
        if (urlWithoutQuery.indexOf('(') < 0) {
            if (urlWithoutQuery.indexOf(')') >= 0) {
                throw new IllegalArgumentException("unexpected closing parenthesis");
            }
            return Collections.emptySet();
        }

        Set<String> keys = new LinkedHashSet<>();
        int index = 0;
        while (index < urlWithoutQuery.length()) {
            int groupStart = urlWithoutQuery.indexOf('(', index);
            if (groupStart < 0) {
                break;
            }
            int groupEnd = urlWithoutQuery.indexOf(')', groupStart + 1);
            if (groupEnd < 0) {
                throw new IllegalArgumentException("unbalanced parenthesis");
            }
            String group = urlWithoutQuery.substring(groupStart + 1, groupEnd);
            if (group.indexOf('(') >= 0 || StringUtils.isBlank(group)) {
                throw new IllegalArgumentException("invalid parenthesis content");
            }
            for (String key : parseKeys(group, ',')) {
                if (!keys.add(key)) {
                    throw new IllegalArgumentException("duplicate parenthesis parameter");
                }
            }
            index = groupEnd + 1;
        }
        if (urlWithoutQuery.indexOf(')', index) >= 0) {
            throw new IllegalArgumentException("unexpected closing parenthesis");
        }
        return keys;
    }

    private Set<String> parseKeys(String content, char separator) {
        if (StringUtils.isBlank(content)) {
            return Collections.emptySet();
        }

        Set<String> keys = new LinkedHashSet<>();
        for (String segment : content.split(String.valueOf(separator), -1)) {
            if (StringUtils.isBlank(segment)) {
                throw new IllegalArgumentException("empty parameter");
            }
            int equalsIndex = segment.indexOf('=');
            String key = equalsIndex < 0 ? segment : segment.substring(0, equalsIndex);
            key = key.trim();
            if (StringUtils.isBlank(key) || !keys.add(key)) {
                throw new IllegalArgumentException("invalid parameter");
            }
        }
        return keys;
    }
}
