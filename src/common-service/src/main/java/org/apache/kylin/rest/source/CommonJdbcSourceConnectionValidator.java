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

package org.apache.kylin.rest.source;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.sdk.datasource.security.AbstractJdbcSourceConnectionValidator;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
public class CommonJdbcSourceConnectionValidator extends AbstractJdbcSourceConnectionValidator {

    private static final String JDBC_COLON = "jdbc:";

    private boolean parsed = false;
    private String scheme;
    private String host;
    private int port;
    private String path;
    private Map<String, List<String>> queryParams;

    @Override
    public boolean isValid() {
        if (!parsed) {
            try {
                parseUrl();
            } catch (Exception e) {
                log.error("Error on parseUrl", e);
                return false;
            }
        }
        // Only query param keys need to be validated currently
        return validateQueryParamKeys();
    }

    private boolean validateQueryParamKeys() {
        Set<String> validUrlParamKeys = settings.getValidUrlParamKeys();
        Set<String> userInputKeys = queryParams.keySet();
        return validUrlParamKeys.containsAll(userInputKeys);
    }

    private void parseUrl() {
        if (parsed) {
            return;
        }
        if (StringUtils.isBlank(url)) {
            throw new IllegalStateException("url cannot be empty");
        }
        if (!url.startsWith(JDBC_COLON)) {
            throw new IllegalStateException("url must start with " + JDBC_COLON);
        }

        String noPrefixUrl = url.substring(JDBC_COLON.length());
        UriComponents uri = UriComponentsBuilder.fromUriString(noPrefixUrl).build();
        scheme = uri.getScheme();
        host = uri.getHost();
        port = uri.getPort();
        path = uri.getPath();
        queryParams = uri.getQueryParams();

        parsed = true;
    }
}
