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

package org.apache.kylin.sdk.datasource.framework.utils;

import java.sql.Connection;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.sdk.datasource.security.JdbcSourceConnectionValidator;
import org.apache.kylin.sdk.datasource.security.JdbcSourceValidationSettings;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcUtils {

    public static final Pattern JDBC_SCHEMA_PATTERN = Pattern.compile("(?<=jdbc:)[\\w\\-]+(?=:)");

    @SneakyThrows
    public static boolean checkConnectionParameter(String driver, String url, String username, String password) {
        Properties connProp = new Properties();
        connProp.put("driverClassName", driver);
        connProp.put("url", url);
        connProp.put("username", username);
        connProp.put("password", password);

        if (KylinConfig.getInstanceFromEnv().isSourceJdbcWhiteListEnabled() && !validateUrlByWhiteList(url)) {
            log.warn("jdbc url white list check failed");
            return false;
        }

        try (val dataSource = BasicDataSourceFactory.createDataSource(connProp);
                Connection conn = dataSource.getConnection()) {
            return true;
        } catch (Exception e) {
            log.warn("jdbc connect check failed", e);
            return false;
        }
    }

    public static boolean validateUrlByWhiteList(String url) {
        try {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            String scheme = null;

            Matcher m = JDBC_SCHEMA_PATTERN.matcher(url);
            if (m.find()) {
                scheme = m.group();
            }
            if (StringUtils.isBlank(scheme) || !config.getSourceJdbcWhiteListSchemes().contains(scheme)) {
                return false;
            }

            JdbcSourceValidationSettings settings = JdbcSourceValidationSettings.builder()
                    .validUrlParamKeys(config.getSourceJdbcWhiteListUrlParamKeysByScheme(scheme)).build();

            JdbcSourceConnectionValidator validator = (JdbcSourceConnectionValidator) ClassUtil
                    .newInstance(config.getSourceJdbcWhiteListValidatorClassByScheme(scheme));

            return validator.settings(settings).url(url).isValid();
        } catch (Exception e) {
            log.error("Error on validate url", e);
            return false;
        }
    }
}
