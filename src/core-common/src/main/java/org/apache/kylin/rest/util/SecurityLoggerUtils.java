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

package org.apache.kylin.rest.util;

import java.util.Locale;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.util.AddressUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecurityLoggerUtils {
    public static final String SECURITY_LOG_APPENDER = "security";
    private static final Logger securityLogger = LoggerFactory.getLogger(SecurityLoggerUtils.SECURITY_LOG_APPENDER);
    private static final String LOGIN = "[Operation: login] user:%s, login time:%s, success:%s, ip and port:%s";
    private static final String LOGOUT = "[Operation: log out] user:%s, logout time:%s, ip and port:%s";
    private SecurityLoggerUtils() {
    }

    public static void recordLoginSuccess(String username) {
        String loginSuccessMsg = String.format(Locale.ROOT, SecurityLoggerUtils.LOGIN, username,
                DateFormat.formatToTimeWithoutMilliStr(System.currentTimeMillis()), Boolean.TRUE,
                AddressUtil.getLocalInstance());
        try (SetLogCategory logCategory = new SetLogCategory(SecurityLoggerUtils.SECURITY_LOG_APPENDER)) {
            securityLogger.info(loginSuccessMsg);
        }
    }

    public static void recordLoginFailed(String username, Exception e) {
        String loginErrorMsg = String.format(Locale.ROOT, SecurityLoggerUtils.LOGIN, username,
                DateFormat.formatToTimeWithoutMilliStr(System.currentTimeMillis()), Boolean.FALSE,
                AddressUtil.getLocalInstance());
        try (SetLogCategory logCategory = new SetLogCategory(SecurityLoggerUtils.SECURITY_LOG_APPENDER)) {
            securityLogger.error(loginErrorMsg, e);
        }
    }

    public static void recordLogout(String username) {
        String logoutMessage = String.format(Locale.ROOT, SecurityLoggerUtils.LOGOUT, username,
                DateFormat.formatToTimeWithoutMilliStr(System.currentTimeMillis()), AddressUtil.getLocalInstance());
        try (SetLogCategory logCategory = new SetLogCategory(SecurityLoggerUtils.SECURITY_LOG_APPENDER)) {
            securityLogger.info(logoutMessage);
        }
    }
}
