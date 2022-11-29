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
package org.apache.kylin.rest.security;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.rest.util.SecurityLoggerUtils;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.session.web.http.SaveSessionException;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class LoginLogFilter extends OncePerRequestFilter {

    @Getter
    private ThreadLocal<LoginInfo> loginInfoThreadLocal = new ThreadLocal<>();

    @Override
    protected void doFilterInternal(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse,
            FilterChain filterChain) throws ServletException, IOException {
        try {
            filterChain.doFilter(httpServletRequest, httpServletResponse);
        } catch (SaveSessionException e) {
            LoginInfo loginInfo = getLoginInfoThreadLocal().get();
            if (null == loginInfo) {
                loginInfo = new LoginInfo();
                loginInfo.setUserName("anonymous");
                getLoginInfoThreadLocal().set(loginInfo);
            }
            loginInfo.setLoginSuccess(Boolean.FALSE);
            loginInfo.setException(e);
            throw e;
        } finally {
            LoginInfo loginInfo = getLoginInfoThreadLocal().get();
            if (null != loginInfo) {
                try {
                    if (loginInfo.getLoginSuccess()) {
                        SecurityLoggerUtils.recordLoginSuccess(loginInfo.getUserName());
                    } else {
                        SecurityLoggerUtils.recordLoginFailed(loginInfo.getUserName(), loginInfo.getException());
                    }
                } catch (Exception e) {
                    logger.error("Failed to log the login status!", e);
                } finally {
                    getLoginInfoThreadLocal().remove();
                }
            }
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class LoginInfo {
        private String userName;
        private Boolean loginSuccess;
        private Exception exception;
    }
}
