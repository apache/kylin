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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.rest.util.SecurityLoggerUtils;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Aspect
@Component
public class SecurityLogAspect {

    @Autowired
    private LoginLogFilter loginLogFilter;

    @AfterReturning("(execution(* org.apache.kylin.rest.security.LimitLoginAuthenticationProvider.authenticate(..)) "
            + "|| execution(* org.apache.kylin.rest.security.OpenAuthenticationProvider.authenticate(..))"
            + "|| execution(* org.apache.kylin.rest.security.LdapAuthenticationProvider.authenticate(..))) "
            + "&& args(authentication)")
    public void doAfterLoginSuccess(Authentication authentication) {
        if (null == loginLogFilter.getLoginInfoThreadLocal().get()) {
            loginLogFilter.getLoginInfoThreadLocal().set(new LoginLogFilter.LoginInfo());
        }

        loginLogFilter.getLoginInfoThreadLocal().get().setUserName(authentication.getName());
        loginLogFilter.getLoginInfoThreadLocal().get().setLoginSuccess(Boolean.TRUE);

    }

    @AfterThrowing(pointcut = "(execution(* org.apache.kylin.rest.security.LimitLoginAuthenticationProvider.authenticate(..)) "
            + "|| execution(* org.apache.kylin.rest.security.OpenAuthenticationProvider.authenticate(..))"
            + "|| execution(* org.apache.kylin.rest.security.LdapAuthenticationProvider.authenticate(..))) "
            + "&& args(authentication)", throwing = "exception")
    public void doAfterLoginError(Authentication authentication, Exception exception) {
        if (null == loginLogFilter.getLoginInfoThreadLocal().get()) {
            loginLogFilter.getLoginInfoThreadLocal().set(new LoginLogFilter.LoginInfo());
        }

        loginLogFilter.getLoginInfoThreadLocal().get().setUserName(authentication.getName());
        loginLogFilter.getLoginInfoThreadLocal().get().setLoginSuccess(Boolean.FALSE);
        loginLogFilter.getLoginInfoThreadLocal().get().setException(exception);
    }

    @AfterReturning(value = "execution(* org.springframework.security.web.authentication.logout.LogoutSuccessHandler.onLogoutSuccess(..)) "
            + "&& args(request, response, authentication)", argNames = "request, response, authentication")
    public void doAfterLogoutSuccess(HttpServletRequest request, HttpServletResponse response,
            Authentication authentication) {
        SecurityLoggerUtils.recordLogout(authentication.getName());
    }
}
