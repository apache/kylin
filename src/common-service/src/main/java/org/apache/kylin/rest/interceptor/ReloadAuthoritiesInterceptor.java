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
package org.apache.kylin.rest.interceptor;

import static org.apache.kylin.common.exception.ServerErrorCode.USER_DATA_SOURCE_CONNECTION_FAILED;

import java.io.PrintWriter;
import java.net.ConnectException;
import java.util.Collections;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.response.ErrorResponse;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.user.ManagedUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.ldap.CommunicationException;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Order(-300)
public class ReloadAuthoritiesInterceptor extends HandlerInterceptorAdapter {

    @Autowired
    @Qualifier("userService")
    protected UserService userService;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();

        if (!KylinConfig.getInstanceFromEnv().isUTEnv() && auth != null
                && !(auth instanceof AnonymousAuthenticationToken) && auth.isAuthenticated()) {
            String name = auth.getName();
            ManagedUser user = null;
            try {
                user = (ManagedUser) userService.loadUserByUsername(name);
            } catch (UsernameNotFoundException e) {
                // can not get user by name
                log.debug("Load user by name exception, set authentication to AnonymousAuthenticationToken", e);
                SecurityContextHolder.getContext().setAuthentication(new AnonymousAuthenticationToken("anonymousUser",
                        "anonymousUser", Collections.singletonList(new SimpleGrantedAuthority("ROLE_ANONYMOUS"))));
                return true;
            } catch (CommunicationException communicationException) {
                boolean present = Optional.ofNullable(communicationException).map(Throwable::getCause)
                        .filter(javax.naming.CommunicationException.class::isInstance).map(Throwable::getCause)
                        .filter(ConnectException.class::isInstance).isPresent();
                if (present) {
                    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                    ErrorResponse errorResponse = new ErrorResponse(Unsafe.getUrlFromHttpServletRequest(request),
                            new KylinException(USER_DATA_SOURCE_CONNECTION_FAILED,
                                    MsgPicker.getMsg().getlDapUserDataSourceConnectionFailed()));
                    response.setCharacterEncoding("UTF-8");
                    PrintWriter writer = response.getWriter();
                    writer.print(JsonUtil.writeValueAsIndentString(errorResponse));
                    writer.flush();
                    writer.close();
                    return false;
                }
                throw communicationException;
            }

            if (user != null && auth instanceof UsernamePasswordAuthenticationToken) {
                UsernamePasswordAuthenticationToken newAuth = new UsernamePasswordAuthenticationToken(name,
                        auth.getCredentials(), user.getAuthorities());
                newAuth.setDetails(user);
                SecurityContextHolder.getContext().setAuthentication(newAuth);
            }
        }
        return true;
    }
}
