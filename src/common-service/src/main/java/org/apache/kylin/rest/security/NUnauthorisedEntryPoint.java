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

import static org.apache.kylin.common.exception.ServerErrorCode.LOGIN_FAILED;
import static org.apache.kylin.common.exception.ServerErrorCode.USER_DATA_SOURCE_CONNECTION_FAILED;
import static org.apache.kylin.common.exception.ServerErrorCode.USER_LOCKED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_LOGIN_FAILED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_UNAUTHORIZED;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.util.Optional;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.response.ErrorResponse;
import org.apache.kylin.common.util.Unsafe;
import org.springframework.http.MediaType;
import org.springframework.ldap.CommunicationException;
import org.springframework.security.authentication.DisabledException;
import org.springframework.security.authentication.InsufficientAuthenticationException;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.stereotype.Component;

@Component(value = "nUnauthorisedEntryPoint")
public class NUnauthorisedEntryPoint implements AuthenticationEntryPoint {

    public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException exception)
            throws IOException, ServletException {
        if (exception instanceof LockedException) {
            setErrorResponse(request, response, HttpServletResponse.SC_BAD_REQUEST,
                    new KylinException(USER_LOCKED, exception.getMessage()));
            return;
        } else if (exception instanceof InsufficientAuthenticationException) {
            setErrorResponse(request, response, HttpServletResponse.SC_UNAUTHORIZED,
                    new KylinException(USER_UNAUTHORIZED));
            return;
        } else if (exception instanceof DisabledException) {
            setErrorResponse(request, response, HttpServletResponse.SC_UNAUTHORIZED,
                    new KylinException(LOGIN_FAILED, MsgPicker.getMsg().getDisabledUser()));
            return;
        }
        boolean present = Optional.ofNullable(exception).map(Throwable::getCause)
                .filter(CommunicationException.class::isInstance).map(Throwable::getCause)
                .filter(javax.naming.CommunicationException.class::isInstance).map(Throwable::getCause)
                .filter(ConnectException.class::isInstance).isPresent();

        if (present) {
            setErrorResponse(request, response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    new KylinException(USER_DATA_SOURCE_CONNECTION_FAILED,
                            MsgPicker.getMsg().getlDapUserDataSourceConnectionFailed()));
            return;
        }

        present = Optional.ofNullable(exception).map(Throwable::getCause)
                .filter(org.springframework.ldap.AuthenticationException.class::isInstance).map(Throwable::getCause)
                .filter(javax.naming.AuthenticationException.class::isInstance).isPresent();

        if (present) {
            setErrorResponse(request, response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, new KylinException(
                    USER_DATA_SOURCE_CONNECTION_FAILED, MsgPicker.getMsg().getLdapUserDataSourceConfigError()));
            return;
        }

        setErrorResponse(request, response, HttpServletResponse.SC_UNAUTHORIZED, new KylinException(USER_LOGIN_FAILED));
    }

    public void setErrorResponse(HttpServletRequest request, HttpServletResponse response, int statusCode, Exception ex)
            throws IOException {
        response.setStatus(statusCode);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        ErrorResponse errorResponse = new ErrorResponse(Unsafe.getUrlFromHttpServletRequest(request), ex);
        String errorStr = JsonUtil.writeValueAsIndentString(errorResponse);
        response.setCharacterEncoding("UTF-8");
        PrintWriter writer = response.getWriter();
        writer.print(errorStr);
        writer.flush();
        writer.close();
    }

}
