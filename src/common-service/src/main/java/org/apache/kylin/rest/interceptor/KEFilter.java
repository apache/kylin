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

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.ExceptionReason;
import org.apache.kylin.common.exception.ExceptionResolve;
import org.apache.kylin.common.exception.code.ErrorMsg;
import org.apache.kylin.common.exception.code.ErrorSuggestion;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class KEFilter extends OncePerRequestFilter {
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws IOException, ServletException {
        String lang = request.getHeader("Accept-Language");
        MsgPicker.setMsg(lang);
        ErrorCode.setMsg(lang);
        ExceptionResolve.setLang(lang);
        ExceptionReason.setLang(lang);
        ErrorMsg.setMsg(lang);
        ErrorSuggestion.setMsg(lang);

        if (("/kylin/api/query".equals(request.getRequestURI())
                || "/kylin/api/async_query".equals(request.getRequestURI()))) {
            QueryContext.reset(); // reset it anyway
            QueryContext.current(); // init query context to set the timer
            QueryContext.currentTrace().startSpan(QueryTrace.HTTP_RECEPTION);
        }

        // Set traceId for KE
        String traceId = RandomUtil.randomUUIDStr();
        ThreadContext.put("traceId", String.format("traceId: %s ", traceId));

        filterChain.doFilter(request, response);

        // clean ThreadContext
        ThreadContext.clearAll();
        // set `traceId` attribute for accesslog
        request.setAttribute("traceId", traceId);
    }

}
