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
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.common.KylinConfig;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.GenericFilterBean;

@Component(value = "fillEmptyAuthorizationFilter")
public class FillEmptyAuthorizationFilter extends GenericFilterBean {

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        boolean skipBasicAuthorization = KylinConfig.getInstanceFromEnv().isSkipBasicAuthorization();
        HttpServletRequest req = (HttpServletRequest) request;
        if (req.getSession(false) == null && skipBasicAuthorization) {
            MutableHttpServletRequest mutableRequest = new MutableHttpServletRequest(req);
            //  set Authorization to skip basic authorization filter, MDow is base64("0:0"), the value is useless
            mutableRequest.putHeader("Authorization", "basic MDow");
            chain.doFilter(mutableRequest, response);
        } else {
            chain.doFilter(request, response);
        }
    }

}
