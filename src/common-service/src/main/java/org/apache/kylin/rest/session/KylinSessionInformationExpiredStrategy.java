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

package org.apache.kylin.rest.session;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_UNAUTHORIZED;
import static org.apache.kylin.rest.util.HttpUtil.formatRequest;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.util.HttpUtil;
import org.springframework.security.web.session.SessionInformationExpiredEvent;
import org.springframework.security.web.session.SessionInformationExpiredStrategy;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class KylinSessionInformationExpiredStrategy implements SessionInformationExpiredStrategy {

    public void onExpiredSessionDetected(SessionInformationExpiredEvent event) throws IOException {
        HttpServletRequest request = event.getRequest();
        HttpServletResponse response = event.getResponse();
        HttpUtil.setErrorResponse(request, response, HttpServletResponse.SC_UNAUTHORIZED,
                new KylinException(USER_UNAUTHORIZED));
        if (log.isDebugEnabled()) {
            log.debug("Detail http request for authentication:\n" + formatRequest(request));
        }
    }
}
