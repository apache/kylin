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

package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.BroadcastEventReadyNotifier;
import org.apache.kylin.rest.config.initialize.BroadcastListener;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.security.BroadcastSecurityContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.databind.JsonNode;

@Controller
@RequestMapping(value = "/api/broadcast", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class BroadcastController extends NBasicController {

    @Autowired
    private BroadcastListener localHandler;

    @PostMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<String> broadcastReceive(@RequestBody JsonNode body, HttpServletRequest request)
            throws IOException {
        BroadcastEventReadyNotifier notifier = BroadcastEventValidator.validateAndDeserialize(body);
        authorizeBroadcast(request);
        BroadcastSecurityContext.runAsTrusted(() -> localHandler.handle(notifier));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    private void authorizeBroadcast(HttpServletRequest request) {
        if (isAdmin()) {
            return;
        }

        String configuredToken = KylinConfig.getInstanceFromEnv().getBroadcastToken();
        String providedToken = request == null ? null
                : request.getHeader(BroadcastEventReadyNotifier.BROADCAST_TOKEN_HEADER);
        if (StringUtils.isNotBlank(configuredToken) && providedToken != null
                && MessageDigest.isEqual(configuredToken.getBytes(StandardCharsets.UTF_8),
                        providedToken.getBytes(StandardCharsets.UTF_8))) {
            return;
        }

        throw new KylinException(PERMISSION_DENIED,
                "Broadcast endpoint requires an authenticated global admin or a valid broadcast token.");
    }

    @PutMapping(value = "/capacity/refresh_all")
    @ResponseBody
    public EnvelopeResponse<String> innerRefreshAll() {
        return new EnvelopeResponse(KylinException.CODE_SUCCESS, "", "");
    }
}
