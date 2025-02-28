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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.SecretKey;
import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.SecretKeyUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.query.util.QueryLimiter;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.HealthResponse;
import org.apache.kylin.rest.service.HealthService;
import org.apache.kylin.tool.daemon.ServiceOpLevelEnum;
import org.apache.kylin.tool.daemon.checker.KEStatusChecker;
import org.apache.kylin.tool.util.ToolUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;
import lombok.Getter;
import lombok.extern.log4j.Log4j;

@Log4j
@Controller
@RequestMapping(value = "/api/kg/health", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class HealthController extends NBasicController {

    private static final int MAX_TOKEN_LENGTH = 64;

    private static final long VALIDATION_DURATION = 20L * 1000;

    @Getter
    private static String KE_PID;

    public synchronized void setKePid(String pid) {
        KE_PID = pid;
    }

    @Autowired
    @Qualifier("healthService")
    private HealthService healthService;

    @ApiOperation(value = "health APIs", tags = { "SM" })
    @PostMapping(value = "/instance_info")
    @ResponseBody
    public EnvelopeResponse<HealthResponse> getHealthStatus(HttpServletRequest request) throws IOException {
        try (BufferedInputStream bis = new BufferedInputStream(request.getInputStream())) {
            byte[] encryptedToken = readEncryptedToken(bis);
            if (!validateToken(encryptedToken)) {
                return new EnvelopeResponse<>(KylinException.CODE_UNDEFINED, null, KEStatusChecker.PERMISSION_DENIED);
            }
            return getHealthStatus();
        }
    }

    @ApiOperation(value = "health APIs", tags = { "SM" })
    @PostMapping(value = "/instance_service/{state}")
    @ResponseBody
    public EnvelopeResponse<String> changeServerState(HttpServletRequest request, @PathVariable String state)
            throws IOException {
        try (BufferedInputStream bis = new BufferedInputStream(request.getInputStream())) {
            byte[] encryptedToken = readEncryptedToken(bis);
            if (!validateToken(encryptedToken)) {
                return new EnvelopeResponse<>(KylinException.CODE_UNDEFINED, "", KEStatusChecker.PERMISSION_DENIED);
            }

            if (ServiceOpLevelEnum.QUERY_DOWN_GRADE.getOpType().equals(state)) {
                QueryLimiter.downgrade();
            } else if (ServiceOpLevelEnum.QUERY_UP_GRADE.getOpType().equals(state)) {
                QueryLimiter.recover();
            } else {
                throw new IllegalArgumentException("Illegal server state: " + state);
            }

            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
        }
    }

    public EnvelopeResponse<HealthResponse> getHealthStatus() {
        HealthResponse.RestartSparkStatusResponse sparkRestartStatus = healthService.getRestartSparkStatus();
        List<HealthResponse.CanceledSlowQueryStatusResponse> canceledSlowQueriesStatus = healthService
                .getCanceledSlowQueriesStatus();
        HealthResponse healthResponse = new HealthResponse(sparkRestartStatus, canceledSlowQueriesStatus);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, healthResponse, "");
    }

    private byte[] readEncryptedToken(BufferedInputStream bis) throws IOException {
        ArrayList<Byte> byteList = Lists.newArrayList();
        int data;
        int readByteCount = 0;
        while ((data = bis.read()) != -1) {
            byteList.add((byte) data);
            if (++readByteCount > MAX_TOKEN_LENGTH) {
                return null;
            }
        }
        byte[] encryptedToken = new byte[readByteCount];
        for (int i = 0; i < readByteCount; i++) {
            encryptedToken[i] = byteList.get(i);
        }
        return encryptedToken;
    }

    private boolean validateToken(byte[] encryptedToken) {
        if (null == encryptedToken || encryptedToken.length == 0) {
            return false;
        }
        try {
            SecretKey secretKey = SecretKeyUtil.getKGSecretKey();
            String originalToken = SecretKeyUtil.decryptToken(secretKey, encryptedToken);
            String[] parts = originalToken.split("_");
            if (parts.length != 2) {
                return false;
            }

            if (null == getKE_PID()) {
                setKePid(ToolUtil.getKylinPid());
            }

            if (!parts[0].equals(getKE_PID())) {
                return false;
            }
            long timestamp = Long.parseLong(parts[1]);
            return System.currentTimeMillis() - timestamp <= VALIDATION_DURATION;
        } catch (Exception e) {
            log.error("Validate token failed! ", e);
            return false;
        }
    }
}
