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
package org.apache.kylin.tool.daemon.handler;

import org.apache.kylin.common.util.SecretKeyUtil;
import org.apache.kylin.tool.daemon.CheckResult;
import org.apache.kylin.tool.daemon.CheckStateHandler;
import org.apache.kylin.tool.daemon.HandleResult;
import org.apache.kylin.tool.daemon.HandleStateEnum;
import org.apache.kylin.tool.daemon.ServiceOpLevelEnum;
import org.apache.kylin.tool.daemon.Worker;
import org.apache.kylin.tool.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public abstract class AbstractCheckStateHandler extends Worker implements CheckStateHandler {
    private static final Logger logger = LoggerFactory.getLogger(AbstractCheckStateHandler.class);

    public boolean upGradeQueryService() {
        return opQueryService(ServiceOpLevelEnum.QUERY_UP_GRADE);
    }

    public boolean downGradeQueryService() {
        return opQueryService(ServiceOpLevelEnum.QUERY_DOWN_GRADE);
    }

    private boolean opQueryService(ServiceOpLevelEnum opLevelEnum) {
        try {
            if (null == getKgSecretKey()) {
                setKgSecretKey(SecretKeyUtil.readKGSecretKeyFromFile());
            }
            Preconditions.checkNotNull(getKgSecretKey(), "kg secret key is null!");

            if (null == getKE_PID()) {
                setKEPid(ToolUtil.getKylinPid());
            }
            byte[] encryptedToken = SecretKeyUtil.generateEncryptedTokenWithPid(getKgSecretKey(), getKE_PID());
            getRestClient().downOrUpGradeKE(opLevelEnum.getOpType(), encryptedToken);
        } catch (Exception e) {
            logger.error("Failed to operate service {}", opLevelEnum.getOpType(), e);
            return false;
        }
        return true;
    }

    abstract HandleResult doHandle(CheckResult checkResult);

    @Override
    public HandleResult handle(CheckResult checkResult) {
        logger.info("Handler: [{}], Health Checker: [{}] check result is {}, message: {}", this.getClass().getName(),
                checkResult.getCheckerName(), checkResult.getCheckState(), checkResult.getReason());

        HandleResult result;
        try {
            result = doHandle(checkResult);
            logger.info("Handler: [{}] handle the check result success ...", this.getClass().getName());
        } catch (Exception e) {
            logger.error("Failed to do handle!", e);
            result = new HandleResult(HandleStateEnum.HANDLE_FAILED);
        }

        if (null == result) {
            result = new HandleResult(HandleStateEnum.HANDLE_WARN);
        }

        if (null == result.getHandleState()) {
            result.setHandleState(HandleStateEnum.HANDLE_WARN);
        }

        return result;
    }

}
