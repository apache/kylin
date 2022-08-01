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

package org.apache.kylin.rest.aspect;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;

import java.util.Objects;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Aspect
@Component
public class TransactionAspect {

    @Pointcut("@annotation(transaction)")
    public void callAt(Transaction transaction) {
        /// just implement it
    }

    @Around("callAt(transaction)")
    public Object around(ProceedingJoinPoint pjp, Transaction transaction) throws TransactionException {
        Object result = null;
        String unitName = UnitOfWork.GLOBAL_UNIT;
        if (transaction.project() != -1) {
            Object unitObject = pjp.getArgs()[transaction.project()];
            if (unitObject instanceof String) {
                unitName = unitObject.toString();
            } else if (unitObject instanceof TransactionProjectUnit) {
                unitName = ((TransactionProjectUnit) unitObject).transactionProjectUnit();
            }
        }

        if (!Objects.equals(UnitOfWork.GLOBAL_UNIT, unitName)) {
            ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .getProject(unitName);
            if (projectInstance == null) {
                throw new KylinException(PROJECT_NOT_EXIST, unitName);
            }
        }

        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(UnitOfWorkParams.builder().unitName(unitName)
                .readonly(transaction.readonly()).maxRetry(transaction.retry()).processor(() -> {
                    try {
                        return pjp.proceed();
                    } catch (Throwable throwable) {
                        throw new RuntimeException(throwable);
                    }
                }).build());
    }
}
