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

import org.apache.kylin.common.KylinConfig;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Aspect
@Component
public class SchedulerEnhancer {

    @Around("@annotation(org.springframework.scheduling.annotation.Scheduled)")
    public void aroundScheduled(ProceedingJoinPoint pjp) throws Throwable {
        val config = KylinConfig.getInstanceFromEnv();
        if (!"query".equals(config.getServerMode())) {
            log.debug("schedule at job leader");
            pjp.proceed();
        }
    }
}
