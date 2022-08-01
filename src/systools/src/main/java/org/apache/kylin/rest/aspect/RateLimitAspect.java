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

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Aspect
@Component
public class RateLimitAspect {

    private Map<String, RateLimiter> limitMap = Maps.newConcurrentMap();

    @Pointcut("@annotation(enableRateLimit)")
    public void callAt(EnableRateLimit enableRateLimit) {
        /// just implement it
    }

    @Around("callAt(enableRateLimit)")
    public void around(ProceedingJoinPoint joinPoint, EnableRateLimit enableRateLimit) throws Throwable {
        log.info("ratelimit aspect start");
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        String methodName = signature.getDeclaringTypeName() + "." + signature.getName();
        double permitsPerSecond = KylinConfig.getInstanceFromEnv().getRateLimitPermitsPerMinute();
        limitMap.putIfAbsent(methodName, RateLimiter.create(permitsPerSecond / 60.0));
        RateLimiter limiter = limitMap.get(methodName);

        if (limiter.tryAcquire()) {
            joinPoint.proceed();
        }
    }
}
