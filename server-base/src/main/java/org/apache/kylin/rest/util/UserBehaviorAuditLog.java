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

package org.apache.kylin.rest.util;

import java.util.Arrays;

import org.apache.kylin.common.persistence.AclEntity;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

@Component("auditLog")
@Aspect
public class UserBehaviorAuditLog {
    private static final Logger logger = LoggerFactory.getLogger(UserBehaviorAuditLog.class);

    /* base audit */
    public void auditAllParamWithSignature(JoinPoint joinPoint) {
        logger.info("User: {} trigger {}, arguments: {}.",
                SecurityContextHolder.getContext().getAuthentication().getName(), joinPoint.getSignature().getName(),
                Arrays.toString(joinPoint.getArgs()));
    }

    /* exception audit */
    public void executeFailed(JoinPoint joinPoint, Throwable reason) {
        logger.info("User: {} execute {} failed, exception: {}",
                SecurityContextHolder.getContext().getAuthentication().getName(), joinPoint.getSignature().getName(),
                reason);
    }

    public void finish(JoinPoint joinPoint) {
        logger.info("User: {} execute {} finished.", SecurityContextHolder.getContext().getAuthentication().getName(),
                joinPoint.getSignature().getName());
    }

    /* special job audit*/
    public void submitJobAudit(JoinPoint joinPoint) {
        logger.info("User: {} submit job of {} for cube {} {}.", joinPoint.getArgs()[7], joinPoint.getArgs()[5],
                joinPoint.getArgs()[0], joinPoint.getArgs()[1]);
    }

    public void optimizeJobAudit(JoinPoint joinPoint) {
        logger.info("User: {} submit job of optimization for cube {}.", joinPoint.getArgs()[2], joinPoint.getArgs()[0]);
    }

    public void recoverSegmentOptimizeJobAudit(JoinPoint joinPoint) {
        logger.info("User: {} submit job of recovering optimizing segment for {}.", joinPoint.getArgs()[1],
                joinPoint.getArgs()[0]);
    }

    /* access audit*/
    public void accessGrantAudit(AclEntity ae, Permission permission, Sid sid) {
        logger.info("User: {} grant {} access[mask: {},  pattern: {}] to entity[{}].",
                SecurityContextHolder.getContext().getAuthentication().getName(), sid, permission.getMask(),
                permission.getPattern(), ae.getId());
    }

    public void accessUpdateAudit(AclEntity ae, int accessEntryId, Permission newPermission) {
        logger.info("User: {} update entity[{}] accessEntryId[{}] to access[mask: {},  pattern: {}].",
                SecurityContextHolder.getContext().getAuthentication().getName(), ae.getId(), accessEntryId,
                newPermission.getMask(), newPermission.getPattern());
    }

    public void accessRevokeAudit(AclEntity ae, int accessEntryId) {
        logger.info("User: {} revoke entity[{}] accessEntryId[{}] access.",
                SecurityContextHolder.getContext().getAuthentication().getName(), ae.getId(), accessEntryId);
    }

    public void accessCleanAudit(AclEntity ae, boolean deleteChildren) {
        logger.info("User: {} clean entity[{}] access [delete children: {}].",
                SecurityContextHolder.getContext().getAuthentication().getName(), ae.getId(), deleteChildren);
    }
}
