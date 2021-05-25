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

package org.apache.kylin.rest.service;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.notify.NotificationContext;
import org.apache.kylin.common.notify.NotificationTransmitter;
import org.apache.kylin.common.notify.util.NotificationConstants;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.RuleValidationException;
import org.apache.kylin.tool.CubeMigrationCLI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Provide migration logic implementation.
 */
@Component("migrationService")
public class MigrationService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(MigrationService.class);

    @Autowired
    private AccessService accessService;

    @Autowired
    private CubeService cubeService;

    private final String localHost = KylinConfig.getInstanceFromEnv().getMigrationLocalAddress();
    private final String envName = KylinConfig.getInstanceFromEnv().getDeployEnv();

    public String checkRule(MigrationRuleSet.Context context) throws RuleValidationException {
        return MigrationRuleSet.apply(context);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION')")
    public void requestMigration(CubeInstance cube, MigrationRuleSet.Context ctx) throws Exception {
        Map<String, String> root = Maps.newHashMap();
        root.put("projectname", ctx.getTgtProjectName());
        root.put("cubename", ctx.getCubeInstance().getName());
        root.put("status", "NEED APPROVE");
        root.put("envname", envName);
        sendMigrationNotification(NotificationConstants.JOB_MIGRATION_REQUEST, getRecipients(cube), root);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public boolean reject(String cubeName, String projectName, String reason) {
        try {
            Map<String, String> root = Maps.newHashMap();
            root.put("cubename", cubeName);
            root.put("rejectedReason", reason);
            root.put("status", "REJECTED");
            root.put("envname", envName);

            sendMigrationNotification(NotificationConstants.JOB_MIGRATION_REJECTED, getRecipients(cubeName), root);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void approve(CubeInstance cube, MigrationRuleSet.Context ctx) throws Exception {
        checkRule(ctx);

        String cubeName = cube.getName();
        String projectName = ctx.getTgtProjectName();
        try {
            sendApprovedMailQuietly(cubeName, projectName);

            // do cube migration
            new CubeMigrationCLI().moveCube(localHost, ctx.getTargetAddress(), cubeName, projectName, "true", "false",
                    "true", "true", "false");

            sendCompletedMailQuietly(cubeName, projectName);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            sendMigrationFailedMailQuietly(cubeName, projectName, e.getMessage());
            throw e;
        }
    }

    private boolean sendApprovedMailQuietly(String cubeName, String projectName) {
        try {
            Map<String, String> root = Maps.newHashMap();
            root.put("projectname", projectName);
            root.put("cubename", cubeName);
            root.put("status", "APPROVED");
            root.put("envname", envName);

            sendMigrationNotification(NotificationConstants.JOB_MIGRATION_APPROVED, getRecipients(cubeName), root);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    private boolean sendCompletedMailQuietly(String cubeName, String projectName) {
        try {
            Map<String, String> root = Maps.newHashMap();
            root.put("projectname", projectName);
            root.put("cubename", cubeName);
            root.put("status", "COMPLETED");
            root.put("envname", envName);

            sendMigrationNotification(NotificationConstants.JOB_MIGRATION_COMPLETED, getRecipients(cubeName), root);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    private boolean sendMigrationFailedMailQuietly(String cubeName, String projectName, String reason) {
        try {
            Map<String, String> root = Maps.newHashMap();
            root.put("projectname", projectName);
            root.put("cubename", cubeName);
            root.put("status", "FAILED");
            root.put("failedReason", reason);
            root.put("envname", envName);

            sendMigrationNotification(NotificationConstants.JOB_MIGRATION_FAILED, getRecipients(cubeName), root);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    public List<String> getCubeAdmins(CubeInstance cubeInstance) {
        ProjectInstance prjInstance = cubeInstance.getProjectInstance();
        AclEntity ae = accessService.getAclEntity("ProjectInstance", prjInstance.getUuid());
        logger.info("ProjectUUID : " + prjInstance.getUuid());
        Acl acl = accessService.getAcl(ae);

        String mailSuffix = KylinConfig.getInstanceFromEnv().getNotificationMailSuffix();
        List<String> cubeAdmins = Lists.newArrayList();
        if (acl != null) {
            for (AccessControlEntry ace : acl.getEntries()) {
                if (ace.getPermission().getMask() == 16) {
                    PrincipalSid ps = (PrincipalSid) ace.getSid();
                    cubeAdmins.add(ps.getPrincipal() + mailSuffix);
                }
            }
        }

        if (cubeAdmins.isEmpty()) {
            throw new BadRequestException("Cube access list is null, please add at least one role in it.");
        }
        return cubeAdmins;
    }

    public Map<String, List<String>> getRecipients(String cubeName) throws Exception {
        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);
        return getRecipients(cubeInstance);
    }

    public Map<String, List<String>> getRecipients(CubeInstance cubeInstance) throws Exception {
        Map<String, List<String>> receivers = Maps.newHashMap();
        List<String> emailRecipients = Lists.newArrayList();
        emailRecipients.addAll(getCubeAdmins(cubeInstance));
        emailRecipients.addAll(cubeInstance.getDescriptor().getNotifyEmailList());
        String[] adminDls = KylinConfig.getInstanceFromEnv().getAdminDls();
        if (adminDls != null) {
            emailRecipients.addAll(Lists.newArrayList(adminDls));
        }
        receivers.put(NotificationConstants.NOTIFY_EMAIL_LIST, emailRecipients);
        receivers.put(NotificationConstants.NOTIFY_DINGTALK_LIST, cubeInstance.getDescriptor().getNotifyDingTalkList());
        return receivers;
    }

    public void sendMigrationNotification(String state, Map<String, List<String>> recipients, Map<String, String> root) {
        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        root.put("requester", submitter);

        String[] titles;

        // No project name for rejected title
        if (state == NotificationConstants.JOB_MIGRATION_REJECTED) {
            titles = new String[]{
                    "MIGRATION",
                    root.get("status"),
                    root.get("envname"),
                    root.get("cubename")
            };
        } else {
            titles = new String[]{
                    "MIGRATION",
                    root.get("status"),
                    root.get("envname"),
                    root.get("projectname"),
                    root.get("cubename")
            };
        }

        Pair<String[], Map<String, Object>> hashMapPair = Pair.newPair(titles, Maps.<String, Object>newHashMap(root));
        NotificationContext notificationContext = new NotificationContext(KylinConfig.getInstanceFromEnv(), recipients, state, hashMapPair);
        new NotificationTransmitter(notificationContext).sendNotification();
    }
}
