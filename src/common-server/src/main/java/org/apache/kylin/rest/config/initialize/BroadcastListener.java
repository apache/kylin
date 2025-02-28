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
package org.apache.kylin.rest.config.initialize;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kylin.common.persistence.transaction.AccessBatchGrantEventNotifier;
import org.apache.kylin.common.persistence.transaction.AccessGrantEventNotifier;
import org.apache.kylin.common.persistence.transaction.AccessRevokeEventNotifier;
import org.apache.kylin.common.persistence.transaction.AclGrantEventNotifier;
import org.apache.kylin.common.persistence.transaction.AclRevokeEventNotifier;
import org.apache.kylin.common.persistence.transaction.AclTCRRevokeEventNotifier;
import org.apache.kylin.common.persistence.transaction.AddCredentialToSparkBroadcastEventNotifier;
import org.apache.kylin.common.persistence.transaction.AuditLogBroadcastEventNotifier;
import org.apache.kylin.common.persistence.transaction.BroadcastEventReadyNotifier;
import org.apache.kylin.common.persistence.transaction.LogicalViewBroadcastNotifier;
import org.apache.kylin.common.persistence.transaction.StopQueryBroadcastEventNotifier;
import org.apache.kylin.guava30.shaded.common.eventbus.Subscribe;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.broadcaster.BroadcastEventHandler;
import org.apache.kylin.rest.broadcaster.Broadcaster;
import org.apache.kylin.rest.security.AdminUserSyncEventNotifier;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.AclTCRService;
import org.apache.kylin.rest.service.AuditLogService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.service.UserAclService;
import org.apache.spark.sql.LogicalViewLoader;
import org.apache.spark.sql.SparderEnv;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class BroadcastListener implements BroadcastEventHandler {

    @Autowired
    private AuditLogService auditLogService;

    @Autowired
    @Qualifier("queryService")
    private QueryService queryService;

    @Autowired
    private AclTCRService aclTCRService;

    @Autowired
    private AccessService accessService;

    @Autowired
    private UserAclService userAclService;

    @Autowired
    private JobService jobService;

    @Autowired
    private Broadcaster broadcaster;

    @PostConstruct
    public void init() {
        broadcaster.register(this);
        broadcaster.start();
    }

    @PreDestroy
    public void cleanup() {
        broadcaster.close();
        broadcaster.unregister();
    }

    @Subscribe
    public void onEventReady(BroadcastEventReadyNotifier notifier) {
        broadcaster.announce(notifier);
    }

    public void handle(BroadcastEventReadyNotifier notifier) throws IOException {
        log.info("accept broadcast Event {}", notifier);
        if (notifier instanceof AuditLogBroadcastEventNotifier) {
            auditLogService.notifyCatchUp();
        } else if (notifier instanceof StopQueryBroadcastEventNotifier) {
            queryService.stopQuery(notifier.getSubject());
        } else if (notifier instanceof AclGrantEventNotifier) {
            aclTCRService.updateAclFromRemote((AclGrantEventNotifier) notifier, null);
        } else if (notifier instanceof AclRevokeEventNotifier) {
            aclTCRService.updateAclFromRemote(null, (AclRevokeEventNotifier) notifier);
        } else if (notifier instanceof AccessGrantEventNotifier) {
            accessService.updateAccess((AccessGrantEventNotifier) notifier, null, null);
        } else if (notifier instanceof AccessBatchGrantEventNotifier) {
            accessService.updateAccess(null, (AccessBatchGrantEventNotifier) notifier, null);
        } else if (notifier instanceof AccessRevokeEventNotifier) {
            accessService.updateAccess(null, null, (AccessRevokeEventNotifier) notifier);
        } else if (notifier instanceof AclTCRRevokeEventNotifier) {
            AclTCRRevokeEventNotifier aclTCRRevokeEventNotifier = (AclTCRRevokeEventNotifier) notifier;
            aclTCRService.revokeAclTCR(aclTCRRevokeEventNotifier.getSid(), aclTCRRevokeEventNotifier.isPrinciple());
        } else if (notifier instanceof AddCredentialToSparkBroadcastEventNotifier) {
            AddCredentialToSparkBroadcastEventNotifier credentialNotifier //
                    = (AddCredentialToSparkBroadcastEventNotifier) notifier;
            SparderEnv.addCredential(new TableExtDesc.RoleCredentialInfo(credentialNotifier.getBucket(),
                    credentialNotifier.getRole(), credentialNotifier.getEndpoint(), credentialNotifier.getType(),
                    credentialNotifier.getRegion()), SparderEnv.getSparkSession());
        } else if (notifier instanceof AdminUserSyncEventNotifier) {
            AdminUserSyncEventNotifier adminUserSyncEventNotifier = (AdminUserSyncEventNotifier) notifier;
            userAclService.syncAdminUserAcl(adminUserSyncEventNotifier.getAdminUserList(),
                    adminUserSyncEventNotifier.isUseEmptyPermission());
        } else if (notifier instanceof LogicalViewBroadcastNotifier) {
            LogicalViewLoader.syncViewAsync();
        }
    }

    @Override
    public void handleLocally(BroadcastEventReadyNotifier notifier) throws IOException {
        handle(notifier);
    }
}
