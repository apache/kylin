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

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

import java.io.IOException;
import java.util.Set;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.AccessBatchGrantEventNotifier;
import org.apache.kylin.common.persistence.transaction.AccessGrantEventNotifier;
import org.apache.kylin.common.persistence.transaction.AccessRevokeEventNotifier;
import org.apache.kylin.common.persistence.transaction.AclGrantEventNotifier;
import org.apache.kylin.common.persistence.transaction.AclRevokeEventNotifier;
import org.apache.kylin.common.persistence.transaction.AclTCRRevokeEventNotifier;
import org.apache.kylin.common.persistence.transaction.AddCredentialToSparkBroadcastEventNotifier;
import org.apache.kylin.common.persistence.transaction.AuditLogBroadcastEventNotifier;
import org.apache.kylin.common.persistence.transaction.BroadcastEventReadyNotifier;
import org.apache.kylin.common.persistence.transaction.EpochCheckBroadcastNotifier;
import org.apache.kylin.common.persistence.transaction.LogicalViewBroadcastNotifier;
import org.apache.kylin.common.persistence.transaction.RefreshVolumeBroadcastEventNotifier;
import org.apache.kylin.common.persistence.transaction.StopQueryBroadcastEventNotifier;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.rest.security.AdminUserSyncEventNotifier;

import com.fasterxml.jackson.databind.JsonNode;

final class BroadcastEventValidator {

    private static final Set<String> ALLOWED_EVENT_TYPES = ImmutableSet.of(
            BroadcastEventReadyNotifier.class.getName(),
            AccessBatchGrantEventNotifier.class.getName(),
            AccessGrantEventNotifier.class.getName(),
            AccessRevokeEventNotifier.class.getName(),
            AclGrantEventNotifier.class.getName(),
            AclRevokeEventNotifier.class.getName(),
            AclTCRRevokeEventNotifier.class.getName(),
            AddCredentialToSparkBroadcastEventNotifier.class.getName(),
            AdminUserSyncEventNotifier.class.getName(),
            AuditLogBroadcastEventNotifier.class.getName(),
            EpochCheckBroadcastNotifier.class.getName(),
            LogicalViewBroadcastNotifier.class.getName(),
            RefreshVolumeBroadcastEventNotifier.class.getName(),
            StopQueryBroadcastEventNotifier.class.getName());

    private BroadcastEventValidator() {
    }

    static BroadcastEventReadyNotifier validateAndDeserialize(JsonNode body) throws IOException {
        if (body == null) {
            throw new KylinException(INVALID_PARAMETER, "Broadcast event body is required.");
        }

        JsonNode typeNode = body.get("@class");
        if (typeNode == null || !typeNode.isTextual() || !ALLOWED_EVENT_TYPES.contains(typeNode.asText())) {
            throw new KylinException(INVALID_PARAMETER, "Unsupported broadcast event type.");
        }
        return JsonUtil.readValue(body.toString(), BroadcastEventReadyNotifier.class);
    }
}
