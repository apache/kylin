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
package org.apache.kylin.rest.security;

import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;

import org.apache.kylin.common.exception.KylinException;

public enum AclPermissionEnum {
    ADMINISTRATION, MANAGEMENT, OPERATION, READ, EMPTY;

    public static String convertToAclPermission(String externalPermission) {
        AclPermissionEnum permission;
        switch (externalPermission) {
        case ExternalAclProvider.ADMINISTRATION:
            permission = ADMINISTRATION;
            break;
        case ExternalAclProvider.MANAGEMENT:
            permission = MANAGEMENT;
            break;
        case ExternalAclProvider.OPERATION:
            permission = OPERATION;
            break;
        case ExternalAclProvider.READ:
            permission = READ;
            break;
        case ExternalAclProvider.EMPTY:
            permission = EMPTY;
            break;
        default:
            throw new KylinException(PERMISSION_DENIED, "invalid permission state: " + externalPermission);
        }
        return permission.name();
    }
}
