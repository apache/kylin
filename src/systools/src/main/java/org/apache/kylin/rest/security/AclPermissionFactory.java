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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.springframework.security.acls.domain.DefaultPermissionFactory;
import org.springframework.security.acls.model.Permission;

/**
 * @author xduo
 * 
 */
public class AclPermissionFactory extends DefaultPermissionFactory {

    public AclPermissionFactory() {
        super();
        registerPublicPermissions(AclPermission.class);
    }

    public static List<Permission> getPermissions() {
        List<Permission> permissions = new ArrayList<Permission>();
        Field[] fields = AclPermission.class.getFields();

        for (Field field : fields) {
            try {
                Object fieldValue = field.get(null);

                if (Permission.class.isAssignableFrom(fieldValue.getClass())) {
                    Permission perm = (Permission) fieldValue;
                    String permissionName = field.getName();
                    if (permissionName.equals(AclPermissionEnum.ADMINISTRATION.name())
                            || permissionName.equals(AclPermissionEnum.MANAGEMENT.name())
                            || permissionName.equals(AclPermissionEnum.OPERATION.name())
                            || permissionName.equals(AclPermissionEnum.READ.name())) {
                        // Found a Permission static field
                        permissions.add(perm);
                    }
                }
            } catch (Exception ignore) {
                //ignore on purpose
            }
        }

        return permissions;
    }

    public static Permission getPermission(String perName) {
        Field[] fields = AclPermission.class.getFields();

        for (Field field : fields) {
            try {
                Object fieldValue = field.get(null);

                if (Permission.class.isAssignableFrom(fieldValue.getClass())) {
                    // Found a Permission static field
                    if (perName.equals(field.getName())) {
                        return (Permission) fieldValue;
                    }
                }
            } catch (Exception ignore) {
                //ignore on purpose
            }
        }

        return null;
    }
}
