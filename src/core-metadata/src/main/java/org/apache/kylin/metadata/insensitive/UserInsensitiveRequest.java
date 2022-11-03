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

package org.apache.kylin.metadata.insensitive;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Unsafe;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;

import lombok.extern.slf4j.Slf4j;

public interface UserInsensitiveRequest extends InsensitiveRequest {

    default List<String> inSensitiveFields() {
        return Collections.singletonList("username");
    }

    @Slf4j
    final class LogHolder {
    }

    @Override
    default void updateField() {
        for (String fieldName : inSensitiveFields()) {
            try {
                Field field = getDeclaredField(this.getClass(), fieldName);
                if (field == null) {
                    return;
                }
                Unsafe.changeAccessibleObject(field, true);
                String username = (String) field.get(this);
                if (StringUtils.isEmpty(username)) {
                    return;
                }

                NKylinUserManager userManager = NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv());
                ManagedUser managedUser = userManager.get(username);
                if (managedUser == null) {
                    return;
                }
                field.set(this, managedUser.getUsername());
            } catch (IllegalAccessException e) {
                LogHolder.log.warn("update username failed ", e);
            }
        }
    }
}
