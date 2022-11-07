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
import java.util.Locale;

import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.annotation.ThirdPartyDependencies;
import org.apache.kylin.metadata.user.ManagedUser;
import org.springframework.security.core.userdetails.UserDetails;

@ThirdPartyDependencies({
        @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager",
                classes = { "StaticUserService"})
})
public abstract class OpenUserService implements UserService {

    @Override
    public abstract List<ManagedUser> listUsers();

    @Override
    public abstract List<String> listAdminUsers();

    @Override
    public void createUser(UserDetails userDetails) {
        throw new UnsupportedOperationException(
                String.format(Locale.ROOT, MsgPicker.getMsg().getUserEditNotAllowedForCustom(), "createUser"));
    }

    @Override
    public void updateUser(UserDetails userDetails) {
        throw new UnsupportedOperationException(
                String.format(Locale.ROOT, MsgPicker.getMsg().getUserEditNotAllowedForCustom(), "updateUser"));
    }

    @Override
    public void deleteUser(String s) {
        throw new UnsupportedOperationException(
                String.format(Locale.ROOT, MsgPicker.getMsg().getUserEditNotAllowedForCustom(), "deleteUser"));
    }

    @Override
    public void changePassword(String s, String s1) {
        throw new UnsupportedOperationException(
                String.format(Locale.ROOT, MsgPicker.getMsg().getUserEditNotAllowedForCustom(), "changePassword"));
    }

    @Override
    public abstract UserDetails loadUserByUsername(String s);
}
