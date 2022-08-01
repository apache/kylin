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

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_USER;
import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.util.PasswordEncodeFactory;
import org.apache.kylin.metadata.user.ManagedUser;
import org.springframework.core.env.Environment;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.RequestBody;

import com.google.common.collect.Lists;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateAdminUserUtils {
    private static PasswordEncoder passwordEncoder = PasswordEncodeFactory.newUserPasswordEncoder();

    private static final SimpleGrantedAuthority ALL_USERS_AUTH = new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS);

    private static final Pattern bcryptPattern = Pattern.compile("\\A\\$2a?\\$\\d\\d\\$[./0-9A-Za-z]{53}");

    public static final String PROFILE_DEFAULT = "testing";

    private static final String PROFILE_CUSTOM = "custom";

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    //do not use aclEvaluate, if there's no users and will come into init() and will call save.
    public static EnvelopeResponse<String> createAdminUser(@RequestBody ManagedUser user, UserService userService,
            Environment env) {
        checkProfile(env);
        user.setUuid(RandomUtil.randomUUIDStr());
        user.setPassword(pwdEncode(user.getPassword()));
        log.info("Creating user: {}", user);
        completeAuthorities(user);
        userService.createUser(user);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    public static String pwdEncode(String pwd) {
        if (bcryptPattern.matcher(pwd).matches())
            return pwd;
        return passwordEncoder.encode(pwd);
    }

    public static void checkProfile(Environment env) {
        val msg = MsgPicker.getMsg();
        if (!env.acceptsProfiles(PROFILE_DEFAULT, PROFILE_CUSTOM)) {
            throw new KylinException(FAILED_UPDATE_USER, msg.getUserEditNotAllowed());
        }
    }

    public static void completeAuthorities(ManagedUser managedUser) {
        List<SimpleGrantedAuthority> detailRoles = Lists.newArrayList(managedUser.getAuthorities());
        if (!detailRoles.contains(ALL_USERS_AUTH)) {
            detailRoles.add(ALL_USERS_AUTH);
        }
        managedUser.setGrantedAuthorities(detailRoles);
    }

    public static void createAllAdmins(UserService userService, Environment env) throws IOException {
        List<ManagedUser> all = userService.listUsers();
        log.info("All {} users", all.size());
        if (all.isEmpty() && env.acceptsProfiles(PROFILE_DEFAULT)) {
            createAdminUser(new ManagedUser("ADMIN", "KYLIN", true, ROLE_ADMIN, Constant.GROUP_ALL_USERS), userService,
                    env);
        }
    }
}
