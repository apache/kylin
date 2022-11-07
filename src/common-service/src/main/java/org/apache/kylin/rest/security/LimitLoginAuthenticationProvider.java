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

import static org.apache.kylin.common.exception.ServerErrorCode.USER_LOCKED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_LOGIN_FAILED;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.metadata.user.NKylinUserManager;
import org.apache.kylin.rest.service.MaintenanceModeSupporter;
import org.apache.kylin.tool.restclient.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import com.google.common.base.Preconditions;

public class LimitLoginAuthenticationProvider extends DaoAuthenticationProvider {

    private static final Logger limitLoginLogger = LoggerFactory.getLogger(LimitLoginAuthenticationProvider.class);

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Autowired(required = false)
    @Qualifier("maintenanceModeService")
    MaintenanceModeSupporter maintenanceModeService;

    private ConcurrentHashMap<String, RestClient> clientMap = new ConcurrentHashMap<>();

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to init Message Digest ", e);
        }

        md.reset();

        ManagedUser managedUser = null;
        String userName = null;

        try {
            if (authentication instanceof UsernamePasswordAuthenticationToken)
                userName = (String) authentication.getPrincipal();

            if (userName != null) {
                NKylinUserManager userManager = NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv());
                managedUser = userManager.get(userName);
                if (managedUser != null) {
                    userName = managedUser.getUsername();
                    authentication = new UsernamePasswordAuthenticationToken(userName, authentication.getCredentials());
                } else {
                    managedUser = (ManagedUser) userService.loadUserByUsername(userName);
                }
                Preconditions.checkNotNull(managedUser);
            }
            updateUserLockStatus(managedUser, userName);
            Authentication auth = super.authenticate(authentication);

            if (managedUser != null && managedUser.getWrongTime() > 0 && !maintenanceModeService.isMaintenanceMode()) {
                managedUser.clearAuthenticateFailedRecord();
                updateUser(managedUser);
            }

            SecurityContextHolder.getContext().setAuthentication(auth);

            return auth;
        } catch (BadCredentialsException e) {
            authenticateFail(managedUser, userName);
            if (managedUser != null && managedUser.isLocked()) {
                if (UserLockRuleUtil.isLockedPermanently(managedUser)) {
                    buildBadCredentialsException(userName, e);
                }
                String msg = MsgPicker.getMsg().getUserBeLocked(UserLockRuleUtil.getLockDurationSeconds(managedUser));
                limitLoginLogger.error(msg, new KylinException(USER_LOCKED, e));
                throw new BadCredentialsException(msg, new KylinException(USER_LOCKED, e));
            } else {
                limitLoginLogger.error(USER_LOGIN_FAILED.getMsg());
                throw new BadCredentialsException(USER_LOGIN_FAILED.getMsg());
            }
        } catch (UsernameNotFoundException e) {
            throw new BadCredentialsException(USER_LOGIN_FAILED.getMsg(), new KylinException(USER_LOGIN_FAILED));
        } catch (IllegalArgumentException e) {
            throw new BadCredentialsException(USER_LOGIN_FAILED.getMsg());
        }
    }

    private void buildBadCredentialsException(String userName, BadCredentialsException e) {
        String msg = String.format(Locale.ROOT, MsgPicker.getMsg().getUserInPermanentlyLockedStatus(), userName);
        limitLoginLogger.error(msg, new KylinException(USER_LOCKED, e));
        throw new BadCredentialsException(msg, new KylinException(USER_LOCKED, e));
    }

    private void authenticateFail(ManagedUser managedUser, String userName) {
        if (userName != null && managedUser != null) {
            managedUser.authenticateFail();
            updateUser(managedUser);
        }
    }

    private void updateUser(ManagedUser managedUser) {
        boolean isOwner = false;
        EpochManager manager = EpochManager.getInstance();
        try {
            isOwner = manager.checkEpochOwner(EpochManager.GLOBAL);
        } catch (Exception e) {
            logger.error("Get global epoch owner failed, update locally.", e);
            return;
        }
        if (isOwner) {
            userService.updateUser(managedUser);
        } else {
            try {
                String owner = manager.getEpochOwner(EpochManager.GLOBAL).split("\\|")[0];
                if (clientMap.get(owner) == null) {
                    clientMap.clear();
                    clientMap.put(owner, new RestClient(owner));
                }
                clientMap.get(owner).updateUser(managedUser);
            } catch (Exception e) {
                logger.error("Failed to update user throw restclient", e);
            }
        }
    }

    private void updateUserLockStatus(ManagedUser managedUser, String userName) {
        if (managedUser != null && managedUser.isLocked()) {

            if (UserLockRuleUtil.isLockedPermanently(managedUser)) {
                buildLockedException(userName);
            }

            long lockedTime = managedUser.getLockedTime();
            long timeDiff = System.currentTimeMillis() - lockedTime;

            if (UserLockRuleUtil.isLockDurationEnded(managedUser, timeDiff)) {
                managedUser.setLocked(false);
                updateUser(managedUser);
            } else {
                long leftSeconds = UserLockRuleUtil.getLockLeftSeconds(managedUser, timeDiff);
                long nextLockSeconds = UserLockRuleUtil.getLockDurationSeconds(managedUser.getWrongTime() + 1);
                String msg = String.format(Locale.ROOT,
                        MsgPicker.getMsg().getUserInLockedStatus(leftSeconds, nextLockSeconds), userName);
                throw new LockedException(msg);
            }
        }
    }

    private void buildLockedException(String userName) {
        throw new LockedException(
                String.format(Locale.ROOT, MsgPicker.getMsg().getUserInPermanentlyLockedStatus(), userName));
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return authentication.equals(UsernamePasswordAuthenticationToken.class);
    }
}
