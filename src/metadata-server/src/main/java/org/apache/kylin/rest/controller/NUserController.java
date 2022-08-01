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

import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_USER_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_PASSWORD;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_USER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PASSWORD;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_USER_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.SHORT_PASSWORD;
import static org.apache.kylin.common.exception.ServerErrorCode.USER_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_AUTH_INFO_NOTFOUND;
import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.kylin.util.PasswordEncodeFactory;
import org.apache.kylin.common.persistence.transaction.AclTCRRevokeEventNotifier;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.rest.config.initialize.AfterMetadataReadyEvent;
import org.apache.kylin.rest.request.PasswordChangeRequest;
import org.apache.kylin.rest.request.UserRequest;
import org.apache.kylin.rest.service.AclTCRService;
import org.apache.kylin.rest.util.CreateAdminUserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/user", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class NUserController extends NBasicController {

    private static final Logger logger = LoggerFactory.getLogger(NUserController.class);

    private static final String PROFILE_DEFAULT = "testing";

    private static final String PROFILE_CUSTOM = "custom";

    @Autowired
    @Qualifier("userService")
    private UserService userService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRService aclTCRService;

    @Autowired
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    @Autowired
    private Environment env;

    private static final Pattern passwordPattern = Pattern
            .compile("^(?=.*\\d)(?=.*[a-zA-Z])(?=.*[~!@#$%^&*(){}|:\"<>?\\[\\];',./`]).{8,}$");
    private static final Pattern bcryptPattern = Pattern.compile("\\A\\$2a?\\$\\d\\d\\$[./0-9A-Za-z]{53}");
    private static final Pattern base64Pattern = Pattern
            .compile("^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$");

    private static PasswordEncoder pwdEncoder = PasswordEncodeFactory.newUserPasswordEncoder();

    private static final SimpleGrantedAuthority ALL_USERS_AUTH = new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS);

    @EventListener(AfterMetadataReadyEvent.class)
    public void init() throws IOException {
        val config = KylinConfig.getInstanceFromEnv();
        if (!config.isUTEnv()) {
            return;
        }
        CreateAdminUserUtils.createAllAdmins(userService, env);
    }

    @ApiOperation(value = "createUser", tags = {
            "MID" }, notes = "Update Body: default_password, locked_time, wrong_time, first_login_failed_time")
    @PostMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    //do not use aclEvaluate, if there's no users and will come into init() and will call save.
    public EnvelopeResponse<String> createUser(@RequestBody ManagedUser user) throws IOException {
        val username = user.getUsername();
        val password = pwdBase64Decode(user.getPassword());
        user.setPassword(password);

        checkUsername(username);
        checkPasswordLength(password);
        checkPasswordCharacter(password);
        checkUserGroupNotEmpty(user.getAuthorities());
        checkUserGroupExists(user.getAuthorities());
        checkUserGroupNotDuplicated(user.getAuthorities());

        return createAdminUser(user);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    //do not use aclEvaluate, if there's no users and will come into init() and will call save.
    public EnvelopeResponse<String> createAdminUser(@RequestBody ManagedUser user) {
        checkProfile();
        user.setUuid(RandomUtil.randomUUIDStr());
        user.setPassword(pwdEncode(user.getPassword()));
        logger.info("Creating user: {}", user);
        completeAuthorities(user);
        user.setDefaultPassword(true);
        userService.createUser(user);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateUser", tags = {
            "MID" }, notes = "Update Body: default_password, locked_time, wrong_time, first_login_failed_time")
    @PutMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    //do not use aclEvaluate, if there's no users and will come into init() and will call save.
    public EnvelopeResponse<String> updateUser(@RequestBody UserRequest userRequest) throws IOException {
        val msg = MsgPicker.getMsg();
        checkProfile();
        val username = userRequest.getUsername();
        checkUsername(username);
        // merge with existing user
        val existing = getManagedUser(username);
        ManagedUser mergeUser = userRequest.updateManager(existing);

        if (StringUtils.equals(getPrincipal(), mergeUser.getUsername()) && mergeUser.isDisabled()) {
            throw new KylinException(FAILED_UPDATE_USER, msg.getSelfDisableForbidden());
        }

        if (StringUtils.equals(getPrincipal(), userRequest.getUsername())) {
            Set<String> userGroupSet = mergeUser.getAuthorities().stream().map(SimpleGrantedAuthority::getAuthority)
                    .collect(Collectors.toSet());
            Set<String> currentUserGroupSet = userGroupService.listUserGroups(mergeUser.getUsername());
            boolean roleChange = !(userGroupSet.size() == currentUserGroupSet.size())
                    || !(userGroupSet.containsAll(currentUserGroupSet));
            if (roleChange) {
                throw new KylinException(FAILED_UPDATE_USER, msg.getSelfEditForbidden());
            }
        }
        if (existing == null) {
            throw new KylinException(USER_NOT_EXIST, String.format(Locale.ROOT, msg.getUserNotFound(), username));
        }
        if (mergeUser.getAuthorities() == null || mergeUser.getAuthorities().isEmpty())
            mergeUser.setGrantedAuthorities(existing.getAuthorities());
        mergeUser.setPassword(pwdBase64Decode(mergeUser.getPassword()));
        if (!mergeUser.isDefaultPassword()) {
            checkPasswordLength(mergeUser.getPassword());
            checkPasswordCharacter(mergeUser.getPassword());
        }
        mergeUser.setPassword(pwdEncode(mergeUser.getPassword()));
        checkUserGroupExists(mergeUser.getAuthorities());
        checkUserGroupNotDuplicated(mergeUser.getAuthorities());
        completeAuthorities(mergeUser);

        boolean noAdminRight = mergeUser.getAuthorities().contains(new SimpleGrantedAuthority(ROLE_ADMIN));
        accessService.checkDefaultAdmin(mergeUser.getUsername(), noAdminRight);

        logger.info("Saving user {}", mergeUser);
        userService.updateUser(mergeUser);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "deleteUser", tags = { "MID" })
    @DeleteMapping(value = "/{uuid:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> deleteByUUID(@PathVariable("uuid") String userUUID) {
        val msg = MsgPicker.getMsg();

        checkProfile();

        ManagedUser toBeDeleteUser = null;
        try {
            toBeDeleteUser = userService.listUsers().parallelStream()
                    .filter(user -> userUUID.equalsIgnoreCase(user.getUuid())).findAny().orElse(null);
        } catch (IOException e) {
            logger.error("List all users is failed!", e);
        }

        if (Objects.isNull(toBeDeleteUser)) {
            throw new KylinException(USER_NOT_EXIST, String.format(Locale.ROOT, msg.getUserNotExist(), userUUID));
        }

        if (StringUtils.equals(getPrincipal(), toBeDeleteUser.getUsername())) {
            throw new KylinException(FAILED_UPDATE_USER, msg.getSelfDeleteForbidden());
        }
        accessService.checkDefaultAdmin(toBeDeleteUser.getUsername(), false);
        //delete user's project ACL
        accessService.revokeProjectPermission(toBeDeleteUser.getUsername(), MetadataConstants.TYPE_USER);
        aclTCRService.revokeAclTCR(toBeDeleteUser.getUsername(), true);
        EventBusFactory.getInstance().postAsync(new AclTCRRevokeEventNotifier(toBeDeleteUser.getUsername(), true));
        userService.deleteUser(toBeDeleteUser.getUsername());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "deleteUser", tags = { "MID" })
    @DeleteMapping(value = "/batch")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> batchDelete(@RequestBody List<String> usernames) throws IOException {
        val msg = MsgPicker.getMsg();

        checkProfile();
        usernames.forEach(this::checkUsername);

        String currentUser = getPrincipal();
        usernames.forEach(username -> {
            if (StringUtils.equals(currentUser, username)) {
                throw new KylinException(FAILED_UPDATE_USER, msg.getSelfDeleteForbidden());
            }
        });
        List<ManagedUser> existedUsers = userService.listUsers();

        List<String> notInList = usernames.stream()
                .filter(t -> existedUsers.stream().map(ManagedUser::getUsername).noneMatch(a -> a.equalsIgnoreCase(t)))
                .collect(Collectors.toList());
        if (notInList.size() > 0) {
            throw new KylinException(USER_NOT_EXIST,
                    String.format(Locale.ROOT, msg.getUserNotFound(), String.join(",", notInList)));
        }

        usernames.forEach(username -> {
            accessService.checkDefaultAdmin(username, false);
            accessService.revokeProjectPermission(username, MetadataConstants.TYPE_USER);
            aclTCRService.revokeAclTCR(username, true);
            EventBusFactory.getInstance().postAsync(new AclTCRRevokeEventNotifier(username, true));
            userService.deleteUser(username);
        });
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "createUser", tags = {
            "MID" }, notes = "Update Body: default_password, locked_time, wrong_time, first_login_failed_time")
    @PostMapping(value = "/batch")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> batchCreate(@RequestBody List<ManagedUser> users) throws IOException {
        for (ManagedUser user : users) {
            checkUsername(user.getUsername());
            val password = pwdBase64Decode(user.getPassword());
            checkPasswordLength(password);
            checkPasswordCharacter(password);
            checkUserGroupNotEmpty(user.getAuthorities());
            checkUserGroupExists(user.getAuthorities());
            checkUserGroupNotDuplicated(user.getAuthorities());
            user.setPassword(password);
            createAdminUser(user);
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    //@DeleteMapping(value = "/{username:.+}")
    //@ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> delete(@PathVariable("username") String username) {
        val msg = MsgPicker.getMsg();

        checkProfile();
        checkUsername(username);

        ManagedUser managedUser = null;
        try {
            managedUser = getManagedUser(username);
        } catch (UsernameNotFoundException e) {
            logger.warn("Delete user failed, user {} not found.", username);
        }
        if (Objects.isNull(managedUser)) {
            throw new KylinException(USER_NOT_EXIST, String.format(Locale.ROOT, msg.getUserNotFound(), username));
        }

        if (StringUtils.equals(getPrincipal(), username)) {
            throw new KylinException(FAILED_UPDATE_USER, msg.getSelfDeleteForbidden());
        }
        accessService.checkDefaultAdmin(username, false);
        //delete user's project ACL
        accessService.revokeProjectPermission(username, MetadataConstants.TYPE_USER);
        aclTCRService.revokeAclTCR(username, true);
        EventBusFactory.getInstance().postAsync(new AclTCRRevokeEventNotifier(username, true));
        userService.deleteUser(username);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "listAllUsers", tags = {
            "MID" }, notes = "Update Param: is_case_sensitive, page_offset, page_size; Update Response: total_size")
    @GetMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<DataResult<List<ManagedUser>>> listAllUsers(
            @RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "is_case_sensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        List<ManagedUser> usersByFuzzyMatching = userService.getManagedUsersByFuzzMatching(nameSeg, isCaseSensitive);
        List<ManagedUser> subList = PagingUtil.cutPage(usersByFuzzyMatching, pageOffset, pageSize);
        //LDAP users dose not have authorities
        for (ManagedUser u : subList) {
            userService.completeUserInfo(u);
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                DataResult.get(subList, usersByFuzzyMatching, pageOffset, pageSize), "");
    }

    @ApiOperation(value = "changePassword", tags = { "MID" })
    @PutMapping(value = "/password")
    @ResponseBody
    //change passwd
    public EnvelopeResponse<String> updateUserPassword(@RequestBody PasswordChangeRequest user) {
        val msg = MsgPicker.getMsg();
        val username = user.getUsername();

        if (!isAdmin() && !StringUtils.equals(getPrincipal(), username)) {
            throw new KylinException(PERMISSION_DENIED, msg.getPermissionDenied());
        }
        accessService.checkDefaultAdmin(username, true);
        val oldPassword = pwdBase64Decode(user.getPassword());
        val newPassword = pwdBase64Decode(user.getNewPassword());

        checkUsername(username);

        checkPasswordLength(newPassword);

        checkPasswordCharacter(newPassword);

        ManagedUser existingUser = getManagedUser(username);
        if (existingUser == null) {
            throw new KylinException(USER_NOT_EXIST, String.format(Locale.ROOT, msg.getUserNotFound(), username));
        }
        val actualOldPassword = existingUser.getPassword();

        // when reset oneself's password (includes ADMIN users), check old password
        if (StringUtils.equals(getPrincipal(), username)) {
            if (!pwdEncoder.matches(oldPassword, actualOldPassword)) {
                throw new KylinException(FAILED_UPDATE_PASSWORD, msg.getOldPasswordWrong());
            }
        }

        if (newPassword.equals(oldPassword)) {
            throw new KylinException(FAILED_UPDATE_PASSWORD, msg.getNewPasswordSameAsOld());
        }

        existingUser.setPassword(pwdEncode(newPassword));
        existingUser.setDefaultPassword(false);

        logger.info("update password for user {}", user);

        existingUser.clearAuthenticateFailedRecord();

        completeAuthorities(existingUser);
        userService.updateUser(existingUser);

        // update authentication
        if (StringUtils.equals(getPrincipal(), user.getUsername())) {
            UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(existingUser,
                    newPassword, existingUser.getAuthorities());
            token.setDetails(SecurityContextHolder.getContext().getAuthentication().getDetails());
            SecurityContextHolder.getContext().setAuthentication(token);
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "auth", tags = { "MID" })
    @PostMapping(value = "/authentication", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<UserDetails> authenticate() {
        EnvelopeResponse<UserDetails> response = authenticatedUser();
        logger.debug("User login: {}", response.getData());
        return response;
    }

    @ApiOperation(value = "updateUser", tags = { "MID" })
    @PostMapping(value = "/update_user")
    @ResponseBody
    public EnvelopeResponse<UserDetails> updateUserWithoutAuth(@RequestBody ManagedUser user) {
        userService.updateUser(user);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    @ApiOperation(value = "authentication", tags = { "MID" })
    @GetMapping(value = "/authentication", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<UserDetails> authenticatedUser() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        UserDetails data;
        val msg = MsgPicker.getMsg();
        if (authentication == null) {
            throw new UnauthorizedException(USER_AUTH_INFO_NOTFOUND);
        }

        if (authentication.getPrincipal() instanceof UserDetails) {
            data = (UserDetails) authentication.getPrincipal();
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
        }

        if (authentication.getDetails() instanceof UserDetails) {
            data = (UserDetails) authentication.getDetails();
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
        }

        throw new UnauthorizedException(USER_AUTH_INFO_NOTFOUND);
    }

    private void checkPasswordCharacter(String password) {
        val msg = MsgPicker.getMsg();
        if (!passwordPattern.matcher(password).matches()) {
            throw new KylinException(INVALID_PASSWORD, msg.getInvalidPassword());
        }
    }

    private void checkProfile() {
        val msg = MsgPicker.getMsg();
        if (!env.acceptsProfiles(PROFILE_DEFAULT, PROFILE_CUSTOM)) {
            throw new KylinException(FAILED_UPDATE_USER, msg.getUserEditNotAllowed());
        }
    }

    private void checkPasswordLength(String password) {
        val msg = MsgPicker.getMsg();
        if (password == null || password.length() < 8)
            throw new KylinException(SHORT_PASSWORD, msg.getShortPassword());
    }

    private void checkUsername(String username) {
        if (env.acceptsProfiles(PROFILE_CUSTOM))
            return;

        val msg = MsgPicker.getMsg();
        if (StringUtils.isEmpty(username)) {
            throw new KylinException(EMPTY_USER_NAME, msg.getEmptyUserName());
        }
        if (username.startsWith(".")) {
            throw new KylinException(INVALID_USER_NAME, msg.getInvalidNameStartWithDot());
        }
        if (!username.equals(username.trim())) {
            throw new KylinException(INVALID_USER_NAME, msg.getInvalidNameStartOrEndWithBlank());
        }
        if (username.length() > 180) {
            throw new KylinException(INVALID_USER_NAME, msg.getInvalidNameLength());
        }
        if (Pattern.compile("[^\\x00-\\xff]").matcher(username).find()) {
            throw new KylinException(INVALID_USER_NAME, msg.getInvalidNameContainsOtherCharacter());
        }
        if (Pattern.compile("[\\\\/:*?\"<>|]").matcher(username).find()) {
            throw new KylinException(INVALID_USER_NAME, msg.getInvalidNameContainsInlegalCharacter());
        }
    }

    private String getPrincipal() {
        String userName;

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null) {
            return null;
        }

        Object principal = authentication.getPrincipal();

        if (principal instanceof UserDetails) {
            userName = ((UserDetails) principal).getUsername();
        } else if (authentication.getDetails() instanceof UserDetails) {
            userName = ((UserDetails) authentication.getDetails()).getUsername();
        } else {
            userName = principal.toString();
        }
        return userName;
    }

    public ManagedUser getManagedUser(String userName) {
        UserDetails details = userService.loadUserByUsername(userName);
        if (details == null)
            return null;
        return (ManagedUser) details;
    }

    private void completeAuthorities(ManagedUser managedUser) {
        List<SimpleGrantedAuthority> detailRoles = Lists.newArrayList(managedUser.getAuthorities());
        if (!detailRoles.contains(ALL_USERS_AUTH)) {
            detailRoles.add(ALL_USERS_AUTH);
        }
        managedUser.setGrantedAuthorities(detailRoles);
    }

    private String pwdEncode(String pwd) {
        if (bcryptPattern.matcher(pwd).matches())
            return pwd;
        return pwdEncoder.encode(pwd);
    }

    /**
     * decode base64 password
     * @param password
     * @return base64 decode password if password is base64 encode else password
     */
    private String pwdBase64Decode(String password) {
        boolean isMatch = base64Pattern.matcher(password).matches();
        if (isMatch) {
            return new String(Base64.decodeBase64(password), Charset.defaultCharset());
        }
        return password;
    }

    private void checkUserGroupNotEmpty(List<SimpleGrantedAuthority> groups) {
        boolean hasEmptyGroup = CollectionUtils.isEmpty(groups)
                || groups.stream().map(SimpleGrantedAuthority::getAuthority).anyMatch(StringUtils::isBlank);
        if (hasEmptyGroup) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getEmptyGroupName());
        }
    }

    private void checkUserGroupExists(List<SimpleGrantedAuthority> groups) throws IOException {
        for (SimpleGrantedAuthority group : groups) {
            if (!userGroupService.exists(group.getAuthority())) {
                throw new KylinException(INVALID_PARAMETER,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getOperationFailedByGroupNotExist(), group));
            }
        }
    }

    private void checkUserGroupNotDuplicated(List<SimpleGrantedAuthority> groups) {
        List<String> authorities = groups.stream().map(SimpleGrantedAuthority::getAuthority)
                .collect(Collectors.toList());
        if (authorities.size() != Sets.newHashSet(authorities).size()) {
            throw new KylinException(INVALID_PARAMETER, "Values in authorities can't be duplicated.");
        }
    }

}
