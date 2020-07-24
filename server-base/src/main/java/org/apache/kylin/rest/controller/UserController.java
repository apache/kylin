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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.request.PasswdChangeRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.UserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.PagingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 * Handle user authentication request to protected kylin rest resources by
 * spring security.
 * 
 * @author xduo
 * 
 */
@Controller
@RequestMapping(value = "/user")
public class UserController extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(UserController.class);

    private static final SimpleGrantedAuthority ALL_USERS_AUTH = new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS);

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("userGroupService")
    private UserGroupService userGroupService;

    private Pattern passwordPattern;
    private Pattern bcryptPattern;
    private BCryptPasswordEncoder pwdEncoder;

    @RequestMapping(value = "/authentication", method = RequestMethod.POST, produces = { "application/json" })
    public UserDetails authenticate() {
        UserDetails userDetails = authenticatedUser();
        logger.debug("User login: {}", userDetails);
        return userDetails;
    }

    @RequestMapping(value = "/authentication", method = RequestMethod.GET, produces = { "application/json" })
    public UserDetails authenticatedUser() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication == null) {
            logger.debug("authentication is null.");
            return null;
        }

        if (authentication.getPrincipal() instanceof UserDetails) {
            return (UserDetails) authentication.getPrincipal();
        }

        if (authentication.getDetails() instanceof UserDetails) {
            return (UserDetails) authentication.getDetails();
        }

        return null;
    }

    @PostConstruct
    public void init() throws IOException {
        passwordPattern = Pattern.compile("^(?=.*\\d)(?=.*[a-zA-Z])(?=.*[~!@#$%^&*(){}|:\"<>?\\[\\];',./`]).{8,}$");
        bcryptPattern = Pattern.compile("\\A\\$2a?\\$\\d\\d\\$[./0-9A-Za-z]{53}");
        pwdEncoder = new BCryptPasswordEncoder();
    }

    private void checkProfileEditAllowed() {
        String securityProfile = KylinConfig.getInstanceFromEnv().getSecurityProfile();
        if (!"testing".equals(securityProfile) && !"custom".equals(securityProfile)) {
            throw new BadRequestException("Action not allowed!");
        }
    }

    @RequestMapping(value = "/{userName:.+}", method = { RequestMethod.POST }, produces = { "application/json" })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    //do not use aclEvaluate, if getManagedUsersByFuzzMatching there's no users and will come into init() and will call save.
    public ManagedUser create(@PathVariable("userName") String userName, @RequestBody ManagedUser user) {
        checkProfileEditAllowed();

        if (StringUtils.equals(getPrincipal(), user.getUsername()) && user.isDisabled()) {
            throw new ForbiddenException("Action not allowed!");
        }

        checkUserName(userName);

        user.setUsername(userName);
        user.setPassword(pwdEncode(user.getPassword()));

        logger.info("Creating {}", user);

        completeAuthorities(user);
        userService.createUser(user);
        return get(userName);
    }

    @RequestMapping(value = "/{userName:.+}", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    //do not use aclEvaluate, if there's no users and will come into init() and will call save.
    public ManagedUser save(@PathVariable("userName") String userName, @RequestBody ManagedUser user) {
        checkProfileEditAllowed();

        if (StringUtils.equals(getPrincipal(), user.getUsername()) && user.isDisabled()) {
            throw new ForbiddenException("Action not allowed!");
        }

        checkUserName(userName);

        user.setUsername(userName);

        // merge with existing user
        try {
            ManagedUser existing = get(userName);
            if (existing != null) {
                if (user.getPassword() == null)
                    user.setPassword(existing.getPassword());
                if (user.getAuthorities() == null || user.getAuthorities().isEmpty())
                    user.setGrantedAuthorities(existing.getAuthorities());
            }
        } catch (UsernameNotFoundException ex) {
            // that is OK, we create new
        }
        logger.info("Saving {}", user);

        user.setPassword(pwdEncode(user.getPassword()));

        completeAuthorities(user);
        userService.updateUser(user);
        return get(userName);
    }

    @RequestMapping(value = "/password", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    //change passwd
    public EnvelopeResponse save(@RequestBody PasswdChangeRequest user) {

        checkProfileEditAllowed();

        if (!this.isAdmin() && !StringUtils.equals(getPrincipal(), user.getUsername())) {
            throw new ForbiddenException("Permission Denied");
        }
        ManagedUser existing = get(user.getUsername());
        checkUserName(user.getUsername());
        checkNewPwdRule(user.getNewPassword());

        if (existing != null) {
            if (!this.isAdmin() && !pwdEncoder.matches(user.getPassword(), existing.getPassword())) {
                throw new BadRequestException("pwd update error");
            }

            existing.setPassword(pwdEncode(user.getNewPassword()));
            existing.setDefaultPassword(false);
            logger.info("update password for user {}", user);

            completeAuthorities(existing);
            userService.updateUser(existing);

            // update authentication
            if (StringUtils.equals(getPrincipal(), user.getUsername())) {
                UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(existing,
                        user.getNewPassword(), existing.getAuthorities());
                token.setDetails(SecurityContextHolder.getContext().getAuthentication().getDetails());
                SecurityContextHolder.getContext().setAuthentication(token);
            }
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, get(user.getUsername()), "");
    }

    private String pwdEncode(String pwd) {
        if (bcryptPattern.matcher(pwd).matches())
            return pwd;

        return pwdEncoder.encode(pwd);
    }

    private void checkUserName(String userName) {
        if (userName == null || userName.isEmpty())
            throw new BadRequestException("empty user name");
    }

    private void checkNewPwdRule(String newPwd) {
        if (!checkPasswordLength(newPwd)) {
            throw new BadRequestException("password length need more then 8 chars");
        }

        if (!checkPasswordCharacter(newPwd)) {
            throw new BadRequestException("pwd update error");
        }
    }

    private boolean checkPasswordLength(String password) {
        return !(password == null || password.length() < 8);
    }

    private boolean checkPasswordCharacter(String password) {
        return passwordPattern.matcher(password).matches();
    }

    @RequestMapping(value = "/{userName:.+}", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public EnvelopeResponse getUser(@PathVariable("userName") String userName) {

        if (!this.isAdmin() && !StringUtils.equals(getPrincipal(), userName)) {
            throw new ForbiddenException("...");
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, get(userName), "");
    }

    private ManagedUser get(@Nullable String userName) {
        checkUserName(userName);

        UserDetails details = userService.loadUserByUsername(userName);
        if (details == null)
            return null;
        return (ManagedUser) details;
    }

    @RequestMapping(value = "/users", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public EnvelopeResponse listAllUsers(@RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "name", required = false) String name,
            @RequestParam(value = "groupName", required = false) String groupName,
            @RequestParam(value = "isFuzzMatch", required = false) boolean isFuzzMatch,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) throws IOException {
        if (project == null) {
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            aclEvaluate.checkProjectAdminPermission(project);
        }
        HashMap<String, Object> data = new HashMap<>();
        List<ManagedUser> usersByFuzzMatching = userService.listUsers(name, groupName, isFuzzMatch);
        List<ManagedUser> subList = PagingUtil.cutPage(usersByFuzzMatching, offset, limit);
        //LDAP users dose not have authorities
        for (ManagedUser u : subList) {
            userService.completeUserInfo(u);
        }
        data.put("users", subList);
        data.put("size", usersByFuzzMatching.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/{userName:.+}", method = { RequestMethod.DELETE }, produces = { "application/json" })
    @ResponseBody
    public EnvelopeResponse delete(@PathVariable("userName") String userName) throws IOException {

        checkProfileEditAllowed();

        if (StringUtils.equals(getPrincipal(), userName)) {
            throw new ForbiddenException("...");
        }

        //delete user's project ACL
        accessService.revokeProjectPermission(userName, MetadataConstants.TYPE_USER);

        //delete user's table/row/column ACL
        //        ACLOperationUtil.delLowLevelACL(userName, MetadataConstants.TYPE_USER);

        checkUserName(userName);
        userService.deleteUser(userName);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, userName, "");
    }

    private void completeAuthorities(ManagedUser managedUser) {
        List<SimpleGrantedAuthority> detailRoles = Lists.newArrayList(managedUser.getAuthorities());
        for (SimpleGrantedAuthority authority : detailRoles) {
            try {
                if (!userGroupService.exists(authority.getAuthority())) {
                    throw new BadRequestException(String.format(Locale.ROOT,
                            "user's authority:%s is not found in user group", authority.getAuthority()));
                }
            } catch (IOException e) {
                logger.error("Get user group error: {}", e.getMessage());
            }
        }
        if (!detailRoles.contains(ALL_USERS_AUTH)) {
            detailRoles.add(ALL_USERS_AUTH);
        }
        managedUser.setGrantedAuthorities(detailRoles);
    }

    private String getPrincipal() {
        String userName = null;

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
}
