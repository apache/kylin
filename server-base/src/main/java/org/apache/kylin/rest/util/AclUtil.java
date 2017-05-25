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

import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.service.AccessService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component("aclUtil")
public class AclUtil {
    private static final Logger logger = LoggerFactory.getLogger(AclUtil.class);

    @Autowired
    private AccessService accessService;

    /**
     * @return <tt>true</tt> the current user has permission to query the cube, otherwise
     * <tt>false</tt>
     */
    public boolean isHasCubePermission(CubeInstance cube) {
        boolean hasCubePermission = false;
        String userName = getCurrentUser().getUsername();
        List<String> userAuthority = getAuthorityList();

        //check if ROLE_ADMIN
        for (String auth : userAuthority) {
            if (auth.equals(Constant.ROLE_ADMIN)) {
                return true;
            }
        }

        AclEntity cubeAe = accessService.getAclEntity("CubeInstance", cube.getId());
        Acl cubeAcl = accessService.getAcl(cubeAe);
        //cube Acl info
        if (cubeAcl != null) {
            if (((PrincipalSid) cubeAcl.getOwner()).getPrincipal().equals(userName)) {
                hasCubePermission = true;
            }

            for (AccessControlEntry cubeAce : cubeAcl.getEntries()) {
                if (cubeAce.getSid() instanceof PrincipalSid && ((PrincipalSid) cubeAce.getSid()).getPrincipal().equals(userName)) {
                    hasCubePermission = true;
                    break;
                } else if (cubeAce.getSid() instanceof GrantedAuthoritySid) {
                    String cubeAuthority = ((GrantedAuthoritySid) cubeAce.getSid()).getGrantedAuthority();
                    if (userAuthority.contains(cubeAuthority)) {
                        hasCubePermission = true;
                        break;
                    }

                }
            }
        }
        return hasCubePermission;
    }

    public UserDetails getCurrentUser() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        UserDetails userDetails = null;
        if (authentication == null) {
            logger.debug("authentication is null.");
            throw new InternalErrorException("Can not find authentication infomation.");
        }
        if (authentication.getPrincipal() instanceof UserDetails) {
            logger.debug("authentication.getPrincipal() is " + authentication.getPrincipal());
            userDetails = (UserDetails) authentication.getPrincipal();
        }
        if (authentication.getDetails() instanceof UserDetails) {
            logger.debug("authentication.getDetails() is " + authentication.getDetails());
            userDetails = (UserDetails) authentication.getDetails();
        }
        return userDetails;
    }

    public List<String> getAuthorityList() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        List<String> userAuthority = new ArrayList<>();
        for (GrantedAuthority auth : authentication.getAuthorities()) {
            userAuthority.add(auth.getAuthority());
        }
        return userAuthority;
    }
}
