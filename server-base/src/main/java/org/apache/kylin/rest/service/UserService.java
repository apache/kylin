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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.provisioning.UserDetailsManager;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
@Component("userService")
public class UserService implements UserDetailsManager {

    private Logger logger = LoggerFactory.getLogger(UserService.class);

    public static final String DIR_PREFIX = "/user/";

    protected ResourceStore aclStore;

    @PostConstruct
    public void init() throws IOException {
        aclStore = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        logger.debug("UserService init");
    }

    @Override
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void createUser(UserDetails user) {
        updateUser(user);
    }

    @Override
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void updateUser(UserDetails user) {
        try {
            deleteUser(user.getUsername());
            String id = getId(user.getUsername());
            aclStore.putResource(id, new UserInfo(user), 0, UserInfoSerializer.getInstance());
            logger.debug("update user : {}", user.getUsername());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteUser(String userName) {
        try {
            String id = getId(userName);
            aclStore.deleteResource(id);
            logger.debug("delete user : {}", userName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void changePassword(String oldPassword, String newPasswor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean userExists(String userName) {
        try {
            logger.debug("judge user exist: {}", userName);
            return aclStore.exists(getId(userName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public UserDetails loadUserByUsername(String userName) throws UsernameNotFoundException {
        try {
            UserInfo userInfo = aclStore.getResource(getId(userName), UserInfo.class, UserInfoSerializer.getInstance());
            logger.debug("load user : {}", userName);
            return wrap(userInfo);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> listUserAuthorities() {
        List<String> all = new ArrayList<String>();
        for (UserDetails user : listUsers()) {
            for (GrantedAuthority auth : user.getAuthorities()) {
                if (!all.contains(auth.getAuthority())) {
                    all.add(auth.getAuthority());
                }
            }
        }
        return all;
    }

    public List<UserDetails> listUsers() {
        List<UserDetails> all = new ArrayList<UserDetails>();
        try {
            List<UserInfo> userInfos = aclStore.getAllResources(DIR_PREFIX, UserInfo.class, UserInfoSerializer.getInstance());
            for (UserInfo info : userInfos) {
                all.add(wrap(info));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to list users", e);
        }
        return all;
    }

    public static String getId(String userName) {
        return DIR_PREFIX + userName;
    }

    private User wrap(UserInfo userInfo) {
        List<GrantedAuthority> authorities = new ArrayList<>();
        List<String> auths = userInfo.getAuthorities();
        for (String str : auths) {
            authorities.add(new UserGrantedAuthority(str));
        }
        return new User(userInfo.getUsername(), userInfo.getPassword(), authorities);
    }

    public static class UserInfoSerializer implements Serializer<UserInfo> {

        private static final UserInfoSerializer serializer = new UserInfoSerializer();

        private UserInfoSerializer() {

        }

        public static UserInfoSerializer getInstance() {
            return serializer;
        }

        @Override
        public void serialize(UserInfo userInfo, DataOutputStream out) throws IOException {
            String json = JsonUtil.writeValueAsString(userInfo);
            out.writeUTF(json);
        }

        @Override
        public UserInfo deserialize(DataInputStream in) throws IOException {
            String json = in.readUTF();
            return JsonUtil.readValue(json, UserInfo.class);
        }
    }

}

class UserInfo extends RootPersistentEntity {
    @JsonProperty()
    private String username;
    @JsonProperty()
    private String password;
    @JsonProperty()
    private List<String> authorities = new ArrayList<>();

    public UserInfo(String username, String password, List<String> authorities) {
        this.username = username;
        this.password = password;
        this.authorities = authorities;
    }

    public UserInfo(UserDetails user) {
        this.username = user.getUsername();
        this.password = user.getPassword();
        for (GrantedAuthority a : user.getAuthorities()) {
            this.authorities.add(a.getAuthority());
        }
    }

    public UserInfo() {
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<String> getAuthorities() {
        return authorities;
    }

    public void setAuthorities(List<String> authorities) {
        this.authorities = authorities;
    }

}

class UserGrantedAuthority implements GrantedAuthority {
    private static final long serialVersionUID = -5128905636841891058L;

    private String authority;

    public UserGrantedAuthority() {
    }

    public UserGrantedAuthority(String authority) {
        setAuthority(authority);
    }

    @Override
    public String getAuthority() {
        return authority;
    }

    public void setAuthority(String authority) {
        this.authority = authority;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((authority == null) ? 0 : authority.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        UserGrantedAuthority other = (UserGrantedAuthority) obj;
        if (authority == null) {
            if (other.authority != null)
                return false;
        } else if (!authority.equals(other.authority))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return authority;
    }
}
