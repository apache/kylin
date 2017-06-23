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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.rest.service.UserGrantedAuthority;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ManagedUser extends RootPersistentEntity implements UserDetails {

    @JsonProperty
    private String username;
    @JsonProperty
    private String password;
    @JsonProperty
    private List<UserGrantedAuthority> authorities = Lists.newArrayList();
    @JsonProperty
    private boolean disabled = false;
    @JsonProperty
    private boolean defaultPassword = false;
    @JsonProperty
    private boolean locked = false;
    @JsonProperty
    private long lockedTime = 0L;
    @JsonProperty
    private int wrongTime = 0;

    //DISABLED_ROLE is a ancient way to represent disabled user
    //now we no longer support such way, however legacy metadata may still contain it
    private static final String DISABLED_ROLE = "--disabled--";

    public ManagedUser() {
    }

    public ManagedUser(@JsonProperty String username, @JsonProperty String password,
            @JsonProperty List<UserGrantedAuthority> authorities, @JsonProperty boolean disabled,
            @JsonProperty boolean defaultPassword, @JsonProperty boolean locked, @JsonProperty long lockedTime,
            @JsonProperty int wrongTime) {
        this.username = username;
        this.password = password;
        this.authorities = authorities;
        this.disabled = disabled;
        this.defaultPassword = defaultPassword;
        this.locked = locked;
        this.lockedTime = lockedTime;
        this.wrongTime = wrongTime;

        caterLegacy();
    }

    public ManagedUser(String username, String password, Boolean defaultPassword, String... authoritiesStr) {
        this.username = username;
        this.password = password;
        this.setDefaultPassword(defaultPassword);

        this.authorities = Lists.newArrayList();
        for (String a : authoritiesStr) {
            authorities.add(new UserGrantedAuthority(a));
        }

        caterLegacy();
    }

    public ManagedUser(String username, String password, Boolean defaultPassword,
            Collection<? extends GrantedAuthority> grantedAuthorities) {
        this.username = username;
        this.password = password;
        this.setDefaultPassword(defaultPassword);

        this.setGrantedAuthorities(grantedAuthorities);

        caterLegacy();
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String userName) {
        this.username = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    private void caterLegacy() {
        Iterator<UserGrantedAuthority> iterator = authorities.iterator();
        while (iterator.hasNext()) {
            if (DISABLED_ROLE.equals(iterator.next().getAuthority())) {
                iterator.remove();
                this.disabled = true;
            }
        }
    }

    public List<UserGrantedAuthority> getAuthorities() {
        return this.authorities;
    }

    public void setGrantedAuthorities(Collection<? extends GrantedAuthority> grantedAuthorities) {
        this.authorities = Lists.newArrayList();
        for (GrantedAuthority grantedAuthority : grantedAuthorities) {
            this.authorities.add(new UserGrantedAuthority(grantedAuthority.getAuthority()));
        }
    }

    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    public boolean isDefaultPassword() {
        return defaultPassword;
    }

    public void setDefaultPassword(boolean defaultPassword) {
        this.defaultPassword = defaultPassword;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public int getWrongTime() {
        return wrongTime;
    }

    public long getLockedTime() {
        return lockedTime;
    }

    public void increaseWrongTime() {
        int wrongTime = this.getWrongTime();
        if (wrongTime == 2) {
            this.setLocked(true);
            this.lockedTime = System.currentTimeMillis();
            this.wrongTime = 0;
        } else {
            this.wrongTime = wrongTime + 1;
        }
    }

    @Override
    public boolean isAccountNonExpired() {
        return true;
    }

    @Override
    public boolean isAccountNonLocked() {
        return !locked;
    }

    @Override
    public boolean isCredentialsNonExpired() {
        return true;
    }

    @Override
    public boolean isEnabled() {
        return !disabled;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((username == null) ? 0 : username.hashCode());
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
        ManagedUser other = (ManagedUser) obj;
        if (username == null) {
            if (other.username != null)
                return false;
        } else if (!username.equals(other.username))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ManagedUser [username=" + username + ", authorities=" + authorities + "]";
    }
}
