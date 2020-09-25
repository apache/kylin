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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.rest.service.UserGrantedAuthority;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ManagedUser extends RootPersistentEntity implements UserDetails {

    @JsonProperty
    private String username;
    @JsonProperty
    private String password;
    @JsonProperty
    @JsonSerialize(using = SimpleGrantedAuthoritySerializer.class)
    @JsonDeserialize(using = SimpleGrantedAuthorityDeserializer.class)
    private List<SimpleGrantedAuthority> authorities = Lists.newArrayList();
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
            @JsonProperty List<SimpleGrantedAuthority> authorities, @JsonProperty boolean disabled,
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
            authorities.add(new SimpleGrantedAuthority(a));
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

    @Override
    public String resourceName() {
        return username;
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
        Iterator<SimpleGrantedAuthority> iterator = authorities.iterator();
        while (iterator.hasNext()) {
            if (DISABLED_ROLE.equals(iterator.next().getAuthority())) {
                iterator.remove();
                this.disabled = true;
            }
        }
    }

    public List<SimpleGrantedAuthority> getAuthorities() {
        return this.authorities;
    }

    public void setGrantedAuthorities(Collection<? extends GrantedAuthority> grantedAuthorities) {
        this.authorities = Lists.newArrayList();
        for (GrantedAuthority grantedAuthority : grantedAuthorities) {
            this.authorities.add(new SimpleGrantedAuthority(grantedAuthority.getAuthority()));
        }
    }

    public void addAuthorities(String auth) {
        if (this.authorities == null) {
            this.authorities = Lists.newArrayList();
        }
        authorities.add(new SimpleGrantedAuthority(auth));
    }

    public void removeAuthorities(String auth) {
        Preconditions.checkNotNull(this.authorities);
        authorities.remove(new SimpleGrantedAuthority(auth));
    }

    public void clearAuthenticateFailedRecord() {
        this.wrongTime = 0;
        this.locked = false;
        this.lockedTime = 0;
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
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ManagedUser that = (ManagedUser) o;
        return disabled == that.disabled && defaultPassword == that.defaultPassword && locked == that.locked
                && lockedTime == that.lockedTime && wrongTime == that.wrongTime
                && username.equalsIgnoreCase(that.username) && Objects.equals(password, that.password)
                && Objects.equals(authorities, that.authorities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), username, password, authorities, disabled, defaultPassword, locked,
                lockedTime, wrongTime);
    }

    @Override
    public String toString() {
        return "ManagedUser [username=" + username + ", authorities=" + authorities + "]";
    }

    private static class SimpleGrantedAuthoritySerializer extends JsonSerializer<List<SimpleGrantedAuthority>> {

        @Override
        public void serialize(List<SimpleGrantedAuthority> value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException, JsonProcessingException {
            List<UserGrantedAuthority> ugaList = Lists.newArrayList();
            for (SimpleGrantedAuthority sga : value) {
                ugaList.add(new UserGrantedAuthority(sga.getAuthority()));
            }

            gen.writeObject(ugaList);
        }
    }

    private static class SimpleGrantedAuthorityDeserializer extends JsonDeserializer<List<SimpleGrantedAuthority>> {

        @Override
        public List<SimpleGrantedAuthority> deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            UserGrantedAuthority[] ugaArray = p.readValueAs(UserGrantedAuthority[].class);
            List<SimpleGrantedAuthority> sgaList = Lists.newArrayList();
            for (UserGrantedAuthority uga : ugaArray) {
                sgaList.add(new SimpleGrantedAuthority(uga.getAuthority()));
            }

            return sgaList;
        }
    }
}
