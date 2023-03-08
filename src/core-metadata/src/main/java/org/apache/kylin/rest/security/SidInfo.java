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

import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.Sid;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class SidInfo {

    @JsonProperty("sid")
    private String sid;
    @JsonProperty("principal")
    private boolean isPrincipal;
    @JsonIgnore
    private Sid sidObj;

    // for Jackson
    public SidInfo() {
    }

    public SidInfo(Sid sid) {
        if (sid instanceof PrincipalSid) {
            this.sid = ((PrincipalSid) sid).getPrincipal();
            this.isPrincipal = true;
        } else if (sid instanceof GrantedAuthoritySid) {
            this.sid = ((GrantedAuthoritySid) sid).getGrantedAuthority();
            this.isPrincipal = false;
        } else
            throw new IllegalStateException();
    }

    public String getSid() {
        return sid;
    }

    public boolean isPrincipal() {
        return isPrincipal;
    }

    public Sid getSidObj() {
        if (sidObj == null) {
            sidObj = isPrincipal ? new PrincipalSid(sid) : new GrantedAuthoritySid(sid);
        }
        return sidObj;
    }
}
