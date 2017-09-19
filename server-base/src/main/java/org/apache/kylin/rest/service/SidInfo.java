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

import java.io.Serializable;

import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.Sid;

/**
 * Created by xiefan on 17-5-2.
 */
class SidInfo implements Serializable {
    private String sid;
    private boolean isPrincipal;

    public SidInfo() {
    }

    public SidInfo(Sid sid) {
        if (sid instanceof PrincipalSid) {
            this.sid = ((PrincipalSid) sid).getPrincipal();
            this.isPrincipal = true;
        } else if (sid instanceof GrantedAuthoritySid) {
            this.sid = ((GrantedAuthoritySid) sid).getGrantedAuthority();
            this.isPrincipal = false;
        }
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public boolean isPrincipal() {
        return isPrincipal;
    }

    public void setPrincipal(boolean isPrincipal) {
        this.isPrincipal = isPrincipal;
    }

}
