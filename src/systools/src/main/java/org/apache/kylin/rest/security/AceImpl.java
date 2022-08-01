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

/**
 * Mimic org.springframework.security.acls.domain.ObjectIdentityImpl
 * Make it Jackson friendly.
 */
package org.apache.kylin.rest.security;

import java.io.Serializable;
import java.util.Comparator;

import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class AceImpl implements AccessControlEntry {

    public static final Comparator<AceImpl> SID_ORDER = new Comparator<AceImpl>() {
        @Override
        public int compare(AceImpl o1, AceImpl o2) {
            if (o1.sidOfAuthority == null) {
                return o2.sidOfAuthority == null ? o1.sidOfPrincipal.compareTo(o2.sidOfPrincipal) : 1;
            } else {
                return o2.sidOfAuthority == null ? -1 : o1.sidOfAuthority.compareTo(o2.sidOfAuthority);
            }
        }
    };

    // ~ Instance fields
    // ================================================================================================

    @JsonProperty("p")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String sidOfPrincipal;
    @JsonProperty("a")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String sidOfAuthority;
    @JsonProperty("m")
    private int permissionMask;

    // non-persistent fields
    private AclRecord acl;
    private int indexInAcl;
    private Sid sid;
    private Permission perm;

    // ~ Constructors
    // ===================================================================================================

    // for Jackson
    public AceImpl() {
    }

    public AceImpl(LegacyAceInfo legacy) {
        this(legacy.getSidInfo(), legacy.getPermissionMask());
    }

    public AceImpl(Sid sid, Permission perm) {
        this(new SidInfo(sid), perm == null ? 0 : perm.getMask());
    }

    public AceImpl(SidInfo sidInfo, int permMask) {
        if (sidInfo.isPrincipal())
            sidOfPrincipal = sidInfo.getSid();
        else
            sidOfAuthority = sidInfo.getSid();

        permissionMask = permMask;
    }

    void init(AclRecord acl, int index) {
        this.acl = acl;
        this.indexInAcl = index;
    }

    // ~ Methods
    // ========================================================================================================

    @Override
    public Acl getAcl() {
        return acl;
    }

    @Override
    public Serializable getId() {
        return indexInAcl;
    }

    @Override
    public Permission getPermission() {
        if (perm == null) {
            perm = acl.aclPermissionFactory.buildFromMask(permissionMask);
        }
        return perm;
    }

    void setPermission(Permission perm) {
        this.permissionMask = perm.getMask();
        this.perm = null;
    }

    public int getPermissionMask() {
        return permissionMask;
    }

    @Override
    public Sid getSid() {
        if (sid == null) {
            if (sidOfPrincipal != null)
                sid = new PrincipalSid(sidOfPrincipal);
            else if (sidOfAuthority != null)
                sid = new GrantedAuthoritySid(sidOfAuthority);
            else
                throw new IllegalStateException();
        }
        return sid;
    }

    @Override
    public boolean isGranting() {
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + permissionMask;
        result = prime * result + ((sidOfAuthority == null) ? 0 : sidOfAuthority.hashCode());
        result = prime * result + ((sidOfPrincipal == null) ? 0 : sidOfPrincipal.hashCode());
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
        AceImpl other = (AceImpl) obj;
        if (permissionMask != other.permissionMask)
            return false;
        if (sidOfAuthority == null) {
            if (other.sidOfAuthority != null)
                return false;
        } else if (!sidOfAuthority.equals(other.sidOfAuthority))
            return false;
        if (sidOfPrincipal == null) {
            if (other.sidOfPrincipal != null)
                return false;
        } else if (!sidOfPrincipal.equals(other.sidOfPrincipal))
            return false;
        return true;
    }

}
