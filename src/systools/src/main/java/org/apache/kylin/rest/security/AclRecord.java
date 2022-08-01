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

package org.apache.kylin.rest.security;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.OwnershipAcl;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.acls.model.UnloadedSidException;
import org.springframework.util.Assert;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class AclRecord extends RootPersistentEntity implements Acl, OwnershipAcl {

    // ~ Instance fields
    // ================================================================================================

    // non-persistent fields
    PermissionFactory aclPermissionFactory;
    @JsonProperty("domainObjectInfo")
    private ObjectIdentityImpl domainObjectInfo;
    @JsonProperty("parentDomainObjectInfo")
    private ObjectIdentityImpl parentDomainObjectInfo;
    @JsonProperty("ownerInfo")
    private SidInfo ownerInfo;
    @JsonProperty("entriesInheriting")
    private boolean entriesInheriting;
    @JsonProperty("entries")
    private List<AceImpl> entries; // the list is ordered by SID, so that grant/revoke by SID is fast
    @JsonProperty("allAceInfo")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, LegacyAceInfo> legacyAceInfo;
    private PermissionGrantingStrategy permissionGrantingStrategy;
    private Acl parentAcl;

    // ~ Constructor / Getter / Setter
    // ================================================================================================

    // for Jackson
    public AclRecord() {
    }

    // new empty ACL
    public AclRecord(ObjectIdentity oid, Sid owner) {
        this.domainObjectInfo = new ObjectIdentityImpl(oid);
        this.ownerInfo = new SidInfo(owner);
    }

    public void init(Acl parentAcl, PermissionFactory aclPermissionFactory,
            PermissionGrantingStrategy permissionGrantingStrategy) {
        this.aclPermissionFactory = aclPermissionFactory;
        this.permissionGrantingStrategy = permissionGrantingStrategy;
        this.parentAcl = parentAcl;

        if (entries == null)
            entries = new ArrayList<>();

        // convert legacy ace
        if (legacyAceInfo != null) {
            for (LegacyAceInfo legacy : legacyAceInfo.values()) {
                entries.add(new AceImpl(legacy));
            }
            legacyAceInfo = null;
        }

        for (int i = 0; i < entries.size(); i++) {
            entries.get(i).init(this, i);
        }

        entries.sort(AceImpl.SID_ORDER);
    }

    @Override
    public String resourceName() {
        return String.valueOf(domainObjectInfo.getIdentifier());
    }

    public SidInfo getOwnerInfo() {
        return ownerInfo;
    }

    public void setOwnerInfo(SidInfo ownerInfo) {
        this.ownerInfo = ownerInfo;
    }

    public boolean isEntriesInheriting() {
        return entriesInheriting;
    }

    public void setEntriesInheriting(boolean entriesInheriting) {
        this.entriesInheriting = entriesInheriting;
    }

    public ObjectIdentityImpl getDomainObjectInfo() {
        return domainObjectInfo;
    }

    public void setDomainObjectInfo(ObjectIdentityImpl domainObjectInfo) {
        this.domainObjectInfo = domainObjectInfo;
    }

    public ObjectIdentityImpl getParentDomainObjectInfo() {
        return parentDomainObjectInfo;
    }

    public void setParentDomainObjectInfo(ObjectIdentityImpl parentDomainObjectInfo) {
        this.parentDomainObjectInfo = parentDomainObjectInfo;
    }

    public Map<String, LegacyAceInfo> getAllAceInfo() {
        return legacyAceInfo;
    }

    public void setAllAceInfo(Map<String, LegacyAceInfo> allAceInfo) {
        this.legacyAceInfo = allAceInfo;
    }

    // ~ Methods
    // ========================================================================================================

    @Override
    public ObjectIdentity getObjectIdentity() {
        return domainObjectInfo;
    }

    @Override
    public Sid getOwner() {
        return ownerInfo.getSidObj();
    }

    @Override
    public void setOwner(Sid newOwner) {
        ownerInfo = new SidInfo(newOwner);
    }

    @Override
    public Acl getParentAcl() {
        return parentAcl;
    }

    @Override
    public void setParent(Acl newParent) {
        AclRecord newP = newParent instanceof MutableAclRecord //
                ? ((MutableAclRecord) newParent).getAclRecord()
                : (AclRecord) newParent;
        parentDomainObjectInfo = newP.domainObjectInfo;
        parentAcl = newP;
    }

    @Override
    public List<AccessControlEntry> getEntries() {
        return new ArrayList<AccessControlEntry>(entries);
    }

    public AccessControlEntry getAccessControlEntryAt(int entryIndex) {
        return entries.get(entryIndex);
    }

    public Sid getAceBySidAndPrincipal(String name, boolean principal) {
        List<Sid> sids = entries.stream().map(AceImpl::getSid).collect(Collectors.toList());
        for (Sid sid : sids) {
            if (principal && (sid instanceof PrincipalSid)
                    && name.equalsIgnoreCase(((PrincipalSid) sid).getPrincipal())) {
                return sid;
            }
            if (!principal && (sid instanceof GrantedAuthoritySid)
                    && Objects.equals(name, ((GrantedAuthoritySid) sid).getGrantedAuthority())) {
                return sid;
            }
        }
        return null;
    }

    public Permission getPermission(Sid sid) {
        synchronized (entries) {
            int p = Collections.binarySearch(entries, new AceImpl(sid, null), AceImpl.SID_ORDER);
            if (p >= 0) {
                return entries.get(p).getPermission();
            }
            return null;
        }
    }

    public void upsertAce(Permission permission, Sid sid) {
        Assert.notNull(sid, "Sid required");

        AceImpl ace = new AceImpl(sid, permission);
        synchronized (entries) {
            int p = Collections.binarySearch(entries, ace, AceImpl.SID_ORDER);
            if (p >= 0) {
                if (permission == null) // null permission means delete
                    entries.remove(p);
                else
                    entries.get(p).setPermission(permission);
            } else {
                if (permission != null) { // if not delete
                    ace.init(this, entries.size());
                    entries.add(-p - 1, ace);
                }
            }
        }
    }

    public void deleteAce(Sid sid) {
        upsertAce(null, sid);
    }

    @Override
    public void insertAce(int atIndexLocation, Permission permission, Sid sid, boolean granting)
            throws NotFoundException {
        Assert.state(granting, "Granting must be true");

        // entries are strictly ordered, given index is ignored
        upsertAce(permission, sid);
    }

    @Override
    public void updateAce(int aceIndex, Permission permission) throws NotFoundException {
        verifyAceIndexExists(aceIndex);

        synchronized (entries) {
            AceImpl ace = entries.get(aceIndex);
            ace.setPermission(permission);
        }
    }

    @Override
    public void deleteAce(int aceIndex) throws NotFoundException {
        verifyAceIndexExists(aceIndex);

        synchronized (entries) {
            entries.remove(aceIndex);
        }
    }

    private void verifyAceIndexExists(int aceIndex) {
        if (aceIndex < 0) {
            throw new NotFoundException("aceIndex must be greater than or equal to zero");
        }
        if (aceIndex >= this.entries.size()) {
            throw new NotFoundException("aceIndex must refer to an index of the AccessControlEntry list. "
                    + "List size is " + entries.size() + ", index was " + aceIndex);
        }
    }

    @Override
    public boolean isGranted(List<Permission> permission, List<Sid> sids, boolean administrativeMode)
            throws NotFoundException, UnloadedSidException {
        Assert.notEmpty(sids, "SIDs required");
        Assert.notEmpty(permission, "Permissions required");

        return permissionGrantingStrategy.isGranted(this, permission, sids, administrativeMode);
    }

    @Override
    public boolean isSidLoaded(List<Sid> sids) {
        // don't support sid filtering yet
        return true;
    }

    @Override
    public String getResourcePath() {
        return ResourceStore.ACL_ROOT + "/" + uuid;
    }
}
