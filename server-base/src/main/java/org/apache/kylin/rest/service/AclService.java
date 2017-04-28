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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.acls.domain.AccessControlEntryImpl;
import org.springframework.security.acls.domain.AclAuthorizationStrategy;
import org.springframework.security.acls.domain.AclImpl;
import org.springframework.security.acls.domain.AuditLogger;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.AlreadyExistsException;
import org.springframework.security.acls.model.ChildrenExistException;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.util.FieldUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

@Component("aclService")
public class AclService implements MutableAclService {

    private static final Logger logger = LoggerFactory.getLogger(AclService.class);

    private final Field fieldAces = FieldUtils.getField(AclImpl.class, "aces");

    private final Field fieldAcl = FieldUtils.getField(AccessControlEntryImpl.class, "acl");

    public static final String DIR_PREFIX = "/acl/";

    @Autowired
    protected PermissionGrantingStrategy permissionGrantingStrategy;

    @Autowired
    protected PermissionFactory aclPermissionFactory;

    @Autowired
    protected AclAuthorizationStrategy aclAuthorizationStrategy;

    @Autowired
    protected AuditLogger auditLogger;

    protected ResourceStore aclStore;

    @Autowired
    protected UserService userService;

    public AclService() throws IOException {
        fieldAces.setAccessible(true);
        fieldAcl.setAccessible(true);
        aclStore = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        logger.debug("Acl service2 create");
    }

    @Override
    public List<ObjectIdentity> findChildren(ObjectIdentity parentIdentity) {
        logger.debug("invoke method : findChildren");
        List<ObjectIdentity> oids = new ArrayList<ObjectIdentity>();
        try {
            List<AclRecord> allAclRecords = aclStore.getAllResources(String.valueOf(DIR_PREFIX), AclRecord.class, AclRecordSerializer.getInstance());
            for (AclRecord record : allAclRecords) {
                DomainObjectInfo parent = record.getParentDomainObjectInfo();
                if (parent != null && parent.getId().equals(String.valueOf(parentIdentity.getIdentifier()))) {
                    DomainObjectInfo child = record.getDomainObjectInfo();
                    oids.add(new ObjectIdentityImpl(child.getType(), child.getId()));
                }
            }
            return oids;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public Acl readAclById(ObjectIdentity object) throws NotFoundException {
        Map<ObjectIdentity, Acl> aclsMap = readAclsById(Arrays.asList(object), null);
        return aclsMap.get(object);
    }

    @Override
    public Acl readAclById(ObjectIdentity object, List<Sid> sids) throws NotFoundException {
        Map<ObjectIdentity, Acl> aclsMap = readAclsById(Arrays.asList(object), sids);
        Assert.isTrue(aclsMap.containsKey(object), "There should have been an Acl entry for ObjectIdentity " + object);
        return aclsMap.get(object);
    }

    @Override
    public Map<ObjectIdentity, Acl> readAclsById(List<ObjectIdentity> objects) throws NotFoundException {
        return readAclsById(objects, null);
    }

    @Override
    public Map<ObjectIdentity, Acl> readAclsById(List<ObjectIdentity> oids, List<Sid> sids) throws NotFoundException {
        logger.debug("invoke method : readAclsById");
        Map<ObjectIdentity, Acl> aclMaps = new HashMap<ObjectIdentity, Acl>();
        try {
            for (ObjectIdentity oid : oids) {
                AclRecord record = aclStore.getResource(getQueryKeyById(String.valueOf(oid.getIdentifier())), AclRecord.class, AclRecordSerializer.getInstance());
                if (record != null) {
                    SidInfo owner = record.getOwnerInfo();
                    Sid ownerSid = (null == owner) ? null : (owner.isPrincipal() ? new PrincipalSid(owner.getSid()) : new GrantedAuthoritySid(owner.getSid()));
                    boolean entriesInheriting = record.isEntriesInheriting();

                    Acl parentAcl = null;
                    DomainObjectInfo parent = record.getParentDomainObjectInfo();
                    if (parent != null) {
                        ObjectIdentity parentObject = new ObjectIdentityImpl(parent.getType(), parent.getId());
                        parentAcl = readAclById(parentObject, null);
                    }

                    AclImpl acl = new AclImpl(oid, oid.getIdentifier(), aclAuthorizationStrategy, permissionGrantingStrategy, parentAcl, null, entriesInheriting, ownerSid);
                    genAces(sids, record, acl);

                    aclMaps.put(oid, acl);
                } else {
                    throw new NotFoundException("Unable to find ACL information for object identity '" + oid + "'");
                }
            }
            return aclMaps;
        } catch (IOException e) {
            throw new NotFoundException(e.getMessage(), e);
        }
    }

    @Override
    public MutableAcl createAcl(ObjectIdentity objectIdentity) throws AlreadyExistsException {
        logger.debug("invoke method : createAcl");
        Acl acl = null;

        try {
            acl = readAclById(objectIdentity);
        } catch (NotFoundException e) {
            //do nothing?
        }
        if (null != acl) {
            throw new AlreadyExistsException("ACL of " + objectIdentity + " exists!");
        }
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        PrincipalSid sid = new PrincipalSid(auth);
        try {
            AclRecord record = new AclRecord(new DomainObjectInfo(objectIdentity), null, new SidInfo(sid), true, null);
            aclStore.putResource(getQueryKeyById(String.valueOf(objectIdentity.getIdentifier())), record, 0, AclRecordSerializer.getInstance());
            logger.debug("ACL of " + objectIdentity + " created successfully.");
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return (MutableAcl) readAclById(objectIdentity);
    }

    @Override
    public void deleteAcl(ObjectIdentity objectIdentity, boolean deleteChildren) throws ChildrenExistException {
        try {
            logger.debug("invoke method : deleteAcl");
            List<ObjectIdentity> children = findChildren(objectIdentity);
            if (!deleteChildren && children.size() > 0) {
                throw new ChildrenExistException("Children exists for " + objectIdentity);
            }
            for (ObjectIdentity oid : children) {
                deleteAcl(oid, deleteChildren);
            }
            aclStore.deleteResource(getQueryKeyById(String.valueOf(objectIdentity.getIdentifier())));
            logger.debug("ACL of " + objectIdentity + " deleted successfully.");
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public MutableAcl updateAcl(MutableAcl mutableAcl) throws NotFoundException {
        try {
            logger.debug("invoke method : updateAcl");
            readAclById(mutableAcl.getObjectIdentity());
        } catch (NotFoundException e) {
            throw e;
        }

        try {
            String id = getQueryKeyById(String.valueOf(mutableAcl.getObjectIdentity().getIdentifier()));
            AclRecord record = aclStore.getResource(id, AclRecord.class, AclRecordSerializer.getInstance());
            aclStore.deleteResource(id);
            //logger.debug("Exist? {}", aclStore.exists(id));
            if (mutableAcl.getParentAcl() != null) {
                record.setParentDomainObjectInfo(new DomainObjectInfo(mutableAcl.getParentAcl().getObjectIdentity()));
            }

            if (record.getAllAceInfo() == null) {
                record.setAllAceInfo(new HashMap<String, AceInfo>());
            }
            Map<String, AceInfo> allAceInfo = record.getAllAceInfo();
            allAceInfo.clear();
            for (AccessControlEntry ace : mutableAcl.getEntries()) {
                if (ace.getSid() instanceof PrincipalSid) {
                    PrincipalSid psid = (PrincipalSid) ace.getSid();
                    String userName = psid.getPrincipal();
                    logger.debug("ACE SID name: " + userName);
                    if (!userService.userExists(userName))
                        throw new UsernameNotFoundException("User " + userName + " does not exist. Please make sure the user has logged in before");
                }
                AceInfo aceInfo = new AceInfo(ace);
                allAceInfo.put(String.valueOf(aceInfo.getSidInfo().getSid()), aceInfo);
            }
            aclStore.putResource(id, record, 0, AclRecordSerializer.getInstance());
            logger.debug("ACL of " + mutableAcl.getObjectIdentity() + " updated successfully.");
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return (MutableAcl) readAclById(mutableAcl.getObjectIdentity());
    }

    private void genAces(List<Sid> sids, AclRecord record, AclImpl acl) throws JsonParseException, JsonMappingException, IOException {
        List<AceInfo> aceInfos = new ArrayList<AceInfo>();
        Map<String, AceInfo> allAceInfos = record.getAllAceInfo();
        if (allAceInfos != null) {
            if (sids != null) {
                // Just return aces in sids
                for (Sid sid : sids) {
                    String sidName = null;
                    if (sid instanceof PrincipalSid) {
                        sidName = ((PrincipalSid) sid).getPrincipal();
                    } else if (sid instanceof GrantedAuthoritySid) {
                        sidName = ((GrantedAuthoritySid) sid).getGrantedAuthority();
                    }
                    AceInfo aceInfo = allAceInfos.get(sidName);
                    if (aceInfo != null) {
                        aceInfos.add(aceInfo);
                    }
                }
            } else {
                aceInfos.addAll(allAceInfos.values());
            }
        } else {
            logger.warn("Get null AllAceInfos from AclRecord");
        }

        List<AccessControlEntry> newAces = new ArrayList<AccessControlEntry>();
        for (int i = 0; i < aceInfos.size(); i++) {
            AceInfo aceInfo = aceInfos.get(i);

            if (null != aceInfo) {
                Sid sid = aceInfo.getSidInfo().isPrincipal() ? new PrincipalSid(aceInfo.getSidInfo().getSid()) : new GrantedAuthoritySid(aceInfo.getSidInfo().getSid());
                AccessControlEntry ace = new AccessControlEntryImpl(Long.valueOf(i), acl, sid, aclPermissionFactory.buildFromMask(aceInfo.getPermissionMask()), true, false, false);
                newAces.add(ace);
            }
        }

        this.setAces(acl, newAces);
    }

    private void setAces(AclImpl acl, List<AccessControlEntry> aces) {
        try {
            fieldAces.set(acl, aces);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Could not set AclImpl entries", e);
        }
    }

    public static String getQueryKeyById(String id) {
        return DIR_PREFIX + id;
    }

    private boolean equal(ObjectIdentity o1, ObjectIdentity o2) {
        if (o1.getIdentifier().equals(o2.getIdentifier()) && o1.getType().equals(o2.getType()))
            return true;
        return false;
    }

    protected static class AclRecordSerializer implements Serializer<AclRecord> {

        private static final AclRecordSerializer serializer = new AclRecordSerializer();

        AclRecordSerializer() {

        }

        public static AclRecordSerializer getInstance() {
            return serializer;
        }

        @Override
        public void serialize(AclRecord obj, DataOutputStream out) throws IOException {
            String jsonStr = JsonUtil.writeValueAsString(obj);
            out.writeUTF(jsonStr);
        }

        @Override
        public AclRecord deserialize(DataInputStream in) throws IOException {
            String jsonStr = in.readUTF();
            return JsonUtil.readValue(jsonStr, AclRecord.class);
        }
    }

}

class AclRecord extends RootPersistentEntity {

    @JsonProperty()
    private DomainObjectInfo domainObjectInfo;

    @JsonProperty()
    private DomainObjectInfo parentDomainObjectInfo;

    @JsonProperty()
    private SidInfo ownerInfo;

    @JsonProperty()
    private boolean entriesInheriting;

    @JsonProperty()
    private Map<String, AceInfo> allAceInfo;

    public AclRecord() {
    }

    public AclRecord(DomainObjectInfo domainObjectInfo, DomainObjectInfo parentDomainObjectInfo, SidInfo ownerInfo, boolean entriesInheriting, Map<String, AceInfo> allAceInfo) {
        this.domainObjectInfo = domainObjectInfo;
        this.parentDomainObjectInfo = parentDomainObjectInfo;
        this.ownerInfo = ownerInfo;
        this.entriesInheriting = entriesInheriting;
        this.allAceInfo = allAceInfo;
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

    public DomainObjectInfo getDomainObjectInfo() {
        return domainObjectInfo;
    }

    public void setDomainObjectInfo(DomainObjectInfo domainObjectInfo) {
        this.domainObjectInfo = domainObjectInfo;
    }

    public DomainObjectInfo getParentDomainObjectInfo() {
        return parentDomainObjectInfo;
    }

    public void setParentDomainObjectInfo(DomainObjectInfo parentDomainObjectInfo) {
        this.parentDomainObjectInfo = parentDomainObjectInfo;
    }

    public Map<String, AceInfo> getAllAceInfo() {
        return allAceInfo;
    }

    public void setAllAceInfo(Map<String, AceInfo> allAceInfo) {
        this.allAceInfo = allAceInfo;
    }

}

class DomainObjectInfo {
    private String id;
    private String type;

    public DomainObjectInfo() {
    }

    public DomainObjectInfo(ObjectIdentity oid) {
        super();
        this.id = (String) oid.getIdentifier();
        this.type = oid.getType();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}

class SidInfo {
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

class AceInfo {
    private SidInfo sidInfo;
    private int permissionMask;

    public AceInfo() {
    }

    public AceInfo(AccessControlEntry ace) {
        super();
        this.sidInfo = new SidInfo(ace.getSid());
        this.permissionMask = ace.getPermission().getMask();
    }

    public SidInfo getSidInfo() {
        return sidInfo;
    }

    public void setSidInfo(SidInfo sidInfo) {
        this.sidInfo = sidInfo;
    }

    public int getPermissionMask() {
        return permissionMask;
    }

    public void setPermissionMask(int permissionMask) {
        this.permissionMask = permissionMask;
    }

}
