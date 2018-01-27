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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.security.springacl.AclRecord;
import org.apache.kylin.rest.security.springacl.MutableAclRecord;
import org.apache.kylin.rest.security.springacl.ObjectIdentityImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.AlreadyExistsException;
import org.springframework.security.acls.model.ChildrenExistException;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

@Component("aclService")
public class AclService implements MutableAclService {
    private static final Logger logger = LoggerFactory.getLogger(AclService.class);

    public static final String DIR_PREFIX = "/acl/";
    public static final Serializer<AclRecord> SERIALIZER = new JsonSerializer<>(AclRecord.class, true);

    // ============================================================================

    @Autowired
    protected PermissionGrantingStrategy permissionGrantingStrategy;

    @Autowired
    protected PermissionFactory aclPermissionFactory;

    //    @Autowired
    //    protected AclAuthorizationStrategy aclAuthorizationStrategy;

    //    @Autowired
    //    protected AuditLogger auditLogger;

    protected ResourceStore aclStore;

    public AclService() throws IOException {
        aclStore = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
    }

    @Override
    public List<ObjectIdentity> findChildren(ObjectIdentity parentIdentity) {
        List<ObjectIdentity> oids = new ArrayList<ObjectIdentity>();
        try {
            List<AclRecord> allAclRecords = aclStore.getAllResources(DIR_PREFIX, AclRecord.class, SERIALIZER);
            for (AclRecord record : allAclRecords) {
                ObjectIdentityImpl parent = record.getParentDomainObjectInfo();
                if (parent != null && parent.equals(parentIdentity)) {
                    ObjectIdentityImpl child = record.getDomainObjectInfo();
                    oids.add(child);
                }
            }
            return oids;
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
    }

    public MutableAclRecord readAcl(ObjectIdentity oid) throws NotFoundException {
        return (MutableAclRecord) readAclById(oid);
    }

    @Override
    public Acl readAclById(ObjectIdentity object) throws NotFoundException {
        Map<ObjectIdentity, Acl> aclsMap = readAclsById(Arrays.asList(object), null);
        return aclsMap.get(object);
    }

    @Override
    public Acl readAclById(ObjectIdentity object, List<Sid> sids) throws NotFoundException {
        Message msg = MsgPicker.getMsg();
        Map<ObjectIdentity, Acl> aclsMap = readAclsById(Arrays.asList(object), sids);
        if (!aclsMap.containsKey(object)) {
            throw new BadRequestException(String.format(msg.getNO_ACL_ENTRY(), object));
        }
        return aclsMap.get(object);
    }

    @Override
    public Map<ObjectIdentity, Acl> readAclsById(List<ObjectIdentity> objects) throws NotFoundException {
        return readAclsById(objects, null);
    }

    @Override
    public Map<ObjectIdentity, Acl> readAclsById(List<ObjectIdentity> oids, List<Sid> sids) throws NotFoundException {
        Map<ObjectIdentity, Acl> aclMaps = new HashMap<>();
        try {
            for (ObjectIdentity oid : oids) {
                AclRecord record = aclStore.getResource(resourceKey(oid), AclRecord.class, SERIALIZER);
                if (record == null) {
                    Message msg = MsgPicker.getMsg();
                    throw new NotFoundException(String.format(msg.getACL_INFO_NOT_FOUND(), oid));
                }

                Acl parentAcl = null;
                if (record.isEntriesInheriting() && record.getParentDomainObjectInfo() != null)
                    parentAcl = readAclById(record.getParentDomainObjectInfo());

                record.init(parentAcl, aclPermissionFactory, permissionGrantingStrategy);

                aclMaps.put(oid, new MutableAclRecord(record));
            }
            return aclMaps;
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
    }

    @Override
    public MutableAcl createAcl(ObjectIdentity objectIdentity) throws AlreadyExistsException {
        try {
            if (aclStore.exists(resourceKey(objectIdentity))) {
                throw new AlreadyExistsException("ACL of " + objectIdentity + " exists!");
            }

            Authentication auth = SecurityContextHolder.getContext().getAuthentication();
            PrincipalSid owner = new PrincipalSid(auth);
            AclRecord record = new AclRecord(objectIdentity, owner);
            record.init(null, aclPermissionFactory, permissionGrantingStrategy);
            aclStore.putResource(resourceKey(objectIdentity), record, 0, SERIALIZER);
            logger.debug("ACL of " + objectIdentity + " created successfully.");
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
        return (MutableAcl) readAclById(objectIdentity);
    }

    @Override
    public void deleteAcl(ObjectIdentity objectIdentity, boolean deleteChildren) throws ChildrenExistException {
        try {
            List<ObjectIdentity> children = findChildren(objectIdentity);
            if (!deleteChildren && children.size() > 0) {
                Message msg = MsgPicker.getMsg();
                throw new BadRequestException(String.format(msg.getIDENTITY_EXIST_CHILDREN(), objectIdentity));
            }
            for (ObjectIdentity oid : children) {
                deleteAcl(oid, deleteChildren);
            }
            aclStore.deleteResource(resourceKey(String.valueOf(objectIdentity.getIdentifier())));
            logger.debug("ACL of " + objectIdentity + " deleted successfully.");
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
    }

    // Try use the updateAclWithRetry() method family whenever possible
    @Override
    public MutableAcl updateAcl(MutableAcl mutableAcl) throws NotFoundException {
        try {
            AclRecord record = ((MutableAclRecord) mutableAcl).getAclRecord();
            String resPath = resourceKey(mutableAcl.getObjectIdentity());
            aclStore.putResource(resPath, record, System.currentTimeMillis(), SERIALIZER);
            logger.debug("ACL of " + mutableAcl.getObjectIdentity() + " updated successfully.");
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
        return mutableAcl;
    }

    // a NULL permission means to delete the ace
    public MutableAclRecord upsertAce(MutableAclRecord acl, final Sid sid, final Permission perm) {
        return updateAclWithRetry(acl, new AclRecordUpdater() {
            @Override
            public void update(AclRecord record) {
                record.upsertAce(perm, sid);
            }
        });
    }
    
    public MutableAclRecord inherit(MutableAclRecord acl, final MutableAclRecord parentAcl) {
        return updateAclWithRetry(acl, new AclRecordUpdater() {
            @Override
            public void update(AclRecord record) {
                record.setEntriesInheriting(true);
                record.setParent(parentAcl);
            }
        });
    }

    public interface AclRecordUpdater {
        void update(AclRecord record);
    }

    private MutableAclRecord updateAclWithRetry(MutableAclRecord acl, AclRecordUpdater updater) {
        int retry = 7;
        while (retry-- > 0) {
            AclRecord record = acl.getAclRecord();

            updater.update(record);
            String resPath = resourceKey(record.getObjectIdentity());
            try {
                aclStore.putResource(resPath, record, System.currentTimeMillis(), SERIALIZER);
                return acl; // here we are done

            } catch (IllegalStateException ise) {
                if (retry <= 0) {
                    logger.error("Retry is out, till got error, abandoning...", ise);
                    throw ise;
                }

                logger.warn("Write conflict to update ACL " + resPath + " retry remaining " + retry + ", will retry...");
                acl = readAcl(acl.getObjectIdentity());

            } catch (IOException e) {
                throw new InternalErrorException(e);
            }
        }
        throw new RuntimeException("should not reach here");
    }

    public static String resourceKey(ObjectIdentity domainObjId) {
        return resourceKey(String.valueOf(domainObjId.getIdentifier()));
    }

    public static String resourceKey(String domainObjId) {
        return DIR_PREFIX + domainObjId;
    }
}
