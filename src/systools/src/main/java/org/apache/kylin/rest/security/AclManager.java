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

import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.SpringContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.acls.domain.ConsoleAuditLogger;
import org.springframework.security.acls.domain.DefaultPermissionGrantingStrategy;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.AlreadyExistsException;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.val;

/**
 */
public class AclManager {

    private static final Logger logger = LoggerFactory.getLogger(AclManager.class);
    private final PermissionGrantingStrategy permissionGrantingStrategy = new DefaultPermissionGrantingStrategy(
            new ConsoleAuditLogger());
    private final PermissionFactory aclPermissionFactory = new AclPermissionFactory();

    // ============================================================================
    private KylinConfig config;
    private CachedCrudAssist<AclRecord> crud;

    public AclManager(KylinConfig config) {
        this.config = config;
        ResourceStore aclStore = ResourceStore.getKylinMetaStore(config);
        this.crud = new CachedCrudAssist<AclRecord>(aclStore, ResourceStore.ACL_ROOT, "", AclRecord.class) {
            @Override
            protected AclRecord initEntityAfterReload(AclRecord acl, String resourceName) {
                val aclPermissionFactory = SpringContext.getBean(PermissionFactory.class);
                val permissionGrantingStrategy = SpringContext.getBean(PermissionGrantingStrategy.class);
                acl.init(null, aclPermissionFactory, permissionGrantingStrategy);
                return acl;
            }
        };
        crud.reloadAll();
    }

    public static AclManager getInstance(KylinConfig config) {
        return config.getManager(AclManager.class);
    }

    // called by reflection
    static AclManager newInstance(KylinConfig config) {
        return new AclManager(config);
    }

    public KylinConfig getConfig() {
        return config;
    }

    public List<AclRecord> listAll() {
        return crud.listAll();
    }

    public void save(AclRecord record) {
        crud.save(record);
    }

    public void delete(String id) {
        crud.delete(id);
    }

    public AclRecord get(String id) {
        return crud.get(id);
    }

    public AclRecord copyForWrite(AclRecord aclRecord) {
        return crud.copyForWrite(aclRecord);
    }

    public List<ObjectIdentity> findChildren(ObjectIdentity parentIdentity) {
        List<ObjectIdentity> oids = Lists.newArrayList();
        Collection<AclRecord> allAclRecords;
        allAclRecords = crud.listAll();
        for (AclRecord record : allAclRecords) {
            ObjectIdentityImpl parent = record.getParentDomainObjectInfo();
            if (parent != null && parent.equals(parentIdentity)) {
                ObjectIdentityImpl child = record.getDomainObjectInfo();
                oids.add(child);
            }
        }
        return oids;
    }

    public MutableAclRecord readAcl(ObjectIdentity oid) {
        try {
            return (MutableAclRecord) readAclById(oid);
        } catch (NotFoundException nfe) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] acl not found for {}", oid);
        }
        return null;
    }

    public Acl readAclById(ObjectIdentity object) {
        return readAclsById(Arrays.asList(object)).get(object);
    }

    public Map<ObjectIdentity, Acl> readAclsById(List<ObjectIdentity> oids) {
        Map<ObjectIdentity, Acl> aclMaps = Maps.newHashMap();
        for (ObjectIdentity oid : oids) {
            AclRecord record = getAclRecordByCache(AclPermissionUtil.objID(oid));
            if (record == null) {
                throw new NotFoundException(
                        String.format(Locale.ROOT, MsgPicker.getMsg().getAclInfoNotFound(), oid));
            }

            Acl parentAcl = null;
            if (record.isEntriesInheriting() && record.getParentDomainObjectInfo() != null)
                parentAcl = readAclById(record.getParentDomainObjectInfo());

            record.init(parentAcl, aclPermissionFactory, permissionGrantingStrategy);

            aclMaps.put(oid, new MutableAclRecord(record));
        }
        return aclMaps;
    }

    public MutableAcl createAcl(ObjectIdentity objectIdentity) {
        AclRecord aclRecord = getAclRecordByCache(AclPermissionUtil.objID(objectIdentity));
        if (aclRecord != null) {
            throw new AlreadyExistsException(String.format(Locale.ROOT, "ACL of %s exists!", objectIdentity));
        }
        AclRecord record = newAclRecord(objectIdentity);
        crud.save(record);
        logger.debug("ACL of {} created successfully.", objectIdentity);

        return (MutableAcl) readAclById(objectIdentity);
    }

    public void deleteAcl(ObjectIdentity objectIdentity, boolean deleteChildren) {
        List<ObjectIdentity> children = findChildren(objectIdentity);
        if (!deleteChildren && !children.isEmpty()) {
            Message msg = MsgPicker.getMsg();
            throw new KylinException(PERMISSION_DENIED,
                    String.format(Locale.ROOT, msg.getIdentityExistChildren(), objectIdentity));
        }
        for (ObjectIdentity oid : children) {
            deleteAcl(oid, deleteChildren);
        }
        crud.delete(AclPermissionUtil.objID(objectIdentity));
        logger.debug("ACL of {} deleted successfully.", objectIdentity);
    }

    public MutableAcl updateAcl(MutableAcl mutableAcl) {
        AclRecord record = ((MutableAclRecord) mutableAcl).getAclRecord();
        crud.save(record);
        logger.debug("ACL of {} updated successfully.", mutableAcl.getObjectIdentity());
        return mutableAcl;
    }

    // a NULL permission means to delete the ace
    public MutableAclRecord upsertAce(MutableAclRecord acl, final Sid sid, final Permission perm) {
        return updateAcl(acl, (AclRecord record) -> record.upsertAce(perm, sid));
    }

    public void batchUpsertAce(MutableAclRecord acl, final Map<Sid, Permission> sidToPerm) {

        updateAcl(acl, (AclRecord record) -> {
            for (Map.Entry<Sid, Permission> entry : sidToPerm.entrySet()) {
                record.upsertAce(entry.getValue(), entry.getKey());
            }
        });

    }

    public MutableAclRecord inherit(MutableAclRecord acl, final MutableAclRecord parentAcl) {

        return updateAcl(acl, (AclRecord record) -> {
            record.setEntriesInheriting(true);
            record.setParent(parentAcl);
        });
    }

    public AclRecord getAclRecordByCache(String id) {
        return crud.get(id);
    }

    public AclRecord newAclRecord(ObjectIdentity objID) {
        AclRecord acl = new AclRecord(objID, getCurrentSid());
        acl.init(null, this.aclPermissionFactory, this.permissionGrantingStrategy);
        acl.updateRandomUuid();
        return acl;
    }

    private Sid getCurrentSid() {
        return new PrincipalSid(SecurityContextHolder.getContext().getAuthentication());
    }

    private MutableAclRecord updateAcl(MutableAclRecord acl, AclRecordUpdater updater) {
        val copyForWrite = crud.copyForWrite(acl.getAclRecord());
        updater.update(copyForWrite);
        crud.save(copyForWrite);
        return readAcl(acl.getObjectIdentity()); // here we are done
    }

    public interface AclRecordUpdater {
        void update(AclRecord copyForWrite);
    }
}
