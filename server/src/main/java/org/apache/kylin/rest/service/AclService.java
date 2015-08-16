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
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.rest.util.Serializer;
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
import org.springframework.security.util.FieldUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * @author xduo
 * 
 */
@Component("aclService")
public class AclService implements MutableAclService {

    private static final Logger logger = LoggerFactory.getLogger(AclService.class);

    public static final String ACL_INFO_FAMILY = "i";
    public static final String ACL_ACES_FAMILY = "a";
    private static final String DEFAULT_TABLE_PREFIX = "kylin_metadata";
    private static final String ACL_TABLE_NAME = "_acl";
    private static final String ACL_INFO_FAMILY_TYPE_COLUMN = "t";
    private static final String ACL_INFO_FAMILY_OWNER_COLUMN = "o";
    private static final String ACL_INFO_FAMILY_PARENT_COLUMN = "p";
    private static final String ACL_INFO_FAMILY_ENTRY_INHERIT_COLUMN = "i";

    private Serializer<SidInfo> sidSerializer = new Serializer<SidInfo>(SidInfo.class);
    private Serializer<DomainObjectInfo> domainObjSerializer = new Serializer<DomainObjectInfo>(DomainObjectInfo.class);
    private Serializer<AceInfo> aceSerializer = new Serializer<AceInfo>(AceInfo.class);

    private String hbaseUrl = null;
    private String tableNameBase = null;
    private String aclTableName = null;

    private final Field fieldAces = FieldUtils.getField(AclImpl.class, "aces");
    private final Field fieldAcl = FieldUtils.getField(AccessControlEntryImpl.class, "acl");

    @Autowired
    protected PermissionGrantingStrategy permissionGrantingStrategy;

    @Autowired
    protected PermissionFactory aclPermissionFactory;

    @Autowired
    protected AclAuthorizationStrategy aclAuthorizationStrategy;

    @Autowired
    protected AuditLogger auditLogger;

    public AclService() throws IOException {
        String metadataUrl = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        // split TABLE@HBASE_URL
        int cut = metadataUrl.indexOf('@');
        tableNameBase = cut < 0 ? DEFAULT_TABLE_PREFIX : metadataUrl.substring(0, cut);
        hbaseUrl = cut < 0 ? metadataUrl : metadataUrl.substring(cut + 1);
        aclTableName = tableNameBase + ACL_TABLE_NAME;

        fieldAces.setAccessible(true);
        fieldAcl.setAccessible(true);

        HBaseConnection.createHTableIfNeeded(hbaseUrl, aclTableName, ACL_INFO_FAMILY, ACL_ACES_FAMILY);
    }

    @Override
    public List<ObjectIdentity> findChildren(ObjectIdentity parentIdentity) {
        List<ObjectIdentity> oids = new ArrayList<ObjectIdentity>();
        Table htable = null;
        try {
            htable = HBaseConnection.get(hbaseUrl).getTable(TableName.valueOf(aclTableName));

            Scan scan = new Scan();
            SingleColumnValueFilter parentFilter = new SingleColumnValueFilter(Bytes.toBytes(ACL_INFO_FAMILY), Bytes.toBytes(ACL_INFO_FAMILY_PARENT_COLUMN), CompareOp.EQUAL, domainObjSerializer.serialize(new DomainObjectInfo(parentIdentity)));
            parentFilter.setFilterIfMissing(true);
            scan.setFilter(parentFilter);

            ResultScanner scanner = htable.getScanner(scan);
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                String id = Bytes.toString(result.getRow());
                String type = Bytes.toString(result.getValue(Bytes.toBytes(ACL_INFO_FAMILY), Bytes.toBytes(ACL_INFO_FAMILY_TYPE_COLUMN)));

                oids.add(new ObjectIdentityImpl(type, id));
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(htable);
        }

        return oids;
    }

    @Override
    public Acl readAclById(ObjectIdentity object) throws NotFoundException {
        Map<ObjectIdentity, Acl> aclsMap = readAclsById(Arrays.asList(object), null);
        //        Assert.isTrue(aclsMap.containsKey(object), "There should have been an Acl entry for ObjectIdentity " + object);

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
        Map<ObjectIdentity, Acl> aclMaps = new HashMap<ObjectIdentity, Acl>();
        Table htable = null;
        Result result = null;
        try {
            htable = HBaseConnection.get(hbaseUrl).getTable(TableName.valueOf(aclTableName));

            for (ObjectIdentity oid : oids) {
                result = htable.get(new Get(Bytes.toBytes(String.valueOf(oid.getIdentifier()))));

                if (null != result && !result.isEmpty()) {
                    SidInfo owner = sidSerializer.deserialize(result.getValue(Bytes.toBytes(ACL_INFO_FAMILY), Bytes.toBytes(ACL_INFO_FAMILY_OWNER_COLUMN)));
                    Sid ownerSid = (null == owner) ? null : (owner.isPrincipal() ? new PrincipalSid(owner.getSid()) : new GrantedAuthoritySid(owner.getSid()));
                    boolean entriesInheriting = Bytes.toBoolean(result.getValue(Bytes.toBytes(ACL_INFO_FAMILY), Bytes.toBytes(ACL_INFO_FAMILY_ENTRY_INHERIT_COLUMN)));

                    Acl parentAcl = null;
                    DomainObjectInfo parentInfo = domainObjSerializer.deserialize(result.getValue(Bytes.toBytes(ACL_INFO_FAMILY), Bytes.toBytes(ACL_INFO_FAMILY_PARENT_COLUMN)));
                    if (null != parentInfo) {
                        ObjectIdentity parentObj = new ObjectIdentityImpl(parentInfo.getType(), parentInfo.getId());
                        parentAcl = readAclById(parentObj, null);
                    }

                    AclImpl acl = new AclImpl(oid, oid.getIdentifier(), aclAuthorizationStrategy, permissionGrantingStrategy, parentAcl, null, entriesInheriting, ownerSid);
                    genAces(sids, result, acl);

                    aclMaps.put(oid, acl);
                } else {
                    throw new NotFoundException("Unable to find ACL information for object identity '" + oid + "'");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(htable);
        }

        return aclMaps;
    }

    @Override
    public MutableAcl createAcl(ObjectIdentity objectIdentity) throws AlreadyExistsException {
        Acl acl = null;

        try {
            acl = readAclById(objectIdentity);
        } catch (NotFoundException e) {
        }
        if (null != acl) {
            throw new AlreadyExistsException("ACL of " + objectIdentity + " exists!");
        }

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        PrincipalSid sid = new PrincipalSid(auth);

        Table htable = null;
        try {
            htable = HBaseConnection.get(hbaseUrl).getTable(TableName.valueOf(aclTableName));
            Put put = new Put(Bytes.toBytes(String.valueOf(objectIdentity.getIdentifier())));
            put.addColumn(Bytes.toBytes(ACL_INFO_FAMILY), Bytes.toBytes(ACL_INFO_FAMILY_TYPE_COLUMN), Bytes.toBytes(objectIdentity.getType()));
            put.addColumn(Bytes.toBytes(ACL_INFO_FAMILY), Bytes.toBytes(ACL_INFO_FAMILY_OWNER_COLUMN), sidSerializer.serialize(new SidInfo(sid)));
            put.addColumn(Bytes.toBytes(ACL_INFO_FAMILY), Bytes.toBytes(ACL_INFO_FAMILY_ENTRY_INHERIT_COLUMN), Bytes.toBytes(true));

            htable.put(put);

            logger.debug("ACL of " + objectIdentity + " created successfully.");
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(htable);
        }

        return (MutableAcl) readAclById(objectIdentity);
    }

    @Override
    public void deleteAcl(ObjectIdentity objectIdentity, boolean deleteChildren) throws ChildrenExistException {
        Table htable = null;
        try {
            htable = HBaseConnection.get(hbaseUrl).getTable(TableName.valueOf(aclTableName));
            Delete delete = new Delete(Bytes.toBytes(String.valueOf(objectIdentity.getIdentifier())));

            List<ObjectIdentity> children = findChildren(objectIdentity);
            if (!deleteChildren && children.size() > 0) {
                throw new ChildrenExistException("Children exists for " + objectIdentity);
            }

            for (ObjectIdentity oid : children) {
                deleteAcl(oid, deleteChildren);
            }

            htable.delete(delete);

            logger.debug("ACL of " + objectIdentity + " deleted successfully.");
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(htable);
        }
    }

    @Override
    public MutableAcl updateAcl(MutableAcl acl) throws NotFoundException {
        try {
            readAclById(acl.getObjectIdentity());
        } catch (NotFoundException e) {
            throw e;
        }

        Table htable = null;
        try {
            htable = HBaseConnection.get(hbaseUrl).getTable(TableName.valueOf(aclTableName));
            Delete delete = new Delete(Bytes.toBytes(String.valueOf(acl.getObjectIdentity().getIdentifier())));
            delete.addFamily(Bytes.toBytes(ACL_ACES_FAMILY));
            htable.delete(delete);

            Put put = new Put(Bytes.toBytes(String.valueOf(acl.getObjectIdentity().getIdentifier())));

            if (null != acl.getParentAcl()) {
                put.addColumn(Bytes.toBytes(ACL_INFO_FAMILY), Bytes.toBytes(ACL_INFO_FAMILY_PARENT_COLUMN), domainObjSerializer.serialize(new DomainObjectInfo(acl.getParentAcl().getObjectIdentity())));
            }

            for (AccessControlEntry ace : acl.getEntries()) {
                AceInfo aceInfo = new AceInfo(ace);
                put.addColumn(Bytes.toBytes(ACL_ACES_FAMILY), Bytes.toBytes(aceInfo.getSidInfo().getSid()), aceSerializer.serialize(aceInfo));
            }

            if (!put.isEmpty()) {
                htable.put(put);

                logger.debug("ACL of " + acl.getObjectIdentity() + " updated successfully.");
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(htable);
        }

        return (MutableAcl) readAclById(acl.getObjectIdentity());
    }

    private void genAces(List<Sid> sids, Result result, AclImpl acl) throws JsonParseException, JsonMappingException, IOException {
        List<AceInfo> aceInfos = new ArrayList<AceInfo>();
        if (null != sids) {
            // Just return aces in sids
            for (Sid sid : sids) {
                String sidName = null;
                if (sid instanceof PrincipalSid) {
                    sidName = ((PrincipalSid) sid).getPrincipal();
                } else if (sid instanceof GrantedAuthoritySid) {
                    sidName = ((GrantedAuthoritySid) sid).getGrantedAuthority();
                }

                AceInfo aceInfo = aceSerializer.deserialize(result.getValue(Bytes.toBytes(ACL_ACES_FAMILY), Bytes.toBytes(sidName)));
                if (null != aceInfo) {
                    aceInfos.add(aceInfo);
                }
            }
        } else {
            NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(ACL_ACES_FAMILY));
            for (byte[] qualifier : familyMap.keySet()) {
                AceInfo aceInfo = aceSerializer.deserialize(familyMap.get(qualifier));

                if (null != aceInfo) {
                    aceInfos.add(aceInfo);
                }
            }
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

    protected static class DomainObjectInfo {
        private String id;
        private String type;

        public DomainObjectInfo() {
        }

        public DomainObjectInfo(ObjectIdentity oid) {
            super();
            this.id = (String) oid.getIdentifier();
            this.type = oid.getType();
        }

        public Serializable getId() {
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

    protected static class SidInfo {
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

    protected static class AceInfo {
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

}
