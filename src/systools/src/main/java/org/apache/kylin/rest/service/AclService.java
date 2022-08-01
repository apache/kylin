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

package org.apache.kylin.rest.service;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.security.AclManager;
import org.apache.kylin.rest.security.MutableAclRecord;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;
import org.springframework.stereotype.Component;

@Component("aclService")
public class AclService implements MutableAclService {

    // ============================================================================

    private AclManager getAclManager() {
        return AclManager.getInstance(KylinConfig.getInstanceFromEnv());
    }

    @Override
    public List<ObjectIdentity> findChildren(ObjectIdentity parentIdentity) {
        return getAclManager().findChildren(parentIdentity);
    }

    public MutableAclRecord readAcl(ObjectIdentity oid) {
        return getAclManager().readAcl(oid);
    }

    @Override
    public Acl readAclById(ObjectIdentity object) {
        return getAclManager().readAclById(object);
    }

    @Override
    public Acl readAclById(ObjectIdentity object, List<Sid> sids) {
        return readAclById(object);
    }

    @Override
    public Map<ObjectIdentity, Acl> readAclsById(List<ObjectIdentity> objects) {
        return readAclsById(objects, null);
    }

    @Override
    public Map<ObjectIdentity, Acl> readAclsById(List<ObjectIdentity> oids, List<Sid> sids) {
        return getAclManager().readAclsById(oids);
    }

    @Override
    public MutableAcl createAcl(ObjectIdentity objectIdentity) {
        return getAclManager().createAcl(objectIdentity);
    }

    @Override
    public void deleteAcl(ObjectIdentity objectIdentity, boolean deleteChildren) {
        getAclManager().deleteAcl(objectIdentity, deleteChildren);
    }

    // Try use the updateAcl() method family whenever possible
    @Override
    public MutableAcl updateAcl(MutableAcl mutableAcl) throws NotFoundException {
        return getAclManager().updateAcl(mutableAcl);
    }

    // a NULL permission means to delete the ace
    MutableAclRecord upsertAce(MutableAclRecord acl, final Sid sid, final Permission perm) {
        return getAclManager().upsertAce(acl, sid, perm);
    }

    void batchUpsertAce(MutableAclRecord acl, final Map<Sid, Permission> sidToPerm) {
        getAclManager().batchUpsertAce(acl, sidToPerm);
    }

    MutableAclRecord inherit(MutableAclRecord acl, final MutableAclRecord parentAcl) {
        return getAclManager().inherit(acl, parentAcl);
    }
}