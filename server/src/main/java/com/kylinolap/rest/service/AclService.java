/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.rest.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.AlreadyExistsException;
import org.springframework.security.acls.model.ChildrenExistException;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Sid;
import org.springframework.stereotype.Component;

/**
 * @author xduo
 * 
 */
@Component("aclService")
public class AclService implements MutableAclService {

    @Autowired
    MutableAclService aclServiceImpl;
    
    @Override
    public List<ObjectIdentity> findChildren(ObjectIdentity parentIdentity) {
        return aclServiceImpl.findChildren(parentIdentity);
    }

    @Override
    public Acl readAclById(ObjectIdentity object) throws NotFoundException {
        return aclServiceImpl.readAclById(object);
    }

    @Override
    public Acl readAclById(ObjectIdentity object, List<Sid> sids) throws NotFoundException {
        return aclServiceImpl.readAclById(object, sids);
    }

    @Override
    public Map<ObjectIdentity, Acl> readAclsById(List<ObjectIdentity> objects) throws NotFoundException {
        return aclServiceImpl.readAclsById(objects);
    }

    @Override
    public Map<ObjectIdentity, Acl> readAclsById(List<ObjectIdentity> objects, List<Sid> sids) throws NotFoundException {
        return aclServiceImpl.readAclsById(objects, sids);
    }

    @Override
    public MutableAcl createAcl(ObjectIdentity objectIdentity) throws AlreadyExistsException {
        return aclServiceImpl.createAcl(objectIdentity);
    }

    @Override
    public void deleteAcl(ObjectIdentity objectIdentity, boolean deleteChildren) throws ChildrenExistException {
        aclServiceImpl.deleteAcl(objectIdentity, deleteChildren);
    }

    @Override
    public MutableAcl updateAcl(MutableAcl acl) throws NotFoundException {
        return aclServiceImpl.updateAcl(acl);
    }
    
}
