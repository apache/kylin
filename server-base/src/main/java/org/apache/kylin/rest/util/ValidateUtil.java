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

package org.apache.kylin.rest.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.TableService;
import org.apache.kylin.rest.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

@Component("validateUtil")
public class ValidateUtil {

    @Autowired
    @Qualifier("userService")
    private UserService userService;

    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    //Identifiers may be user or user authority(you may call role or group)
    public void validateIdentifiers(String name, String type) throws IOException {
        if (type.equalsIgnoreCase(MetadataConstants.TYPE_USER) && !userService.userExists(name)) {
            throw new RuntimeException("Operation failed, user:" + name + " not exists");
        }
        if (type.equalsIgnoreCase(MetadataConstants.TYPE_GROUP) && !userGroupService.exists(name)) {
            throw new RuntimeException("Operation failed, group:" + name + " not exists");
        }
    }

    //get all users/user authorities that has permission in the project
    public Set<String> getAllIdentifiers(String project, String type) throws IOException {
        List<Sid> allSids = getAllSids(project);
        if (type.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            return getAllUsers(allSids);
        } else {
            return getAllAuthorities(allSids);
        }
    }

    private Set<String> getAllAuthorities(List<Sid> allSids) {
        Set<String> allAuthorities = new TreeSet<>();
        allAuthorities.add(Constant.ROLE_ADMIN);
        for (Sid sid : allSids) {
            if (sid instanceof GrantedAuthoritySid) {
                allAuthorities.add(((GrantedAuthoritySid) sid).getGrantedAuthority());
            }
        }
        return allAuthorities;
    }

    private Set<String> getAllUsers(List<Sid> allSids) throws IOException {
        Set<String> allUsers = new TreeSet<>();
        // add users that is global admin
        for (ManagedUser managedUser : userService.listUsers()) {
            if (managedUser.getAuthorities().contains(new SimpleGrantedAuthority(Constant.ROLE_ADMIN))) {
                allUsers.add(managedUser.getUsername());
            }
        }

        for (Sid sid : allSids) {
            if (sid instanceof PrincipalSid) {
                allUsers.add(((PrincipalSid) sid).getPrincipal());
            }
        }
        return allUsers;
    }

    private List<Sid> getAllSids(String project) {
        List<Sid> allSids = new ArrayList<>();
        ProjectInstance prj = projectService.getProjectManager().getProject(project);
        AclEntity ae = accessService.getAclEntity("ProjectInstance", prj.getUuid());
        Acl acl = accessService.getAcl(ae);
        if (acl != null && acl.getEntries() != null) {
            for (AccessControlEntry ace : acl.getEntries()) {
                allSids.add(ace.getSid());
            }
        }
        return allSids;
    }

    public void validateTable(String project, String table) throws IOException {
        List<TableDesc> tableDescs = tableService.getTableDescByProject(project, false);
        Set<String> tables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (TableDesc tableDesc : tableDescs) {
            tables.add(tableDesc.getDatabase() + "." + tableDesc.getName());
        }

        if (!tables.contains(table)) {
            throw new RuntimeException("Operation failed, table:" + table + " not exists");
        }
    }

    public void validateColumn(String project, String table, Collection<String> columns) throws IOException {
        Preconditions.checkState(columns != null && columns.size() > 0);
        Set<String> cols = getAllColumns(project, table);
        for (String c : columns) {
            if (!cols.contains(c)) {
                throw new RuntimeException("Operation failed, column:" + c + " not exists");
            }
        }
    }

    public void validateArgs(String... args) {
        for (String arg : args) {
            Preconditions.checkState(!StringUtils.isEmpty(arg));
        }
    }

    private Set<String> getAllColumns(String project, String table) throws IOException {
        List<TableDesc> tableDescByProject = tableService.getTableDescByProject(project, true);
        Set<String> cols = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        for (TableDesc tableDesc : tableDescByProject) {
            String tbl = tableDesc.getDatabase() + "." + tableDesc.getName();
            if (tbl.equalsIgnoreCase(table)) {
                for (ColumnDesc column : tableDesc.getColumns()) {
                    cols.add(column.getName());
                }
                break;
            }
        }
        return cols;
    }
}
