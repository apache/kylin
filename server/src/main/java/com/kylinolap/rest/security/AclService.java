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

package com.kylinolap.rest.security;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.jdbc.JdbcMutableAclService;
import org.springframework.security.acls.jdbc.LookupStrategy;
import org.springframework.security.acls.model.AclCache;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Sid;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author xduo
 */
public class AclService extends JdbcMutableAclService {

    private final String findChildrenSql = "select obj.object_id_identity as obj_id, class.class as class "
            + "from acl_object_identity obj, acl_object_identity parent, acl_class class "
            + "where obj.parent_object = parent.id and obj.object_id_class = class.id "
            + "and parent.object_id_identity = ? and parent.object_id_class = ("
            + "select id FROM acl_class where acl_class.class = ?)";

    public AclService(DataSource dataSource, LookupStrategy lookupStrategy, AclCache aclCache) {
        super(dataSource, lookupStrategy, aclCache);
    }

    public Sid getSid(String sid, boolean isPrincepal) {
        if (isPrincepal) {
            return new PrincipalSid(sid);
        } else {
            return new GrantedAuthoritySid(sid);
        }
    }

    @Override
    public List<ObjectIdentity> findChildren(ObjectIdentity parentIdentity) {
        Object[] args = {parentIdentity.getIdentifier(), parentIdentity.getType()};
        List<ObjectIdentity> objects =
                jdbcTemplate.query(findChildrenSql, args, new RowMapper<ObjectIdentity>() {
                    public ObjectIdentity mapRow(ResultSet rs, int rowNum) throws SQLException {
                        String javaType = rs.getString("class");
                        String identifier = new String(rs.getString("obj_id"));

                        return new ObjectIdentityImpl(javaType, identifier);
                    }
                });

        if (objects.size() == 0) {
            return null;
        }

        return objects;
    }
}
