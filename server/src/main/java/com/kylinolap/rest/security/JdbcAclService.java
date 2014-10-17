package com.kylinolap.rest.security;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.jdbc.JdbcMutableAclService;
import org.springframework.security.acls.jdbc.LookupStrategy;
import org.springframework.security.acls.model.AclCache;
import org.springframework.security.acls.model.ObjectIdentity;

public class JdbcAclService extends JdbcMutableAclService {

    private final String findChildrenSql = "select obj.object_id_identity as obj_id, class.class as class " + "from acl_object_identity obj, acl_object_identity parent, acl_class class " + "where obj.parent_object = parent.id and obj.object_id_class = class.id " + "and parent.object_id_identity = ? and parent.object_id_class = (" + "select id FROM acl_class where acl_class.class = ?)";

    public JdbcAclService(DataSource dataSource, LookupStrategy lookupStrategy, AclCache aclCache) {
        super(dataSource, lookupStrategy, aclCache);
    }

    @Override
    public List<ObjectIdentity> findChildren(ObjectIdentity parentIdentity) {
        Object[] args = { parentIdentity.getIdentifier(), parentIdentity.getType() };
        List<ObjectIdentity> objects = jdbcTemplate.query(findChildrenSql, args, new RowMapper<ObjectIdentity>() {
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
