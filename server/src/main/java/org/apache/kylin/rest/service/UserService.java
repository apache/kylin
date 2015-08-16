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
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.security.UserManager;
import org.apache.kylin.rest.util.Serializer;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * @author xduo
 * 
 */
@Component("userService")
public class UserService implements UserManager {

    private Serializer<UserGrantedAuthority[]> ugaSerializer = new Serializer<UserGrantedAuthority[]>(UserGrantedAuthority[].class);

    public static final String USER_AUTHORITY_FAMILY = "a";
    private static final String DEFAULT_TABLE_PREFIX = "kylin_metadata";
    private static final String USER_TABLE_NAME = "_user";
    private static final String USER_AUTHORITY_COLUMN = "c";
    private String hbaseUrl = null;
    private String tableNameBase = null;
    private String userTableName = null;

    public UserService() throws IOException {
        String metadataUrl = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        // split TABLE@HBASE_URL
        int cut = metadataUrl.indexOf('@');
        tableNameBase = cut < 0 ? DEFAULT_TABLE_PREFIX : metadataUrl.substring(0, cut);
        hbaseUrl = cut < 0 ? metadataUrl : metadataUrl.substring(cut + 1);
        userTableName = tableNameBase + USER_TABLE_NAME;
        HBaseConnection.createHTableIfNeeded(hbaseUrl, userTableName, USER_AUTHORITY_FAMILY, QueryService.USER_QUERY_FAMILY);
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        Table htable = null;
        try {
            htable = HBaseConnection.get(hbaseUrl).getTable(TableName.valueOf(userTableName));

            Get get = new Get(Bytes.toBytes(username));
            get.addFamily(Bytes.toBytes(USER_AUTHORITY_FAMILY));
            Result result = htable.get(get);

            Collection<? extends GrantedAuthority> authorities = null;
            if (null != result && !result.isEmpty()) {
                byte[] gaBytes = result.getValue(Bytes.toBytes(USER_AUTHORITY_FAMILY), Bytes.toBytes(USER_AUTHORITY_COLUMN));
                authorities = Arrays.asList(ugaSerializer.deserialize(gaBytes));
            } else {
                throw new UsernameNotFoundException("User " + username + " not found.");
            }

            return new User(username, "N/A", authorities);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(htable);
        }
    }

    @Override
    public void createUser(UserDetails user) {
        updateUser(user);
    }

    @Override
    public void updateUser(UserDetails user) {
        Table htable = null;
        try {
            byte[] userAuthorities = serialize(user.getAuthorities());
            htable = HBaseConnection.get(hbaseUrl).getTable(TableName.valueOf(userTableName));
            Put put = new Put(Bytes.toBytes(user.getUsername()));
            put.addColumn(Bytes.toBytes(USER_AUTHORITY_FAMILY), Bytes.toBytes(USER_AUTHORITY_COLUMN), userAuthorities);

            htable.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(htable);
        }
    }

    @Override
    public void deleteUser(String username) {
        Table htable = null;
        try {
            htable = HBaseConnection.get(hbaseUrl).getTable(TableName.valueOf(userTableName));
            Delete delete = new Delete(Bytes.toBytes(username));

            htable.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(htable);
        }
    }

    @Override
    public void changePassword(String oldPassword, String newPassword) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean userExists(String username) {
        Table htable = null;
        try {
            htable = HBaseConnection.get(hbaseUrl).getTable(TableName.valueOf(userTableName));
            Result result = htable.get(new Get(Bytes.toBytes(username)));

            return null != result && !result.isEmpty();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(htable);
        }
    }

    @Override
    public List<String> getUserAuthorities() {
        Scan s = new Scan();
        s.addColumn(Bytes.toBytes(USER_AUTHORITY_FAMILY), Bytes.toBytes(USER_AUTHORITY_COLUMN));

        List<String> authorities = new ArrayList<String>();
        Table htable = null;
        ResultScanner scanner = null;
        try {
            htable = HBaseConnection.get(hbaseUrl).getTable(TableName.valueOf(userTableName));
            scanner = htable.getScanner(s);

            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                byte[] uaBytes = result.getValue(Bytes.toBytes(USER_AUTHORITY_FAMILY), Bytes.toBytes(USER_AUTHORITY_COLUMN));
                Collection<? extends GrantedAuthority> authCollection = Arrays.asList(ugaSerializer.deserialize(uaBytes));

                for (GrantedAuthority auth : authCollection) {
                    if (!authorities.contains(auth.getAuthority())) {
                        authorities.add(auth.getAuthority());
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(scanner);
            IOUtils.closeQuietly(htable);
        }

        return authorities;
    }

    public static class UserGrantedAuthority implements GrantedAuthority {
        private static final long serialVersionUID = -5128905636841891058L;
        private String authority;

        public UserGrantedAuthority() {
        }

        @Override
        public String getAuthority() {
            return authority;
        }

        public void setAuthority(String authority) {
            this.authority = authority;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((authority == null) ? 0 : authority.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            UserGrantedAuthority other = (UserGrantedAuthority) obj;
            if (authority == null) {
                if (other.authority != null)
                    return false;
            } else if (!authority.equals(other.authority))
                return false;
            return true;
        }
    }

    private byte[] serialize(Collection<? extends GrantedAuthority> auths) throws JsonProcessingException {
        if (null == auths) {
            return null;
        }

        return JsonUtil.writeValueAsBytes(auths);
    }
}
