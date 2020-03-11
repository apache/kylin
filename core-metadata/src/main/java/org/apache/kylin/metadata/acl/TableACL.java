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

package org.apache.kylin.metadata.acl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.CaseInsensitiveStringSet;
import org.apache.kylin.metadata.MetadataConstants;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.collect.ImmutableSet;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class TableACL extends RootPersistentEntity {

    //user1 : [DB.TABLE1, DB.TABLE2], means that user1 can not query DB.TABLE1, DB.TABLE2
    @JsonProperty()
    private TableACLEntry userTableBlackList = new TableACLEntry();
    @JsonProperty()
    private TableACLEntry groupTableBlackList = new TableACLEntry();

    private String project;
    
    void init(String project) {
        this.project = project;
    }
    
    @Override
    public String resourceName() {
        return project;
    }
    
    public Set<String> getTableBlackList(String username, Set<String> groups) {
        Set<String> tableBlackList = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        tableBlackList.addAll(userTableBlackList.getTableBlackList(username));
        //if user is in group, add group's black list
        for (String group : groups) {
            tableBlackList.addAll(groupTableBlackList.getTableBlackList(group));
        }
        return tableBlackList;
    }

    private TableACLEntry currentEntry(String type) {
        if (type.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            return userTableBlackList;
        } else {
            return groupTableBlackList;
        }
    }

    //get Identifiers(users or groups) that can not query the table
    public List<String> getNoAccessList(String table, String type) {
        return currentEntry(type).getNoAccessList(table);
    }

    public List<String> getCanAccessList(String table, Set<String> allIdentifiers, String type) {
        return currentEntry(type).getCanAccessList(table, allIdentifiers);
    }

    public Set<String> getBlockedTablesByUser(String username, String type) {
        return currentEntry(type).get(username) == null ? Sets.<String>newHashSet()
                : currentEntry(type).get(username).getTables();
    }

    public TableACL add(String name, String table, String type) {
        currentEntry(type).add(name, table);
        return this;
    }

    public TableACL delete(String name, String table, String type) {
        currentEntry(type).delete(name, table);
        return this;
    }

    public TableACL delete(String name, String type) {
        currentEntry(type).delete(name);
        return this;
    }

    TableACL deleteByTbl(String table) {
        userTableBlackList.deleteByTbl(table);
        groupTableBlackList.deleteByTbl(table);
        return this;
    }

    public boolean contains(String name, String type) {
        return currentEntry(type).containsKey(name);
    }

    public int size() {
        return userTableBlackList.size() + groupTableBlackList.size();
    }

    public int size(String type) {
        return currentEntry(type).size();
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    private static class TableACLEntry extends HashMap<String, TableBlackList> {
        private void add(String name, String table) {
            TableBlackList tableBlackList = super.get(name);
            if (tableBlackList == null) {
                tableBlackList = new TableBlackList();
                super.put(name, tableBlackList);
            }

            //before add, check exists
            validateACLNotExists(name, table, tableBlackList);
            tableBlackList.addTbl(table);
        }

        private void delete(String name, String table) {
            validateACLExists(name, table);
            TableBlackList tableBlackList = super.get(name);
            tableBlackList.removeTbl(table);
            if (tableBlackList.isEmpty()) {
                super.remove(name);
            }
        }

        private void delete(String username) {
            validateACLExists(username);
            super.remove(username);
        }

        private void deleteByTbl(String table) {
            Iterator<Map.Entry<String, TableBlackList>> it = super.entrySet().iterator();
            while (it.hasNext()) {
                TableBlackList tableBlackList = it.next().getValue();
                tableBlackList.removeTbl(table);
                if (tableBlackList.isEmpty()) {
                    it.remove();
                }
            }
        }

        private Set<String> getTableBlackList(String name) {
            TableBlackList tableBlackList = super.get(name);
            //table interceptor will use this, return an empty set than null
            if (tableBlackList == null) {
                tableBlackList = new TableBlackList();
            }
            return tableBlackList.getTables();
        }

        private List<String> getNoAccessList(String table) {
            List<String> results = new ArrayList<>();
            for (String identifiers : super.keySet()) {
                if (super.get(identifiers).contains(table)) {
                    results.add(identifiers);
                }
            }
            return results;
        }

        private List<String> getCanAccessList(String table, Set<String> allIdentifiers) {
            List<String> list = Lists.newArrayList(allIdentifiers);
            List<String> blocked = getNoAccessList(table);
            list.removeAll(blocked);
            return list;
        }

        private void validateACLExists(String username) {
            if (super.get(username) == null || super.get(username).isEmpty()) {
                throw new RuntimeException("Operation fail, can not grant user table query permission.User:" + username
                        + " already has permission!");
            }
        }

        private void validateACLNotExists(String username, String table, TableBlackList tableBlackList) {
            if (tableBlackList.contains(table)) {
                throw new RuntimeException("Operation fail, can not revoke user's table query permission.Table ACL " + table
                        + ":" + username + " already exists!");
            }
        }

        private void validateACLExists(String name, String table) {
            if (super.get(name) == null || (!super.get(name).contains(table))) {
                throw new RuntimeException("Operation fail, can not grant user table query permission.Table ACL " + table
                        + ":" + name + " is not found!");
            }
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE,
            getterVisibility = JsonAutoDetect.Visibility.NONE,
            isGetterVisibility = JsonAutoDetect.Visibility.NONE,
            setterVisibility = JsonAutoDetect.Visibility.NONE)
    private static class TableBlackList {
        //user1 : [DB.TABLE1, DB.TABLE2], means that user1 can not query DB.TABLE1, DB.TABLE2
        @JsonProperty()
        private CaseInsensitiveStringSet tables = new CaseInsensitiveStringSet();

        private boolean isEmpty() {
            return tables.isEmpty();
        }

        private boolean contains(String s) {
            return tables.contains(s);
        }

        private void addTbl(String s) {
            tables.add(s);
        }

        private void removeTbl(String s) {
            tables.remove(s);
        }

        private Set<String> getTables() {
            return ImmutableSet.copyOf(tables);
        }
    }
}