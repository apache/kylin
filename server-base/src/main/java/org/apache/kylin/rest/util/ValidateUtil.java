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

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.TableService;
import org.apache.kylin.rest.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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

    public void validateUser(String username) {
        if (!userService.userExists(username)) {
            throw new RuntimeException("Operation failed, user:" + username + " not exists");
        }
    }

    public void validateTable(String project, String table) throws IOException {
        List<TableDesc> tableDescs = tableService.getTableDescByProject(project, false);
        List<String> tables = new ArrayList<>();
        for (TableDesc tableDesc : tableDescs) {
            tables.add(tableDesc.getDatabase() + "." + tableDesc.getName());
        }

        if (!tables.contains(table)) {
            throw new RuntimeException("Operation failed, table:" + table + " not exists");
        }
    }

    public void validateColumn(String project, String table, Collection<String> columns) throws IOException {
        Preconditions.checkState(columns != null && columns.size() > 0);
        List<String> cols = getAllColumns(project, table);
        for (String c : columns) {
            if (!cols.contains(c)) {
                throw new RuntimeException("Operation failed, column:" + c + " not exists");
            }
        }
    }

    private List<String> getAllColumns(String project, String table) throws IOException {
        List<TableDesc> tableDescByProject = tableService.getTableDescByProject(project, true);
        List<String> cols = new ArrayList<>();

        for (TableDesc tableDesc : tableDescByProject) {
            String tbl = tableDesc.getDatabase() + "." + tableDesc.getName();
            if (tbl.equals(table)) {
                for (ColumnDesc column : tableDesc.getColumns()) {
                    cols.add(column.getName());
                }
                break;
            }
        }
        return cols;
    }

    public List<String > getAllUsers() throws IOException {
        List<ManagedUser> managedUsers = userService.listUsers();
        List<String> allUsers = new ArrayList<>();
        for (ManagedUser managedUser : managedUsers) {
            allUsers.add(managedUser.getUsername());
        }
        return allUsers;
    }

    public void vaildateArgs(String... args) {
        for (String arg : args) {
            Preconditions.checkState(!StringUtils.isEmpty(arg));
        }
    }
}
