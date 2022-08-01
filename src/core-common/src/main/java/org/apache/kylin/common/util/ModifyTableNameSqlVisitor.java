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

package org.apache.kylin.common.util;

import java.util.Locale;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import com.google.common.collect.ImmutableList;

public class ModifyTableNameSqlVisitor extends SqlBasicVisitor<Object> {
    private final String oldAliasName;
    private final String newAliasName;

    public ModifyTableNameSqlVisitor(String oldAliasName, String newAliasName) {
        this.oldAliasName = oldAliasName;
        this.newAliasName = newAliasName;
    }

    @Override
    public Object visit(SqlIdentifier id) {
        if (id.names.size() == 2) {
            String table = id.names.get(0).toUpperCase(Locale.ROOT).trim();
            String column = id.names.get(1).toUpperCase(Locale.ROOT).trim();
            if (table.equalsIgnoreCase(oldAliasName)) {
                id.names = ImmutableList.of(newAliasName, column);
            }
        }
        return null;
    }
}
