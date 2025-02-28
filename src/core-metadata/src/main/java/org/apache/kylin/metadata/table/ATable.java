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

package org.apache.kylin.metadata.table;

import java.util.Arrays;
import java.util.Locale;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DatabaseDesc;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import lombok.Getter;
import lombok.Setter;

public abstract class ATable extends RootPersistentEntity {

    protected final DatabaseDesc database = new DatabaseDesc();
    protected String identity = null;
    protected String name;

    @Getter
    @Setter
    @JsonProperty("columns")
    protected ColumnDesc[] columns;

    public String getCaseSensitiveIdentity() {
        if (identity == null) {
            if (this.getCaseSensitiveDatabase().equals("null")) {
                identity = String.format(Locale.ROOT, "%s", this.getCaseSensitiveName());
            } else {
                identity = String.format(Locale.ROOT, "%s.%s", this.getCaseSensitiveDatabase(),
                        this.getCaseSensitiveName());
            }
        }
        return identity;
    }

    public String getName() {
        return name == null ? null : name.toUpperCase(Locale.ROOT);
    }

    @JsonGetter("name")
    public String getCaseSensitiveName() {
        return this.name;
    }

    public String getDatabase() {
        return database.getName().toUpperCase(Locale.ROOT);
    }

    @JsonGetter("database")
    public String getCaseSensitiveDatabase() {
        return database.getName();
    }

    @JsonSetter("database")
    public void setDatabase(String database) {
        this.database.setName(database);
    }

    @Override
    public String resourceName() {
        return generateResourceName(getProject(), getIdentity());
    }

    public static String generateResourceName(String project, String identity) {
        return project + "." + identity;
    }

    public String getIdentity() {
        String originIdentity = getCaseSensitiveIdentity();
        return originIdentity.toUpperCase(Locale.ROOT);
    }

    public String getLowerCaseIdentity() {
        String originIdentity = getCaseSensitiveIdentity();
        return originIdentity.toLowerCase(Locale.ROOT);
    }

    @JsonSetter("name")
    public void setName(String name) {
        if (name != null) {
            String[] splits = StringSplitter.split(name, ".");
            if (splits.length == 2) {
                this.setDatabase(splits[0]);
                this.name = splits[1];
            } else if (splits.length == 1) {
                this.name = splits[0];
            }
            identity = null;
        } else {
            this.name = null;
        }
    }

    public void init(String project) {
        this.project = project;

        if (columns != null) {
            Arrays.sort(columns, (col1, col2) -> {
                Integer id1 = Integer.parseInt(col1.getId());
                Integer id2 = Integer.parseInt(col2.getId());
                return id1.compareTo(id2);
            });

            for (ColumnDesc col : columns) {
                col.init(this);
            }
        }
    }

}
