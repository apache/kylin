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

package org.apache.kylin.metadata.query;

import java.util.List;

import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.rest.model.Query;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@SuppressWarnings("serial")
public class QueryRecord extends RootPersistentEntity {

    @JsonProperty
    private List<Query> queries = Lists.newArrayList();

    @JsonProperty
    private String username;

    public QueryRecord(String project, String username) {
        this.project = project;
        this.username = username;
    }

    @Override
    public MetadataType resourceType() {
        return MetadataType.QUERY_RECORD;
    }

    @Override
    public String resourceName() {
        return generateResourceName(project, username);
    }

    public static String generateResourceName(String project, String username) {
        return project + "." + username;
    }
}
