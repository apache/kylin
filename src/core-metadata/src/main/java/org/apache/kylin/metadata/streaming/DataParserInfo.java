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

package org.apache.kylin.metadata.streaming;

import java.io.Serializable;
import java.util.List;

import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.MetadataConstants;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@Data
public class DataParserInfo extends RootPersistentEntity implements Serializable {

    @JsonProperty("project")
    private String project;
    @JsonProperty("class_name")
    private String className;
    @JsonProperty("jar_name")
    private String jarName;

    @JsonProperty("streaming_tables")
    private List<String> streamingTables = Lists.newCopyOnWriteArrayList();

    public DataParserInfo() {
    }

    public DataParserInfo(String project, String className, String jarName) {
        this.project = project;
        this.className = className;
        this.jarName = jarName;
    }

    @Override
    public String resourceName() {
        return generateResourcePath(project, className);
    }

    public static String generateResourcePath(String project, String className) {
        return project + "." + className;
    }

    @Override
    public MetadataType resourceType() {
        return MetadataType.DATA_PARSER;
    }
}
