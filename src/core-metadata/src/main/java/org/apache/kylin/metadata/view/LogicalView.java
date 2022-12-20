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

package org.apache.kylin.metadata.view;

import java.io.Serializable;
import java.util.Locale;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

/**
 * Logical views which only defined in KYLIN
 */
@Data
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE,
    isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class LogicalView extends RootPersistentEntity implements Serializable {

  @JsonProperty("table_name")
  private String tableName;

  @JsonProperty("created_sql")
  private String createdSql;

  @JsonProperty("modified_user")
  private String modifiedUser;

  @JsonProperty("created_project")
  private String createdProject;

  public LogicalView() {}

  public LogicalView(String tableName, String createdSql, String modifiedUser, String createdProject) {
    this.tableName = tableName.toUpperCase(Locale.ROOT);
    this.createdSql = createdSql;
    this.modifiedUser = modifiedUser;
    this.createdProject = createdProject;
  }

  @Override
  public String resourceName() {
    return tableName.toUpperCase(Locale.ROOT);
  }

  @Override
  public String getResourcePath() {
    return ResourceStore.VIEW_ROOT + "/" + resourceName();
  }
}
