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

package org.apache.kylin.rest.response;

import org.apache.kylin.metadata.view.LogicalView;
import org.jetbrains.annotations.NotNull;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE,
    isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class LogicalViewResponse implements Comparable<LogicalViewResponse> {
  @JsonProperty("table_name")
  private String tableName;

  @JsonProperty("created_sql")
  private String createdSql;

  @JsonProperty("modified_user")
  private String modifiedUser;

  @JsonProperty("created_project")
  private String createdProject;

  public LogicalViewResponse(LogicalView view) {
    this.tableName = view.getTableName();
    this.createdSql = view.getCreatedSql();
    this.modifiedUser = view.getModifiedUser();
    this.createdProject = view.getCreatedProject();
  }

  @Override
  public int compareTo(@NotNull LogicalViewResponse o) {
    return this.getTableName().compareTo(o.getTableName());
  }
}
