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
package org.apache.spark.ddl;

import java.util.Set;

import lombok.Data;
import static org.apache.spark.ddl.DDLConstant.CREATE_LOGICAL_VIEW;
import static org.apache.spark.ddl.DDLConstant.DROP_LOGICAL_VIEW;
import static org.apache.spark.ddl.DDLConstant.HIVE_VIEW;
import static org.apache.spark.ddl.DDLConstant.REPLACE_LOGICAL_VIEW;

@Data
public class DDLCheckContext {

  public static final int LOGICAL_VIEW_CREATE_COMMAND = 2;
  public static final int LOGICAL_VIEW_DROP_COMMAND = 3;
  public static final int HIVE_COMMAND = 1;

  private String sql;
  private String project;
  private String userName;
  private Set<String> groups;
  private boolean kerberosEnv;
  private String commandType = HIVE_VIEW;
  private String logicalViewName;
  private String restrict;

  public DDLCheckContext(String sql, String project, String restrict, String userName, Set<String> groups,
      boolean kerberosEnv) {
    this.sql = sql;
    this.project = project;
    this.restrict = restrict;
    this.userName = userName;
    this.groups = groups;
    this.kerberosEnv = kerberosEnv;
  }

  public String getSql() {
    return sql;
  }

  public String getProject() {
    return project;
  }

  public String getUserName() {
    return userName;
  }

  public Set<String> getGroups() {
    return groups;
  }

  public boolean isLogicalViewCommand() {
    return commandType.equals(REPLACE_LOGICAL_VIEW) || commandType.equals(CREATE_LOGICAL_VIEW)
        || commandType.equals(DROP_LOGICAL_VIEW);
  }
  public boolean isHiveCommand() {
    return commandType.equals(HIVE_VIEW);
  }
}
