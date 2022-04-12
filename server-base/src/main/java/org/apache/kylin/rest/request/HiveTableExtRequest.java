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

package org.apache.kylin.rest.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

public class HiveTableExtRequest extends HiveTableRequest {
    public HiveTableExtRequest() {
    }
    private String project;

    @JsonProperty("qualified_table_name")
    private String qualifiedTableName;

    @Min(10)
    private int rows;

    public HiveTableExtRequest(String project, String qualifiedTableName, int rows) {
        this.project = project;
        this.qualifiedTableName = qualifiedTableName;
        this.rows = rows;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getQualifiedTableName() {
        return qualifiedTableName;
    }

    public void setQualifiedTableName(String qualifiedTableName) {
        this.qualifiedTableName = qualifiedTableName;
    }

    public int getRows() {
        return rows;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }
}
