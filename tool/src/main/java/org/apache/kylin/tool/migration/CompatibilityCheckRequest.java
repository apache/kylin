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

package org.apache.kylin.tool.migration;

import java.util.List;

public class CompatibilityCheckRequest {
    private List<String> tableDescDataList;
    private String modelDescData;
    private String projectName;

    public List<String> getTableDescDataList() {
        return tableDescDataList;
    }

    public void setTableDescDataList(List<String> tableDescDataList) {
        this.tableDescDataList = tableDescDataList;
    }

    public String getModelDescData() {
        return modelDescData;
    }

    public void setModelDescData(String modelDescData) {
        this.modelDescData = modelDescData;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }
}
