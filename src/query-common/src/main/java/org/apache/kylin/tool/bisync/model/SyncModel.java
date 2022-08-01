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
package org.apache.kylin.tool.bisync.model;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SyncModel {

    private String projectName;

    private String modelName;

    private String host;

    private String port;

    private JoinTreeNode joinTree;

    private Map<String, ColumnDef> columnDefMap;

    private List<MeasureDef> metrics;

    private Set<String[]> hierarchies;

    public JoinTreeNode getJoinTree() {
        return joinTree;
    }

    public void setJoinTree(JoinTreeNode joinTree) {
        this.joinTree = joinTree;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public Map<String, ColumnDef> getColumnDefMap() {
        return columnDefMap;
    }

    public void setColumnDefMap(Map<String, ColumnDef> columnDefMap) {
        this.columnDefMap = columnDefMap;
    }

    public Set<String[]> getHierarchies() {
        return hierarchies;
    }

    public void setHierarchies(Set<String[]> hierarchies) {
        this.hierarchies = hierarchies;
    }

    public List<MeasureDef> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<MeasureDef> metrics) {
        this.metrics = metrics;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

}
