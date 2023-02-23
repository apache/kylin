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

package org.apache.kylin.tool;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;

public class ProjectTool {

    /**
     * System.out must be used to pass values to the shell script
     * @param args Don't pass in parameters
     */
    public static void main(String[] args) {
        ProjectTool projectTool = new ProjectTool();
        System.out.println(projectTool.projectsList());
    }

    /**
     *
     * @return all projects such as: project1,project2,...
     */
    public String projectsList() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        List<String> projects = NProjectManager.getInstance(kylinConfig).listAllProjects().stream()
                .map(ProjectInstance::getName).collect(Collectors.toList());
        return StringUtils.strip(projects.toString(), "[]");
    }

}
