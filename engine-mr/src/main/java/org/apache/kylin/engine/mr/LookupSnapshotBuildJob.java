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

package org.apache.kylin.engine.mr;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;

public class LookupSnapshotBuildJob extends DefaultChainedExecutable {

    public static final Integer DEFAULT_PRIORITY = 30;

    private static final String DEPLOY_ENV_NAME = "envName";
    private static final String PROJECT_INSTANCE_NAME = "projectName";

    private static final String JOB_TYPE = "Lookup ";

    public static LookupSnapshotBuildJob createJob(CubeInstance cube, String tableName, String submitter,
            KylinConfig kylinConfig) {
        return initJob(cube, tableName, submitter, kylinConfig);
    }

    private static LookupSnapshotBuildJob initJob(CubeInstance cube, String tableName, String submitter,
            KylinConfig kylinConfig) {
        List<ProjectInstance> projList = ProjectManager.getInstance(kylinConfig).findProjects(cube.getType(),
                cube.getName());
        if (projList == null || projList.size() == 0) {
            throw new RuntimeException("Cannot find the project containing the cube " + cube.getName() + "!!!");
        } else if (projList.size() >= 2) {
            String msg = "Find more than one project containing the cube " + cube.getName()
                    + ". It does't meet the uniqueness requirement!!! ";
            throw new RuntimeException(msg);
        }

        LookupSnapshotBuildJob result = new LookupSnapshotBuildJob();
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss", Locale.ROOT);
        format.setTimeZone(TimeZone.getTimeZone(kylinConfig.getTimeZone()));
        result.setDeployEnvName(kylinConfig.getDeployEnv());
        result.setProjectName(projList.get(0).getName());
        CubingExecutableUtil.setCubeName(cube.getName(), result.getParams());
        result.setName(JOB_TYPE + " CUBE - " + cube.getName() + " - " + " TABLE - " + tableName + " - "
                + format.format(new Date(System.currentTimeMillis())));
        result.setSubmitter(submitter);
        result.setNotifyList(cube.getDescriptor().getNotifyList());
        return result;
    }

    protected void setDeployEnvName(String name) {
        setParam(DEPLOY_ENV_NAME, name);
    }

    public String getDeployEnvName() {
        return getParam(DEPLOY_ENV_NAME);
    }

    public String getProjectName() {
        return getParam(PROJECT_INSTANCE_NAME);
    }

    public void setProjectName(String name) {
        setParam(PROJECT_INSTANCE_NAME, name);
    }

    @Override
    public int getDefaultPriority() {
        return DEFAULT_PRIORITY;
    }
}
