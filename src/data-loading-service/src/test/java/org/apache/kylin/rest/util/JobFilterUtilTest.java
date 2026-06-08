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

package org.apache.kylin.rest.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.rest.JobFilter;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.response.UserProjectPermissionResponse;
import org.apache.kylin.rest.security.AclPermissionEnum;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.TableExtService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class JobFilterUtilTest {

    // JobTimeFilterEnum.ALL -> avoids relative-date computation in getQueryStartTime
    private static final int TIME_FILTER_ALL = 4;

    private ModelService modelService;
    private TableExtService tableExtService;
    private ProjectService projectService;

    @Before
    public void setUp() {
        modelService = Mockito.mock(ModelService.class);
        tableExtService = Mockito.mock(TableExtService.class);
        projectService = Mockito.mock(ProjectService.class);
    }

    private JobFilter newJobFilter(String project, boolean reverse) {
        return new JobFilter(Lists.newArrayList(), Lists.newArrayList(), TIME_FILTER_ALL, "", "", false, project, "",
                reverse);
    }

    private UserProjectPermissionResponse permissionResponse(String projectName) {
        ProjectInstance instance = new ProjectInstance();
        instance.setName(projectName);
        return new UserProjectPermissionResponse(instance, AclPermissionEnum.OPERATION.name());
    }

    @Test
    public void testGetJobMapperFilterEmptyProjectPopulatesProjects() throws Exception {
        Mockito.when(projectService.getProjectsFilterByExactMatchAndPermissionWrapperUserPermission(null, false,
                AclPermissionEnum.OPERATION))
                .thenReturn(Lists.newArrayList(permissionResponse("p1"), permissionResponse("p2")));

        JobMapperFilter filter = JobFilterUtil.getJobMapperFilter(newJobFilter(null, true), 0, 10, modelService,
                tableExtService, projectService);

        Assert.assertEquals(Lists.newArrayList("p1", "p2"), filter.getProjects());
        Assert.assertTrue(StringUtils.isEmpty(filter.getProject()));
        Mockito.verify(projectService, Mockito.times(1))
                .getProjectsFilterByExactMatchAndPermissionWrapperUserPermission(null, false,
                        AclPermissionEnum.OPERATION);
    }

    @Test
    public void testGetJobMapperFilterEmptyProjectNoPermittedProjects() throws Exception {
        Mockito.when(projectService.getProjectsFilterByExactMatchAndPermissionWrapperUserPermission(null, false,
                AclPermissionEnum.OPERATION)).thenReturn(Lists.newArrayList());

        JobMapperFilter filter = JobFilterUtil.getJobMapperFilter(newJobFilter("", true), 0, 10, modelService,
                tableExtService, projectService);

        Assert.assertNotNull(filter.getProjects());
        Assert.assertTrue(filter.getProjects().isEmpty());
    }

    @Test
    public void testGetJobMapperFilterWithProjectKeepsProjectsNull() throws Exception {
        JobMapperFilter filter = JobFilterUtil.getJobMapperFilter(newJobFilter("default", true), 5, 20, modelService,
                tableExtService, projectService);

        Assert.assertEquals("default", filter.getProject());
        // single-project branch must not populate the multi-project list
        Assert.assertNull(filter.getProjects());
        Assert.assertEquals(5, filter.getOffset());
        Assert.assertEquals(20, filter.getLimit());
        Mockito.verify(projectService, Mockito.never()).getProjectsFilterByExactMatchAndPermissionWrapperUserPermission(
                Mockito.any(), Mockito.anyBoolean(), Mockito.any());
    }

    @Test
    public void testGetJobMapperFilterOrderType() throws Exception {
        JobMapperFilter reversed = JobFilterUtil.getJobMapperFilter(newJobFilter("default", true), 0, 10, modelService,
                tableExtService, projectService);
        Assert.assertEquals("DESC", reversed.getOrderType());

        JobMapperFilter ascending = JobFilterUtil.getJobMapperFilter(newJobFilter("default", false), 0, 10,
                modelService, tableExtService, projectService);
        Assert.assertEquals("ASC", ascending.getOrderType());
    }
}
