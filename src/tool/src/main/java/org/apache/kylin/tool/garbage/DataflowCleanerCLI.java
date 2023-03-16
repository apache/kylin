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
package org.apache.kylin.tool.garbage;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataflowCleanerCLI {

    public static void main(String[] args) {
        execute();
        System.out.println("Cleanup dataflow finished.");
        Unsafe.systemExit(0);
    }

    public static void execute() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(config);
        for (ProjectInstance project : projectManager.listAllProjects()) {
            log.info("Start dataflow cleanup for project<{}>", project.getName());
            try {
                cleanupRedundantIndex(project);
            } catch (Exception e) {
                log.warn("Clean dataflow for project<{}> failed", project.getName(), e);
            }
            log.info("Dataflow cleanup for project<{}> finished", project.getName());
        }
    }

    private static void cleanupRedundantIndex(ProjectInstance project) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val models = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project.getName())
                    .listUnderliningDataModels();
            for (NDataModel model : models) {
                removeLayouts(model);
            }
            return 0;
        }, project.getName());
    }

    private static void removeLayouts(NDataModel model) {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val dataflow = dataflowManager.getDataflow(model.getUuid());
        val layoutIds = getLayouts(dataflow);
        val toBeRemoved = Sets.<Long> newHashSet();
        for (NDataSegment segment : dataflow.getSegments()) {
            toBeRemoved.addAll(segment.getSegDetails().getAllLayouts().stream().map(NDataLayout::getLayoutId)
                    .filter(id -> !layoutIds.contains(id)).collect(Collectors.toSet()));
        }
        dataflowManager.removeLayouts(dataflow, Lists.newArrayList(toBeRemoved));
    }

    private static List<Long> getLayouts(NDataflow dataflow) {
        val cube = dataflow.getIndexPlan();
        val layouts = cube.getAllLayouts();
        return layouts.stream().map(LayoutEntity::getId).collect(Collectors.toList());
    }
}
