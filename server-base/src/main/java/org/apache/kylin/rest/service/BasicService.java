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

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.streaming.StreamingManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.manager.ExecutableManager;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.badquery.BadQueryHistoryManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.storage.hybrid.HybridManager;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

public abstract class BasicService {

    public KylinConfig getConfig() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        if (kylinConfig == null) {
            throw new IllegalArgumentException("Failed to load kylin config instance");
        }

        return kylinConfig;
    }

    public MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(getConfig());
    }

    public CubeManager getCubeManager() {
        return CubeManager.getInstance(getConfig());
    }

    public StreamingManager getStreamingManager() {
        return StreamingManager.getInstance(getConfig());
    }

    public KafkaConfigManager getKafkaManager() throws IOException {
        return KafkaConfigManager.getInstance(getConfig());
    }

    public CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(getConfig());
    }

    public ProjectManager getProjectManager() {
        return ProjectManager.getInstance(getConfig());
    }

    public HybridManager getHybridManager() {
        return HybridManager.getInstance(getConfig());
    }

    public ExecutableManager getExecutableManager() {
        return ExecutableManager.getInstance(getConfig());
    }

    public BadQueryHistoryManager getBadQueryHistoryManager() {
        return BadQueryHistoryManager.getInstance(getConfig());
    }

    protected List<CubingJob> listAllCubingJobs(final String cubeName, final String projectName, final Set<ExecutableState> statusList, final Map<String, Output> allOutputs) {
        return listAllCubingJobs(cubeName, projectName, statusList, -1L, -1L, allOutputs);
    }

    protected List<CubingJob> listAllCubingJobs(final String cubeName, final String projectName, final Set<ExecutableState> statusList, long timeStartInMillis, long timeEndInMillis, final Map<String, Output> allOutputs) {
        List<CubingJob> results = Lists.newArrayList(FluentIterable.from(getExecutableManager().getAllExecutables(timeStartInMillis, timeEndInMillis)).filter(new Predicate<AbstractExecutable>() {
            @Override
            public boolean apply(AbstractExecutable executable) {
                if (executable instanceof CubingJob) {
                    if (cubeName == null) {
                        return true;
                    }
                    return CubingExecutableUtil.getCubeName(executable.getParams()).equalsIgnoreCase(cubeName);
                } else {
                    return false;
                }
            }
        }).transform(new Function<AbstractExecutable, CubingJob>() {
            @Override
            public CubingJob apply(AbstractExecutable executable) {
                return (CubingJob) executable;
            }
        }).filter(Predicates.and(new Predicate<CubingJob>() {
            @Override
            public boolean apply(CubingJob executable) {
                if (null == projectName || null == getProjectManager().getProject(projectName)) {
                    return true;
                } else {
                    ProjectInstance project = getProjectManager().getProject(projectName);
                    return project.containsRealization(RealizationType.CUBE, CubingExecutableUtil.getCubeName(executable.getParams()));
                }
            }
        }, new Predicate<CubingJob>() {
            @Override
            public boolean apply(CubingJob executable) {
                try {
                    Output output = allOutputs.get(executable.getId());
                    ExecutableState state = output.getState();
                    boolean ret = statusList.contains(state);
                    return ret;
                } catch (Exception e) {
                    throw e;
                }
            }
        })));
        return results;
    }

    protected List<CubingJob> listAllCubingJobs(final String cubeName, final String projectName, final Set<ExecutableState> statusList) {
        return listAllCubingJobs(cubeName, projectName, statusList, getExecutableManager().getAllOutputs());
    }

    protected List<CubingJob> listAllCubingJobs(final String cubeName, final String projectName) {
        return listAllCubingJobs(cubeName, projectName, EnumSet.allOf(ExecutableState.class), getExecutableManager().getAllOutputs());
    }

}
