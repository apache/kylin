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
import org.apache.kylin.engine.streaming.StreamingManager;
import org.apache.kylin.invertedindex.IIDescManager;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.manager.ExecutableManager;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.storage.hybrid.HybridManager;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
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
    
    public final MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(getConfig());
    }

    public final CubeManager getCubeManager() {
        return CubeManager.getInstance(getConfig());
    }

    public final StreamingManager getStreamingManager() {
        return StreamingManager.getInstance(getConfig());
    }

    public final KafkaConfigManager getKafkaManager() throws IOException {
        return  KafkaConfigManager.getInstance(getConfig());
    }

    public final CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(getConfig());
    }

    public final ProjectManager getProjectManager() {
        return ProjectManager.getInstance(getConfig());
    }

    public final HybridManager getHybridManager() {
        return HybridManager.getInstance(getConfig());
    }

    public final ExecutableManager getExecutableManager() {
        return ExecutableManager.getInstance(getConfig());
    }

    public final IIDescManager getIIDescManager() {
        return IIDescManager.getInstance(getConfig());
    }

    public final IIManager getIIManager() {
        return IIManager.getInstance(getConfig());
    }

    protected List<CubingJob> listAllCubingJobs(final String cubeName, final String projectName, final Set<ExecutableState> statusList, final Map<String, Output> allOutputs) {
        List<CubingJob> results = Lists.newArrayList(FluentIterable.from(getExecutableManager().getAllExecutables()).filter(new Predicate<AbstractExecutable>() {
            @Override
            public boolean apply(AbstractExecutable executable) {
                if (executable instanceof CubingJob) {
                    if (cubeName == null) {
                        return true;
                    }
                    return ((CubingJob) executable).getCubeName().equalsIgnoreCase(cubeName);
                } else {
                    return false;
                }
            }
        }).transform(new Function<AbstractExecutable, CubingJob>() {
            @Override
            public CubingJob apply(AbstractExecutable executable) {
                return (CubingJob) executable;
            }
        }).filter(new Predicate<CubingJob>() {
            @Override
            public boolean apply(CubingJob executable) {
                if (null == projectName || null == getProjectManager().getProject(projectName)) {
                    return true;
                } else {
                    ProjectInstance project = getProjectManager().getProject(projectName);
                    return project.containsRealization(RealizationType.CUBE, executable.getCubeName());
                }
            }
        }).filter(new Predicate<CubingJob>() {
            @Override
            public boolean apply(CubingJob executable) {
                return statusList.contains(allOutputs.get(executable.getId()).getState());
            }
        }));
        return results;
    }

    protected List<CubingJob> listAllCubingJobs(final String cubeName, final String projectName, final Set<ExecutableState> statusList, long timeStartInMillis, long timeEndInMillis, final Map<String, Output> allOutputs) {
        List<CubingJob> results = Lists.newArrayList(FluentIterable.from(getExecutableManager().getAllExecutables(timeStartInMillis, timeEndInMillis)).filter(new Predicate<AbstractExecutable>() {
            @Override
            public boolean apply(AbstractExecutable executable) {
                if (executable instanceof CubingJob) {
                    if (cubeName == null) {
                        return true;
                    }
                    return ((CubingJob) executable).getCubeName().equalsIgnoreCase(cubeName);
                } else {
                    return false;
                }
            }
        }).transform(new Function<AbstractExecutable, CubingJob>() {
            @Override
            public CubingJob apply(AbstractExecutable executable) {
                return (CubingJob) executable;
            }
        }).filter(new Predicate<CubingJob>() {
            @Override
            public boolean apply(CubingJob executable) {
                if (null == projectName || null == getProjectManager().getProject(projectName)) {
                    return true;
                } else {
                    ProjectInstance project = getProjectManager().getProject(projectName);
                    return project.containsRealization(RealizationType.CUBE, executable.getCubeName());
                }
            }
        }).filter(new Predicate<CubingJob>() {
            @Override
            public boolean apply(CubingJob executable) {
                return statusList.contains(allOutputs.get(executable.getId()).getState());
            }
        }));
        return results;
    }

    protected List<CubingJob> listAllCubingJobs(final String cubeName, final String projectName, final Set<ExecutableState> statusList) {
        return listAllCubingJobs(cubeName, projectName, statusList, getExecutableManager().getAllOutputs());
    }

    protected List<CubingJob> listAllCubingJobs(final String cubeName, final String projectName) {
        return listAllCubingJobs(cubeName, projectName, EnumSet.allOf(ExecutableState.class), getExecutableManager().getAllOutputs());
    }

}
