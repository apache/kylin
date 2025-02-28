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

package org.apache.kylin.common.scheduler;

import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.kylin.common.Singletons;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProjectSerialEventBus {

    public static class TimingDispatcher implements Runnable {

        public static final Duration INTERVAL = Duration.ofMinutes(10L);

        @Override
        public void run() {
            log.info("ProjectSerialEventBus.TimingDispatcher invokes dispatch");
            ProjectSerialEventBus.getInstance().dispatch();
        }
    }

    // timeout 30 minutes
    private static final long TIMEOUT_MILLISECONDS = 30L * 60L * 1000L;

    public static ProjectSerialEventBus getInstance() {
        return Singletons.getInstance(ProjectSerialEventBus.class);
    }

    private final EventBusFactory eventBus = EventBusFactory.getInstance();
    private final Queue<SchedulerEventNotifier> eventsQueue = new LinkedList<>();
    private final Set<RunningProject> runningProjects = new HashSet<>();
    private final Consumer<SchedulerEventNotifier> finishProjectCallback //
            = event -> finishProjectAndDispatch(event.getProject());

    private ProjectSerialEventBus() {
    }

    public synchronized void postAsync(SchedulerEventNotifier event) {
        log.info("Post event {} on ProjectSerialEventBus", event);
        event.setCallback(finishProjectCallback);
        eventsQueue.add(event);
        if (!runningProjects.contains(RunningProject.wrapForComparison(event.getProject()))) {
            dispatch();
        }
    }

    public synchronized void dispatch() {
        // Remove expired running projects at first
        runningProjects.removeIf(RunningProject::isExpired);
        // Try dispatch events
        Iterator<SchedulerEventNotifier> it = eventsQueue.iterator();
        while (it.hasNext()) {
            SchedulerEventNotifier e = it.next();
            String project = e.getProject();
            if (!runningProjects.contains(RunningProject.wrapForComparison(project))) {
                log.info("ProjectSerialEventBus dispatch event: {}", e);
                eventBus.postAsync(e);
                runningProjects.add(RunningProject.newInstance(project));
                it.remove();
            }
        }
    }

    public synchronized void finishProjectAndDispatch(String project) {
        log.info("ProjectSerialEventBus project({}) event finished", project);
        runningProjects.remove(RunningProject.wrapForComparison(project));
        dispatch();
    }

    @EqualsAndHashCode
    @ToString
    static class RunningProject {

        static RunningProject newInstance(String project) {
            return new RunningProject(project, System.currentTimeMillis());
        }

        static RunningProject wrapForComparison(String project) {
            return new RunningProject(project, -1L);
        }

        private final String project;

        @EqualsAndHashCode.Exclude
        private final long beginTime;

        private RunningProject(String project, long beginTime) {
            this.project = project;
            this.beginTime = beginTime;
        }

        boolean isExpired() {
            if (System.currentTimeMillis() - beginTime > TIMEOUT_MILLISECONDS) {
                log.warn("ProjectSerialEventBus RunningProject expired: {}", this);
                return true;
            }
            return false;
        }
    }
}
