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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.LinkedList;
import java.util.Set;

import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

@MetadataInfo
public class ProjectSerialEventBusTest {

    private ProjectSerialEventBus projectSerialEventBus;

    private EventBusFactory eventBusFactory;

    @BeforeEach
    public void setUp() throws Exception {
        projectSerialEventBus = spy(ProjectSerialEventBus.getInstance());
        eventBusFactory = mock(EventBusFactory.class);
        ReflectionTestUtils.setField(projectSerialEventBus, "eventBus", eventBusFactory);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testInnerQueueStatus() throws Exception {
        LinkedList<SchedulerEventNotifier> innerEventsQueue = (LinkedList<SchedulerEventNotifier>) ReflectionTestUtils
                .getField(projectSerialEventBus, "eventsQueue");
        Set<ProjectSerialEventBus.RunningProject> runningProjects = (Set<ProjectSerialEventBus.RunningProject>) ReflectionTestUtils
                .getField(projectSerialEventBus, "runningProjects");

        projectSerialEventBus.postAsync(new ProjectEscapedNotifier("project1"));
        assertEquals(1, runningProjects.size());
        projectSerialEventBus.postAsync(new ProjectControlledNotifier("project1"));
        assertEquals(1, runningProjects.size());
        projectSerialEventBus.postAsync(new ProjectEscapedNotifier("project2"));
        assertEquals(2, runningProjects.size());
        projectSerialEventBus.postAsync(new ProjectControlledNotifier("project2"));
        assertEquals(2, runningProjects.size());

        verify(projectSerialEventBus, times(2)).dispatch();
        assertEquals(2, innerEventsQueue.size());
        assertEquals("project1", innerEventsQueue.get(0).getProject());
        assertTrue(innerEventsQueue.get(0) instanceof ProjectControlledNotifier);
        assertEquals("project2", innerEventsQueue.get(1).getProject());
        assertTrue(innerEventsQueue.get(1) instanceof ProjectControlledNotifier);

        projectSerialEventBus.finishProjectAndDispatch("project1");
        assertEquals(2, runningProjects.size());
        projectSerialEventBus.finishProjectAndDispatch("project2");
        assertEquals(2, runningProjects.size());

        assertEquals(0, innerEventsQueue.size());

        projectSerialEventBus.finishProjectAndDispatch("project1");
        assertEquals(1, runningProjects.size());
        projectSerialEventBus.finishProjectAndDispatch("project2");
        assertEquals(0, runningProjects.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testTimingDispatcherRun() {
        LinkedList<SchedulerEventNotifier> innerEventsQueue = (LinkedList<SchedulerEventNotifier>) ReflectionTestUtils
                .getField(projectSerialEventBus, "eventsQueue");
        Set<ProjectSerialEventBus.RunningProject> runningProjects = (Set<ProjectSerialEventBus.RunningProject>) ReflectionTestUtils
                .getField(projectSerialEventBus, "runningProjects");

        innerEventsQueue.add(new ProjectEscapedNotifier("project1"));
        assertEquals(1, innerEventsQueue.size());
        assertEquals(0, runningProjects.size());

        new ProjectSerialEventBus.TimingDispatcher().run();

        assertEquals(0, innerEventsQueue.size());
        assertEquals(1, runningProjects.size());
    }

    @Test
    void testRunningProjectExpired() {
        ProjectSerialEventBus.RunningProject rp = ProjectSerialEventBus.RunningProject.newInstance("project1");
        final long DURATION_31_MINUTES_MILLIS = 31L * 60L * 1000L;
        ReflectionTestUtils.setField(rp, "beginTime", System.currentTimeMillis() - DURATION_31_MINUTES_MILLIS);
        assertTrue(rp.isExpired());
    }
}
