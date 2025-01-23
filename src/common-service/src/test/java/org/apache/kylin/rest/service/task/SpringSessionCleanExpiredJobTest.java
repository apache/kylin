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

package org.apache.kylin.rest.service.task;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.rest.util.SpringContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.session.jdbc.JdbcIndexedSessionRepository;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SpringContext.class)
@PowerMockIgnore("javax.management.*")
public class SpringSessionCleanExpiredJobTest {

    @Test
    public void testSpringSessionClean() {
        JdbcIndexedSessionRepository jdbcIndexedSessionRepository = prepareSpringSessionBean();
        SpringSessionCleanExpiredJob springSessionCleanExpiredJob = new SpringSessionCleanExpiredJob();
        ExecuteResult executeResult = springSessionCleanExpiredJob.doWork(null);
        assertEquals(ExecuteResult.State.SUCCEED, executeResult.state());
        Mockito.verify(jdbcIndexedSessionRepository).cleanUpExpiredSessions();
    }

    private static JdbcIndexedSessionRepository prepareSpringSessionBean() {
        ApplicationContext applicationContext = Mockito.mock(ApplicationContext.class);
        JdbcIndexedSessionRepository jdbcIndexedSessionRepository = Mockito.mock(JdbcIndexedSessionRepository.class);
        Mockito.when(applicationContext.getBean(JdbcIndexedSessionRepository.class))
                .thenReturn(jdbcIndexedSessionRepository);
        PowerMockito.mockStatic(SpringContext.class);
        PowerMockito.when(SpringContext.getApplicationContext()).thenReturn(applicationContext);
        return jdbcIndexedSessionRepository;
    }
}
