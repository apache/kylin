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

import static org.hamcrest.core.Is.is;

import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.LoggingEvent;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.Signature;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.context.SecurityContextImpl;

@RunWith(MockitoJUnitRunner.class)
public class UserBehaviorAuditLogTest {

    @Mock
    private Appender mockAppender;

    @Captor
    private ArgumentCaptor captorLoggingEvent;

    @Before
    public void setup() {
        LogManager.getLogger(UserBehaviorAuditLog.class).addAppender(mockAppender);

        SecurityContextHolder holder = new SecurityContextHolder();

        SecurityContext securityContext = new SecurityContextImpl();
        Authentication auth = new TestingAuthenticationToken("user_name", "");
        securityContext.setAuthentication(auth);

        holder.setContext(securityContext);

    }

    @After
    public void teardown() {
        LogManager.getLogger(UserBehaviorAuditLog.class).removeAppender(mockAppender);
    }

    public JoinPoint mockBaseJoinPoint() {
        JoinPoint mockJoinPoint = Mockito.mock(JoinPoint.class);
        Signature mockSignature = Mockito.mock(Signature.class);
        Mockito.when(mockJoinPoint.getSignature()).thenReturn(mockSignature);
        Mockito.when(mockJoinPoint.getSignature().getName()).thenReturn("method_name");

        return mockJoinPoint;
    }

    @Test
    public void testAuditLogauditAllParamWithSignature() {
        UserBehaviorAuditLog auditLog = new UserBehaviorAuditLog();

        JoinPoint mockJoinPoint = mockBaseJoinPoint();
        Object[] args = { "args1", "args2", "args3" };
        Mockito.when(mockJoinPoint.getArgs()).thenReturn(args);

        auditLog.auditAllParamWithSignature(mockJoinPoint);

        Mockito.verify(mockAppender).doAppend((LoggingEvent) captorLoggingEvent.capture());
        LoggingEvent loggingEvent = (LoggingEvent) captorLoggingEvent.getValue();
        Assert.assertThat(loggingEvent.getRenderedMessage(),
                is("User: user_name trigger method_name, arguments: [args1, args2, args3]."));
    }

    @Test
    public void testExecuteFailed() {
        UserBehaviorAuditLog auditLog = new UserBehaviorAuditLog();

        JoinPoint mockJoinPoint = mockBaseJoinPoint();
        Throwable reason = new Throwable("reason");

        auditLog.executeFailed(mockJoinPoint, reason);

        Mockito.verify(mockAppender).doAppend((LoggingEvent) captorLoggingEvent.capture());
        LoggingEvent loggingEvent = (LoggingEvent) captorLoggingEvent.getValue();
        Assert.assertThat(loggingEvent.getRenderedMessage(),
                is("User: user_name execute method_name failed, exception: java.lang.Throwable: reason"));
    }

    @Test
    public void testFinish() {
        UserBehaviorAuditLog auditLog = new UserBehaviorAuditLog();

        JoinPoint mockJoinPoint = Mockito.mock(JoinPoint.class);
        Signature mockSignature = Mockito.mock(Signature.class);
        Mockito.when(mockJoinPoint.getSignature()).thenReturn(mockSignature);
        Mockito.when(mockJoinPoint.getSignature().getName()).thenReturn("method_name");

        auditLog.finish(mockJoinPoint);

        Mockito.verify(mockAppender).doAppend((LoggingEvent) captorLoggingEvent.capture());
        LoggingEvent loggingEvent = (LoggingEvent) captorLoggingEvent.getValue();
        Assert.assertThat(loggingEvent.getRenderedMessage(), is("User: user_name execute method_name finished."));
    }

    @Test
    public void testSubmitJobAudit() {
        UserBehaviorAuditLog auditLog = new UserBehaviorAuditLog();

        JoinPoint mockJoinPoint = mockBaseJoinPoint();
        CubeInstance cube = new CubeInstance();
        SegmentRange.TSRange tsRange = new SegmentRange.TSRange(0L, 99999L);
        cube.setName("cube_name");
        Object[] args = { cube, tsRange, tsRange, null, null, CubeBuildTypeEnum.BUILD, true, "user_name", 0 };
        Mockito.when(mockJoinPoint.getArgs()).thenReturn(args);

        auditLog.submitJobAudit(mockJoinPoint);

        Mockito.verify(mockAppender).doAppend((LoggingEvent) captorLoggingEvent.capture());
        LoggingEvent loggingEvent = (LoggingEvent) captorLoggingEvent.getValue();
        Assert.assertThat(loggingEvent.getRenderedMessage(),
                is("User: user_name submit job of BUILD for cube CUBE[name=cube_name] TSRange[0,99999)."));
    }

    @Test
    public void testOptimizeJobAudit() {
        UserBehaviorAuditLog auditLog = new UserBehaviorAuditLog();

        JoinPoint mockJoinPoint = mockBaseJoinPoint();
        CubeInstance cube = new CubeInstance();
        cube.setName("cube_name");
        Object[] args = { cube, null, "user_name" };
        Mockito.when(mockJoinPoint.getArgs()).thenReturn(args);

        auditLog.optimizeJobAudit(mockJoinPoint);

        Mockito.verify(mockAppender).doAppend((LoggingEvent) captorLoggingEvent.capture());
        LoggingEvent loggingEvent = (LoggingEvent) captorLoggingEvent.getValue();
        Assert.assertThat(loggingEvent.getRenderedMessage(),
                is("User: user_name submit job of optimization for cube CUBE[name=cube_name]."));
    }

    @Test
    public void testRecoverSegmentOptimizeJobAudit() {
        UserBehaviorAuditLog auditLog = new UserBehaviorAuditLog();

        JoinPoint mockJoinPoint = mockBaseJoinPoint();
        CubeInstance cube = new CubeInstance();
        cube.setName("cube_name");
        CubeSegment segment = new CubeSegment();
        segment.setCubeInstance(cube);
        segment.setName("segment_name");
        Object[] args = { segment, "user_name" };
        Mockito.when(mockJoinPoint.getArgs()).thenReturn(args);

        auditLog.recoverSegmentOptimizeJobAudit(mockJoinPoint);

        Mockito.verify(mockAppender).doAppend((LoggingEvent) captorLoggingEvent.capture());
        LoggingEvent loggingEvent = (LoggingEvent) captorLoggingEvent.getValue();
        Assert.assertThat(loggingEvent.getRenderedMessage(),
                is("User: user_name submit job of recovering optimizing segment for cube_name[segment_name]."));
    }

    private AclEntity ae = new AclEntity() {
        @Override
        public String getId() {
            return "uuid";
        }
    };

    private Permission permission = new Permission() {
        @Override
        public int getMask() {
            return 16;
        }

        @Override
        public String getPattern() {
            return "pattern";
        }
    };

    private Sid sid = new Sid() {
        @Override
        public String toString() {
            return "PrincipalSid[user_name]";
        }
    };

    @Test
    public void testAccessGrantAudit() {
        UserBehaviorAuditLog auditLog = new UserBehaviorAuditLog();

        auditLog.accessGrantAudit(ae, permission, sid);

        Mockito.verify(mockAppender).doAppend((LoggingEvent) captorLoggingEvent.capture());
        LoggingEvent loggingEvent = (LoggingEvent) captorLoggingEvent.getValue();
        Assert.assertThat(loggingEvent.getRenderedMessage(), is(
                "User: user_name grant PrincipalSid[user_name] access[mask: 16,  pattern: pattern] to entity[uuid]."));
    }

    @Test
    public void testAccessUpdateAudit() {
        UserBehaviorAuditLog auditLog = new UserBehaviorAuditLog();

        auditLog.accessUpdateAudit(ae, 0, permission);

        Mockito.verify(mockAppender).doAppend((LoggingEvent) captorLoggingEvent.capture());
        LoggingEvent loggingEvent = (LoggingEvent) captorLoggingEvent.getValue();
        Assert.assertThat(loggingEvent.getRenderedMessage(),
                is("User: user_name update entity[uuid] accessEntryId[0] to access[mask: 16,  pattern: pattern]."));
    }

    @Test
    public void testAccessRevokeAudit() {
        UserBehaviorAuditLog auditLog = new UserBehaviorAuditLog();

        auditLog.accessRevokeAudit(ae, 0);

        Mockito.verify(mockAppender).doAppend((LoggingEvent) captorLoggingEvent.capture());
        LoggingEvent loggingEvent = (LoggingEvent) captorLoggingEvent.getValue();
        Assert.assertThat(loggingEvent.getRenderedMessage(),
                is("User: user_name revoke entity[uuid] accessEntryId[0] access."));
    }

    @Test
    public void testAccessCleanAudit() {
        UserBehaviorAuditLog auditLog = new UserBehaviorAuditLog();

        auditLog.accessCleanAudit(ae, true);

        Mockito.verify(mockAppender).doAppend((LoggingEvent) captorLoggingEvent.capture());
        LoggingEvent loggingEvent = (LoggingEvent) captorLoggingEvent.getValue();
        Assert.assertThat(loggingEvent.getRenderedMessage(),
                is("User: user_name clean entity[uuid] access [delete children: true]."));
    }
}
