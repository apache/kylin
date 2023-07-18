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

package org.apache.kylin.rest.broadcaster;

import java.io.IOException;
import java.util.Arrays;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.AddS3CredentialToSparkBroadcastEventNotifier;
import org.apache.kylin.common.persistence.transaction.AuditLogBroadcastEventNotifier;
import org.apache.kylin.common.persistence.transaction.BroadcastEventReadyNotifier;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.cluster.DefaultClusterManager;
import org.apache.kylin.rest.config.initialize.BroadcastListener;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.AdminUserSyncEventNotifier;
import org.apache.kylin.rest.security.UserAclManager;
import org.apache.kylin.rest.service.AuditLogService;
import org.apache.kylin.rest.service.UserAclService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.spark.sql.SparderEnv;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.util.ReflectionTestUtils;

import org.apache.kylin.metadata.epoch.EpochManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import static org.apache.kylin.common.persistence.transaction.BroadcastEventReadyNotifier.BroadcastScopeEnum.WHOLE_NODES;

@Slf4j
@MetadataInfo(onlyProps = true)
class BroadcasterTest {

    private SpringApplication application;

    @BeforeEach
    void setup() {
        this.application = new SpringApplication(Config.class);
        this.application.setWebApplicationType(WebApplicationType.NONE);
    }

    @Test
    void testBroadcast() {
        try (ConfigurableApplicationContext context = this.application.run("--kylin.server.mode=all")) {
            SpringContext springContext = context.getBean(SpringContext.class);
            ReflectionTestUtils.setField(springContext, "applicationContext", context);
            Broadcaster broadcaster = context.getBean(Broadcaster.class);
            broadcaster.announce(new BroadcastEventReadyNotifier());
            ClusterManager clusterManager = (ClusterManager) ReflectionTestUtils.getField(broadcaster,
                    "clusterManager");
            Assertions.assertNotNull(clusterManager);
            Assertions.assertEquals(clusterManager.getClass(), DefaultClusterManager.class);
        }
    }

    @Test
    void testBroadcastWithAnnounceContains() {
        try (ConfigurableApplicationContext context = this.application.run("--kylin.server.mode=all")) {
            SpringContext springContext = context.getBean(SpringContext.class);
            ReflectionTestUtils.setField(springContext, "applicationContext", context);
            Broadcaster broadcaster = context.getBean(Broadcaster.class);

            BroadcastEventReadyNotifier eventReadyNotifier = new BroadcastEventReadyNotifier();
            broadcaster.announce(eventReadyNotifier);
            // announce twice
            broadcaster.announce(eventReadyNotifier);

            Assertions.assertSame(WHOLE_NODES, eventReadyNotifier.getBroadcastScope());
        }
    }

    @Test
    void testBroadcastSyncAdminUserAcl() throws Exception {
        EpochManager epochManager = EpochManager.getInstance();
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, true);
        BroadcastListener broadcastListener = new BroadcastListener();
        val userAclService = Mockito.spy(UserAclService.class);
        ReflectionTestUtils.setField(userAclService, "userService", Mockito.spy(UserService.class));
        ReflectionTestUtils.setField(broadcastListener, "userAclService", userAclService);
        broadcastListener.handle(new AdminUserSyncEventNotifier(Arrays.asList("admin"), true));
        val userAclManager = UserAclManager.getInstance(KylinConfig.getInstanceFromEnv());
        Assert.assertTrue(userAclManager.get("admin").hasPermission(AclPermission.DATA_QUERY.getMask()));
    }

    @Test
    void testBroadcastAddS3Conf() throws Exception {
        BroadcastListener broadcastListener = new BroadcastListener();
        broadcastListener.handle(new AddS3CredentialToSparkBroadcastEventNotifier("aa", "bb", "cc"));
        Assert.assertTrue(SparderEnv.getSparkSession().conf().contains(String.format("fs.s3a.bucket.%s.assumed.role.arn", "aa")));
    }

    @Test
    void testBroadcastWithAuditLog() {
        BroadcastListener broadcastListener = new BroadcastListener();
        val auditLogService = Mockito.spy(AuditLogService.class);
        ReflectionTestUtils.setField(broadcastListener, "auditLogService", auditLogService);
        String errorMsg = "";
        try {
            broadcastListener.handle(new AuditLogBroadcastEventNotifier());
        } catch (IOException e) {
            errorMsg = e.getMessage();
        }
        Assertions.assertTrue(errorMsg.isEmpty());
    }

    @Configuration
    static class Config {
        @Bean
        @Primary
        public SpringContext springContext() {
            return Mockito.spy(new SpringContext());
        }

        @Bean
        public ClusterManager clusterManager() {
            return new DefaultClusterManager(7070);
        }

        @Bean
        public Broadcaster broadcaster() {
            return new Broadcaster(clusterManager());
        }
    }

}
