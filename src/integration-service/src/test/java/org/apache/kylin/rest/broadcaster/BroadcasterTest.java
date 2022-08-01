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

import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.common.persistence.transaction.BroadcastEventReadyNotifier;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.cluster.DefaultClusterManager;
import org.apache.kylin.rest.config.initialize.BroadcastListener;
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

import lombok.extern.slf4j.Slf4j;

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
            Broadcaster broadcaster = Broadcaster.getInstance(KylinConfig.getInstanceFromEnv(),
                    new BroadcastListener() {
                        @Override
                        public void handle(BroadcastEventReadyNotifier notifier) {
                            log.info("received notifier {}.", notifier);
                        }
                    });

            broadcaster.announce(new BroadcastEventReadyNotifier());

            await().atLeast(2, TimeUnit.SECONDS);

            ClusterManager clusterManager = (ClusterManager) ReflectionTestUtils.getField(broadcaster,
                    "clusterManager");
            Assertions.assertNotNull(clusterManager);
            Assertions.assertEquals(clusterManager.getClass(), DefaultClusterManager.class);
        }
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
    }

}
