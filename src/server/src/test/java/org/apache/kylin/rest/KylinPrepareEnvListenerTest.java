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
package org.apache.kylin.rest;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kylin.rest.config.KylinPropertySourceConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultBootstrapContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.context.config.ConfigDataEnvironmentPostProcessor;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.env.EnvironmentPostProcessorsFactory;
import org.springframework.boot.logging.DeferredLogs;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Profiles;

class KylinPrepareEnvListenerTest {

    private SpringApplication application;

    @BeforeEach
    void setup() {
        this.application = new SpringApplication(Config.class);
        this.application.setWebApplicationType(WebApplicationType.NONE);
    }

    @Test
    void testNotDefaultActiveProfile() {
        try (ConfigurableApplicationContext context = this.application.run("--kylin.server.mode=all")) {
            Assertions.assertTrue(context.getEnvironment().acceptsProfiles(Profiles.of("dev")));
        }
    }

    @Test
    void testEnvironmentProcessOrder() {
        try (ConfigurableApplicationContext context = this.application.run("--kylin.server.mode=all")) {
            List<EnvironmentPostProcessor> processors = EnvironmentPostProcessorsFactory
                    .fromSpringFactories(application.getClassLoader())
                    .getEnvironmentPostProcessors(new DeferredLogs(), new DefaultBootstrapContext());
            Assertions.assertFalse(processors.isEmpty());
            System.out.println(processors.stream().map(EnvironmentPostProcessor::getClass).map(Class::getSimpleName)
                    .collect(Collectors.joining(",")));

            Map<? extends Class<? extends EnvironmentPostProcessor>, Integer> classIndexMap = processors.stream()
                    .collect(Collectors.toMap(EnvironmentPostProcessor::getClass, processors::indexOf));

            Integer configDataEnvIndex = classIndexMap.get(ConfigDataEnvironmentPostProcessor.class);
            Integer kylinPrepareEnvIndex = classIndexMap.get(KylinPrepareEnvListener.class);
            Integer kylinPropertySourceIndex = classIndexMap.get(KylinPropertySourceConfiguration.class);

            Assertions.assertTrue(configDataEnvIndex < kylinPrepareEnvIndex);
            Assertions.assertTrue(kylinPrepareEnvIndex < kylinPropertySourceIndex);
        }
    }

    @Test
    void testSystemProperty() {
        KylinPropertySourceConfiguration configuration = new KylinPropertySourceConfiguration();
        Properties properties = new Properties();
        properties.setProperty("kylin.system.property.org.springframework.ldap.core.keyCaseFold", "none");
        configuration.setSystemProperty(properties);
        String property = System.getProperty("org.springframework.ldap.core.keyCaseFold");
        Assertions.assertEquals("none", property);
    }

    @Configuration
    static class Config {

    }

}
