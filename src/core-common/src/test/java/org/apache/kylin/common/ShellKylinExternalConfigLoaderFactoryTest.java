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

package org.apache.kylin.common;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import io.kyligence.config.core.loader.IExternalConfigLoader;
import io.kyligence.config.external.loader.NacosExternalConfigLoader;

@MetadataInfo
class ShellKylinExternalConfigLoaderFactoryTest {

    @Test
    void testGetConfigLoader() {
        IExternalConfigLoader configLoader = ShellKylinExternalConfigLoaderFactory.getConfigLoader();
        Assertions.assertInstanceOf(KylinExternalConfigLoader.class, configLoader);
    }

    @Test
    void testGetConfigLoaderByConfigYaml() throws IOException {
        Path confPath = Paths.get(KylinConfig.getKylinHome(), "conf");
        confPath.toFile().mkdir();
        Path configFile = Files.createFile(Paths.get(KylinConfig.getKylinHome(), "conf", "config.yaml"));

        String content = "spring:\n" + "  cloud:\n" + "    nacos:\n" + "      config:\n"
                + "        server-addr: ${NACOS_CONFIG_SERVER_ADDR}\n" + "\n" + "kylin:\n" + "  external:\n"
                + "    config:\n" + "      infos:\n" + "        - target: org.apache.kylin.common.KylinConfig\n"
                + "          type: nacos\n" + "          properties:\n"
                + "            app: \"${APP_NAME:ShellKylinExternalConfigLoaderFactoryTest}\"\n"
                + "            zhName: \"${APP_DISPLAY_NAME}\"\n"
                + "            dataIds: \"${APP_NAME:yinglong-common-booter}-kylin-config\"\n"
                + "            group: \"${TENANT_ID}\"\n" + "            autoRefresh: true\n"
                + "            needInit: true\n"
                + "            initConfigContent: \"${KYLIN_HOME}/conf/init.properties\"\n"
                + "            configPropertyFileType: \"PROPERTIES\"\n"
                + "            configLibrary: \"${KYLIN_HOME}/conf/config_library.csv\"";

        Files.write(configFile, content.getBytes(StandardCharsets.UTF_8));
        try (MockedConstruction<NacosExternalConfigLoader> mockedConstruction = Mockito
                .mockConstruction(NacosExternalConfigLoader.class)) {
            IExternalConfigLoader configLoader = ShellKylinExternalConfigLoaderFactory.getConfigLoader();
            Assertions.assertInstanceOf(NacosExternalConfigLoader.class, configLoader);
        }
    }

    @Test
    @SetEnvironmentVariable(key = KylinConfig.USE_LEGACY_CONFIG, value = "true")
    void testGetConfigLoaderWithConfigYamlAndUSE_LEGACY_CONFIG() throws IOException {
        Path confPath = Paths.get(KylinConfig.getKylinHome(), "conf");
        confPath.toFile().mkdir();
        Path configFile = Files.createFile(Paths.get(KylinConfig.getKylinHome(), "conf", "config.yaml"));

        String content = "spring:\n" + "  cloud:\n" + "    nacos:\n" + "      config:\n"
                + "        server-addr: ${NACOS_CONFIG_SERVER_ADDR}\n" + "\n" + "kylin:\n" + "  external:\n"
                + "    config:\n" + "      infos:\n" + "        - target: org.apache.kylin.common.KylinConfig\n"
                + "          type: nacos\n" + "          properties:\n"
                + "            app: \"${APP_NAME:ShellKylinExternalConfigLoaderFactoryTest}\"\n"
                + "            zhName: \"${APP_DISPLAY_NAME}\"\n"
                + "            dataIds: \"${APP_NAME:yinglong-common-booter}-kylin-config\"\n"
                + "            group: \"${TENANT_ID}\"\n" + "            autoRefresh: true\n"
                + "            needInit: true\n"
                + "            initConfigContent: \"${KYLIN_HOME}/conf/init.properties\"\n"
                + "            configPropertyFileType: \"PROPERTIES\"\n"
                + "            configLibrary: \"${KYLIN_HOME}/conf/config_library.csv\"";

        Files.write(configFile, content.getBytes(StandardCharsets.UTF_8));
        try (MockedConstruction<NacosExternalConfigLoader> mockedConstruction = Mockito
                .mockConstruction(NacosExternalConfigLoader.class)) {
            IExternalConfigLoader configLoader = ShellKylinExternalConfigLoaderFactory.getConfigLoader();
            Assertions.assertInstanceOf(KylinExternalConfigLoader.class, configLoader);
        }
    }
}