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

import static org.apache.kylin.common.KylinConfig.getSitePropertiesFile;
import static org.apache.kylin.common.KylinConfig.useLegacyConfig;
import static org.apache.kylin.common.KylinConfigBase.getKylinHome;

import java.nio.file.Paths;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.FileUrlResource;

import com.alibaba.cloud.nacos.NacosConfigProperties;

import io.kyligence.config.core.conf.ExternalConfigProperties;
import io.kyligence.config.core.loader.IExternalConfigLoader;
import io.kyligence.config.external.loader.NacosExternalConfigLoader;
import lombok.val;
import lombok.experimental.UtilityClass;

/**
 * without spring context, create config loader manually.
 * used by xxTool, xxCLI
 */
@UtilityClass
public class ShellKylinExternalConfigLoaderFactory {

    private static final long serialVersionUID = 8140984602630371871L;

    private static final Logger logger = LoggerFactory.getLogger(ShellKylinExternalConfigLoaderFactory.class);

    public static IExternalConfigLoader getConfigLoader() {
        if (useLegacyConfig()) {
            return new KylinExternalConfigLoader(getSitePropertiesFile());
        }
        IExternalConfigLoader configLoader = null;
        try {
            if (Paths.get(getKylinHome(), "conf", "config.yaml").toFile().exists()) {
                val environment = getEnvironment();

                val externalConfigProperties = Binder.get(environment)
                        .bind("kylin.external.config", ExternalConfigProperties.class).get();

                val nacosConfigProperties = Binder.get(environment)
                        .bind(NacosConfigProperties.PREFIX, NacosConfigProperties.class).get();

                val infos = externalConfigProperties.getInfos();
                configLoader = infos.stream().filter(info -> info.getTarget().equals(KylinConfig.class.getName())).map(
                        info -> new NacosExternalConfigLoader(info.getProperties(), nacosConfigProperties, environment))
                        .findAny().orElse(null);
            }
        } catch (Exception e) {
            logger.warn("Can not load external config from config.yaml, use file external config loader", e);
        }

        if (configLoader != null) {
            return configLoader;
        }

        return new KylinExternalConfigLoader(getSitePropertiesFile());
    }

    /**
     * env can get properties from system property, system env and file://${KYLIN_HOME}/config/config.yaml
     * @return
     */
    private static Environment getEnvironment() {
        StandardEnvironment environment = new StandardEnvironment();

        // file://${KYLIN_HOME}/conf/config.yaml
        try {
            YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
            List<PropertySource<?>> propertySources = loader.load("config-application-yaml",
                    new FileUrlResource(Paths.get(getKylinHome(), "conf", "config.yaml").toFile().getAbsolutePath()));
            propertySources.forEach(propertySource -> environment.getPropertySources().addLast(propertySource));
        } catch (Exception e) {
            logger.warn("Can not load config.yaml", e);
        }

        return environment;
    }
}
