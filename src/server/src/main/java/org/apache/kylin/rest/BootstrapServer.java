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

import org.apache.catalina.connector.Connector;
import org.apache.coyote.http11.Http11NioProtocol;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.loadbalancer.annotation.LoadBalancerClient;
import org.springframework.cloud.zookeeper.discovery.ZookeeperInstance;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.session.config.annotation.web.http.EnableSpringHttpSession;
import org.springframework.session.web.http.CookieSerializer;
import org.springframework.session.web.http.DefaultCookieSerializer;

import io.kyligence.kap.guava20.shaded.common.base.Charsets;
import io.kyligence.kap.guava20.shaded.common.hash.Hashing;
import lombok.val;

@ImportResource(locations = { "applicationContext.xml", "kylinSecurity.xml" })
@SpringBootApplication
@EnableScheduling
@EnableAsync
@EnableCaching
@EnableDiscoveryClient
@LoadBalancerClient(name = "spring-boot-provider", configuration = org.apache.kylin.rest.LoadBalanced.class)
@EnableSpringHttpSession
public class BootstrapServer implements ApplicationListener<ApplicationEvent> {

    private static final Logger logger = LoggerFactory.getLogger(BootstrapServer.class);

    public static void main(String[] args) {
        SpringApplication.run(BootstrapServer.class, args);
    }

    @Bean
    public ServletWebServerFactory servletContainer() {
        TomcatServletWebServerFactory tomcat = new TomcatServletWebServerFactory();
        tomcat.addContextCustomizers(context -> context.setRequestCharacterEncoding("UTF-8"));
        if (KylinConfig.getInstanceFromEnv().isServerHttpsEnabled()) {
            tomcat.addAdditionalTomcatConnectors(createSslConnector());
        }
        return tomcat;
    }

    private Connector createSslConnector() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
        Http11NioProtocol protocol = (Http11NioProtocol) connector.getProtocolHandler();

        connector.setScheme("https");
        connector.setSecure(true);
        connector.setPort(kylinConfig.getServerHttpsPort());
        protocol.setSSLEnabled(true);
        protocol.setKeystoreType(kylinConfig.getServerHttpsKeyType());
        protocol.setKeystoreFile(kylinConfig.getServerHttpsKeystore());
        protocol.setKeyAlias(kylinConfig.getServerHttpsKeyAlias());
        String serverHttpsKeyPassword = kylinConfig.getServerHttpsKeyPassword();
        if (EncryptUtil.isEncrypted(serverHttpsKeyPassword)) {
            serverHttpsKeyPassword = EncryptUtil.decryptPassInKylin(serverHttpsKeyPassword);
        }
        protocol.setKeystorePass(serverHttpsKeyPassword);
        return connector;
    }

    @Bean
    public CookieSerializer cookieSerializer() {
        val serializer = new DefaultCookieSerializer();
        val url = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        String cookieName = url.getIdentifier()
                + (url.getParameter("url") == null ? "" : "_" + url.getParameter("url"));
        cookieName = cookieName.replaceAll("\\W", "_");
        cookieName = Hashing.sha256().newHasher().putString(cookieName, Charsets.UTF_8).hash().toString();
        serializer.setCookieName(cookieName);
        return serializer;
    }

    @Bean
    public InstanceSerializer<ZookeeperInstance> zookeeperInstanceInstanceSerializer() {
        return new JsonInstanceSerializer<ZookeeperInstance>(ZookeeperInstance.class) {
            @Override
            public ServiceInstance<ZookeeperInstance> deserialize(byte[] bytes) throws Exception {
                try {
                    return super.deserialize(bytes);
                } catch (Exception e) {
                    logger.warn("Zookeeper instance deserialize failed", e);
                    return null;
                }
            }
        };
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if (event instanceof ApplicationReadyEvent) {
            logger.info("init backend end...");
        } else if (event instanceof ContextClosedEvent) {
            logger.info("Stop Kyligence node...");
            EpochManager.getInstance().releaseOwnedEpochs();
        }
    }
}
