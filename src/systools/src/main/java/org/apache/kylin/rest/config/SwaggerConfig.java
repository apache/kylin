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
package org.apache.kylin.rest.config;

import static springfox.documentation.builders.PathSelectors.ant;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@EnableSwagger2
@Configuration
@ConditionalOnProperty(name = "kylin.swagger.enabled", havingValue = "true")
public class SwaggerConfig {

    public static final String LICENSE = "Apache 2.0";
    public static final String SWAGGER_LICENSE_URL = "http://www.apache.org/licenses/LICENSE-2.0.html";
    public static final String TITLE = "Kyligence Enterprise API";

    @Order(2)
    @Bean(value = "v4 public")
    public Docket restApiOpen() {
        return new Docket(DocumentationType.SWAGGER_2).apiInfo(apiInfoOpen()).groupName("v4 public").select()
                .apis(KylinRequestHandlerSelectors.baseCurrentPackage("org.apache.kylin.rest.controller.open"))
                .build();
    }

    @Order(1)
    @Bean(value = "ke4 internal")
    public Docket restApiV4() {
        return new Docket(DocumentationType.SWAGGER_2).apiInfo(apiInfoV4()).groupName("v4 internal").select()
                .apis(KylinRequestHandlerSelectors.baseCurrentPackage("org.apache.kylin.rest.controller")).build();
    }

    @Order(3)
    @Bean(value = "v2 public")
    public Docket restApiV2() {
        return new Docket(DocumentationType.SWAGGER_2).apiInfo(apiInfoV2()).groupName("v2 public").select()
                .paths(ant("/api").negate())
                .apis(KylinRequestHandlerSelectors.baseCurrentPackage("org.apache.kylin.rest.controller.v2")).build();
    }

    private ApiInfo apiInfoOpen() {
        return new ApiInfoBuilder().title(TITLE).description("Newten Open API").license(LICENSE)
                .licenseUrl(SWAGGER_LICENSE_URL).version("4.0.8").build();
    }

    private ApiInfo apiInfoV4() {
        return new ApiInfoBuilder().title(TITLE).description("Newten API").license(LICENSE)
                .licenseUrl(SWAGGER_LICENSE_URL).version("4.0.7").build();
    }

    private ApiInfo apiInfoV2() {
        return new ApiInfoBuilder().title(TITLE).description("V2 API").license(LICENSE).licenseUrl(SWAGGER_LICENSE_URL)
                .version("3.0.0").build();
    }
}
