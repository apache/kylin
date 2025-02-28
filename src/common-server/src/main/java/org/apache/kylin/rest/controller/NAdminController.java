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

package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/admin", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NAdminController extends NBasicController {

    @ApiOperation(value = "getPublicConfig", tags = { "MID" }, notes = "Update Param: project_name")
    @GetMapping(value = "/public_config")
    @ResponseBody
    public EnvelopeResponse<String> getPublicConfig() throws IOException {

        final String whiteListProperties = KylinConfig.getInstanceFromEnv().getPropertiesWhiteList();

        Collection<String> propertyKeys = Lists.newArrayList();
        if (StringUtils.isNotEmpty(whiteListProperties)) {
            propertyKeys.addAll(Arrays.asList(whiteListProperties.split(",")));
        }

        propertyKeys.add("kylin.env.smart-mode-enabled");
        propertyKeys.add("kylin.source.load-hive-tablename-enabled");
        propertyKeys.add("kylin.kerberos.project-level-enabled");
        propertyKeys.add("kylin.web.stack-trace.enabled");
        propertyKeys.add("kylin.metadata.random-admin-password.enabled");
        propertyKeys.add("kylin.model.recommendation-page-size");
        propertyKeys.add("kylin.model.dimension-measure-name.max-length");
        propertyKeys.add("kylin.favorite.import-sql-max-size");
        propertyKeys.add("kylin.model.suggest-model-sql-limit");
        propertyKeys.add("kylin.query.query-history-download-max-size");
        propertyKeys.add("kylin.model.measure-name-check-enabled");
        propertyKeys.add("kylin.security.remove-ldap-custom-security-limit-enabled");
        propertyKeys.add("kylin.source.ddl.logical-view.enabled");
        propertyKeys.add("kylin.source.ddl.hive.enabled");
        propertyKeys.add("kylin.source.ddl.logical-view.database");
        propertyKeys.add("kylin.storage.check-quota-enabled");
        propertyKeys.add("kylin.table.load-threshold-enabled");
        propertyKeys.add("kylin.index.enable-operator-design");

        if (!KylinConfig.getInstanceFromEnv().isAllowedNonAdminGenerateQueryDiagPackage()) {
            propertyKeys.add("kylin.security.allow-non-admin-generate-query-diag-package");
        }

        final String config = KylinConfig.getInstanceFromEnv().exportToString(propertyKeys)
                + addPropertyWithKylinInfoCheck() + addPropertyInMetadata();

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, config, "");
    }

    @ApiOperation(value = "health APIs", tags = { "SM" })
    @GetMapping(value = "/instance_info")
    @ResponseBody
    public EnvelopeResponse<Map<String, String>> getInstanceConfig() {

        Map<String, String> data = Maps.newHashMap();

        ZoneId zoneId = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).toZoneId();
        data.put("instance.timezone", zoneId.toString());

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
    }

    private String addPropertyInMetadata() {
        Properties properties = new Properties();
        ResourceGroupManager manager = ResourceGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
        properties.put("resource_group_enabled", manager.isResourceGroupEnabled());
        return getPropertyString(properties);
    }

    private String getPropertyString(Properties properties) {
        final StringBuilder sb = new StringBuilder();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append('\n');
        }
        return sb.toString();
    }

    private String addPropertyWithKylinInfoCheck() {
        Properties properties = new Properties();
        properties.put("kylin.streaming.enabled", KylinConfig.getInstanceFromEnv().isStreamingEnabled());
        return getPropertyString(properties);
    }
}
