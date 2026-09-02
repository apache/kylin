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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.constant.Constants.HIDDEN_VALUE;
import static org.apache.kylin.common.constant.Constants.ON_PREMISES;
import static org.apache.kylin.common.util.EncryptUtil.ENC_PREFIX;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.springframework.stereotype.Service;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ConfigService extends BasicService {

    public Properties fetchAll() {
        val config = getConfig();
        val properties = config.exportToProperties();
        if (config.isRandomEncryptKeyEnabled() && !AclPermissionUtil.isAdmin()) {
            properties.replaceAll((k, v) -> StringUtils.startsWith(v.toString(), ENC_PREFIX) ? HIDDEN_VALUE : v);
        }
        return properties;
    }

    public Boolean isCloud() {
        val kapConfig = KapConfig.getInstanceFromEnv();
        return !kapConfig.getChannelUser().equals(ON_PREMISES);
    }
}
