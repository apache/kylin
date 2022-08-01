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
package org.apache.kylin.common.exception.code;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.exception.ErrorCodeException;
import org.apache.kylin.common.util.FileUtils;
import org.apache.kylin.common.util.ResourceUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import lombok.extern.slf4j.Slf4j;

/**
 * The new ErrorCode will gradually replace org.apache.kylin.common.exception.ErrorCode
 */
@Slf4j
public class ErrorCode implements Serializable {

    private static final String ERROR_CODE_FILE = "kylin_errorcode_conf.properties";
    private static final ImmutableSet<String> CODE_SET;

    static {
        try {
            URL resource = ResourceUtils.getServerConfUrl(ERROR_CODE_FILE);
            log.info("loading error code {}", resource.getPath());
            CODE_SET = ImmutableMap
                    .copyOf(new ConcurrentHashMap<>(FileUtils.readFromPropertiesFile(resource.openStream()))).keySet();
            log.info("loading error code successful");
        } catch (IOException e) {
            throw new ErrorCodeException("loading error code failed.", e);
        }
    }

    private final String keCode;

    public ErrorCode(String keCode) {
        if (!CODE_SET.contains(keCode)) {
            throw new ErrorCodeException("Error code [" + keCode + "] must be defined in the error code file");
        }
        this.keCode = keCode;
    }

    public String getCode() {
        return this.keCode;
    }

}
