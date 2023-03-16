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

package org.apache.kylin.metadata.jar;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_INVALID_SUPPORT_LIST;

import java.util.Locale;
import java.util.Set;

import org.apache.kylin.common.exception.KylinException;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

public enum JarTypeEnum {

    STREAMING_CUSTOM_PARSER;

    private static final Set<String> jarTypeEnums = Sets.newHashSet(STREAMING_CUSTOM_PARSER.name());

    public static void validate(String jarType) {
        if (!jarTypeEnums.contains(jarType.toUpperCase(Locale.ROOT))) {
            throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, jarType, "STREAMING_CUSTOM_PARSER");
        }
    }
}
