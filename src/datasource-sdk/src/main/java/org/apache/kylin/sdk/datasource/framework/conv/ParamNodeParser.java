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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ParamNodeParser {
    private static final Logger logger = LoggerFactory.getLogger(ParamNodeParser.class);

    private static final Pattern PARAM_PATTERN = Pattern.compile("\\$(\\d+)");

    static int parseParamIdx(String paramNodeValue) {
        if (!StringUtils.isEmpty(paramNodeValue)) {
            try {
                Matcher m = PARAM_PATTERN.matcher(paramNodeValue);
                if (m.matches()) {
                    return Integer.parseInt(m.group(1));
                }
            } catch (Throwable e) {
                logger.error("Failed to parse the value to get the parameter id.", e);
            }
        }
        return -1; // return negative value if not matched.
    }
}