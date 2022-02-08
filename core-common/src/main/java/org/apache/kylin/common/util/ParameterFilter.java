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

package org.apache.kylin.common.util;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class ParameterFilter {
    private static final Logger logger = LoggerFactory.getLogger(ParameterFilter.class);

    public static final String PARAMETER_REGULAR_EXPRESSION = "[ &`>|{}()$;\\-#~!+*\\\\]+";
    public static final String URI_REGULAR_EXPRESSION = "[^\\w%,@/:=?.\"\\[\\]]";
    public static final String HIVE_PROPERTY_REGULAR_EXPRESSION = "[ <>()$;\\-#!+*\"'/=%@]+";
    public static final String SPARK_CONF_REGULAR_EXPRESSION = "[`$|&;]+";

    /**
     * <pre>
     * Check parameter for preventing command injection, replace illegal character into empty character.
     *
     * Note:
     * 1. Whitespace is also refused because parameter is a single word, should not contains it
     * 2. Some character may be illegal but still be accepted because commandParameter maybe a URI/path expression,
     *     you may check "Character part" in https://docs.oracle.com/javase/8/docs/api/java/net/URI.html,
     *     here is the character which is not banned.
     *
     *     1. dot .
     *     2. slash /
     *     3. colon :
     *     4. equal =
     *     5. ?
     *     6. @
     *     7. bracket []
     *     8. comma ,
     *     9. %
     * </pre>
     */
    public static String checkParameter(String commandParameter) {
        return checkParameter(commandParameter, PARAMETER_REGULAR_EXPRESSION, false);
    }

    public static String checkURI(String commandParameter) {
        return checkParameter(commandParameter, URI_REGULAR_EXPRESSION, false);
    }

    public static String checkHiveProperty(String hiveProperty) {
        return checkParameter(hiveProperty, HIVE_PROPERTY_REGULAR_EXPRESSION, false);
    }

    public static String checkSparkConf(String sparkConf) {
        return checkParameter(sparkConf, SPARK_CONF_REGULAR_EXPRESSION, true);
    }

    private static String checkParameter(String commandParameter, String rex, boolean throwException) {
        String repaired = commandParameter.replaceAll(rex, "");
        if (repaired.length() != commandParameter.length()) {
            if (throwException) {
                throw new IllegalArgumentException("Detected illegal character in " + commandParameter + " by " + rex);
            } else {
                logger.warn("Detected illegal character in command {} by {} , replace it to {}.",
                        commandParameter, rex, repaired);
            }
        }
        return repaired;
    }
}
