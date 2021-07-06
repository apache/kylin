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
package org.apache.kylin.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Standardize and clarify config property of KylinConfig,
 * which helps to make them easy to understand.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER,
        ElementType.CONSTRUCTOR, ElementType.LOCAL_VARIABLE, ElementType.PACKAGE})
public @interface ConfigTag {

    Tag[] value();

    enum Tag {
        /**
         * Indicate this property will be removed soon.
         */
        DEPRECATED,

        NOT_CLEAR,

        /**
         * The implementation is not complement
         */
        NOT_IMPLEMENTED,

        /**
         * To be categorized
         */
        UNCATEGORIZED,

        /**
         * For hacker or developer
         */
        DEBUG_HACK,

        /**
         * Support cube level configuration
         */
        CUBE_LEVEL,

        /**
         * Support project level configuration
         */
        PROJECT_LEVEL,

        /**
         * Only support global configuration
         */
        GLOBAL_LEVEL
    }
}