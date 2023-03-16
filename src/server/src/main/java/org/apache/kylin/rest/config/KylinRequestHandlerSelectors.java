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

import org.springframework.util.ClassUtils;

import org.apache.kylin.guava30.shaded.common.base.Function;
import org.apache.kylin.guava30.shaded.common.base.Optional;
import org.apache.kylin.guava30.shaded.common.base.Predicate;
import springfox.documentation.RequestHandler;

public class KylinRequestHandlerSelectors {
    private KylinRequestHandlerSelectors() {
    }

    private static Optional<? extends Class<?>> declaringClass(RequestHandler input) {
        return Optional.fromNullable(input.declaringClass());
    }

    public static Predicate<RequestHandler> baseCurrentPackage(final String basePackage) {
        return input -> (Boolean) KylinRequestHandlerSelectors.declaringClass(input)
                .transform(KylinRequestHandlerSelectors.handlerPackage(basePackage)).or(false);
    }

    private static Function<Class<?>, Boolean> handlerPackage(final String basePackage) {
        return input -> ClassUtils.getPackageName(input).startsWith(basePackage)
                && 1 == ClassUtils.getPackageName(input).substring(basePackage.length()).split(".").length;
    }

}
