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
package org.apache.kylin.junit;

import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kylin.junit.annotation.MultiTimezoneTest;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;

public class MultiTimezoneProvider implements TestTemplateInvocationContextProvider {

    private static final String METHOD_CONTEXT_KEY = "context";

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        if (!context.getTestMethod().isPresent()) {
            return false;
        }

        Method testMethod = context.getTestMethod().get();
        val annotations = findAnnotation(context.getElement(), MultiTimezoneTest.class);
        if (!annotations.isPresent()) {
            return false;
        }
        val methodContext = new MultiTimezoneContext(testMethod, annotations.get().timezones());
        getStore(context).put(METHOD_CONTEXT_KEY, methodContext);
        return true;

    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        return getStore(context).get(METHOD_CONTEXT_KEY, MultiTimezoneContext.class).getTimezones().stream()
                .map(x -> new TimezoneInvocationContext(x));
    }

    private ExtensionContext.Store getStore(ExtensionContext context) {
        return context.getStore(
                ExtensionContext.Namespace.create(MultiTimezoneProvider.class, context.getRequiredTestMethod()));
    }

    @Data
    static class MultiTimezoneContext {
        private Method testMethod;
        private List<String> timezones;

        public MultiTimezoneContext(Method testMethod, String[] timezones) {
            this.testMethod = testMethod;
            this.timezones = Lists.newArrayList(timezones);
        }
    }

    @AllArgsConstructor
    static class TimezoneInvocationContext implements TestTemplateInvocationContext {

        private String timezone;

        @Override
        public String getDisplayName(int invocationIndex) {
            return "[" + timezone + "]";
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return Collections.singletonList(new TimezoneExtension(timezone));
        }
    }

}
