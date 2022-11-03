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
package io.kyligence.kap.metadata.recommendation.ref;

import java.lang.reflect.Method;
import java.util.Set;

import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;

import com.google.common.collect.Sets;

import lombok.val;

public class BrokenRefProxy implements MethodInterceptor {

    private static Set<String> methods = Sets.newHashSet("setId", "getId");

    public static <T extends RecommendationRef> T getProxy(Class<T> clazz, int id) {
        val proxy = new BrokenRefProxy();
        T brokenEntity = (T) Enhancer.create(clazz, proxy);
        brokenEntity.setId(id);
        return brokenEntity;
    }

    public static <T extends RecommendationRef> boolean isNullOrBroken(T entity) {
        return entity == null || entity.isBroken();
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        if (methods.contains(method.getName())) {
            return proxy.invokeSuper(obj, args);
        }
        switch (method.getName()) {
        case "isBroken":
            return true;
        case "getDataType":
            return null;
        default:
            return null;
        }
    }
}
