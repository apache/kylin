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
package org.apache.kylin.common.persistence;

import java.lang.reflect.Method;

import org.apache.kylin.common.util.CompressionUtils;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;

import lombok.AllArgsConstructor;

/**
 * 19/01/2024 hellozepp(lisheng.zhanglin@163.com)
 */
@AllArgsConstructor
public class RawResourceCompressProxy implements MethodInterceptor {

    private RawResource resource;

    public static RawResource createProxy(RawResource target) {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(target.getClass());
        enhancer.setCallback(new RawResourceCompressProxy(target));
        enhancer.setClassLoader(target.getClass().getClassLoader());
        return (RawResource) enhancer.create();
    }

    @Override
    public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
        if ("getContent".equals(method.getName())) {
            return CompressionUtils.compress((byte[]) method.invoke(resource, objects));
        }
        return method.invoke(resource, objects);
    }
}
