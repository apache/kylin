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
package org.apache.kylin.tool.util;

import org.junit.Test;

public class ToolMainWrapperTest {

    @Test
    public void test() {
        String[] args = new String[] {};
        ToolMainWrapper.wrap(args, () -> {
            System.out.println("test");
        });

        Thread.currentThread().setName("is-not-main");
        ToolMainWrapper.wrap(args, () -> {
            throw new RuntimeException("test");
        });

        ToolMainWrapper.wrap(args, () -> {
            throw new OutOfMemoryError("Java heap space");
        });

        Thread.currentThread().setName("main");
    }

}
