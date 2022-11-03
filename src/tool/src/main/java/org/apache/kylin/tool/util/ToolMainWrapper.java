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

import static org.apache.kylin.tool.util.ScreenPrintUtil.systemExitWhenMainThread;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ToolMainWrapper {

    private ToolMainWrapper() {
        // only for tool main function, wrapper main code.
    }

    public static void wrap(String[] args, Wrapper wrapper) {
        wrap(1, args, wrapper);
    }

    public static void wrap(int errorExitCode, String[] args, Wrapper wrapper) {
        try {
            wrapper.run();
        } catch (Throwable e) {
            int last = Thread.currentThread().getStackTrace().length - 1;
            log.error("Failed to run tool: {} {}", Thread.currentThread().getStackTrace()[last].getClassName(),
                    StringUtils.join(args, StringUtils.SPACE), e);
            systemExitWhenMainThread(errorExitCode);
        }
    }

    public interface Wrapper {
        void run() throws Exception;
    }

}
