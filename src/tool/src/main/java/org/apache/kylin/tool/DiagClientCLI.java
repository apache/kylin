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
package org.apache.kylin.tool;

import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.tool.util.ToolMainWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiagClientCLI {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    public static void main(String[] args) {
        logger.info("Start to collect full diagnosis info.");
        ToolMainWrapper.wrap(args, () -> {
            DiagClientTool diagClientTool = new DiagClientTool();
            diagClientTool.execute(args);
        });

        logger.info("Collect full diagnosis info completely.");
        Unsafe.systemExit(0);
    }
}
