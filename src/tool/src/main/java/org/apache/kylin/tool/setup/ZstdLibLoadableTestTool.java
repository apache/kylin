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

package org.apache.kylin.tool.setup;

import java.io.File;

import org.apache.kylin.common.util.Unsafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.util.Native;

public class ZstdLibLoadableTestTool {

    public static final Logger logger = LoggerFactory.getLogger(ZstdLibLoadableTestTool.class);

    public static void main(String[] args) {
        logger.info("Start ZstdLibLoadableTestTool");
        File tmpDir = args.length == 1 ? new File(args[0]) : null;
        try {
            Native.load(tmpDir);
            logger.info("zstd lib loading succeed.");
        } catch (Throwable t) {
            logger.error("zstd lib loading failed", t);
            Unsafe.systemExit(1);
        }
        Unsafe.systemExit(0);
    }
}
