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
 *
 */

package org.apache.kylin.tool.extractor;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

/**
 * https://docs.oracle.com/javase/8/docs/technotes/tools/unix/jstack.html
 */
public class JStackExtractor extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(JStackExtractor.class);

    public JStackExtractor() {
        super();
        packageType = "jstack";
    }

    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) {
        try {
            File logDir = new File(exportDir, "logs");
            File jstackDumpFile = new File(logDir, String.format(Locale.ROOT, "jstack.log.%s", System.currentTimeMillis()));
            dumpKylinJStack(jstackDumpFile);
        } catch (IOException e) {
            logger.error("IO Error on dump jstack", e);
        }
    }

    private static void dumpKylinJStack(File outputFile) throws IOException {
        String jstackDumpCmd = String.format(Locale.ROOT, "jstack -l %s", getKylinPid());
        Pair<Integer, String> result = new CliCommandExecutor().execute(jstackDumpCmd, null, null);
        FileUtils.writeStringToFile(outputFile, result.getSecond());
    }
}
