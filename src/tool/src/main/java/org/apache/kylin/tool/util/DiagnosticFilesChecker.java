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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiagnosticFilesChecker {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private DiagnosticFilesChecker() {
    }

    public static synchronized void writeMsgToFile(String msg, Long time, File file) {
        String interruptString = Thread.currentThread().isInterrupted() ? "(INTERRUPT)" : "";
        String content = String.format(Locale.ROOT, "TIME OF EXTRACT %s %s: %dms", msg, interruptString, time);
        logger.info(content);

        String charsetName = Charset.defaultCharset().name();
        try (OutputStream fos = new FileOutputStream(file, true);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos, charsetName))) {
            writer.write(content);
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
