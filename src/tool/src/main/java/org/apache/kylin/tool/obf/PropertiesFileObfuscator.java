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
package org.apache.kylin.tool.obf;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Properties;

import lombok.val;

public abstract class PropertiesFileObfuscator extends FileObfuscator {
    protected PropertiesFileObfuscator(ObfLevel level, MappingRecorder recorder, ResultRecorder resultRecorder) {
        super(level, recorder, resultRecorder);
    }

    abstract void obfuscateProperties(Properties input);

    @Override
    void doObfuscateFile(File orig) {
        String path = orig.getAbsolutePath();
        Properties p = readProperties(orig);
        try (BufferedWriter bw = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(orig), Charset.defaultCharset()))) {
            for (val kv : p.entrySet()) {
                bw.write(kv.getKey() + "=" + kv.getValue());
                bw.newLine();
            }
            bw.flush();
            logger.info("{} processed successfully", path);
            resultRecorder.addSuccessFile(path);
        } catch (IOException e) {
            logger.info("{} processed failed", path);
            resultRecorder.addFailedFile(path);
        }
    }

    private Properties readProperties(File orig) {
        Properties p = new Properties();
        try (InputStream in = new BufferedInputStream(new FileInputStream(orig))) {
            p.load(in);
            obfuscateProperties(p);
        } catch (Exception e) {
            logger.info("Failed to load properties file:{}.", orig.getAbsolutePath(), e);
        }
        return p;
    }
}
