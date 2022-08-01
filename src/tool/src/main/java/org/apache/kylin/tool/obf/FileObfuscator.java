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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FileObfuscator extends AbstractObfuscator {
    protected static final Logger logger = LoggerFactory.getLogger("obfuscation");

    FileObfuscator(ObfLevel level, MappingRecorder recorder, ResultRecorder resultRecorder) {
        super(level, recorder, resultRecorder);
    }

    public void obfuscate(File orig, FileFilter fileFilter) {
        if (orig == null || !orig.exists())
            return;

        if (orig.isDirectory()) {
            obfuscateDirectory(orig, fileFilter);
        } else {
            obfuscateFile(orig);
        }
    }

    private void obfuscateDirectory(File directory, FileFilter fileFilter) {
        File[] files = fileFilter == null ? directory.listFiles() : directory.listFiles(fileFilter);
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    obfuscateDirectory(f, fileFilter);
                } else {
                    obfuscateFile(f);
                }
            }
        }
    }

    protected void obfuscateFile(File orig) {
        if (orig == null) {
            return;
        }

        String path = orig.getAbsolutePath();

        logger.info("Start to process file: {}", path);

        try {
            switch (level) {
            case OBF:
                doObfuscateFile(orig);
                break;
            case RAW:
                break;
            default:
                break;
            }
            logger.info("{} process successfully", path);
            resultRecorder.addSuccessFile(path);
        } catch (Exception e) {
            logger.warn("{} processed failed", path);
            resultRecorder.addFailedFile(path);
        }
    }

    abstract void doObfuscateFile(File orig) throws IOException;

}
