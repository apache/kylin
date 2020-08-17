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

import org.apache.commons.io.FileUtils;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public class AutoDeleteDirectory implements Closeable {

    private final File tempFile;

    public AutoDeleteDirectory(File file) {
        try {
            tempFile = file;
            init();
        } catch (IOException e) {
            throw new IllegalArgumentException("create temp file " + file + " failed", e);
        }
    }

    public AutoDeleteDirectory(String prefix, String suffix) {
        try {
            tempFile = File.createTempFile(prefix, suffix);
            init();
        } catch (IOException e) {
            throw new IllegalArgumentException("create temp file " + prefix + "****" + suffix + " failed", e);
        }
    }

    private void init() throws IOException {
        Preconditions.checkNotNull(tempFile);
        if (tempFile.exists())
            FileUtils.forceDelete(tempFile); // we need a directory, so delete the file first
        tempFile.mkdirs();
    }

    public String getAbsolutePath() {
        return tempFile.getAbsolutePath();
    }

    public AutoDeleteDirectory child(String child) {
        return new AutoDeleteDirectory(new File(tempFile, child));
    }

    public File getFile() {
        return tempFile;
    }

    @Override
    public void close() throws IOException {
        if (tempFile != null && tempFile.exists()) {
            FileUtils.forceDelete(tempFile);
        }
    }
}
