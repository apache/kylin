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

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

public class RawResource implements AutoCloseable {

    private final String path;
    private final long lastModified;
    private final InputStream content;

    public RawResource(String path, long lastModified) {
        this(path, lastModified, (InputStream) null);
    }

    public RawResource(String path, long lastModified, InputStream content) {
        this.path = path;
        this.lastModified = lastModified;
        this.content = content;
    }

    public RawResource(String path, long lastModified, IOException brokenContentException) {
        this(path, lastModified, wrap(brokenContentException));
    }

    private static InputStream wrap(final IOException brokenContentException) {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                throw brokenContentException;
            }
        };
    }

    public String path() {
        return path;
    }

    public long lastModified() {
        return lastModified;
    }

    public InputStream content() {
        return content;
    }

    @Override
    public void close() {
        if (content != null) {
            IOUtils.closeQuietly(content);
        }
    }
}
