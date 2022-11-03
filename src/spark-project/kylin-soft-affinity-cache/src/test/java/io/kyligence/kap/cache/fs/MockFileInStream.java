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
package io.kyligence.kap.cache.fs;

import alluxio.client.file.FileInStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public final class MockFileInStream extends FileInStream {
    private final ByteArrayInputStream mStream;
    private final long mLength;

    /**
     * Creates a mock {@link FileInStream} which will supply the given bytes.
     *
     * @param bytes the bytes to supply
     */
    public MockFileInStream(byte[] bytes) {
        mStream = new ByteArrayInputStream(bytes);
        mLength = bytes.length;
    }

    @Override
    public int read() {
        return mStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return mStream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return mStream.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
        mStream.close();
    }

    @Override
    public long skip(long n) {
        return mStream.skip(n);
    }

    @Override
    public void seek(long n) {
        mStream.reset();
        mStream.skip(n);
    }

    @Override
    public long getPos() throws IOException {
        return mLength - remaining();
    }

    @Override
    public int positionedRead(long position, byte[] buffer, int offset, int length)
            throws IOException {
        throw new UnsupportedOperationException("positionedRead not implemented for mock FileInStream");
    }

    @Override
    public long remaining() {
        return mStream.available();
    }
}
