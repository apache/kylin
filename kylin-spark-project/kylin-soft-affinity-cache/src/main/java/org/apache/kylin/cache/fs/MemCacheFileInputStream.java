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

package org.apache.kylin.cache.fs;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MemCacheFileInputStream extends FSInputStream implements ByteBufferReadable {

    private static final Logger LOG = LoggerFactory.getLogger(MemCacheFileInputStream.class);

    private final Statistics statistics;
    private ByteBuffer buf;
    private Path file;
    private int fileLength;

    public MemCacheFileInputStream(Path file, ByteBuffer buf, int fileLength,
                                   Statistics statistics) throws IOException {
        this.file = file;
        this.buf = buf;
        this.fileLength = fileLength;
        this.statistics = statistics;
        assert this.fileLength == buf.capacity();

        // reset ByteBuffer
        this.buf.flip();
        this.buf.limit(fileLength);
    }

    @Override
    public synchronized long getPos() throws IOException {
        if (buf == null)
            throw new IOException(
                    "Reading file " + this.file.toString() + " error, stream was closed");
        return buf.position();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public synchronized int available() throws IOException {
        if (buf == null)
            throw new IOException(
                    "Reading file " + this.file.toString() + " error, stream was closed");
        return buf.remaining();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("Mark/reset not supported");
    }

    public synchronized int read() throws IOException {
        if (buf == null)
            throw new IOException(
                    "Reading file " + this.file.toString() + " error, stream was closed");
        if (!buf.hasRemaining())
            return -1; // EOF
        statistics.incrementBytesRead(1);
        return buf.get() & 0xFF;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
        if (off < 0 || len < 0 || b.length - off < len)
            throw new IndexOutOfBoundsException();
        if (len == 0)
            return 0;
        if (buf == null)
            throw new IOException(
                    "Reading file " + this.file.toString() + " error, stream was closed");
        if (!buf.hasRemaining())
            return -1; // No bytes were read before EOF.

        int read = Math.min(buf.remaining(), len);
        if (read > 0) {
            buf.get(b, off, read);
            statistics.incrementBytesRead(read);
        }
        return read;
    }

    @Override
    public synchronized int read(long pos, byte[] b, int off, int len) throws IOException {
        if (len == 0)
            return 0;
        if (pos < 0 || pos > buf.limit())
            throw new EOFException(
                    "Reading file " + this.file.toString() + " error, position is negative");
        if (b == null || off < 0 || len < 0 || b.length - off < len) {
            throw new IllegalArgumentException(
                    "Reading file " + this.file.toString() + " error, invalid arguments: " +
                            off + " " + len);
        }
        int oldPos = buf.position();
        buf.position((int)pos);
        int got = Math.min(buf.remaining(), len);
        try {
            if (got > 0) {
                buf.get(b, off, len);
                statistics.incrementBytesRead(got);
            }
        } finally {
            buf.position(oldPos);
        }
        return got;
    }

    @Override
    public synchronized int read(ByteBuffer b) throws IOException {
        if (!b.hasRemaining())
            return 0;
        if (buf == null)
            throw new IOException(
                    "Reading file " + this.file.toString() + " error, stream was closed");
        if (!buf.hasRemaining()) {
            return -1;
        }
        int got = Math.min(b.remaining(), buf.remaining());
        if (got > 0) {
            byte[] readedBytes = new byte[got];
            buf.get(readedBytes, 0, got);
            b.put(readedBytes, 0, got);
            statistics.incrementBytesRead(got);
        }
        return got;
    }

    @Override
    public synchronized void seek(long p) throws IOException {
        if (p < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }
        if (p > buf.limit()) {
            throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
        }
        if (buf == null)
            throw new IOException(
                    "Reading file " + this.file.toString() + " error, stream was closed");
        buf.position((int)p);
    }

    @Override
    public synchronized long skip(long n) throws IOException {
        if (n < 0)
            return -1;
        if (buf == null)
            throw new IOException(
                    "Reading file " + this.file.toString() + " error, stream was closed");
        if (n > buf.remaining()) {
            throw new EOFException("Attempted to skip past the end of the file");
        }
        buf.position(buf.position() + (int) n);
        return n;
    }

    @Override
    public synchronized void close() throws IOException {
        if (buf != null) {
            buf = null;
        }
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public ByteBuffer getBuf() {
        return buf;
    }

    public void setBuf(ByteBuffer buf) {
        this.buf = buf;
    }

    public Path getFile() {
        return file;
    }

    public void setFile(Path file) {
        this.file = file;
    }
}