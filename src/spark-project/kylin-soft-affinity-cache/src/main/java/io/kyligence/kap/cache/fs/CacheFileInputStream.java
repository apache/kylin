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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Locale;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DirectBufferPool;

import alluxio.client.file.FileInStream;

public class CacheFileInputStream extends FSInputStream implements ByteBufferReadable {

    public static final int EINVAL = -0x16;

    private static final String READ_FILE_LOG = "Reading file %s error, stream was closed";

    private final DirectBufferPool bufferPool;
    private final Statistics statistics;
    private final FileInStream mInputStream;

    private ByteBuffer buf;
    private final Path file;
    private boolean mClosed = false;

    public CacheFileInputStream(Path file, FileInStream inputStream, DirectBufferPool directBufferPool,
            Statistics statistics, int size) {
        this.file = file;
        this.mInputStream = inputStream;
        this.bufferPool = directBufferPool;
        if (this.bufferPool != null) {
            this.buf = this.bufferPool.getBuffer(size);
        } else {
            this.buf = ByteBuffer.allocate(size);
        }
        this.statistics = statistics;
        this.buf.limit(0);
    }

    @Override
    public synchronized long getPos() throws IOException {
        if (buf == null)
            throw new IOException(String.format(Locale.ROOT, READ_FILE_LOG, this.file.toString()));
        return mInputStream.getPos() - buf.remaining();
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
        return false;
    }

    @Override
    public synchronized int available() throws IOException {
        if (buf == null)
            throw new IOException(String.format(Locale.ROOT, READ_FILE_LOG, this.file.toString()));
        return buf.remaining() + mInputStream.available();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new IOException("Mark/reset not supported");
    }

    @Override
    public synchronized int read() throws IOException {
        if (buf == null)
            throw new IOException(String.format(Locale.ROOT, READ_FILE_LOG, this.file.toString()));
        if (!buf.hasRemaining() && refill())
            return -1; // EOF
        assert buf.hasRemaining();
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
            throw new IOException(String.format(Locale.ROOT, READ_FILE_LOG, this.file.toString()));
        if (!buf.hasRemaining() && len <= buf.capacity() && refill())
            return -1; // No bytes were read before EOF.

        int read = Math.min(buf.remaining(), len);
        if (read > 0) {
            buf.get(b, off, read);
            statistics.incrementBytesRead(read);
            off += read;
            len -= read;
        }
        if (len == 0)
            return read;
        // buf is empty, read data from mInputStream directly
        int more = readInternal(b, off, len);
        if (more <= 0) {
            if (read > 0) {
                return read;
            } else {
                return -1;
            }
        }
        // read byte[], buf must be empty
        buf.position(0);
        buf.limit(0);
        return read + more;
    }

    protected synchronized int readInternal(byte[] b, int off, int len) throws IOException {
        if (len == 0)
            return 0;
        if (buf == null)
            throw new IOException(String.format(Locale.ROOT, READ_FILE_LOG, this.file.toString()));
        if (b == null || off < 0 || len < 0 || b.length - off < len) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Reading file %s error,  invalid arguments: off %s len %s ", this.file.toString(), off, len));
        }
        int got = mInputStream.read(b, off, len);
        if (got == 0)
            return -1;
        if (got == EINVAL)
            throw new IOException(String.format(Locale.ROOT, READ_FILE_LOG, this.file.toString()));
        if (got < 0)
            throw new IOException(String.format(Locale.ROOT, READ_FILE_LOG, this.file.toString()));
        statistics.incrementBytesRead(got);
        return got;
    }

    private boolean refill() throws IOException {
        buf.clear();
        int read = readInternal(buf);
        if (read <= 0) {
            buf.limit(0);
            return true; // EOF
        }
        statistics.incrementBytesRead(-read);
        buf.flip();
        return false;
    }

    @Override
    public synchronized int read(long pos, byte[] b, int off, int len) throws IOException {
        if (len == 0)
            return 0;
        if (pos < 0)
            throw new EOFException("Reading file " + this.file.toString() + " error, position is negative");
        if (b == null || off < 0 || len < 0 || b.length - off < len) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Reading file %s error,  invalid arguments: off %s len %s ", this.file.toString(), off, len));
        }
        long oldPos = mInputStream.getPos();
        mInputStream.seek(pos);
        int got = -1;
        try {
            got = mInputStream.read(b, off, len);
            if (got == 0)
                return -1;
            if (got == EINVAL)
                throw new IOException(String.format(Locale.ROOT, READ_FILE_LOG, this.file.toString()));
            if (got < 0)
                throw new IOException(String.format(Locale.ROOT, READ_FILE_LOG, this.file.toString()));
        } finally {
            mInputStream.seek(oldPos);
        }
        statistics.incrementBytesRead(got);
        return got;
    }

    @Override
    public synchronized int read(ByteBuffer b) throws IOException {
        if (!b.hasRemaining())
            return 0;
        if (buf == null)
            throw new IOException(String.format(Locale.ROOT, READ_FILE_LOG, this.file.toString()));
        if (!buf.hasRemaining() && b.remaining() <= buf.capacity() && refill()) {
            return -1;
        }
        int got = Math.min(b.remaining(), buf.remaining());
        if (got > 0) {
            byte[] readedBytes = new byte[got];
            buf.get(readedBytes, 0, got);
            b.put(readedBytes, 0, got);
            statistics.incrementBytesRead(got);
        }
        if (!b.hasRemaining())
            return got;
        int more = readInternal(b);
        if (more <= 0)
            return got > 0 ? got : -1;
        buf.position(0);
        buf.limit(0);
        return got + more;
    }

    protected synchronized int readInternal(ByteBuffer b) throws IOException {
        if (!b.hasRemaining())
            return 0;
        int got;
        if (b.hasArray()) {
            // for heap bytebuffer
            got = readInternal(b.array(), b.position(), b.remaining());
            if (got <= 0)
                return got;
            b.position(b.position() + got);
        } else {
            assert b.isDirect();
            got = mInputStream.read(b, b.position(), b.remaining());
            if (got == EINVAL)
                throw new IOException(String.format(Locale.ROOT, READ_FILE_LOG, this.file.toString()));
            if (got < 0)
                throw new IOException(String.format(Locale.ROOT, READ_FILE_LOG, this.file.toString()));
            if (got == 0)
                return -1;
            statistics.incrementBytesRead(got);
        }
        return got;
    }

    @Override
    public synchronized void seek(long p) throws IOException {
        if (p < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }
        if (buf == null)
            throw new IOException(String.format(Locale.ROOT, READ_FILE_LOG, this.file.toString()));
        if (p < mInputStream.getPos() && p >= mInputStream.getPos() - buf.limit()) {
            buf.position((int) (p - (mInputStream.getPos() - buf.limit())));
        } else {
            buf.position(0);
            buf.limit(0);
            mInputStream.seek(p);
        }
    }

    @Override
    public synchronized long skip(long n) throws IOException {
        if (n < 0)
            return -1;
        if (buf == null)
            throw new IOException(String.format(Locale.ROOT, READ_FILE_LOG, this.file.toString()));
        if (n < buf.remaining()) {
            buf.position(buf.position() + (int) n);
        } else {
            while (mInputStream.skip(n - buf.remaining()) > 0) {
                buf.position(0);
                buf.limit(0);
            }
        }
        return n;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!mClosed) {
            mInputStream.close();
            mClosed = true;
        }
        if (this.bufferPool != null && buf != null) {
            this.bufferPool.returnBuffer(buf);
        }
        buf = null;
    }
}