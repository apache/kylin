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
import java.io.InputStream;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alluxio.client.file.FileInStream;
import alluxio.exception.ExceptionMessage;

@NotThreadSafe
public class AlluxioHdfsFileInputStream extends InputStream
        implements Seekable, PositionedReadable, ByteBufferReadable {
    private static final Logger logger = LoggerFactory.getLogger(AlluxioHdfsFileInputStream.class);

    private final Statistics statistics;
    private final FileInStream fileInStream;

    private boolean streamClosed = false;

    /**
     * Constructs a new stream for reading a file from HDFS.
     *
     * @param inputStream the input stream
     * @param stats filesystem statistics
     */
    public AlluxioHdfsFileInputStream(FileInStream inputStream, Statistics stats) {
        fileInStream = inputStream;
        statistics = stats;
    }

    @Override
    public int available() throws IOException {
        if (streamClosed) {
            throw new IOException("Cannot query available bytes from a closed stream.");
        }
        return (int) fileInStream.remaining();
    }

    @Override
    public void close() throws IOException {
        if (streamClosed) {
            return;
        }
        fileInStream.close();
        streamClosed = true;
    }

    @Override
    public long getPos() throws IOException {
        return fileInStream.getPos();
    }

    @Override
    public int read() throws IOException {
        if (streamClosed) {
            throw new IOException(ExceptionMessage.READ_CLOSED_STREAM.getMessage());
        }

        int data = fileInStream.read();
        if (data != -1 && statistics != null) {
            statistics.incrementBytesRead(1);
            logger.debug("Read one byte.");
        }
        return data;
    }

    @Override
    public int read(byte[] buffer) throws IOException {
        return read(buffer, 0, buffer.length);
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        if (streamClosed) {
            throw new IOException(ExceptionMessage.READ_CLOSED_STREAM.getMessage());
        }

        int bytesRead = fileInStream.read(buffer, offset, length);
        if (bytesRead != -1 && statistics != null) {
            statistics.incrementBytesRead(bytesRead);
            logger.debug("Read {} bytes.", bytesRead);
        }
        return bytesRead;
    }

    public int read(ByteBuffer buf, int off, int len) throws IOException {
        byte[] b = new byte[buf.remaining()];
        int totalBytesRead = read(b, off, len);
        if (totalBytesRead == -1) {
            return -1;
        }
        buf.put(b, off, totalBytesRead);
        return totalBytesRead;
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
        if (streamClosed) {
            throw new IOException(ExceptionMessage.READ_CLOSED_STREAM.getMessage());
        }
        int bytesRead;
        if (buf.hasArray() || !buf.isDirect()) {
            bytesRead = fileInStream.read(buf.array(), buf.position(), buf.remaining());
            if (bytesRead > 0) {
                buf.position(buf.position() + bytesRead);
            }
        } else {
            bytesRead = fileInStream.read(buf);
        }
        if (bytesRead != -1 && statistics != null) {
            statistics.incrementBytesRead(bytesRead);
            logger.debug("Read {} byte buffer {}.", bytesRead, buf.hasArray());
        }
        return bytesRead;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        if (streamClosed) {
            throw new IOException(ExceptionMessage.READ_CLOSED_STREAM.getMessage());
        }

        int bytesRead = fileInStream.positionedRead(position, buffer, offset, length);
        if (bytesRead != -1 && statistics != null) {
            statistics.incrementBytesRead(bytesRead);
            logger.debug("Read {} {} byte buffer.", position, bytesRead);
        }
        return bytesRead;
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        int totalBytesRead = 0;
        while (totalBytesRead < length) {
            int bytesRead = read(position + totalBytesRead, buffer, offset + totalBytesRead, length - totalBytesRead);
            if (bytesRead == -1) {
                throw new EOFException();
            }
            totalBytesRead += bytesRead;
        }
        logger.info("Read fully {} {} byte buffer.", position, totalBytesRead);
    }

    @Override
    public void seek(long pos) throws IOException {
        try {
            fileInStream.seek(pos);
        } catch (IllegalArgumentException e) { // convert back to IOException
            throw new IOException(e);
        }
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        throw new IOException(ExceptionMessage.NOT_SUPPORTED.getMessage());
    }

    @Override
    public long skip(long n) throws IOException {
        if (streamClosed) {
            throw new IOException("Cannot skip bytes in a closed stream.");
        }
        return fileInStream.skip(n);
    }
}