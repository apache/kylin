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

package org.apache.kylin.cube.inmemcubing;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStore;
import org.apache.kylin.gridtable.IGTWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A disk store that allows concurrent read and exclusive write.
 */
public class ConcurrentDiskStore implements IGTStore, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(MemDiskStore.class);
    private static final boolean debug = true;

    private static final int STREAM_BUFFER_SIZE = 8192;

    final private GTInfo info;
    final private Object lock;

    final private File diskFile;
    final private boolean delOnClose;

    private Writer activeWriter;
    private HashSet<Reader> activeReaders = new HashSet<Reader>();
    private FileChannel writeChannel;
    private FileChannel readChannel; // sharable across multi-threads

    public ConcurrentDiskStore(GTInfo info) throws IOException {
        this(info, File.createTempFile("ConcurrentDiskStore", ""), true);
    }

    public ConcurrentDiskStore(GTInfo info, File diskFile) throws IOException {
        this(info, diskFile, false);
    }

    private ConcurrentDiskStore(GTInfo info, File diskFile, boolean delOnClose) throws IOException {
        this.info = info;
        this.lock = this;
        this.diskFile = diskFile;
        this.delOnClose = delOnClose;

        // in case user forget to call close()
        if (delOnClose)
            diskFile.deleteOnExit();

        if (debug)
            logger.debug(this + " disk file " + diskFile.getAbsolutePath());
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public IGTWriter rebuild() throws IOException {
        return newWriter(0);
    }

    @Override
    public IGTWriter append() throws IOException {
        return newWriter(diskFile.length());
    }

    private IGTWriter newWriter(long startOffset) throws IOException {
        synchronized (lock) {
            if (activeWriter != null || !activeReaders.isEmpty())
                throw new IllegalStateException();

            openWriteChannel(startOffset);
            activeWriter = new Writer(startOffset);
            return activeWriter;
        }
    }

    private void closeWriter(Writer w) {
        synchronized (lock) {
            if (activeWriter != w)
                throw new IllegalStateException();

            activeWriter = null;
            closeWriteChannel();
        }
    }

    @Override
    public IGTScanner scan(GTScanRequest scanRequest) throws IOException {
        return newReader();
    }

    private IGTScanner newReader() throws IOException {
        synchronized (lock) {
            if (activeWriter != null)
                throw new IllegalStateException();

            openReadChannel();
            Reader r = new Reader(0);
            activeReaders.add(r);
            return r;
        }
    }

    private void closeReader(Reader r) throws IOException {
        synchronized (lock) {
            if (activeReaders.contains(r) == false)
                throw new IllegalStateException();

            activeReaders.remove(r);
            if (activeReaders.isEmpty())
                closeReadChannel();
        }
    }

    private class Reader implements IGTScanner {
        final DataInputStream din;
        long fileLen;
        long readOffset;
        long count;

        Reader(long startOffset) throws IOException {
            this.fileLen = diskFile.length();
            this.readOffset = startOffset;

            if (debug)
                logger.debug(ConcurrentDiskStore.this + " read start @ " + readOffset);

            InputStream in = new InputStream() {
                byte[] tmp = new byte[1];

                @Override
                public int read() throws IOException {
                    int n = read(tmp, 0, 1);
                    if (n <= 0)
                        return -1;
                    else
                        return (int) tmp[0];
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    if (available() <= 0)
                        return -1;

                    int lenToGo = Math.min(available(), len);
                    int nRead = 0;
                    while (lenToGo > 0) {
                        int n = readChannel.read(ByteBuffer.wrap(b, off, lenToGo), readOffset);

                        lenToGo -= n;
                        nRead += n;
                        off += n;
                        readOffset += n;
                    }
                    return nRead;
                }

                @Override
                public int available() throws IOException {
                    return (int) (fileLen - readOffset);
                }
            };
            din = new DataInputStream(new BufferedInputStream(in, STREAM_BUFFER_SIZE));
        }

        @Override
        public void close() throws IOException {
            din.close();
            closeReader(this);

            if (debug)
                logger.debug(ConcurrentDiskStore.this + " read end @ " + readOffset);
        }

        @Override
        public Iterator<GTRecord> iterator() {
            count = 0;
            return new Iterator<GTRecord>() {
                GTRecord record = new GTRecord(info);
                GTRecord next;
                ByteBuffer buf = ByteBuffer.allocate(info.getMaxRecordLength());

                @Override
                public boolean hasNext() {
                    if (next != null)
                        return true;

                    try {
                        if (din.available() > 0) {
                            int len = din.readInt();
                            din.read(buf.array(), buf.arrayOffset(), len);
                            buf.clear();
                            buf.limit(len);
                            record.loadColumns(info.getAllColumns(), buf);
                            next = record;
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    return next != null;
                }

                @Override
                public GTRecord next() {
                    if (next == null) {
                        hasNext();
                        if (next == null)
                            throw new NoSuchElementException();
                    }
                    GTRecord r = next;
                    next = null;
                    count++;
                    return r;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public GTInfo getInfo() {
            return info;
        }

    }

    private class Writer implements IGTWriter {
        final DataOutputStream dout;
        final ByteBuffer buf;
        long writeOffset;

        Writer(long startOffset) {
            this.writeOffset = startOffset;
            this.buf = ByteBuffer.allocate(info.getMaxRecordLength());

            if (debug)
                logger.debug(ConcurrentDiskStore.this + " write start @ " + writeOffset);

            OutputStream out = new OutputStream() {
                byte[] tmp = new byte[1];

                @Override
                public void write(int b) throws IOException {
                    tmp[0] = (byte) b;
                    write(tmp, 0, 1);
                }

                @Override
                public void write(byte[] bytes, int offset, int length) throws IOException {
                    while (length > 0) {
                        int n = writeChannel.write(ByteBuffer.wrap(bytes, offset, length), writeOffset);
                        offset += n;
                        length -= n;
                        writeOffset += n;
                    }
                }
            };
            dout = new DataOutputStream(new BufferedOutputStream(out, STREAM_BUFFER_SIZE));
        }

        @Override
        public void write(GTRecord rec) throws IOException {
            buf.clear();
            rec.exportColumns(info.getAllColumns(), buf);

            int len = buf.position();
            dout.writeInt(len);
            dout.write(buf.array(), buf.arrayOffset(), len);
        }

        @Override
        public void close() throws IOException {
            dout.close();
            closeWriter(this);

            if (debug)
                logger.debug(ConcurrentDiskStore.this + " write end @ " + writeOffset);
        }
    }

    private void openWriteChannel(long startOffset) throws IOException {
        if (startOffset > 0) { // TODO does not support append yet
            writeChannel = FileChannel.open(diskFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
        } else {
            diskFile.delete();
            writeChannel = FileChannel.open(diskFile.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        }
    }

    private void closeWriteChannel() {
        IOUtils.closeQuietly(writeChannel);
        writeChannel = null;
    }

    private void openReadChannel() throws IOException {
        if (readChannel == null) {
            readChannel = FileChannel.open(diskFile.toPath(), StandardOpenOption.READ);
        }
    }

    private void closeReadChannel() throws IOException {
        IOUtils.closeQuietly(readChannel);
        readChannel = null;
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            if (activeWriter != null || !activeReaders.isEmpty())
                throw new IllegalStateException();

            if (delOnClose) {
                diskFile.delete();
            }

            if (debug)
                logger.debug(this + " closed");
        }
    }

    @Override
    public String toString() {
        return "ConcurrentDiskStore@" + (info.getTableName() == null ? this.hashCode() : info.getTableName());
    }

}
