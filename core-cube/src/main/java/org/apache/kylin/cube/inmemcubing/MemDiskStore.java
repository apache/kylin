/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.kylin.cube.inmemcubing;

import static org.apache.kylin.common.util.MemoryBudgetController.ONE_MB;

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
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.common.util.MemoryBudgetController.MemoryConsumer;
import org.apache.kylin.common.util.MemoryBudgetController.NotEnoughBudgetException;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStore;
import org.apache.kylin.gridtable.IGTWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemDiskStore implements IGTStore, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(MemDiskStore.class);
    private static final boolean debug = true;

    private static final int STREAM_BUFFER_SIZE = 8192;
    private static final int MEM_CHUNK_SIZE_MB = 5;

    private final GTInfo info;
    private final Object lock; // all public methods that read/write object states are synchronized on this lock
    private final MemPart memPart;
    private final DiskPart diskPart;
    private final boolean delOnClose;

    private Writer ongoingWriter;

    public MemDiskStore(GTInfo info, MemoryBudgetController budgetCtrl) throws IOException {
        this(info, budgetCtrl, File.createTempFile("MemDiskStore", ""), true);
    }

    public MemDiskStore(GTInfo info, MemoryBudgetController budgetCtrl, File diskFile) throws IOException {
        this(info, budgetCtrl, diskFile, false);
    }

    private MemDiskStore(GTInfo info, MemoryBudgetController budgetCtrl, File diskFile, boolean delOnClose) throws IOException {
        this.info = info;
        this.lock = this;
        this.memPart = new MemPart(budgetCtrl);
        this.diskPart = new DiskPart(diskFile);
        this.delOnClose = delOnClose;

        // in case user forget to call close()
        if (delOnClose)
            diskFile.deleteOnExit();
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
        return newWriter(length());
    }

    private Writer newWriter(long startOffset) throws IOException {
        synchronized (lock) {
            if (ongoingWriter != null)
                throw new IllegalStateException();

            ongoingWriter = new Writer(startOffset);
            return ongoingWriter;
        }
    }

    @Override
    public IGTScanner scan(GTScanRequest scanRequest) throws IOException {
        synchronized (lock) {
            return new Reader();
        }
    }

    @Override
    public void close() throws IOException {
        // synchronized inside the parts close()
        memPart.close();
        diskPart.close();
    }

    public long length() {
        synchronized (lock) {
            return Math.max(memPart.tailOffset(), diskPart.tailOffset);
        }
    }

    @Override
    public String toString() {
        return "MemDiskStore@" + (info.getTableName() == null ? this.hashCode() : info.getTableName());
    }

    private class Reader implements IGTScanner {

        final DataInputStream din;
        long readOffset = 0;
        long memRead = 0;
        long diskRead = 0;
        int nReadCalls = 0;
        int count = 0;

        Reader() throws IOException {
            diskPart.openRead();
            if (debug)
                logger.debug(MemDiskStore.this + " read start @ " + readOffset);

            InputStream in = new InputStream() {
                byte[] tmp = new byte[1];
                MemChunk memChunk;

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
                    synchronized (lock) {
                        nReadCalls++;
                        if (available() <= 0)
                            return -1;

                        if (memChunk == null && memPart.headOffset() <= readOffset && readOffset < memPart.tailOffset()) {
                            memChunk = memPart.seekMemChunk(readOffset);
                        }

                        int lenToGo = Math.min(available(), len);

                        int nRead = 0;
                        while (lenToGo > 0) {
                            int n;
                            if (memChunk != null) {
                                if (memChunk.headOffset() > readOffset) {
                                    memChunk = null;
                                    continue;
                                }
                                if (readOffset >= memChunk.tailOffset()) {
                                    memChunk = memChunk.next;
                                    continue;
                                }
                                int chunkOffset = (int) (readOffset - memChunk.headOffset());
                                n = Math.min((int) (memChunk.tailOffset() - readOffset), lenToGo);
                                System.arraycopy(memChunk.data, chunkOffset, b, off, n);
                                memRead += n;
                            } else {
                                n = diskPart.read(readOffset, b, off, lenToGo);
                                diskRead += n;
                            }
                            lenToGo -= n;
                            nRead += n;
                            off += n;
                            readOffset += n;
                        }
                        return nRead;
                    }
                }

                @Override
                public int available() throws IOException {
                    synchronized (lock) {
                        return (int) (length() - readOffset);
                    }
                }
            };

            din = new DataInputStream(new BufferedInputStream(in, STREAM_BUFFER_SIZE));
        }

        @Override
        public void close() throws IOException {
            synchronized (lock) {
                din.close();
                diskPart.closeRead();
                if (debug)
                    logger.debug(MemDiskStore.this + " read end @ " + readOffset + ", " + (memRead) + " from mem, " + (diskRead) + " from disk, " + nReadCalls + " read() calls");
            }
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

        @Override
        public long getScannedRowCount() {
            return count;
        }

    }

    private class Writer implements IGTWriter {

        final DataOutputStream dout;
        final ByteBuffer buf;
        long writeOffset;
        long memWrite = 0;
        long diskWrite = 0;
        int nWriteCalls;
        boolean closed = false;

        Writer(long startOffset) throws IOException {
            buf = ByteBuffer.allocate(info.getMaxRecordLength());
            writeOffset = 0; // TODO does not support append yet
            memPart.clear();
            diskPart.clear();
            diskPart.openWrite(false);
            if (debug)
                logger.debug(MemDiskStore.this + " write start @ " + writeOffset);

            memPart.activateMemWrite();

            OutputStream out = new OutputStream() {
                byte[] tmp = new byte[1];
                boolean memPartActivated = true;

                @Override
                public void write(int b) throws IOException {
                    tmp[0] = (byte) b;
                    write(tmp, 0, 1);
                }

                @Override
                public void write(byte[] bytes, int offset, int length) throws IOException {
                    // lock inside memPart.write() and diskPart.write()
                    nWriteCalls++;
                    while (length > 0) {
                        int n;
                        if (memPartActivated) {
                            n = memPart.write(bytes, offset, length, writeOffset);
                            memWrite += n;
                            if (n == 0) {
                                memPartActivated = false;
                            }
                        } else {
                            n = diskPart.write(writeOffset, bytes, offset, length);
                            diskWrite += n;
                        }
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
            synchronized (lock) {
                if (!closed) {
                    dout.close();
                    memPart.deactivateMemWrite();
                }

                if (memPart.asyncFlusher == null) {
                    assert writeOffset == diskPart.tailOffset;
                    diskPart.closeWrite();
                    ongoingWriter = null;
                    if (debug)
                        logger.debug(MemDiskStore.this + " write end @ " + writeOffset + ", " + (memWrite) + " to mem, " + (diskWrite) + " to disk, " + nWriteCalls + " write() calls");
                } else {
                    // the asyncFlusher will call this close() again later
                }
                closed = true;
            }
        }
    }

    private static class MemChunk {
        long diskOffset;
        int length;
        byte[] data;
        MemChunk next;

        boolean isFull() {
            return length == data.length;
        }

        long headOffset() {
            return diskOffset;
        }

        long tailOffset() {
            return diskOffset + length;
        }

        int freeSpace() {
            return data.length - length;
        }
    }

    private class MemPart implements Closeable, MemoryConsumer {

        final MemoryBudgetController budgetCtrl;

        // async flush thread checks this flag out of sync block
        volatile boolean writeActivated;
        MemChunk firstChunk;
        MemChunk lastChunk;
        int chunkCount;

        Thread asyncFlusher;
        MemChunk asyncFlushChunk;
        long asyncFlushDiskOffset;
        Throwable asyncFlushException;

        MemPart(MemoryBudgetController budgetCtrl) {
            this.budgetCtrl = budgetCtrl;
        }

        long headOffset() {
            return firstChunk == null ? 0 : firstChunk.headOffset();
        }

        long tailOffset() {
            return lastChunk == null ? 0 : lastChunk.tailOffset();
        }

        public MemChunk seekMemChunk(long diskOffset) {
            MemChunk c = firstChunk;
            while (c != null && c.headOffset() <= diskOffset) {
                if (diskOffset < c.tailOffset())
                    break;
                c = c.next;
            }
            return c;
        }

        public int write(byte[] bytes, int offset, int length, long diskOffset) {
            int needMoreMem = 0;

            synchronized (lock) {
                if (writeActivated == false)
                    return 0;

                // write is only expected at the tail
                if (diskOffset != tailOffset())
                    return 0;

                if (chunkCount == 0 || lastChunk.isFull())
                    needMoreMem = (chunkCount + 1) * MEM_CHUNK_SIZE_MB;
            }

            // call to budgetCtrl.reserve() must be out of synchronized block, or deadlock may happen between MemoryConsumers
            if (needMoreMem > 0) {
                try {
                    budgetCtrl.reserve(this, needMoreMem);
                } catch (NotEnoughBudgetException ex) {
                    deactivateMemWrite();
                    return 0;
                }
            }

            synchronized (lock) {
                if (needMoreMem > 0 && (chunkCount == 0 || lastChunk.isFull())) {
                    MemChunk chunk = new MemChunk();
                    chunk.diskOffset = diskOffset;
                    chunk.data = new byte[ONE_MB * MEM_CHUNK_SIZE_MB - 48]; // -48 for MemChunk overhead
                    if (chunkCount == 0) {
                        firstChunk = lastChunk = chunk;
                    } else {
                        lastChunk.next = chunk;
                        lastChunk = chunk;
                    }
                    chunkCount++;
                }

                int n = Math.min(lastChunk.freeSpace(), length);
                System.arraycopy(bytes, offset, lastChunk.data, lastChunk.length, n);
                lastChunk.length += n;

                if (n > 0)
                    asyncFlush(lastChunk, diskOffset, n);

                return n;
            }
        }

        private void asyncFlush(MemChunk lastChunk, long diskOffset, int n) {
            if (asyncFlushChunk == null) {
                asyncFlushChunk = lastChunk;
                asyncFlushDiskOffset = diskOffset;
            }

            if (asyncFlusher == null) {
                asyncFlusher = new Thread() {
                    public void run() {
                        asyncFlushException = null;
                        if (debug)
                            logger.debug(MemDiskStore.this + " async flush started @ " + asyncFlushDiskOffset);
                        try {
                            while (writeActivated) {
                                flushToDisk();
                                Thread.sleep(10);
                            }
                            flushToDisk();

                            if (debug)
                                logger.debug(MemDiskStore.this + " async flush ended @ " + asyncFlushDiskOffset);

                            synchronized (lock) {
                                asyncFlusher = null;
                                asyncFlushChunk = null;
                                if (ongoingWriter.closed) {
                                    ongoingWriter.close(); // call writer.close() again to clean up
                                }
                            }
                        } catch (Throwable ex) {
                            asyncFlushException = ex;
                        }
                    }
                };
                asyncFlusher.start();
            }
        }

        private void flushToDisk() throws IOException {
            byte[] data;
            int offset = 0;
            int length = 0;
            int flushedLen = 0;

            while (true) {
                data = null;
                synchronized (lock) {
                    asyncFlushDiskOffset += flushedLen; // bytes written in last loop
                    //                    if (debug)
                    //                        logger.debug(GTMemDiskStore.this + " async flush @ " + asyncFlushDiskOffset);
                    if (asyncFlushChunk != null && asyncFlushChunk.tailOffset() == asyncFlushDiskOffset) {
                        asyncFlushChunk = asyncFlushChunk.next;
                    }
                    if (asyncFlushChunk != null) {
                        data = asyncFlushChunk.data;
                        offset = (int) (asyncFlushDiskOffset - asyncFlushChunk.headOffset());
                        length = asyncFlushChunk.length - offset;
                    }
                }

                if (data == null)
                    break;

                flushedLen = diskPart.write(asyncFlushDiskOffset, data, offset, length);
            }
        }

        @Override
        public int freeUp(int mb) {
            synchronized (lock) {
                int mbReleased = 0;
                while (chunkCount > 0 && mbReleased < mb) {
                    if (firstChunk == asyncFlushChunk)
                        break;

                    mbReleased += MEM_CHUNK_SIZE_MB;
                    chunkCount--;
                    if (chunkCount == 0) {
                        firstChunk = lastChunk = null;
                    } else {
                        MemChunk next = firstChunk.next;
                        firstChunk.next = null;
                        firstChunk = next;
                    }
                }
                return mbReleased;
            }
        }

        public void activateMemWrite() {
            if (budgetCtrl.getTotalBudgetMB() > 0) {
                writeActivated = true;
                if (debug)
                    logger.debug(MemDiskStore.this + " mem write activated");
            }
        }

        public void deactivateMemWrite() {
            writeActivated = false;
            if (debug)
                logger.debug(MemDiskStore.this + " mem write de-activated");
        }

        public void clear() {
            chunkCount = 0;
            firstChunk = lastChunk = null;
            budgetCtrl.reserve(this, 0);
        }

        @Override
        public void close() throws IOException {
            synchronized (lock) {
                if (asyncFlushException != null)
                    throwAsyncException(asyncFlushException);
            }
            try {
                asyncFlusher.join();
            } catch (NullPointerException npe) {
                // that's fine, async flusher may not present
            } catch (InterruptedException e) {
                logger.warn("async join interrupted", e);
            }
            synchronized (lock) {
                if (asyncFlushException != null)
                    throwAsyncException(asyncFlushException);

                clear();
            }
        }

        private void throwAsyncException(Throwable ex) throws IOException {
            if (ex instanceof IOException)
                throw (IOException) ex;
            else
                throw new IOException(ex);
        }

        @Override
        public String toString() {
            return MemDiskStore.this.toString();
        }

    }

    private class DiskPart implements Closeable {
        final File diskFile;
        FileChannel writeChannel;
        FileChannel readChannel;
        int readerCount = 0; // allow parallel readers
        long tailOffset;

        DiskPart(File diskFile) throws IOException {
            this.diskFile = diskFile;
            this.tailOffset = diskFile.length();
            if (debug)
                logger.debug(MemDiskStore.this + " disk file " + diskFile.getAbsolutePath());
        }

        public void openRead() throws IOException {
            if (readChannel == null) {
                readChannel = FileChannel.open(diskFile.toPath(), StandardOpenOption.READ);
            }
            readerCount++;
        }

        public int read(long diskOffset, byte[] bytes, int offset, int length) throws IOException {
            return readChannel.read(ByteBuffer.wrap(bytes, offset, length), diskOffset);
        }

        public void closeRead() throws IOException {
            closeRead(false);
        }

        private void closeRead(boolean force) throws IOException {
            readerCount--;
            if (readerCount == 0 || force) {
                if (readChannel != null) {
                    readChannel.close();
                    readChannel = null;
                }
            }
        }

        public void openWrite(boolean append) throws IOException {
            if (append) {
                writeChannel = FileChannel.open(diskFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
                tailOffset = diskFile.length();
            } else {
                diskFile.delete();
                writeChannel = FileChannel.open(diskFile.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                tailOffset = 0;
            }
        }

        public int write(long diskOffset, byte[] bytes, int offset, int length) throws IOException {
            synchronized (lock) {
                int n = writeChannel.write(ByteBuffer.wrap(bytes, offset, length), diskOffset);
                tailOffset = Math.max(diskOffset + n, tailOffset);
                return n;
            }
        }

        public void closeWrite() throws IOException {
            if (writeChannel != null) {
                writeChannel.close();
                writeChannel = null;
            }
        }

        public void clear() throws IOException {
            diskFile.delete();
            tailOffset = 0;
        }

        @Override
        public void close() throws IOException {
            synchronized (lock) {
                closeWrite();
                closeRead(true);
                if (delOnClose) {
                    diskFile.delete();
                }
            }
        }
    }

}
