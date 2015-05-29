package org.apache.kylin.storage.gridtable.memstore;

import static org.apache.kylin.common.util.MemoryBudgetController.*;

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
import java.util.BitSet;
import java.util.NoSuchElementException;

import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.common.util.MemoryBudgetController.MemoryConsumer;
import org.apache.kylin.common.util.MemoryBudgetController.NotEnoughBudgetException;
import org.apache.kylin.storage.gridtable.GTInfo;
import org.apache.kylin.storage.gridtable.GTRecord;
import org.apache.kylin.storage.gridtable.GTRowBlock;
import org.apache.kylin.storage.gridtable.GTScanRequest;
import org.apache.kylin.storage.gridtable.IGTStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GTMemDiskStore implements IGTStore, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(GTMemDiskStore.class);
    private static final boolean debug = false;

    private static final int STREAM_BUFFER_SIZE = 8192;
    private static final int MEM_CHUNK_SIZE_MB = 1;

    final GTInfo info;
    final MemPart memPart;
    final DiskPart diskPart;
    final boolean delOnClose;

    public GTMemDiskStore(GTInfo info, MemoryBudgetController budgetCtrl) throws IOException {
        this(info, budgetCtrl, File.createTempFile("GTMemDiskStore", ""), true);
    }

    public GTMemDiskStore(GTInfo info, MemoryBudgetController budgetCtrl, File diskFile) throws IOException {
        this(info, budgetCtrl, diskFile, false);
    }

    private GTMemDiskStore(GTInfo info, MemoryBudgetController budgetCtrl, File diskFile, boolean delOnClose) throws IOException {
        this.info = info;
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
    public IGTStoreWriter rebuild(int shard) throws IOException {
        return new Writer(0);
    }

    @Override
    public IGTStoreWriter append(int shard, GTRowBlock.Writer fillLast) throws IOException {
        return new Writer(diskPart.tailOffset);
    }

    @Override
    public IGTStoreScanner scan(GTRecord pkStart, GTRecord pkEnd, BitSet selectedColBlocks, GTScanRequest additionalPushDown) throws IOException {
        return new Reader();
    }

    @Override
    public void close() throws IOException {
        memPart.close();
        diskPart.close();
    }

    @Override
    public String toString() {
        return "MemDiskStore@" + this.hashCode();
    }

    private class Reader implements IGTStoreScanner {

        final DataInputStream din;
        long diskOffset = 0;
        long memRead = 0;
        long diskRead = 0;
        int nReadCalls = 0;

        GTRowBlock block = GTRowBlock.allocate(info);
        GTRowBlock next = null;

        Reader() throws IOException {
            diskPart.openRead();
            if (debug)
                logger.debug(GTMemDiskStore.this + " read start @ " + diskOffset);

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
                    nReadCalls++;
                    if (available() <= 0)
                        return -1;

                    if (memChunk == null && memPart.headOffset() <= diskOffset && diskOffset < memPart.tailOffset()) {
                        memChunk = memPart.seekMemChunk(diskOffset);
                    }

                    int lenToGo = Math.min(available(), len);

                    int nRead = 0;
                    while (lenToGo > 0) {
                        int n;
                        if (memChunk != null) {
                            if (memChunk.headOffset() > diskOffset) {
                                memChunk = null;
                                continue;
                            }
                            if (diskOffset >= memChunk.tailOffset()) {
                                memChunk = memChunk.next;
                                continue;
                            }
                            int chunkOffset = (int) (diskOffset - memChunk.headOffset());
                            n = Math.min((int) (memChunk.tailOffset() - diskOffset), lenToGo);
                            System.arraycopy(memChunk.data, chunkOffset, b, off, n);
                            memRead += n;
                        } else {
                            n = diskPart.read(diskOffset, b, off, lenToGo);
                            diskRead += n;
                        }
                        lenToGo -= n;
                        nRead += n;
                        off += n;
                        diskOffset += n;
                    }
                    return nRead;
                }

                @Override
                public int available() throws IOException {
                    return (int) (diskPart.tailOffset - diskOffset);
                }
            };

            din = new DataInputStream(new BufferedInputStream(in, STREAM_BUFFER_SIZE));
        }

        @Override
        public boolean hasNext() {
            if (next != null)
                return true;

            try {
                if (din.available() > 0) {
                    block.importFrom(din);
                    next = block;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return next != null;
        }

        @Override
        public GTRowBlock next() {
            if (next == null) {
                hasNext();
                if (next == null)
                    throw new NoSuchElementException();
            }
            GTRowBlock r = next;
            next = null;
            return r;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            din.close();
            diskPart.closeRead();
            if (debug)
                logger.debug(GTMemDiskStore.this + " read end @ " + diskOffset + ", " + (memRead) + " from mem, " + (diskRead) + " from disk, " + nReadCalls + " read() calls");
        }

    }

    private class Writer implements IGTStoreWriter {

        final DataOutputStream dout;
        long diskOffset;
        long memWrite = 0;
        long diskWrite = 0;
        int nWriteCalls;

        Writer(long startOffset) throws IOException {
            diskOffset = 0; // TODO does not support append yet
            memPart.clear();
            diskPart.clear();
            diskPart.openWrite(false);
            if (debug)
                logger.debug(GTMemDiskStore.this + " write start @ " + diskOffset);

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
                    nWriteCalls++;
                    while (length > 0) {
                        int n;
                        if (memPartActivated) {
                            n = memPart.write(bytes, offset, length, diskOffset);
                            memWrite += n;
                            if (n == 0) {
                                memPartActivated = false;
                            }
                        } else {
                            n = diskPart.write(diskOffset, bytes, offset, length);
                            diskWrite += n;
                        }
                        offset += n;
                        length -= n;
                        diskOffset += n;
                    }
                }
            };
            dout = new DataOutputStream(new BufferedOutputStream(out, STREAM_BUFFER_SIZE));
        }

        @Override
        public void write(GTRowBlock block) throws IOException {
            block.export(dout);
        }

        @Override
        public void close() throws IOException {
            dout.close();
            memPart.finishAsyncFlush();
            diskPart.closeWrite();
            assert diskOffset == diskPart.tailOffset;
            if (debug)
                logger.debug(GTMemDiskStore.this + " write end @ " + diskOffset + ", " + (memWrite) + " to mem, " + (diskWrite) + " to disk, " + nWriteCalls + " write() calls");
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

        // read & write won't go together, but write() / asyncDiskWrite() / freeUp() can happen at the same time
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

        synchronized public MemChunk seekMemChunk(long diskOffset) {
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

            synchronized (this) {
                if (writeActivated == false)
                    return 0;

                // write is only expected at the tail
                if (diskOffset != tailOffset())
                    return 0;

                if (chunkCount == 0 || lastChunk.isFull())
                    needMoreMem = (chunkCount + 1) * MEM_CHUNK_SIZE_MB;
            }

            // call to budgetCtrl.reserve() out of synchronized block, or deadlock may happen between MemoryConsumers
            if (needMoreMem > 0) {
                try {
                    budgetCtrl.reserve(this, needMoreMem);
                } catch (NotEnoughBudgetException ex) {
                    deactivateMemWrite();
                    return 0;
                }
            }

            synchronized (this) {
                if (needMoreMem > 0) {
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
                            logger.debug(GTMemDiskStore.this + " async flush started @ " + asyncFlushDiskOffset);
                        try {
                            while (writeActivated) {
                                flushToDisk();
                                Thread.sleep(10);
                            }
                            flushToDisk();
                        } catch (Throwable ex) {
                            asyncFlushException = ex;
                        }
                        if (debug)
                            logger.debug(GTMemDiskStore.this + " async flush ended @ " + asyncFlushDiskOffset);
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
                synchronized (memPart) {
                    asyncFlushDiskOffset += flushedLen; // bytes written in last loop
                    if (debug)
                        logger.debug(GTMemDiskStore.this + " async flush @ " + asyncFlushDiskOffset);
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

        public void finishAsyncFlush() throws IOException {
            deactivateMemWrite();
            if (asyncFlusher != null) {
                try {
                    asyncFlusher.join();
                } catch (InterruptedException e) {
                    logger.warn("", e);
                }
                asyncFlusher = null;
                asyncFlushChunk = null;

                if (asyncFlushException != null) {
                    if (asyncFlushException instanceof IOException)
                        throw (IOException) asyncFlushException;
                    else
                        throw new IOException(asyncFlushException);
                }
            }
        }

        @Override
        synchronized public int freeUp(int mb) {
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

        public void activateMemWrite() {
            writeActivated = true;
            if (debug)
                logger.debug(GTMemDiskStore.this + " mem write activated");
        }

        public void deactivateMemWrite() {
            writeActivated = false;
            if (debug)
                logger.debug(GTMemDiskStore.this + " mem write de-activated");
        }

        synchronized public void clear() {
            budgetCtrl.reserve(this, 0);
            chunkCount = 0;
            firstChunk = lastChunk = null;
        }

        @Override
        public void close() throws IOException {
            finishAsyncFlush();
            clear();
        }

        @Override
        public String toString() {
            return GTMemDiskStore.this.toString();
        }

    }

    private class DiskPart implements Closeable {
        final File diskFile;
        FileChannel writeChannel;
        FileChannel readChannel;
        long tailOffset;

        DiskPart(File diskFile) throws IOException {
            this.diskFile = diskFile;
            this.tailOffset = diskFile.length();
            if (debug)
                logger.debug(GTMemDiskStore.this + " disk file " + diskFile.getAbsolutePath());
        }

        public void openRead() throws IOException {
            readChannel = FileChannel.open(diskFile.toPath(), StandardOpenOption.READ);
            tailOffset = diskFile.length();
        }

        public int read(long diskOffset, byte[] bytes, int offset, int length) throws IOException {
            return readChannel.read(ByteBuffer.wrap(bytes, offset, length), diskOffset);
        }

        public void closeRead() throws IOException {
            if (readChannel != null) {
                readChannel.close();
                readChannel = null;
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
            int n = writeChannel.write(ByteBuffer.wrap(bytes, offset, length), diskOffset);
            tailOffset = Math.max(diskOffset + n, tailOffset);
            return n;
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
            closeWrite();
            closeRead();
            if (delOnClose) {
                diskFile.delete();
            }
        }
    }

}
