package org.apache.kylin.storage.gridtable.diskstore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.UUID;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.storage.gridtable.GTInfo;
import org.apache.kylin.storage.gridtable.GTRowBlock;
import org.apache.kylin.storage.gridtable.GTScanRequest;
import org.apache.kylin.storage.gridtable.IGTStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 */
public class GTDiskStore implements IGTStore {

    private static final Logger logger = LoggerFactory.getLogger(GTDiskStore.class);

    private final String identifier;
    private final FileSystem fileSystem;
    private final DiskStoreWriter writer;
    private final GTInfo gtInfo;

    public GTDiskStore(GTInfo gtInfo) {
        this.gtInfo = gtInfo;
        this.fileSystem = new LocalFileSystem();
        this.identifier = generateIdentifier(fileSystem);
        logger.info("disk store created, identifier:" + identifier);
        this.writer = new DiskStoreWriter(fileSystem.getWriter(getRowBlockFile(identifier)));
        deleteTmpFilesOnExit();
    }

    @Override
    public GTInfo getInfo() {
        return gtInfo;
    }

    private String generateIdentifier(FileSystem fs) {
        while (true) {
            String identifier = UUID.randomUUID().toString();
            final String path = getRootDirectory(identifier);
            if (fs.createDirectory(path)) {
                return identifier;
            }
        }
    }

    private String getRootDirectory(String identifier) {
        return "/tmp/kylin/gtdiskstore/" + identifier;
    }

    private String getRowBlockFile(String identifier) {
        return getRootDirectory(identifier) + "/rowblock";
    }

    private class DiskStoreWriter implements IGTStoreWriter {

        private final DataOutputStream outputStream;

        DiskStoreWriter(OutputStream outputStream) {
            this.outputStream = new DataOutputStream(outputStream);
        }

        @Override
        public void write(GTRowBlock block) throws IOException {
            final int blockSize = block.exportLength();
            outputStream.writeInt(blockSize);
            block.export(outputStream);
            outputStream.flush();
        }

        @Override
        public void close() throws IOException {
            outputStream.close();
        }
    }

    public long memoryUsage() {
        return 0;
    }

    @Override
    public IGTStoreWriter rebuild(int shard) throws IOException {
        return writer;
    }

    @Override
    public IGTStoreWriter append(int shard, GTRowBlock.Writer fillLast) throws IOException {
        return writer;
    }

    private class DiskStoreScanner implements IGTStoreScanner {

        private final DataInputStream inputStream;
        private int blockSize = 0;

        DiskStoreScanner(InputStream inputStream) {
            this.inputStream = new DataInputStream(inputStream);
        }

        @Override
        public void close() throws IOException {
            inputStream.close();
        }

        @Override
        public boolean hasNext() {
            try {
                blockSize = inputStream.readInt();
                return blockSize > 0;
            } catch (EOFException e) {
                return false;
            } catch (IOException e) {
                logger.error("input stream fail", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public GTRowBlock next() {
            GTRowBlock block = new GTRowBlock(gtInfo);
            ByteBuffer buffer = ByteBuffer.allocate(blockSize);
            int count = blockSize;
            while (count > 0) {
                try {
                    count -= inputStream.read(buffer.array(), buffer.position(), buffer.remaining());
                } catch (IOException e) {
                    logger.error("input stream fail", e);
                    throw new RuntimeException(e);
                }
            }
            Preconditions.checkArgument(count == 0, "invalid read count:" + count + " block size:" + blockSize);
            block.load(buffer);
            return block;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public IGTStoreScanner scan(ByteArray pkStart, ByteArray pkEnd, BitSet selectedColBlocks, GTScanRequest additionalPushDown) throws IOException {
        return new DiskStoreScanner(fileSystem.getReader(getRowBlockFile(identifier)));
    }

    public void drop() throws IOException {
        try {
            writer.close();
        } catch (Exception e) {
            logger.error("error to close writer", e);
        }
        deleteTmpFilesOnExit();
    }

    private void deleteTmpFilesOnExit() {
        fileSystem.deleteOnExit(getRowBlockFile(identifier));
        fileSystem.deleteOnExit(getRootDirectory(identifier));
    }

}
