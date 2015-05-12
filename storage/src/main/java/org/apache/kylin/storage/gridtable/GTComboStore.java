package org.apache.kylin.storage.gridtable;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.storage.gridtable.diskstore.GTDiskStore;
import org.apache.kylin.storage.gridtable.memstore.GTSimpleMemStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.BitSet;

/**
 * Created by qianzhou on 5/6/15.
 */
public class GTComboStore implements IGTStore {

    private static final Logger logger = LoggerFactory.getLogger(GTComboStore.class);

    private final GTInfo gtInfo;

    private void convert(IGTStore input, IGTStore output) throws IOException {
        final IGTStoreScanner scanner = input.scan(ScanKey.makeScanKey(gtInfo, new GTRecord(gtInfo)), ScanKey.makeScanKey(gtInfo, new GTRecord(gtInfo)), null, null);
        final IGTStoreWriter writer = output.rebuild(-1);
        while (scanner.hasNext()) {
            writer.write(scanner.next());
        }
    }

    private GTDiskStore gtDiskStore;
    private GTSimpleMemStore gtSimpleMemStore;

    public GTComboStore(GTInfo gtInfo) {
        this(gtInfo, true);
    }

    public GTComboStore(GTInfo gtInfo, boolean useMemStore) {
        this.gtInfo = gtInfo;
        if (useMemStore) {
            this.gtSimpleMemStore = new GTSimpleMemStore(gtInfo);
        } else {
            this.gtDiskStore = new GTDiskStore(gtInfo);
        }
    }

    private IGTStore getCurrent() {
        if (gtSimpleMemStore != null) {
            return gtSimpleMemStore;
        } else {
            return gtDiskStore;
        }
    }

    public void switchToMemStore() {
        try {
            if (gtSimpleMemStore == null) {
                gtSimpleMemStore = new GTSimpleMemStore(gtInfo);
                convert(gtDiskStore, gtSimpleMemStore);
                gtDiskStore.drop();
                gtDiskStore = null;
            }
        } catch (IOException e) {
            logger.error("fail to switch to mem store", e);
            throw new RuntimeException(e);
        }
        logger.info("switch to mem store");
    }

    public void switchToDiskStore() {
        try {
            if (gtDiskStore == null) {
                gtDiskStore = new GTDiskStore(gtInfo);
                convert(gtSimpleMemStore, gtDiskStore);
                gtSimpleMemStore.drop();
                gtSimpleMemStore = null;
            }
        } catch (IOException e) {
            logger.error("fail to switch to disk store", e);
            throw new RuntimeException(e);
        }
        logger.info("switch to disk store");
    }

    @Override
    public long memoryUsage() {
        return getCurrent().memoryUsage();
    }

    @Override
    public IGTStoreWriter rebuild(int shard) throws IOException {
        return getCurrent().rebuild(shard);
    }

    @Override
    public IGTStoreWriter append(int shard, GTRowBlock.Writer fillLast) throws IOException {
        return getCurrent().append(shard, fillLast);
    }

    @Override
    public IGTStoreScanner scan(ByteArray pkStart, ByteArray pkEnd, BitSet selectedColBlocks, GTScanRequest additionalPushDown) throws IOException {
        return getCurrent().scan(pkStart, pkEnd, selectedColBlocks, additionalPushDown);
    }

    @Override
    public void drop() throws IOException {
        if (gtSimpleMemStore != null) {
            gtSimpleMemStore.drop();
        }
        if (gtDiskStore != null) {
            gtDiskStore.drop();
        }
    }
}
