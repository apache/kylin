package org.apache.kylin.storage.cube;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.storage.gridtable.GTInfo;
import org.apache.kylin.storage.gridtable.GTRecord;
import org.apache.kylin.storage.gridtable.GTRowBlock;
import org.apache.kylin.storage.gridtable.GTRowBlock.Writer;
import org.apache.kylin.storage.gridtable.GTScanRequest;
import org.apache.kylin.storage.gridtable.IGTStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class CubeHBaseReadonlyStore implements IGTStore {

    public static final Logger logger = LoggerFactory.getLogger(CubeHBaseReadonlyStore.class);

    public static final int SCAN_CACHE = 1024;

    final private GTInfo info;
    final private CubeSegment cubeSeg;
    final private Cuboid cuboid;

    public CubeHBaseReadonlyStore(GTInfo info, CubeSegment cubeSeg, Cuboid cuboid) {
        this.info = info;
        this.cubeSeg = cubeSeg;
        this.cuboid = cuboid;
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public IGTStoreWriter rebuild(int shard) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IGTStoreWriter append(int shard, Writer fillLast) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IGTStoreScanner scan(GTRecord pkStart, GTRecord pkEnd, BitSet selectedColumnBlocks, GTScanRequest additionalPushDown) throws IOException {
        // TODO enable coprocessor

        // primary key (also the 0th column block) is always selected
        final BitSet selectedColBlocks = (BitSet) selectedColumnBlocks.clone();
        selectedColBlocks.set(0);

        // globally shared connection, does not require close
        HConnection hbaseConn = HBaseConnection.get(cubeSeg.getCubeInstance().getConfig().getStorageUrl());

        final HTableInterface hbaseTable = hbaseConn.getTable(cubeSeg.getStorageLocationIdentifier());
        final List<Pair<byte[], byte[]>> hbaseColumns = makeHBaseColumns(selectedColBlocks);

        Scan hbaseScan = buildScan(pkStart, pkEnd, hbaseColumns);
        final ResultScanner scanner = hbaseTable.getScanner(hbaseScan);
        final Iterator<Result> iterator = scanner.iterator();
        final GTRowBlock oneBlock = new GTRowBlock(info); // avoid object creation
        oneBlock.setNumberOfRows(1); // row block is always disabled for cubes, row block contains only one record 

        return new IGTStoreScanner() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public GTRowBlock next() {
                Result result = iterator.next();

                // dimensions, set to primary key, also the 0th column block
                byte[] rowkey = result.getRow();
                oneBlock.getPrimaryKey().set(rowkey, RowConstants.ROWKEY_CUBOIDID_LEN, rowkey.length - RowConstants.ROWKEY_CUBOIDID_LEN);
                oneBlock.getCellBlock(0).set(rowkey, RowConstants.ROWKEY_CUBOIDID_LEN, rowkey.length - RowConstants.ROWKEY_CUBOIDID_LEN);

                // metrics
                int hbaseColIdx = 0;
                for (int colBlockIdx = selectedColBlocks.nextSetBit(1); colBlockIdx >= 0; colBlockIdx = selectedColBlocks.nextSetBit(colBlockIdx + 1)) {
                    Pair<byte[], byte[]> hbaseColumn = hbaseColumns.get(hbaseColIdx++);
                    Cell cell = result.getColumnLatestCell(hbaseColumn.getFirst(), hbaseColumn.getSecond());
                    oneBlock.getCellBlock(colBlockIdx).set(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                }
                return oneBlock;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() throws IOException {
                scanner.close();
                hbaseTable.close();
            }
        };
    }

    private Scan buildScan(GTRecord pkStart, GTRecord pkEnd, List<Pair<byte[], byte[]>> selectedColumns) {
        Scan scan = new Scan();
        scan.setCaching(SCAN_CACHE);
        scan.setCacheBlocks(true);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(Boolean.TRUE));

        for (Pair<byte[], byte[]> hbaseColumn : selectedColumns) {
            byte[] byteFamily = hbaseColumn.getFirst();
            byte[] byteQualifier = hbaseColumn.getSecond();
            scan.addColumn(byteFamily, byteQualifier);
        }

        scan.setStartRow(makeRowKeyToScan(pkStart, (byte) 0x00));
        scan.setStopRow(makeRowKeyToScan(pkEnd, (byte) 0xff));
        //TODO fuzzy match
        return scan;
    }

    private byte[] makeRowKeyToScan(GTRecord pkRec, byte fill) {
        ByteArray pk = GTRecord.exportScanKey(pkRec);
        int pkMaxLen = info.getMaxColumnLength(info.getPrimaryKey());
        
        byte[] buf = new byte[pkMaxLen + RowConstants.ROWKEY_CUBOIDID_LEN];
        Arrays.fill(buf, fill);
        
        System.arraycopy(cuboid.getBytes(), 0, buf, 0, RowConstants.ROWKEY_CUBOIDID_LEN);
        if (pk != null && pk.array() != null) {
            System.arraycopy(pk.array(), pk.offset(), buf, RowConstants.ROWKEY_CUBOIDID_LEN, pk.length());
        }
        return buf;
    }

    private List<Pair<byte[], byte[]>> makeHBaseColumns(BitSet selectedColBlocks) {
        List<Pair<byte[], byte[]>> result = Lists.newArrayList();

        int colBlockIdx = 1; // start from 1; the 0th column block is primary key which maps to rowkey
        HBaseMappingDesc hbaseMapping = cubeSeg.getCubeDesc().getHbaseMapping();
        for (HBaseColumnFamilyDesc familyDesc : hbaseMapping.getColumnFamily()) {
            byte[] byteFamily = Bytes.toBytes(familyDesc.getName());
            for (HBaseColumnDesc hbaseColDesc : familyDesc.getColumns()) {
                if (selectedColBlocks.get(colBlockIdx)) {
                    byte[] byteQualifier = Bytes.toBytes(hbaseColDesc.getQualifier());
                    result.add(new Pair<byte[], byte[]>(byteFamily, byteQualifier));
                }
                colBlockIdx++;
            }
        }

        return result;
    }

}
