package org.apache.kylin.storage.cube;

import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowValueDecoder;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.storage.gridtable.GTInfo;
import org.apache.kylin.storage.gridtable.GTRowBlock;
import org.apache.kylin.storage.gridtable.GTRowBlock.Writer;
import org.apache.kylin.storage.gridtable.GTScanRequest;
import org.apache.kylin.storage.gridtable.IGTStore;
import org.apache.kylin.storage.hbase.CubeSegmentTupleIterator;
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
    public String getStorageDescription() {
        return cubeSeg.toString();
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
    public IGTStoreScanner scan(ByteArray pkStart, ByteArray pkEnd, BitSet selectedColBlocks, GTScanRequest additionalPushDown) throws IOException {
        // TODO enable coprocessor

        // globally shared connection, does not require close
        HConnection hbaseConn = HBaseConnection.get(cubeSeg.getCubeInstance().getConfig().getStorageUrl());
        
        final HTableInterface hbaseTable = hbaseConn.getTable(cubeSeg.getStorageLocationIdentifier());
        final List<Pair<byte[], byte[]>> hbaseColumns = makeHBaseColumns(selectedColBlocks);

        Scan hbaseScan = buildScan(pkStart, pkEnd, hbaseColumns);
        final ResultScanner scanner = hbaseTable.getScanner(hbaseScan);
        final Iterator<Result> iterator = scanner.iterator();

        return new IGTStoreScanner() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public GTRowBlock next() {
                Result result = iterator.next();
                result.getRow();
                for (Pair<byte[], byte[]> hbaseColumn : hbaseColumns) {
                    result.getColumnLatestCell(hbaseColumn.getFirst(), hbaseColumn.getSecond());
                }
                return null;
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

    private Scan buildScan(ByteArray pkStart, ByteArray pkEnd, List<Pair<byte[], byte[]>> selectedColumns) {
        Scan scan = new Scan();
        scan.setCaching(SCAN_CACHE);
        scan.setCacheBlocks(true);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(Boolean.TRUE));

        for (Pair<byte[], byte[]> hbaseColumn : selectedColumns) {
            byte[] byteFamily = hbaseColumn.getFirst();
            byte[] byteQualifier = hbaseColumn.getSecond();
            scan.addColumn(byteFamily, byteQualifier);
        }

        scan.setStartRow(pkStart.copy().array());
        scan.setStopRow(pkEnd.copy().array());
        return scan;
    }

    private List<Pair<byte[], byte[]>> makeHBaseColumns(BitSet selectedColBlocks) {
        List<Pair<byte[], byte[]>> result = Lists.newArrayList();

        int colBlockIdx = 0;
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
