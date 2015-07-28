package org.apache.kylin.storage.cube;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public abstract class CubeHBaseRPC {

    public static final Logger logger = LoggerFactory.getLogger(CubeHBaseRPC.class);

    public static final int SCAN_CACHE = 1024;

    final protected CubeSegment cubeSeg;
    final protected Cuboid cuboid;
    final protected GTInfo fullGTInfo;

    public CubeHBaseRPC(CubeSegment cubeSeg, Cuboid cuboid,GTInfo fullGTInfo) {
        this.cubeSeg = cubeSeg;
        this.cuboid = cuboid;
        this.fullGTInfo = fullGTInfo;
    }

    abstract IGTScanner getGTScanner(GTScanRequest scanRequest) throws IOException;

    public static Scan buildScan(RawScan rawScan) {
        Scan scan = new Scan();
        scan.setCaching(SCAN_CACHE);
        scan.setCacheBlocks(true);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(Boolean.TRUE));

        if (rawScan.startKey != null) {
            scan.setStartRow(rawScan.startKey);
        }
        if (rawScan.endKey != null) {
            scan.setStopRow(rawScan.endKey);
        }
        if (rawScan.fuzzyKey != null) {
            applyFuzzyFilter(scan, rawScan.fuzzyKey);
        }
        if (rawScan.hbaseColumns != null) {
            applyHBaseColums(scan, rawScan.hbaseColumns);
        }

        return scan;
    }

    protected RawScan prepareRawScan(GTRecord pkStart, GTRecord pkEnd, List<Pair<byte[], byte[]>> selectedColumns) {
        byte[] start = makeRowKeyToScan(pkStart, (byte) 0x00);
        byte[] end = makeRowKeyToScan(pkEnd, (byte) 0xff);

        //TODO fuzzy match

        return new RawScan(start, end, selectedColumns, null);
    }

    private byte[] makeRowKeyToScan(GTRecord pkRec, byte fill) {
        ByteArray pk = GTRecord.exportScanKey(pkRec);
        int pkMaxLen = pkRec.getInfo().getMaxColumnLength(pkRec.getInfo().getPrimaryKey());

        byte[] buf = new byte[pkMaxLen + RowConstants.ROWKEY_CUBOIDID_LEN];
        Arrays.fill(buf, fill);

        System.arraycopy(cuboid.getBytes(), 0, buf, 0, RowConstants.ROWKEY_CUBOIDID_LEN);
        if (pk != null && pk.array() != null) {
            System.arraycopy(pk.array(), pk.offset(), buf, RowConstants.ROWKEY_CUBOIDID_LEN, pk.length());
        }
        return buf;
    }

    protected List<Pair<byte[], byte[]>> makeHBaseColumns(ImmutableBitSet selectedColBlocks) {
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

    //possible to use binary search as cells might be sorted
    public static Cell findCell(List<Cell> cells, byte[] familyName, byte[] columnName) {
        for (Cell c : cells) {
            if (BytesUtil.compareBytes(familyName, 0, c.getFamilyArray(), c.getFamilyOffset(), familyName.length) == 0 && //
                    BytesUtil.compareBytes(columnName, 0, c.getQualifierArray(), c.getQualifierOffset(), columnName.length) == 0) {
                return c;
            }
        }
        return null;
    }

    public static void applyHBaseColums(Scan scan, List<Pair<byte[], byte[]>> hbaseColumns) {
        for (Pair<byte[], byte[]> hbaseColumn : hbaseColumns) {
            byte[] byteFamily = hbaseColumn.getFirst();
            byte[] byteQualifier = hbaseColumn.getSecond();
            scan.addColumn(byteFamily, byteQualifier);
        }
    }

    public static void applyFuzzyFilter(Scan scan, List<org.apache.kylin.common.util.Pair<byte[], byte[]>> fuzzyKeys) {
        if (fuzzyKeys != null && fuzzyKeys.size() > 0) {
            FuzzyRowFilter rowFilter = new FuzzyRowFilter(convertToHBasePair(fuzzyKeys));

            Filter filter = scan.getFilter();
            if (filter != null) {
                // may have existed InclusiveStopFilter, see buildScan
                FilterList filterList = new FilterList();
                filterList.addFilter(filter);
                filterList.addFilter(rowFilter);
                scan.setFilter(filterList);
            } else {
                scan.setFilter(rowFilter);
            }
        }
    }

    private static List<org.apache.hadoop.hbase.util.Pair<byte[], byte[]>> convertToHBasePair(List<org.apache.kylin.common.util.Pair<byte[], byte[]>> pairList) {
        List<org.apache.hadoop.hbase.util.Pair<byte[], byte[]>> result = Lists.newArrayList();
        for (org.apache.kylin.common.util.Pair pair : pairList) {
            org.apache.hadoop.hbase.util.Pair element = new org.apache.hadoop.hbase.util.Pair(pair.getFirst(), pair.getSecond());
            result.add(element);
        }

        return result;
    }

}
