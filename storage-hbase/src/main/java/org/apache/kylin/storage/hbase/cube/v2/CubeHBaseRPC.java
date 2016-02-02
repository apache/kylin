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

package org.apache.kylin.storage.hbase.cube.v2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.FuzzyKeyEncoder;
import org.apache.kylin.cube.kv.FuzzyMaskEncoder;
import org.apache.kylin.cube.kv.LazyRowKeyEncoder;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public abstract class CubeHBaseRPC {

    public static final Logger logger = LoggerFactory.getLogger(CubeHBaseRPC.class);

    final protected CubeSegment cubeSeg;
    final protected Cuboid cuboid;
    final protected GTInfo fullGTInfo;
    final private RowKeyEncoder fuzzyKeyEncoder;
    final private RowKeyEncoder fuzzyMaskEncoder;

    public CubeHBaseRPC(CubeSegment cubeSeg, Cuboid cuboid, GTInfo fullGTInfo) {
        this.cubeSeg = cubeSeg;
        this.cuboid = cuboid;
        this.fullGTInfo = fullGTInfo;

        this.fuzzyKeyEncoder = new FuzzyKeyEncoder(cubeSeg, cuboid);
        this.fuzzyMaskEncoder = new FuzzyMaskEncoder(cubeSeg, cuboid);
    }

    abstract IGTScanner getGTScanner(final List<GTScanRequest> scanRequests) throws IOException;

    public static Scan buildScan(RawScan rawScan) {
        Scan scan = new Scan();
        scan.setCaching(rawScan.hbaseCaching);
        scan.setMaxResultSize(rawScan.hbaseMaxResultSize);
        scan.setCacheBlocks(true);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(Boolean.TRUE));

        if (rawScan.startKey != null) {
            scan.setStartRow(rawScan.startKey);
        }
        if (rawScan.endKey != null) {
            scan.setStopRow(rawScan.endKey);
        }
        if (rawScan.fuzzyKeys != null) {
            applyFuzzyFilter(scan, rawScan.fuzzyKeys);
        }
        if (rawScan.hbaseColumns != null) {
            applyHBaseColums(scan, rawScan.hbaseColumns);
        }

        return scan;
    }

    protected RawScan preparedHBaseScan(GTRecord pkStart, GTRecord pkEnd, List<GTRecord> fuzzyKeys, ImmutableBitSet selectedColBlocks) {
        final List<Pair<byte[], byte[]>> selectedColumns = makeHBaseColumns(selectedColBlocks);

        LazyRowKeyEncoder encoder = new LazyRowKeyEncoder(cubeSeg, cuboid);
        byte[] start = encoder.createBuf();
        byte[] end = encoder.createBuf();

        encoder.setBlankByte(RowConstants.ROWKEY_LOWER_BYTE);
        encoder.encode(pkStart, pkStart.getInfo().getPrimaryKey(), start);

        encoder.setBlankByte(RowConstants.ROWKEY_UPPER_BYTE);
        encoder.encode(pkEnd, pkEnd.getInfo().getPrimaryKey(), end);
        byte[] temp = new byte[end.length + 1];//append extra 0 to the end key to make it inclusive while scanning
        System.arraycopy(end, 0, temp, 0, end.length);
        end = temp;

        List<Pair<byte[], byte[]>> hbaseFuzzyKeys = translateFuzzyKeys(fuzzyKeys);

        KylinConfig config = cubeSeg.getCubeDesc().getConfig();
        int hbaseCaching = config.getHBaseScanCacheRows();
        int hbaseMaxResultSize = config.getHBaseScanMaxResultSize();
        if (isMemoryHungry(selectedColBlocks))
            hbaseCaching /= 10;

        return new RawScan(start, end, selectedColumns, hbaseFuzzyKeys, hbaseCaching, hbaseMaxResultSize);
    }

    protected List<RawScan> preparedHBaseScans(GTRecord pkStart, GTRecord pkEnd, List<GTRecord> fuzzyKeys, ImmutableBitSet selectedColBlocks) {
        final List<Pair<byte[], byte[]>> selectedColumns = makeHBaseColumns(selectedColBlocks);
        List<RawScan> ret = Lists.newArrayList();

        LazyRowKeyEncoder encoder = new LazyRowKeyEncoder(cubeSeg, cuboid);
        byte[] start = encoder.createBuf();
        byte[] end = encoder.createBuf();
        List<byte[]> startKeys;
        List<byte[]> endKeys;

        encoder.setBlankByte(RowConstants.ROWKEY_LOWER_BYTE);
        encoder.encode(pkStart, pkStart.getInfo().getPrimaryKey(), start);
        startKeys = encoder.getRowKeysDifferentShards(start);

        encoder.setBlankByte(RowConstants.ROWKEY_UPPER_BYTE);
        encoder.encode(pkEnd, pkEnd.getInfo().getPrimaryKey(), end);
        endKeys = encoder.getRowKeysDifferentShards(end);
        endKeys = Lists.transform(endKeys, new Function<byte[], byte[]>() {
            @Override
            public byte[] apply(byte[] input) {
                byte[] shardEnd = new byte[input.length + 1];//append extra 0 to the end key to make it inclusive while scanning
                System.arraycopy(input, 0, shardEnd, 0, input.length);
                return shardEnd;
            }
        });

        Preconditions.checkState(startKeys.size() == endKeys.size());
        List<Pair<byte[], byte[]>> hbaseFuzzyKeys = translateFuzzyKeys(fuzzyKeys);

        KylinConfig config = cubeSeg.getCubeDesc().getConfig();
        int hbaseCaching = config.getHBaseScanCacheRows();
        int hbaseMaxResultSize = config.getHBaseScanMaxResultSize();
        if (isMemoryHungry(selectedColBlocks))
            hbaseCaching /= 10;

        for (short i = 0; i < startKeys.size(); ++i) {
            ret.add(new RawScan(startKeys.get(i), endKeys.get(i), selectedColumns, hbaseFuzzyKeys, hbaseCaching, hbaseMaxResultSize));
        }
        return ret;

    }

    private boolean isMemoryHungry(ImmutableBitSet selectedColBlocks) {
        ImmutableBitSet selectColumns = fullGTInfo.selectColumns(selectedColBlocks);
        return fullGTInfo.getMaxColumnLength(selectColumns) > 1024;
    }

    /**
     * translate GTRecord format fuzzy keys to hbase expected format
     * @return
     */
    private List<Pair<byte[], byte[]>> translateFuzzyKeys(List<GTRecord> fuzzyKeys) {
        if (fuzzyKeys == null || fuzzyKeys.isEmpty()) {
            return Collections.emptyList();
        }

        List<Pair<byte[], byte[]>> ret = Lists.newArrayList();
        for (GTRecord gtRecordFuzzyKey : fuzzyKeys) {
            byte[] hbaseFuzzyKey = fuzzyKeyEncoder.createBuf();
            byte[] hbaseFuzzyMask = fuzzyMaskEncoder.createBuf();

            fuzzyKeyEncoder.encode(gtRecordFuzzyKey, gtRecordFuzzyKey.getInfo().getPrimaryKey(), hbaseFuzzyKey);
            fuzzyMaskEncoder.encode(gtRecordFuzzyKey, gtRecordFuzzyKey.getInfo().getPrimaryKey(), hbaseFuzzyMask);

            ret.add(new Pair<byte[], byte[]>(hbaseFuzzyKey, hbaseFuzzyMask));
        }

        return ret;
    }

    /**
     * prune untouched hbase columns
     */
    protected List<Pair<byte[], byte[]>> makeHBaseColumns(ImmutableBitSet selectedColBlocks) {
        List<Pair<byte[], byte[]>> result = Lists.newArrayList();

        int colBlkIndex = 1;
        HBaseMappingDesc hbaseMapping = cubeSeg.getCubeDesc().getHbaseMapping();
        for (HBaseColumnFamilyDesc familyDesc : hbaseMapping.getColumnFamily()) {
            byte[] byteFamily = Bytes.toBytes(familyDesc.getName());
            for (HBaseColumnDesc hbaseColDesc : familyDesc.getColumns()) {
                if (selectedColBlocks.get(colBlkIndex)) {
                    byte[] byteQualifier = Bytes.toBytes(hbaseColDesc.getQualifier());
                    result.add(new Pair<byte[], byte[]>(byteFamily, byteQualifier));
                }
                colBlkIndex++;
            }
        }

        return result;
    }

    /**
     * for each selected hbase column, it might contain values of multiple GT columns.
     * The mapping should be passed down to storage
     */
    protected List<List<Integer>> getHBaseColumnsGTMapping(ImmutableBitSet selectedColBlocks) {

        List<List<Integer>> ret = Lists.newArrayList();

        int colBlkIndex = 1;
        int metricOffset = fullGTInfo.getPrimaryKey().trueBitCount();

        HBaseMappingDesc hbaseMapping = cubeSeg.getCubeDesc().getHbaseMapping();
        for (HBaseColumnFamilyDesc familyDesc : hbaseMapping.getColumnFamily()) {
            for (HBaseColumnDesc hbaseColDesc : familyDesc.getColumns()) {
                if (selectedColBlocks.get(colBlkIndex)) {
                    int[] metricIndexes = hbaseColDesc.getMeasureIndex();
                    Integer[] gtIndexes = new Integer[metricIndexes.length];
                    for (int i = 0; i < gtIndexes.length; i++) {
                        gtIndexes[i] = metricIndexes[i] + metricOffset;
                    }
                    ret.add(Arrays.asList(gtIndexes));
                }
                colBlkIndex++;
            }
        }

        Preconditions.checkState(selectedColBlocks.trueBitCount() == ret.size() + 1);
        return ret;
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
        for (org.apache.kylin.common.util.Pair<byte[], byte[]> pair : pairList) {
            org.apache.hadoop.hbase.util.Pair<byte[], byte[]> element = new org.apache.hadoop.hbase.util.Pair<byte[], byte[]>(pair.getFirst(), pair.getSecond());
            result.add(element);
        }

        return result;
    }

    protected void logScan(RawScan rawScan, String tableName) {
        StringBuilder info = new StringBuilder();
        info.append("Visiting hbase table ").append(tableName).append(": ");
        if (cuboid.requirePostAggregation()) {
            info.append("cuboid require post aggregation, from ");
        } else {
            info.append("cuboid exact match, from ");
        }
        info.append(cuboid.getInputID());
        info.append(" to ");
        info.append(cuboid.getId());
        info.append("\nStart: ");
        info.append(rawScan.getStartKeyAsString());
        info.append(" (");
        info.append(Bytes.toStringBinary(rawScan.startKey) + ")");
        info.append("\nStop:  ");
        info.append(rawScan.getEndKeyAsString());
        info.append(" (");
        info.append(Bytes.toStringBinary(rawScan.endKey) + ")");
        if (rawScan.fuzzyKeys != null && rawScan.fuzzyKeys.size() != 0) {
            info.append("\nFuzzy key counts: " + rawScan.fuzzyKeys.size());
            info.append(". Fuzzy keys : ");
            info.append(rawScan.getFuzzyKeyAsString());
        } else {
            info.append("\nNo Fuzzy Key");
        }
        logger.info(info.toString());
    }

}
