/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.storage.hbase;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.persistence.StorageException;
import com.kylinolap.common.util.Array;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.cube.kv.RowKeyDecoder;
import com.kylinolap.cube.kv.RowValueDecoder;
import com.kylinolap.metadata.model.cube.CubeDesc.DeriveInfo;
import com.kylinolap.metadata.model.cube.HBaseColumnDesc;
import com.kylinolap.metadata.model.cube.MeasureDesc;
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.storage.StorageContext;
import com.kylinolap.storage.filter.TupleFilter;
import com.kylinolap.storage.hbase.observer.CoprocessorEnabler;
import com.kylinolap.storage.tuple.ITupleIterator;
import com.kylinolap.storage.tuple.Tuple;
import com.kylinolap.storage.tuple.Tuple.IDerivedColumnFiller;
import com.kylinolap.storage.tuple.TupleInfo;

/**
 * @author xjiang
 * 
 */
public class CubeSegmentTupleIterator implements ITupleIterator {

    public static final Logger logger = LoggerFactory.getLogger(CubeSegmentTupleIterator.class);

    public static final int SCAN_CACHE = 1024;

    public static final ITupleIterator EMPTY_TUPLE_ITERATOR = new ITupleIterator() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Tuple next() {
            return null;
        }

        @Override
        public void close() {
        }
    };

    private final CubeInstance cube;
    private final CubeSegment cubeSeg;
    private final Collection<TblColRef> dimensions;
    private final TupleFilter filter;
    private final Collection<TblColRef> groupBy;
    private final Collection<RowValueDecoder> rowValueDecoders;
    private final StorageContext context;
    private final String tableName;
    private final HTableInterface table;
    private final RowKeyDecoder rowKeyDecoder;
    private final Iterator<HBaseKeyRange> rangeIterator;

    private Scan scan;
    private ResultScanner scanner;
    private Iterator<Result> resultIterator;
    private TupleInfo tupleInfo;
    private Tuple tuple;
    private int scanCount;

    public CubeSegmentTupleIterator(CubeSegment cubeSeg, Collection<HBaseKeyRange> keyRanges, HConnection conn, Collection<TblColRef> dimensions, TupleFilter filter, Collection<TblColRef> groupBy, Collection<RowValueDecoder> rowValueDecoders, StorageContext context) {
        this.cube = cubeSeg.getCubeInstance();
        this.cubeSeg = cubeSeg;
        this.dimensions = dimensions;
        this.filter = filter;
        this.groupBy = groupBy;
        this.rowValueDecoders = rowValueDecoders;
        this.context = context;
        this.tableName = cubeSeg.getStorageLocationIdentifier();
        this.rowKeyDecoder = new RowKeyDecoder(this.cubeSeg);
        this.scanCount = 0;

        try {
            this.table = conn.getTable(tableName);
        } catch (Throwable t) {
            throw new StorageException("Error when open connection to table " + tableName, t);
        }
        this.rangeIterator = keyRanges.iterator();
        scanNextRange();
    }

    @Override
    public void close() {
        closeScanner();
        closeTable();
    }

    private void closeScanner() {
        if (logger.isDebugEnabled() && scan != null) {
            logger.debug("Scan " + scan.toString());
            byte[] metricsBytes = scan.getAttribute(Scan.SCAN_ATTRIBUTES_METRICS_DATA);
            if (metricsBytes != null) {
                ScanMetrics scanMetrics = ProtobufUtil.toScanMetrics(metricsBytes);
                logger.debug("HBase Metrics: " + "count={}, ms={}, bytes={}, remote_bytes={}, regions={}, not_serving_region={}, rpc={}, rpc_retries={}, remote_rpc={}, remote_rpc_retries={}", new Object[] { scanCount, scanMetrics.sumOfMillisSecBetweenNexts, scanMetrics.countOfBytesInResults, scanMetrics.countOfBytesInRemoteResults, scanMetrics.countOfRegions, scanMetrics.countOfNSRE, scanMetrics.countOfRPCcalls, scanMetrics.countOfRPCRetries, scanMetrics.countOfRemoteRPCcalls, scanMetrics.countOfRemoteRPCRetries });
            }
        }
        try {
            if (scanner != null) {
                scanner.close();
                scanner = null;
            }
        } catch (Throwable t) {
            throw new StorageException("Error when close scanner for table " + tableName, t);
        }
    }

    private void closeTable() {
        try {
            if (table != null) {
                table.close();
            }
        } catch (Throwable t) {
            throw new StorageException("Error when close table " + tableName, t);
        }
    }

    @Override
    public boolean hasNext() {
        return rangeIterator.hasNext() || resultIterator.hasNext();
    }

    @Override
    public Tuple next() {
        // get next result from hbase
        Result result = null;
        while (hasNext()) {
            if (resultIterator.hasNext()) {
                result = this.resultIterator.next();
                scanCount++;
                break;
            } else {
                scanNextRange();
            }
        }
        if (result == null) {
            return null;
        }
        // translate result to tuple
        try {
            translateResult(result, this.tuple);
        } catch (IOException e) {
            throw new IllegalStateException("Can't translate result " + result, e);
        }
        return this.tuple;
    }

    private void scanNextRange() {
        if (this.rangeIterator.hasNext()) {
            closeScanner();
            HBaseKeyRange keyRange = this.rangeIterator.next();
            this.tupleInfo = buildTupleInfo(keyRange.getCuboid());
            this.tuple = new Tuple(this.tupleInfo);

            this.resultIterator = doScan(keyRange);
        } else {
            this.resultIterator = Collections.<Result> emptyList().iterator();
        }
    }

    private final Iterator<Result> doScan(HBaseKeyRange keyRange) {

        Iterator<Result> iter = null;
        try {
            scan = buildScan(keyRange);
            applyFuzzyFilter(scan, keyRange);
            logScan(keyRange);

            scanner = CoprocessorEnabler.scanWithCoprocessorIfBeneficial(cubeSeg, keyRange.getCuboid(), filter, groupBy, rowValueDecoders, context, table, scan);

            iter = scanner.iterator();
        } catch (Throwable t) {
            String msg = MessageFormat.format("Error when scan from lower key {1} to upper key {2} on table {0}.", tableName, Bytes.toString(keyRange.getStartKey()), Bytes.toString(keyRange.getStopKey()));
            throw new StorageException(msg, t);
        }
        return iter;
    }

    private void logScan(HBaseKeyRange keyRange) {
        StringBuilder info = new StringBuilder();
        info.append("\nScan hbase table ").append(tableName).append(": ");
        if (keyRange.getCuboid().requirePostAggregation()) {
            info.append("cuboid require post aggregation, from ");
        } else {
            info.append("cuboid exact match, from ");
        }
        info.append(keyRange.getCuboid().getInputID());
        info.append(" to ");
        info.append(keyRange.getCuboid().getId());
        info.append("\nStart: ");
        info.append(keyRange.getStartKeyAsString());
        info.append("     - ");
        info.append(Bytes.toStringBinary(keyRange.getStartKey()));
        info.append("\nStop:  ");
        info.append(keyRange.getStopKeyAsString());
        info.append(" - ");
        info.append(Bytes.toStringBinary(keyRange.getStopKey()));
        if (this.scan.getFilter() != null) {
            info.append("\nFuzzy: ");
            info.append(keyRange.getFuzzyKeyAsString());
        }
        logger.info(info.toString());
    }

    private Scan buildScan(HBaseKeyRange keyRange) {
        Scan scan = new Scan();
        scan.setCaching(SCAN_CACHE);
        scan.setCacheBlocks(true);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(Boolean.TRUE));
        for (RowValueDecoder valueDecoder : this.rowValueDecoders) {
            HBaseColumnDesc hbaseColumn = valueDecoder.getHBaseColumn();
            byte[] byteFamily = Bytes.toBytes(hbaseColumn.getColumnFamilyName());
            byte[] byteQualifier = Bytes.toBytes(hbaseColumn.getQualifier());
            scan.addColumn(byteFamily, byteQualifier);
        }
        scan.setStartRow(keyRange.getStartKey());
        scan.setStopRow(keyRange.getStopKey());
        return scan;
    }

    private void applyFuzzyFilter(Scan scan, HBaseKeyRange keyRange) {
        List<Pair<byte[], byte[]>> fuzzyKeys = keyRange.getFuzzyKeys();
        if (fuzzyKeys != null && fuzzyKeys.size() > 0) {
            FuzzyRowFilter rowFilter = new FuzzyRowFilter(fuzzyKeys);

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

    private TupleInfo buildTupleInfo(Cuboid cuboid) {
        TupleInfo info = new TupleInfo();
        int index = 0;
        rowKeyDecoder.setCuboid(cuboid);
        List<TblColRef> rowColumns = rowKeyDecoder.getColumns();
        List<String> colNames = rowKeyDecoder.getNames(context.getAliasMap());
        for (int i = 0; i < rowColumns.size(); i++) {
            TblColRef column = rowColumns.get(i);
            if (!dimensions.contains(column)) {
                continue;
            }
            // add normal column
            info.setField(colNames.get(i), rowColumns.get(i), rowColumns.get(i).getType().getName(), index++);
        }

        // derived columns and filler
        Map<Array<TblColRef>, List<DeriveInfo>> hostToDerivedInfo = cubeSeg.getCubeDesc().getHostToDerivedInfo(rowColumns, null);
        for (Entry<Array<TblColRef>, List<DeriveInfo>> entry : hostToDerivedInfo.entrySet()) {
            TblColRef[] hostCols = entry.getKey().data;
            for (DeriveInfo deriveInfo : entry.getValue()) {
                // mark name for each derived field
                for (TblColRef derivedCol : deriveInfo.columns) {
                    String derivedField = getFieldName(derivedCol, context.getAliasMap());
                    info.setField(derivedField, derivedCol, derivedCol.getType().getName(), index++);
                }
                // add filler
                info.addDerivedColumnFiller(Tuple.newDerivedColumnFiller(rowColumns, hostCols, deriveInfo, info, CubeManager.getInstance(this.cube.getConfig()), cubeSeg));
            }
        }

        for (RowValueDecoder rowValueDecoder : this.rowValueDecoders) {
            List<String> names = rowValueDecoder.getNames();
            MeasureDesc[] measures = rowValueDecoder.getMeasures();
            for (int i = 0; i < measures.length; i++) {
                String dataType = measures[i].getFunction().getSQLType();
                info.setField(names.get(i), null, dataType, index++);
            }
        }
        return info;
    }

    private String getFieldName(TblColRef column, Map<TblColRef, String> aliasMap) {
        String name = null;
        if (aliasMap != null) {
            name = aliasMap.get(column);
        }
        if (name == null) {
            name = column.getName();
        }
        return name;
    }

    private void translateResult(Result res, Tuple tuple) throws IOException {
        // groups
        byte[] rowkey = res.getRow();
        rowKeyDecoder.decode(rowkey);
        List<TblColRef> columns = rowKeyDecoder.getColumns();
        List<String> dimensionNames = rowKeyDecoder.getNames(context.getAliasMap());
        List<String> dimensionValues = rowKeyDecoder.getValues();
        for (int i = 0; i < dimensionNames.size(); i++) {
            TblColRef column = columns.get(i);
            if (!tuple.hasColumn(column)) {
                continue;
            }
            tuple.setDimensionValue(dimensionNames.get(i), dimensionValues.get(i));
        }

        // derived
        for (IDerivedColumnFiller filler : tupleInfo.getDerivedColumnFillers()) {
            filler.fillDerivedColumns(dimensionValues, tuple);
        }

        // aggregations
        for (RowValueDecoder rowValueDecoder : this.rowValueDecoders) {
            HBaseColumnDesc hbaseColumn = rowValueDecoder.getHBaseColumn();
            String columnFamily = hbaseColumn.getColumnFamilyName();
            String qualifier = hbaseColumn.getQualifier();
            // FIXME: avoidable bytes array creation, why not use res.getValueAsByteBuffer directly?
            byte[] valueBytes = res.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
            rowValueDecoder.decode(valueBytes);
            List<String> measureNames = rowValueDecoder.getNames();
            Object[] measureValues = rowValueDecoder.getValues();
            BitSet projectionIndex = rowValueDecoder.getProjectionIndex();
            for (int i = projectionIndex.nextSetBit(0); i >= 0; i = projectionIndex.nextSetBit(i + 1)) {
                tuple.setMeasureValue(measureNames.get(i), measureValues[i]);
            }
        }
    }
}
