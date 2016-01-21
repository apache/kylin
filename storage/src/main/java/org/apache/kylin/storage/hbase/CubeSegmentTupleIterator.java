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

package org.apache.kylin.storage.hbase;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

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
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.StorageException;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowKeyDecoder;
import org.apache.kylin.cube.kv.RowValueDecoder;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hbase.coprocessor.observer.ObserverEnabler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * @author xjiang
 * 
 */
public class CubeSegmentTupleIterator implements ITupleIterator {

    public static final Logger logger = LoggerFactory.getLogger(CubeSegmentTupleIterator.class);

    private final CubeInstance cube;
    private final CubeSegment cubeSeg;
    private final Collection<TblColRef> dimensions;
    private final TupleFilter filter;
    private final Collection<TblColRef> groupBy;
    private final List<RowValueDecoder> rowValueDecoders;
    private final StorageContext context;
    private final String tableName;
    private final HTableInterface table;
    private final RowKeyDecoder rowKeyDecoder;
    private final Iterator<HBaseKeyRange> rangeIterator;

    private Scan scan;
    private ResultScanner scanner;
    private Iterator<Result> resultIterator;
    private CubeTupleConverter cubeTupleConverter;
    private TupleInfo tupleInfo;
    private Tuple oneTuple;
    private Tuple next;
    private int scanCount;
    private int scanCountDelta;

    final List<MeasureType<?>> measureTypes;
    final List<MeasureType.IAdvMeasureFiller> advMeasureFillers;
    final List<Pair<Integer, Integer>> advMeasureIndexInRV;//first=> which rowValueDecoders,second => metric index

    private int advMeasureRowsRemaining;
    private int advMeasureRowIndex;

    public CubeSegmentTupleIterator(CubeSegment cubeSeg, List<HBaseKeyRange> keyRanges, HConnection conn, Collection<TblColRef> dimensions, TupleFilter filter, Collection<TblColRef> groupBy, List<RowValueDecoder> rowValueDecoders, StorageContext context) {
        this.cube = cubeSeg.getCubeInstance();
        this.cubeSeg = cubeSeg;
        this.dimensions = dimensions;
        this.filter = filter;
        this.groupBy = groupBy;
        this.rowValueDecoders = rowValueDecoders;
        this.context = context;
        this.tableName = cubeSeg.getStorageLocationIdentifier();
        this.rowKeyDecoder = new RowKeyDecoder(this.cubeSeg);
        
        measureTypes = Lists.newArrayList();
        advMeasureFillers = Lists.newArrayListWithCapacity(1);
        advMeasureIndexInRV = Lists.newArrayListWithCapacity(1);

        this.cubeTupleConverter = new CubeTupleConverter();
        this.tupleInfo = buildTupleInfo(keyRanges.get(0).getCuboid());
        this.oneTuple = new Tuple(this.tupleInfo);
        this.rangeIterator = keyRanges.iterator();


        try {
            this.table = conn.getTable(tableName);
        } catch (Throwable t) {
            throw new StorageException("Error when open connection to table " + tableName, t);
        }
    }

    @Override
    public void close() {
        closeScanner();
        closeTable();
    }

    private void closeScanner() {
        flushScanCountDelta();

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

    private void flushScanCountDelta() {
        context.increaseTotalScanCount(scanCountDelta);
        scanCountDelta = 0;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext() {

        if (next != null)
            return true;

        // consume any left rows from advanced measure filler
        if (advMeasureRowsRemaining > 0) {
            for (MeasureType.IAdvMeasureFiller filler : advMeasureFillers) {
                filler.fillTuplle(oneTuple, advMeasureRowIndex);
            }
            advMeasureRowIndex++;
            advMeasureRowsRemaining--;
            next = oneTuple;
            return true;
        }

        if (resultIterator == null) {
            if (rangeIterator.hasNext() == false)
                return false;

            resultIterator = doScan(rangeIterator.next());
        }

        if (resultIterator.hasNext() == false) {
            closeScanner();
            resultIterator = null;
            return hasNext();
        }

        Result result = resultIterator.next();
        scanCount++;
        if (++scanCountDelta >= 1000)
            flushScanCountDelta();

        // translate into tuple
        List<MeasureType.IAdvMeasureFiller> retFillers = null;
        try {
            retFillers = translateResult(result, oneTuple);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // the simple case
        if (retFillers == null) {
            next = oneTuple;
            return true;
        }

        // advanced measure filling, like TopN, will produce multiple tuples out of one record
        advMeasureRowsRemaining = -1;
        for (MeasureType.IAdvMeasureFiller filler : retFillers) {
            if (advMeasureRowsRemaining < 0)
                advMeasureRowsRemaining = filler.getNumOfRows();
            if (advMeasureRowsRemaining != filler.getNumOfRows())
                throw new IllegalStateException();
        }
        if (advMeasureRowsRemaining < 0)
            throw new IllegalStateException();

        advMeasureRowIndex = 0;
        return hasNext();
    }

    @Override
    public Tuple next() {
        if (next == null) {
            hasNext();
            if (next == null)
                throw new NoSuchElementException();
        }
        Tuple r = next;
        next = null;
        return r;
    }

    private final Iterator<Result> doScan(HBaseKeyRange keyRange) {

        Iterator<Result> iter = null;
        try {
            scan = buildScan(keyRange);
            applyFuzzyFilter(scan, keyRange);
            logScan(keyRange);

            scanner = ObserverEnabler.scanWithCoprocessorIfBeneficial(cubeSeg, keyRange.getCuboid(), filter, groupBy, rowValueDecoders, context, table, scan);

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
        tuneScanParameters(scan);
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

    private void tuneScanParameters(Scan scan) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        
        scan.setCaching(config.getHBaseScanCacheRows());
        scan.setMaxResultSize(config.getHBaseScanMaxResultSize());
        scan.setCacheBlocks(true);

        // cache less when there are memory hungry measures
        if (RowValueDecoder.hasMemHungryMeasures(rowValueDecoders)) {
            scan.setCaching(scan.getCaching() / 10);
        }
    }

    private void applyFuzzyFilter(Scan scan, HBaseKeyRange keyRange) {
        List<Pair<byte[], byte[]>> fuzzyKeys = keyRange.getFuzzyKeys();
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

    private List<org.apache.hadoop.hbase.util.Pair<byte[], byte[]>> convertToHBasePair(List<org.apache.kylin.common.util.Pair<byte[], byte[]>> pairList) {
        List<org.apache.hadoop.hbase.util.Pair<byte[], byte[]>> result = Lists.newArrayList();
        for (org.apache.kylin.common.util.Pair<byte[], byte[]> pair : pairList) {
            org.apache.hadoop.hbase.util.Pair<byte[], byte[]> element = new org.apache.hadoop.hbase.util.Pair<byte[], byte[]>(pair.getFirst(), pair.getSecond());
            result.add(element);
        }

        return result;
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
                cubeTupleConverter.addDerivedColumnFiller(CubeTupleConverter.newDerivedColumnFiller(rowColumns, hostCols, deriveInfo, info, CubeManager.getInstance(this.cube.getConfig()), cubeSeg));
            }
        }

        for (int i = 0; i < rowValueDecoders.size(); i++) {
            RowValueDecoder rowValueDecoder = rowValueDecoders.get(i);
            List<String> measureNames = rowValueDecoder.getNames();
            MeasureDesc[] measures = rowValueDecoder.getMeasures();

            BitSet projectionIndex = rowValueDecoder.getProjectionIndex();
            for (int mi = projectionIndex.nextSetBit(0); mi >= 0; mi = projectionIndex.nextSetBit(mi + 1)) {
                FunctionDesc aggrFunc = measures[mi].getFunction();
                String dataType = measures[mi].getFunction().getRewriteFieldType().getName();
                info.setField(measureNames.get(mi), null, dataType, index++);

                MeasureType<?> measureType = aggrFunc.getMeasureType();
                if (measureType.needAdvancedTupleFilling()) {
                    Map<TblColRef, Dictionary<String>> dictionaryMap = buildDictionaryMap(measureType.getColumnsNeedDictionary(aggrFunc));
                    advMeasureFillers.add(measureType.getAdvancedTupleFiller(aggrFunc, tupleInfo, dictionaryMap));
                    advMeasureIndexInRV.add(Pair.newPair(i, mi));
                    measureTypes.add(null);
                } else {
                    measureTypes.add(measureType);
                }
            }
            //            for (int i = 0; i < measures.length; i++) {
            //                String dataType = measures[i].getFunction().getRewriteFieldType().getName();
            //                info.setField(measureNames.get(i), null, dataType, index++);
            //            }
        }
        return info;
    }

    // load only needed dictionaries
    private Map<TblColRef, Dictionary<String>> buildDictionaryMap(List<TblColRef> columnsNeedDictionary) {
        Map<TblColRef, Dictionary<String>> result = Maps.newHashMap();
        for (TblColRef col : columnsNeedDictionary) {
            result.put(col, cubeSeg.getDictionary(col));
        }
        return result;
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

    private List<MeasureType.IAdvMeasureFiller> translateResult(Result res, Tuple tuple) throws IOException {
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
        for (CubeTupleConverter.IDerivedColumnFiller filler : cubeTupleConverter.getDerivedColumnFillers()) {
            filler.fillDerivedColumns(dimensionValues, tuple);
        }

        // aggregations
        int measureIndex = 0;
        for (RowValueDecoder rowValueDecoder : this.rowValueDecoders) {
            HBaseColumnDesc hbaseColumn = rowValueDecoder.getHBaseColumn();
            String columnFamily = hbaseColumn.getColumnFamilyName();
            String qualifier = hbaseColumn.getQualifier();
            byte[] valueBytes = res.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
            rowValueDecoder.decode(valueBytes);
            List<String> measureNames = rowValueDecoder.getNames();
            Object[] measureValues = rowValueDecoder.getValues();
            BitSet projectionIndex = rowValueDecoder.getProjectionIndex();
            for (int i = projectionIndex.nextSetBit(0); i >= 0; i = projectionIndex.nextSetBit(i + 1)) {
                if (measureTypes.get(measureIndex) != null) {
                    measureTypes.get(measureIndex).fillTupleSimply(tuple, tupleInfo.getFieldIndex(measureNames.get(i)), measureValues[i]);
                }
            }
            measureIndex++;
        }

        // advanced measure filling, due to possible row split, will complete at caller side
        if (advMeasureFillers.isEmpty()) {
            return null;
        } else {
            for (int i = 0; i < advMeasureFillers.size(); i++) {
                Pair<Integer, Integer> metricLocation = advMeasureIndexInRV.get(i);
                Object measureValue = rowValueDecoders.get(metricLocation.getFirst()).getValues()[metricLocation.getSecond()];
                advMeasureFillers.get(i).reload(measureValue);
            }
            return advMeasureFillers;
        }
    }
}
