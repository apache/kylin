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

package org.apache.kylin.storage.hbase.cube.v1;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.StorageException;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hbase.cube.v1.coprocessor.observer.ObserverEnabler;
import org.apache.kylin.storage.hbase.cube.v1.filter.FuzzyRowFilterV2;
import org.apache.kylin.storage.hbase.steps.RowValueDecoder;
import org.apache.kylin.storage.translate.HBaseKeyRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * @author xjiang
 *
 */
public class CubeSegmentTupleIterator implements ITupleIterator {

    public static final Logger logger = LoggerFactory.getLogger(CubeSegmentTupleIterator.class);

    protected final CubeSegment cubeSeg;
    private final TupleFilter filter;
    private final Collection<TblColRef> groupBy;
    protected final List<RowValueDecoder> rowValueDecoders;
    private final StorageContext context;
    private final String tableName;
    private final Table table;

    protected CubeTupleConverter tupleConverter;
    protected final Iterator<HBaseKeyRange> rangeIterator;
    protected final Tuple oneTuple; // avoid new instance

    private Scan scan;
    private ResultScanner scanner;
    protected Iterator<Result> resultIterator;
    protected int scanCount;
    protected int scanCountDelta;
    protected Tuple next;
    protected final Cuboid cuboid;

    private List<MeasureType.IAdvMeasureFiller> advMeasureFillers;
    private int advMeasureRowsRemaining;
    private int advMeasureRowIndex;

    public CubeSegmentTupleIterator(CubeSegment cubeSeg, List<HBaseKeyRange> keyRanges, Connection conn, //
            Set<TblColRef> dimensions, TupleFilter filter, Set<TblColRef> groupBy, //
            List<RowValueDecoder> rowValueDecoders, StorageContext context, TupleInfo returnTupleInfo) {
        this.cubeSeg = cubeSeg;
        this.filter = filter;
        this.groupBy = groupBy;
        this.rowValueDecoders = rowValueDecoders;
        this.context = context;
        this.tableName = cubeSeg.getStorageLocationIdentifier();

        cuboid = keyRanges.get(0).getCuboid();
        for (HBaseKeyRange range : keyRanges) {
            assert cuboid.equals(range.getCuboid());
        }

        this.tupleConverter = new CubeTupleConverter(cubeSeg, cuboid, rowValueDecoders, returnTupleInfo);
        this.oneTuple = new Tuple(returnTupleInfo);
        this.rangeIterator = keyRanges.iterator();

        try {
            this.table = conn.getTable(TableName.valueOf(tableName));
        } catch (Throwable t) {
            throw new StorageException("Error when open connection to table " + tableName, t);
        }
    }

    @Override
    public boolean hasNext() {

        if (next != null)
            return true;

        // consume any left rows from advanced measure filler
        if (advMeasureRowsRemaining > 0) {
            for (MeasureType.IAdvMeasureFiller filler : advMeasureFillers) {
                filler.fillTuple(oneTuple, advMeasureRowIndex);
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
        advMeasureFillers = tupleConverter.translateResult(result, oneTuple);

        // the simple case
        if (advMeasureFillers == null) {
            next = oneTuple;
            return true;
        }

        // advanced measure filling, like TopN, will produce multiple tuples out of one record
        advMeasureRowsRemaining = -1;
        for (MeasureType.IAdvMeasureFiller filler : advMeasureFillers) {
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

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    protected final Iterator<Result> doScan(HBaseKeyRange keyRange) {
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
        info.append("Scan hbase table ").append(tableName).append(": ");
        if (keyRange.getCuboid().requirePostAggregation()) {
            info.append(" cuboid require post aggregation, from ");
        } else {
            info.append(" cuboid exact match, from ");
        }
        info.append(keyRange.getCuboid().getInputID());
        info.append(" to ");
        info.append(keyRange.getCuboid().getId());
        info.append(" Start: ");
        info.append(keyRange.getStartKeyAsString());
        info.append(" - ");
        info.append(Bytes.toStringBinary(keyRange.getStartKey()));
        info.append(" Stop:  ");
        info.append(keyRange.getStopKeyAsString());
        info.append(" - ");
        info.append(Bytes.toStringBinary(keyRange.getStopKey()));
        if (this.scan.getFilter() != null) {
            info.append(" Fuzzy key counts: " + keyRange.getFuzzyKeys().size());
            info.append(" Fuzzy: ");
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
        KylinConfig config = cubeSeg.getCubeDesc().getConfig();

        scan.setCaching(config.getHBaseScanCacheRows());
        scan.setMaxResultSize(config.getHBaseScanMaxResultSize());
        scan.setCacheBlocks(true);

        // cache less when there are memory hungry measures
        //        if (RowValueDecoder.hasMemHungryMeasures(rowValueDecoders)) {
        //            scan.setCaching(scan.getCaching() / 10);
        //        }
    }

    private void applyFuzzyFilter(Scan scan, HBaseKeyRange keyRange) {
        List<org.apache.kylin.common.util.Pair<byte[], byte[]>> fuzzyKeys = keyRange.getFuzzyKeys();
        if (fuzzyKeys != null && fuzzyKeys.size() > 0) {

            //FuzzyRowFilterV2 is a back ported from https://issues.apache.org/jira/browse/HBASE-13761
            //However we found a bug of it and fixed it in https://issues.apache.org/jira/browse/HBASE-14269
            //After fix the performance is not much faster than the original one. So by default use defalt one.
            boolean useFuzzyRowFilterV2 = false;
            Filter fuzzyFilter = null;
            if (useFuzzyRowFilterV2) {
                fuzzyFilter = new FuzzyRowFilterV2(convertToHBasePair(fuzzyKeys));
            } else {
                fuzzyFilter = new FuzzyRowFilter(convertToHBasePair(fuzzyKeys));
            }

            Filter filter = scan.getFilter();
            if (filter != null) {
                throw new RuntimeException("Scan filter not empty : " + filter);
            } else {
                scan.setFilter(fuzzyFilter);
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

    protected void closeScanner() {
        flushScanCountDelta();

        if (logger.isDebugEnabled() && scan != null) {
            byte[] metricsBytes = scan.getAttribute(Scan.SCAN_ATTRIBUTES_METRICS_DATA);
            if (metricsBytes != null) {
                ScanMetrics scanMetrics = ProtobufUtil.toScanMetrics(metricsBytes);
                logger.debug("HBase Metrics when scanning " + this.tableName + " count={}, ms={}, bytes={}, remote_bytes={}, regions={}, not_serving_region={}, rpc={}, rpc_retries={}, remote_rpc={}, remote_rpc_retries={}", //
                        new Object[] { scanCount, scanMetrics.sumOfMillisSecBetweenNexts, scanMetrics.countOfBytesInResults, scanMetrics.countOfBytesInRemoteResults, scanMetrics.countOfRegions, scanMetrics.countOfNSRE, scanMetrics.countOfRPCcalls, scanMetrics.countOfRPCRetries, scanMetrics.countOfRemoteRPCcalls, scanMetrics.countOfRemoteRPCRetries });
            }
            scan = null;
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
    public void close() {
        logger.info("Closing CubeSegmentTupleIterator");
        closeScanner();
        closeTable();
    }

    protected void flushScanCountDelta() {
        context.increaseTotalScanCount(scanCountDelta);
        scanCountDelta = 0;
    }

}
