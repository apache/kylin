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

import java.text.MessageFormat;
import java.util.*;

import com.google.common.collect.Lists;
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
import org.apache.kylin.common.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.kylin.common.persistence.StorageException;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowValueDecoder;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hbase.coprocessor.observer.ObserverEnabler;
import org.apache.kylin.storage.tuple.Tuple;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Range;

/**
 * @author xjiang
 *
 */
public class CubeSegmentTupleIterator implements ITupleIterator {

    public static final Logger logger = LoggerFactory.getLogger(CubeSegmentTupleIterator.class);

    public static final int SCAN_CACHE = 1024;

    private final CubeSegment cubeSeg;
    private final TupleFilter filter;
    private final Collection<TblColRef> groupBy;
    private final Collection<RowValueDecoder> rowValueDecoders;
    private final StorageContext context;
    private final String tableName;
    private final HTableInterface table;

    private final CubeTupleConverter tupleConverter;
    private final Iterator<HBaseKeyRange> rangeIterator;
    private final Tuple oneTuple; // avoid new instance

    private Scan scan;
    private ResultScanner scanner;
    private Iterator<Result> resultIterator;
    private int scanCount;
    private Tuple next;

    public CubeSegmentTupleIterator(CubeSegment cubeSeg, List<HBaseKeyRange> keyRanges, HConnection conn, //
            Set<TblColRef> dimensions, TupleFilter filter, Set<TblColRef> groupBy, //
            List<RowValueDecoder> rowValueDecoders, StorageContext context, TupleInfo returnTupleInfo) {
        this.cubeSeg = cubeSeg;
        this.filter = filter;
        this.groupBy = groupBy;
        this.rowValueDecoders = rowValueDecoders;
        this.context = context;
        this.tableName = cubeSeg.getStorageLocationIdentifier();

        Cuboid cuboid = keyRanges.get(0).getCuboid();
        for (HBaseKeyRange range : keyRanges) {
            assert cuboid.equals(range.getCuboid());
        }

        this.tupleConverter = new CubeTupleConverter(cubeSeg, cuboid, rowValueDecoders, returnTupleInfo);
        this.oneTuple = new Tuple(returnTupleInfo);
        this.rangeIterator = keyRanges.iterator();
        this.scanCount = 0;

        try {
            this.table = conn.getTable(tableName);
        } catch (Throwable t) {
            throw new StorageException("Error when open connection to table " + tableName, t);
        }
    }

    @Override
    public Range<Long> getCacheExcludedPeriod() {
        return null;
    }

    @Override
    public boolean hasNext() {
        if (next != null)
            return true;

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
        tupleConverter.translateResult(result, oneTuple);
        next = oneTuple;
        return true;
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
        List<org.apache.kylin.common.util.Pair<byte[], byte[]>> fuzzyKeys = keyRange.getFuzzyKeys();
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
        for(org.apache.kylin.common.util.Pair pair : pairList) {
            org.apache.hadoop.hbase.util.Pair element = new org.apache.hadoop.hbase.util.Pair(pair.getFirst(), pair.getSecond());
            result.add(element);
        }

        return result;
    }

    private void closeScanner() {
        if (logger.isDebugEnabled() && scan != null) {
            logger.debug("Scan " + scan.toString());
            byte[] metricsBytes = scan.getAttribute(Scan.SCAN_ATTRIBUTES_METRICS_DATA);
            if (metricsBytes != null) {
                ScanMetrics scanMetrics = ProtobufUtil.toScanMetrics(metricsBytes);
                logger.debug("HBase Metrics: " + "count={}, ms={}, bytes={}, remote_bytes={}, regions={}, not_serving_region={}, rpc={}, rpc_retries={}, remote_rpc={}, remote_rpc_retries={}", //
                        new Object[] { scanCount, scanMetrics.sumOfMillisSecBetweenNexts, scanMetrics.countOfBytesInResults, scanMetrics.countOfBytesInRemoteResults, scanMetrics.countOfRegions, scanMetrics.countOfNSRE, scanMetrics.countOfRPCcalls, scanMetrics.countOfRPCRetries, scanMetrics.countOfRemoteRPCcalls, scanMetrics.countOfRemoteRPCRetries });
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
    public void close() {
        logger.info("Closing CubeSegmentTupleIterator");
        closeScanner();
        closeTable();
    }

}
