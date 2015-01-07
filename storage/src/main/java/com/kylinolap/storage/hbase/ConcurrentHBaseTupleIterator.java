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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.StorageException;
import com.kylinolap.common.util.Array;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.cube.kv.RowKeyDecoder;
import com.kylinolap.cube.kv.RowValueDecoder;
import com.kylinolap.metadata.model.cube.CubeDesc;
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
 * @author xduo
 * 
 */
public class ConcurrentHBaseTupleIterator implements ITupleIterator {

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentHBaseTupleIterator.class);
    public static final int SCAN_CACHE = 1024;
    private static final int MAX_QUEUED_RESULTS = 4096;
    private static ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(KylinConfig.getInstanceFromEnv().getConcurrentScanThreadCount()));

    private final HConnection conn;
    private final Map<CubeSegment, Collection<HBaseKeyRange>> segmentKeyRanges;
    private final int limit;
    private final boolean isLimitEnable;
    private final int threshold;
    private final boolean acceptPartialResult;
    private final Collection<TblColRef> dimensions;
    private final TupleFilter filter;
    private final Collection<TblColRef> groupBy;
    private final Map<TblColRef, String> aliasMap;
    private final Collection<RowValueDecoder> rowValueDecoders;
    private final StorageContext context;
    private final CubeInstance cube;

    private AtomicLong scanCounter = new AtomicLong(0);
    private AtomicLong rangesCounter = null;
    private BlockingQueue<Tuple> tupleQueue = new LinkedBlockingQueue<Tuple>(MAX_QUEUED_RESULTS);
    private List<ListenableFuture<Long>> scanFutures = new ArrayList<ListenableFuture<Long>>();
    private List<Throwable> scanExceptions = Collections.synchronizedList(new ArrayList<Throwable>());

    private Tuple next;

    public ConcurrentHBaseTupleIterator(HConnection conn, Map<CubeSegment, Collection<HBaseKeyRange>> segmentKeyRanges, CubeDesc cubeDesc, CubeInstance cube, Collection<TblColRef> dimensions, TupleFilter filter, Collection<TblColRef> groupBy, Collection<RowValueDecoder> rowValueDecoders, StorageContext context) {
        this.conn = conn;
        this.segmentKeyRanges = segmentKeyRanges;
        this.cube = cube;
        this.dimensions = dimensions;
        this.groupBy = groupBy;
        this.aliasMap = context.getAliasMap();
        this.filter = filter;
        this.rowValueDecoders = rowValueDecoders;
        this.limit = context.getLimit();
        this.isLimitEnable = context.isLimitEnabled();
        this.threshold = context.getThreshold();
        this.acceptPartialResult = context.isAcceptPartialResult();
        this.context = context;

        rangesCounter = new AtomicLong(0);
        for (Collection<HBaseKeyRange> ranges : this.segmentKeyRanges.values()) {
            rangesCounter.addAndGet(ranges.size());
        }
        logger.debug("Start to scan " + rangesCounter.get() + " ranges..");

        for (CubeSegment cubeSegment : this.segmentKeyRanges.keySet()) {
            for (HBaseKeyRange keyRange : this.segmentKeyRanges.get(cubeSegment)) {
                Collection<RowValueDecoder> localRowValueDecoders = new ArrayList<RowValueDecoder>();
                for (RowValueDecoder rowValueDecoder : this.rowValueDecoders) {
                    localRowValueDecoders.add(new RowValueDecoder(rowValueDecoder));
                }

                ListenableFuture<Long> scanFuture = executor.submit(new RangeScanCallable(cubeSegment, keyRange, localRowValueDecoders));
                Futures.addCallback(scanFuture, new FutureCallback<Long>() {
                    public void onSuccess(Long scanCount) {
                        rangesCounter.decrementAndGet();
                        scanCounter.addAndGet(scanCount);
                        logger.debug("Scan count " + scanCount);
                    }

                    public void onFailure(Throwable thrown) {
                        logger.debug("Scan failed due to " + thrown, thrown);
                        scanExceptions.add(thrown);
                    }
                });

                scanFutures.add(scanFuture);
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                executor.shutdown();
            }
        });
    }

    @Override
    public void close() {
        logger.debug("Total scan count " + scanCounter);
        context.setTotalScanCount(scanCounter.get());
        cancalScanTasks();
    }

    @Override
    public boolean hasNext() {
        while (rangesCounter.get() > 0 || tupleQueue.size() > 0) {
            if (!validate()) {
                return false;
            }

            try {
                this.next = tupleQueue.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                scanExceptions.add(e);
                logger.error(e.getLocalizedMessage(), e);
            }

            if (this.next != null)
                return true;
        }

        return false;
    }

    @Override
    public Tuple next() {
        return this.next;
    }

    private void cancalScanTasks() {
        for (ListenableFuture<Long> scanFuture : scanFutures) {
            if (!scanFuture.isDone()) {
                try {
                    scanFuture.cancel(true);
                } catch (Exception e) {
                    logger.error(e.getLocalizedMessage(), e);
                }
            }
        }
    }

    private boolean validate() {
        if (scanExceptions.size() > 0) {
            throw new RuntimeException(scanExceptions.get(0).getLocalizedMessage());
        }

        if (scanCounter.get() >= threshold) {
            if (acceptPartialResult == false) {
                throw new ScanOutOfLimitException("Scan row count exceeded limit: " + limit + ", please add filter condition to narrow down backend scan range, like where clause.");
            }
            context.setPartialResultReturned(true);
            return false;
        }

        if (isLimitEnable && scanCounter.get() >= limit) {
            return false;
        }

        return true;
    }

    private class RangeScanCallable implements Callable<Long> {
        private CubeSegment cubeSeg = null;
        private String tableName = null;
        private RowKeyDecoder rowKeyDecoder = null;
        private HBaseKeyRange keyRange = null;
        private Collection<RowValueDecoder> rowValueDecoders = null;

        private long scanCount = 0;
        private HTableInterface table = null;
        private ResultScanner scanner = null;

        public RangeScanCallable(CubeSegment cubeSeg, HBaseKeyRange keyRange, Collection<RowValueDecoder> rowValueDecoders) {
            super();
            this.cubeSeg = cubeSeg;
            this.keyRange = keyRange;
            this.rowValueDecoders = rowValueDecoders;

            this.tableName = cubeSeg.getStorageLocationIdentifier();
            this.rowKeyDecoder = new RowKeyDecoder(cubeSeg);
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.util.concurrent.Callable#call()
         */
        @Override
        public Long call() throws Exception {
            TupleInfo tupleInfo = buildTupleInfo(keyRange.getCuboid());

            try {
                this.table = conn.getTable(this.tableName);

                Iterator<Result> resultIterator = doScan(keyRange);

                while (resultIterator.hasNext()) {
                    scanCount++;

                    // translate result to tuple
                    try {
                        tupleQueue.put(translateResult(resultIterator.next(), tupleInfo));
                    } catch (IOException e) {
                        throw new IllegalStateException("Can't translate result " + resultIterator.next(), e);
                    }
                }

                logger.debug("Add " + scanCount + " new tuples to tuple queue");
            } catch (Throwable t) {
                throw new StorageException("Error when open connection to table " + this.tableName, t);
            } finally {
                this.closeScanner();
                this.closeTable();
            }

            return scanCount;
        }

        private final Iterator<Result> doScan(HBaseKeyRange keyRange) {
            logScan(keyRange);
            Iterator<Result> iter = null;
            try {
                Scan scan = buildScan(keyRange);
                applyFuzzyFilter(scan, keyRange);
                scanner = CoprocessorEnabler.scanWithCoprocessorIfBeneficial(cubeSeg, keyRange.getCuboid(), filter, groupBy, rowValueDecoders, context, table, scan);
                iter = scanner.iterator();
            } catch (Throwable t) {
                String msg = MessageFormat.format("Error when scan from lower key {1} to upper key {2} on table {0}.", tableName, Bytes.toString(keyRange.getStartKey()), Bytes.toString(keyRange.getStopKey()));
                throw new StorageException(msg, t);
            }

            return iter;
        }

        private String logScan(HBaseKeyRange keyRange) {
            StringBuilder sb = new StringBuilder();
            sb.append("Scan ").append(tableName).append(" from ").append(keyRange.getStartKeyAsString()).append(" to ").append(keyRange.getStopKeyAsString()).append(" on ");
            for (RowValueDecoder valueDecoder : rowValueDecoders) {
                HBaseColumnDesc hbaseColumn = valueDecoder.getHBaseColumn();
                sb.append(hbaseColumn.toString());
                sb.append(",");
            }
            String info = sb.toString();
            logger.info(info);
            return info;
        }

        private Scan buildScan(HBaseKeyRange keyRange) {
            Scan scan = new Scan();
            scan.setCaching(SCAN_CACHE);
            scan.setCacheBlocks(true);
            for (RowValueDecoder valueDecoder : rowValueDecoders) {
                HBaseColumnDesc hbaseColumn = valueDecoder.getHBaseColumn();
                byte[] byteFamily = Bytes.toBytes(hbaseColumn.getColumnFamilyName());
                byte[] byteQualifier = Bytes.toBytes(hbaseColumn.getQualifier());
                scan.addColumn(byteFamily, byteQualifier);
            }

            scan.setStartRow(keyRange.getStartKey());

            // scan.setStopRow(keyRange.getStopKey());
            // what we need is an inclusive end:
            InclusiveStopFilter endFilter = new InclusiveStopFilter(keyRange.getStopKey());
            scan.setFilter(endFilter);
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

                logger.info("Add FuzzyRowFilter: " + keyRange.getFuzzyKeyAsString());
            }
        }

        private TupleInfo buildTupleInfo(Cuboid cuboid) {
            TupleInfo info = new TupleInfo();
            int index = 0;
            rowKeyDecoder.setCuboid(cuboid);
            List<TblColRef> rowColumns = rowKeyDecoder.getColumns();
            List<String> colNames = rowKeyDecoder.getNames(aliasMap);
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
                        String derivedField = getFieldName(derivedCol, aliasMap);
                        info.setField(derivedField, derivedCol, derivedCol.getType().getName(), index++);
                    }
                    // add filler
                    info.addDerivedColumnFiller(Tuple.newDerivedColumnFiller(rowColumns, hostCols, deriveInfo, info, CubeManager.getInstance(cube.getConfig()), cubeSeg));
                }
            }

            for (RowValueDecoder rowValueDecoder : rowValueDecoders) {
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

        private Tuple translateResult(final Result res, TupleInfo tupleInfo) throws IOException {
            Tuple tuple = new Tuple(tupleInfo);
            // groups
            byte[] rowkey = res.getRow();
            rowKeyDecoder.decode(rowkey);
            List<TblColRef> columns = rowKeyDecoder.getColumns();
            List<String> dimensionNames = rowKeyDecoder.getNames(aliasMap);
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
            for (RowValueDecoder rowValueDecoder : rowValueDecoders) {
                HBaseColumnDesc hbaseColumn = rowValueDecoder.getHBaseColumn();
                String columnFamily = hbaseColumn.getColumnFamilyName();
                String qualifier = hbaseColumn.getQualifier();
                // FIXME: avoidable bytes array creation
                byte[] valueBytes = res.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
                rowValueDecoder.decode(valueBytes);
                List<String> measureNames = rowValueDecoder.getNames();
                Object[] measureValues = rowValueDecoder.getValues();
                BitSet projectionIndex = rowValueDecoder.getProjectionIndex();
                for (int i = projectionIndex.nextSetBit(0); i >= 0; i = projectionIndex.nextSetBit(i + 1)) {
                    tuple.setMeasureValue(measureNames.get(i), measureValues[i]);
                }
            }

            return tuple;
        }

        private void closeScanner() {
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
            closeScanner();

            try {
                if (table != null) {
                    table.close();
                    table = null;
                }
            } catch (Throwable t) {
                throw new StorageException("Error when close table " + tableName, t);
            }
        }
    }

}
