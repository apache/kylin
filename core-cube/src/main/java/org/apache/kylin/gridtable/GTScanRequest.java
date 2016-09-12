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

package org.apache.kylin.gridtable;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.SerializeToByteBuffer;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class GTScanRequest {

    private static final Logger logger = LoggerFactory.getLogger(GTScanRequest.class);
    
    //it's not necessary to increase the checkInterval to very large because the check cost is not high
    //changing it might break org.apache.kylin.query.ITKylinQueryTest.testTimeoutQuery()
    public static final int terminateCheckInterval = 100;

    private GTInfo info;
    private List<GTScanRange> ranges;
    private ImmutableBitSet columns;
    private transient ImmutableBitSet selectedColBlocks;

    // optional filtering
    private TupleFilter filterPushDown;

    // optional aggregation
    private ImmutableBitSet aggrGroupBy;
    private ImmutableBitSet aggrMetrics;
    private String[] aggrMetricsFuncs;//

    // hint to storage behavior
    private String storageBehavior;
    private long startTime;
    private long timeout;
    private boolean allowStorageAggregation;
    private double aggCacheMemThreshold;
    private int storageScanRowNumThreshold;
    private int storagePushDownLimit;

    // runtime computed fields
    private transient boolean doingStorageAggregation = false;

    GTScanRequest(GTInfo info, List<GTScanRange> ranges, ImmutableBitSet dimensions, ImmutableBitSet aggrGroupBy, //
            ImmutableBitSet aggrMetrics, String[] aggrMetricsFuncs, TupleFilter filterPushDown, boolean allowStorageAggregation, //
            double aggCacheMemThreshold, int storageScanRowNumThreshold, int storagePushDownLimit, String storageBehavior, long startTime, long timeout) {
        this.info = info;
        if (ranges == null) {
            this.ranges = Lists.newArrayList(new GTScanRange(new GTRecord(info), new GTRecord(info)));
        } else {
            this.ranges = ranges;
        }
        this.columns = dimensions;
        this.filterPushDown = filterPushDown;

        this.aggrGroupBy = aggrGroupBy;
        this.aggrMetrics = aggrMetrics;
        this.aggrMetricsFuncs = aggrMetricsFuncs;

        this.storageBehavior = storageBehavior;
        this.startTime = startTime;
        this.timeout = timeout;
        this.allowStorageAggregation = allowStorageAggregation;
        this.aggCacheMemThreshold = aggCacheMemThreshold;
        this.storageScanRowNumThreshold = storageScanRowNumThreshold;
        this.storagePushDownLimit = storagePushDownLimit;

        validate(info);
    }

    private void validate(GTInfo info) {
        if (hasAggregation()) {
            if (aggrGroupBy.intersects(aggrMetrics))
                throw new IllegalStateException();
            if (aggrMetrics.cardinality() != aggrMetricsFuncs.length)
                throw new IllegalStateException();

            if (columns == null)
                columns = ImmutableBitSet.EMPTY;

            columns = columns.or(aggrGroupBy);
            columns = columns.or(aggrMetrics);
        }

        if (columns == null)
            columns = info.colAll;

        this.selectedColBlocks = info.selectColumnBlocks(columns);

        if (hasFilterPushDown()) {
            validateFilterPushDown(info);
        }
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    private void validateFilterPushDown(GTInfo info) {
        if (!hasFilterPushDown())
            return;

        Set<TblColRef> filterColumns = Sets.newHashSet();
        TupleFilter.collectColumns(filterPushDown, filterColumns);

        for (TblColRef col : filterColumns) {
            // filter columns must belong to the table
            info.validateColRef(col);
            // filter columns must be returned to satisfy upper layer evaluation (calcite)
            columns = columns.set(col.getColumnDesc().getZeroBasedIndex());
        }

        // un-evaluatable filter must be removed
        if (!TupleFilter.isEvaluableRecursively(filterPushDown)) {
            Set<TblColRef> unevaluableColumns = Sets.newHashSet();
            filterPushDown = GTUtil.convertFilterUnevaluatable(filterPushDown, info, unevaluableColumns);

            // columns in un-evaluatable filter must be returned without loss so upper layer can do final evaluation
            if (hasAggregation()) {
                for (TblColRef col : unevaluableColumns) {
                    aggrGroupBy = aggrGroupBy.set(col.getColumnDesc().getZeroBasedIndex());
                }
            }
        }
    }

    public IGTScanner decorateScanner(IGTScanner scanner) throws IOException {
        return decorateScanner(scanner, true, true, Long.MAX_VALUE);
    }

    /**
     * filterToggledOn,aggrToggledOn are only for profiling/test use.
     * in normal cases they are all true.
     * 
     * Refer to CoprocessorBehavior for explanation
     */
    public IGTScanner decorateScanner(IGTScanner scanner, boolean filterToggledOn, boolean aggrToggledOn, long deadline) throws IOException {
        IGTScanner result = scanner;
        if (!filterToggledOn) { //Skip reading this section if you're not profiling! 
            int scanned = lookAndForget(result);
            return new EmptyGTScanner(scanned);
        } else {

            if (this.hasFilterPushDown()) {
                result = new GTFilterScanner(result, this);
            }

            if (!aggrToggledOn) {//Skip reading this section if you're not profiling! 
                long scanned = result.getScannedRowCount();
                lookAndForget(result);
                return new EmptyGTScanner(scanned);
            }

            if (!this.isAllowStorageAggregation()) {
                logger.info("pre aggregation is not beneficial, skip it");
            } else if (this.hasAggregation()) {
                logger.info("pre aggregating results before returning");
                this.doingStorageAggregation = true;
                result = new GTAggregateScanner(result, this, deadline);
            } else {
                logger.info("has no aggregation, skip it");
            }
            return result;
        }

    }

    public boolean isDoingStorageAggregation() {
        return doingStorageAggregation;
    }

    //touch every byte of the cell so that the cost of scanning will be truly reflected
    private int lookAndForget(IGTScanner scanner) {
        byte meaninglessByte = 0;
        int scanned = 0;
        for (GTRecord gtRecord : scanner) {
            scanned++;
            for (ByteArray col : gtRecord.getInternal()) {
                if (col != null) {
                    int endIndex = col.offset() + col.length();
                    for (int i = col.offset(); i < endIndex; ++i) {
                        meaninglessByte += col.array()[i];
                    }
                }
            }
        }
        System.out.println("Meaningless byte is " + meaninglessByte);
        IOUtils.closeQuietly(scanner);
        return scanned;
    }

    public boolean hasFilterPushDown() {
        return filterPushDown != null;
    }

    //TODO BUG?  select sum() from fact, no aggr by
    public boolean hasAggregation() {
        return !aggrGroupBy.isEmpty() || !aggrMetrics.isEmpty();
    }

    public GTInfo getInfo() {
        return info;
    }

    public List<GTScanRange> getGTScanRanges() {
        return ranges;
    }

    public void clearScanRanges() {
        this.ranges = Lists.newArrayList();
    }

    public ImmutableBitSet getSelectedColBlocks() {
        return selectedColBlocks;
    }

    public ImmutableBitSet getColumns() {
        return columns;
    }

    public TupleFilter getFilterPushDown() {
        return filterPushDown;
    }

    public ImmutableBitSet getDimensions() {
        return this.getColumns().andNot(this.getAggrMetrics());
    }

    public ImmutableBitSet getAggrGroupBy() {
        return aggrGroupBy;
    }

    public ImmutableBitSet getAggrMetrics() {
        return aggrMetrics;
    }

    public String[] getAggrMetricsFuncs() {
        return aggrMetricsFuncs;
    }

    public boolean isAllowStorageAggregation() {
        return allowStorageAggregation;
    }

    public double getAggCacheMemThreshold() {
        if (aggCacheMemThreshold < 0)
            return 0;
        else
            return aggCacheMemThreshold;
    }

    public void disableAggCacheMemCheck() {
        this.aggCacheMemThreshold = 0;
    }

    public int getStorageScanRowNumThreshold() {
        return storageScanRowNumThreshold;
    }

    public int getStoragePushDownLimit() {
        return this.storagePushDownLimit;
    }

    public String getStorageBehavior() {
        return storageBehavior;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getTimeout() {
        return timeout;
    }

    @Override
    public String toString() {
        return "GTScanRequest [range=" + ranges + ", columns=" + columns + ", filterPushDown=" + filterPushDown + ", aggrGroupBy=" + aggrGroupBy + ", aggrMetrics=" + aggrMetrics + ", aggrMetricsFuncs=" + Arrays.toString(aggrMetricsFuncs) + "]";
    }

    public byte[] toByteArray() {
        ByteBuffer byteBuffer = SerializeToByteBuffer.retrySerialize(new SerializeToByteBuffer.IWriter() {
            @Override
            public void write(ByteBuffer byteBuffer) throws BufferOverflowException {
                GTScanRequest.serializer.serialize(GTScanRequest.this, byteBuffer);
            }
        });
        return Arrays.copyOf(byteBuffer.array(), byteBuffer.position());
    }

    public static final BytesSerializer<GTScanRequest> serializer = new BytesSerializer<GTScanRequest>() {
        @Override
        public void serialize(GTScanRequest value, ByteBuffer out) {
            GTInfo.serializer.serialize(value.info, out);

            BytesUtil.writeVInt(value.ranges.size(), out);
            for (GTScanRange range : value.ranges) {
                serializeGTRecord(range.pkStart, out);
                serializeGTRecord(range.pkEnd, out);
                BytesUtil.writeVInt(range.fuzzyKeys.size(), out);
                for (GTRecord f : range.fuzzyKeys) {
                    serializeGTRecord(f, out);
                }
            }

            ImmutableBitSet.serializer.serialize(value.columns, out);
            BytesUtil.writeByteArray(GTUtil.serializeGTFilter(value.filterPushDown, value.info), out);

            ImmutableBitSet.serializer.serialize(value.aggrGroupBy, out);
            ImmutableBitSet.serializer.serialize(value.aggrMetrics, out);
            BytesUtil.writeAsciiStringArray(value.aggrMetricsFuncs, out);
            BytesUtil.writeVInt(value.allowStorageAggregation ? 1 : 0, out);
            out.putDouble(value.aggCacheMemThreshold);
            BytesUtil.writeVInt(value.storageScanRowNumThreshold, out);
            BytesUtil.writeVInt(value.storagePushDownLimit, out);
            BytesUtil.writeVLong(value.startTime, out);
            BytesUtil.writeVLong(value.timeout, out);
            BytesUtil.writeUTFString(value.storageBehavior, out);
        }

        @Override
        public GTScanRequest deserialize(ByteBuffer in) {
            GTInfo sInfo = GTInfo.serializer.deserialize(in);

            List<GTScanRange> sRanges = Lists.newArrayList();
            int sRangesCount = BytesUtil.readVInt(in);
            for (int rangeIdx = 0; rangeIdx < sRangesCount; rangeIdx++) {
                GTRecord sPkStart = deserializeGTRecord(in, sInfo);
                GTRecord sPkEnd = deserializeGTRecord(in, sInfo);
                List<GTRecord> sFuzzyKeys = Lists.newArrayList();
                int sFuzzyKeySize = BytesUtil.readVInt(in);
                for (int i = 0; i < sFuzzyKeySize; i++) {
                    sFuzzyKeys.add(deserializeGTRecord(in, sInfo));
                }
                GTScanRange sRange = new GTScanRange(sPkStart, sPkEnd, sFuzzyKeys);
                sRanges.add(sRange);
            }

            ImmutableBitSet sColumns = ImmutableBitSet.serializer.deserialize(in);
            TupleFilter sGTFilter = GTUtil.deserializeGTFilter(BytesUtil.readByteArray(in), sInfo);

            ImmutableBitSet sAggGroupBy = ImmutableBitSet.serializer.deserialize(in);
            ImmutableBitSet sAggrMetrics = ImmutableBitSet.serializer.deserialize(in);
            String[] sAggrMetricFuncs = BytesUtil.readAsciiStringArray(in);
            boolean sAllowPreAggr = (BytesUtil.readVInt(in) == 1);
            double sAggrCacheGB = in.getDouble();
            int storageScanRowNumThreshold = BytesUtil.readVInt(in);
            int storagePushDownLimit = BytesUtil.readVInt(in);
            long startTime = BytesUtil.readVLong(in);
            long timeout = BytesUtil.readVLong(in);
            String storageBehavior = BytesUtil.readUTFString(in);

            return new GTScanRequestBuilder().setInfo(sInfo).setRanges(sRanges).setDimensions(sColumns).//
            setAggrGroupBy(sAggGroupBy).setAggrMetrics(sAggrMetrics).setAggrMetricsFuncs(sAggrMetricFuncs).//
            setFilterPushDown(sGTFilter).setAllowStorageAggregation(sAllowPreAggr).setAggCacheMemThreshold(sAggrCacheGB).//
            setStorageScanRowNumThreshold(storageScanRowNumThreshold).setStoragePushDownLimit(storagePushDownLimit).//
            setStartTime(startTime).setTimeout(timeout).setStorageBehavior(storageBehavior).createGTScanRequest();
        }

        private void serializeGTRecord(GTRecord gtRecord, ByteBuffer out) {
            BytesUtil.writeVInt(gtRecord.cols.length, out);
            for (ByteArray col : gtRecord.cols) {
                col.exportData(out);
            }
        }

        private GTRecord deserializeGTRecord(ByteBuffer in, GTInfo sInfo) {
            int colLength = BytesUtil.readVInt(in);
            ByteArray[] sCols = new ByteArray[colLength];
            for (int i = 0; i < colLength; i++) {
                sCols[i] = ByteArray.importData(in);
            }
            return new GTRecord(sInfo, sCols);
        }

    };
}
