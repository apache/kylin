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

package org.apache.kylin.storage.hbase.ii.coprocessor.endpoint;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.RangeUtil;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.index.TableRecord;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.cache.TsConditionExtractor;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorFilter;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorProjector;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorRowType;
import org.apache.kylin.storage.hbase.common.coprocessor.FilterDecorator;
import org.apache.kylin.storage.hbase.ii.coprocessor.endpoint.generated.IIProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.google.common.collect.Sets;
import com.google.protobuf.HBaseZeroCopyByteString;

/**
 */
public class EndpointTupleIterator implements ITupleIterator {

    private final static Logger logger = LoggerFactory.getLogger(EndpointTupleIterator.class);

    private final IISegment seg;

    private final String factTableName;
    private final List<TblColRef> columns;
    private final TupleInfo tupleInfo;
    private final TableRecordInfo tableRecordInfo;
    private final EndpointTupleConverter tupleConverter;

    private final CoprocessorRowType pushedDownRowType;
    private final CoprocessorFilter pushedDownFilter;
    private final CoprocessorProjector pushedDownProjector;
    private final EndpointAggregators pushedDownAggregators;
    private final Range<Long> tsRange;//timestamp column condition's interval

    private Iterator<List<IIProtos.IIResponseInternal.IIRow>> regionResponsesIterator = null;
    private ITupleIterator tupleIterator = null;
    private HTableInterface table = null;

    private TblColRef partitionCol;
    private long lastDataTime = -1;
    private int rowsInAllMetric = 0;

    public EndpointTupleIterator(IISegment segment, TupleFilter rootFilter, Collection<TblColRef> groupBy, List<FunctionDesc> measures, StorageContext context, HConnection conn, TupleInfo returnTupleInfo) throws Throwable {

        String tableName = segment.getStorageLocationIdentifier();
        table = conn.getTable(tableName);
        factTableName = segment.getIIDesc().getFactTableName();

        if (rootFilter == null) {
            rootFilter = ConstantTupleFilter.TRUE;
        }

        if (groupBy == null) {
            groupBy = Sets.newHashSet();
        }

        if (measures == null) {
            measures = Lists.newArrayList();
        }

        //this method will change measures
        rewriteMeasureParameters(measures, segment.getColumns());

        this.seg = segment;
        this.columns = segment.getColumns();

        this.tupleInfo = returnTupleInfo;
        this.tupleConverter = new EndpointTupleConverter(columns, measures, returnTupleInfo);
        this.tableRecordInfo = new TableRecordInfo(this.seg);

        this.pushedDownRowType = CoprocessorRowType.fromTableRecordInfo(tableRecordInfo, this.columns);
        this.pushedDownFilter = CoprocessorFilter.fromFilter(new ClearTextDictionary(this.tableRecordInfo), rootFilter, FilterDecorator.FilterConstantsTreatment.AS_IT_IS);

        for (TblColRef column : this.pushedDownFilter.getInevaluableColumns()) {
            groupBy.add(column);
        }

        this.pushedDownProjector = CoprocessorProjector.makeForEndpoint(tableRecordInfo, groupBy);
        this.pushedDownAggregators = EndpointAggregators.fromFunctions(tableRecordInfo, measures);

        int tsCol = this.tableRecordInfo.getTimestampColumn();
        this.partitionCol = this.columns.get(tsCol);
        this.tsRange = TsConditionExtractor.extractTsCondition(this.partitionCol, rootFilter);

        if (this.tsRange == null) {
            logger.info("TsRange conflict for endpoint, return empty directly");
            this.tupleIterator = ITupleIterator.EMPTY_TUPLE_ITERATOR;
        } else {
            logger.info("The tsRange being pushed is " + RangeUtil.formatTsRange(tsRange));
        }

        IIProtos.IIRequest endpointRequest = prepareRequest();
        Collection<IIProtos.IIResponse> compressedShardResults = getResults(endpointRequest, table);

        //decompress
        Collection<IIProtos.IIResponseInternal> shardResults = new ArrayList<>();
        for (IIProtos.IIResponse input : compressedShardResults) {
            byte[] compressed = HBaseZeroCopyByteString.zeroCopyGetBytes(input.getBlob());
            try {
                byte[] decompressed = CompressionUtils.decompress(compressed);
                shardResults.add(IIProtos.IIResponseInternal.parseFrom(decompressed));
            } catch (Exception e) {
                throw new RuntimeException("decompress endpoint response error");
            }
        }

        this.lastDataTime = Collections.min(Collections2.transform(shardResults, new Function<IIProtos.IIResponseInternal, Long>() {
            @Nullable
            @Override
            public Long apply(IIProtos.IIResponseInternal input) {

                IIProtos.IIResponseInternal.Stats status = input.getStats();
                logger.info("Endpoints all returned, stats from shard {}: start moment:{}, finish moment: {}, elapsed ms: {}, scanned slices: {}, latest slice time is {}",//
                        new Object[] { String.valueOf(status.getMyShard()),//
                                DateFormat.formatToTimeStr(status.getServiceStartTime()),//
                                DateFormat.formatToTimeStr(status.getServiceEndTime()),//
                                String.valueOf(status.getServiceEndTime() - status.getServiceStartTime()),//
                                String.valueOf(status.getScannedSlices()), DateFormat.formatToTimeStr(status.getLatestDataTime()) });

                return status.getLatestDataTime();
            }
        }));

        this.regionResponsesIterator = Collections2.transform(shardResults, new Function<IIProtos.IIResponseInternal, List<IIProtos.IIResponseInternal.IIRow>>() {
            @Nullable
            @Override
            public List<IIProtos.IIResponseInternal.IIRow> apply(@Nullable IIProtos.IIResponseInternal input) {
                return input.getRowsList();
            }
        }).iterator();

        if (this.regionResponsesIterator.hasNext()) {
            this.tupleIterator = new SingleRegionTupleIterator(this.regionResponsesIterator.next());
        } else {
            this.tupleIterator = ITupleIterator.EMPTY_TUPLE_ITERATOR;
        }
    }

    /**
     * measure comes from query engine, does not contain enough information
     */
    private void rewriteMeasureParameters(List<FunctionDesc> measures, List<TblColRef> columns) {
        for (FunctionDesc functionDesc : measures) {
            if (functionDesc.isCount()) {
                functionDesc.setReturnType("bigint");
            } else {
                boolean updated = false;
                for (TblColRef column : columns) {
                    if (column.isSameAs(factTableName, functionDesc.getParameter().getValue())) {
                        if (functionDesc.isCountDistinct()) {
                            //TODO: default precision might need be configurable
                            String iiDefaultHLLC = "hllc10";
                            functionDesc.setReturnType(iiDefaultHLLC);
                        } else {
                            functionDesc.setReturnType(column.getColumnDesc().getType().toString());
                        }
                        functionDesc.getParameter().setColRefs(ImmutableList.of(column));
                        updated = true;
                        break;
                    }
                }
                if (!updated) {
                    throw new RuntimeException("Func " + functionDesc + " is not related to any column in fact table " + factTableName);
                }
            }
        }
    }

    @Override
    public boolean hasNext() {
        while (!this.tupleIterator.hasNext()) {
            if (this.regionResponsesIterator.hasNext()) {
                this.tupleIterator = new SingleRegionTupleIterator(this.regionResponsesIterator.next());
            } else {
                return false;
            }
        }
        return true;
    }

    @Override
    public ITuple next() {
        rowsInAllMetric++;

        if (!hasNext()) {
            throw new IllegalStateException("No more ITuple in EndpointTupleIterator");
        }

        ITuple tuple = this.tupleIterator.next();
        return tuple;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();

    }

    @Override
    public void close() {
        IOUtils.closeQuietly(table);
        logger.info("Closed after " + rowsInAllMetric + " rows are fetched");
    }

    /**
     * tells storage layer cache what time period of data should not be cached.
     * for static storage like cube, it will return null
     * for dynamic storage like ii, it will for example exclude the last two minutes for possible data latency
     * @return
     */
    public Range<Long> getCacheExcludedPeriod() {
        Preconditions.checkArgument(lastDataTime != -1, "lastDataTime is not set yet");
        return Ranges.greaterThan(lastDataTime);
    }

    private IIProtos.IIRequest prepareRequest() throws IOException {
        IIProtos.IIRequest.Builder builder = IIProtos.IIRequest.newBuilder();

        if (this.tsRange != null) {
            byte[] tsRangeBytes = SerializationUtils.serialize(this.tsRange);
            builder.setTsRange(HBaseZeroCopyByteString.wrap(tsRangeBytes));
        }

        builder.setType(HBaseZeroCopyByteString.wrap(CoprocessorRowType.serialize(pushedDownRowType))) //
                .setFilter(HBaseZeroCopyByteString.wrap(CoprocessorFilter.serialize(pushedDownFilter))) //
                .setProjector(HBaseZeroCopyByteString.wrap(CoprocessorProjector.serialize(pushedDownProjector))) //
                .setAggregator(HBaseZeroCopyByteString.wrap(EndpointAggregators.serialize(pushedDownAggregators)));

        IIProtos.IIRequest request = builder.build();

        return request;
    }

    private Collection<IIProtos.IIResponse> getResults(final IIProtos.IIRequest request, HTableInterface table) throws Throwable {
        Map<byte[], IIProtos.IIResponse> results = table.coprocessorService(IIProtos.RowsService.class, null, null, new Batch.Call<IIProtos.RowsService, IIProtos.IIResponse>() {
            public IIProtos.IIResponse call(IIProtos.RowsService rowsService) throws IOException {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<IIProtos.IIResponse> rpcCallback = new BlockingRpcCallback<>();
                rowsService.getRows(controller, request, rpcCallback);
                IIProtos.IIResponse response = rpcCallback.get();
                if (controller.failedOnException()) {
                    throw controller.getFailedOn();
                }

                return response;
            }
        });

        return results.values();
    }

    /**
     * Internal class to handle iterators for a single region's returned rows
     */
    class SingleRegionTupleIterator implements ITupleIterator {
        private List<IIProtos.IIResponseInternal.IIRow> rows;
        private int index = 0;

        //not thread safe!
        private TableRecord tableRecord;
        private List<Object> measureValues;
        private Tuple tuple;

        public SingleRegionTupleIterator(List<IIProtos.IIResponseInternal.IIRow> rows) {
            this.rows = rows;
            this.index = 0;
            this.tableRecord = tableRecordInfo.createTableRecord();
            this.tuple = new Tuple(tupleInfo);
        }

        @Override
        public boolean hasNext() {
            return index < rows.size();
        }

        @Override
        public ITuple next() {
            if (!hasNext()) {
                throw new IllegalStateException("No more Tuple in the SingleRegionTupleIterator");
            }

            IIProtos.IIResponseInternal.IIRow currentRow = rows.get(index);
            byte[] columnsBytes = HBaseZeroCopyByteString.zeroCopyGetBytes(currentRow.getColumns());
            this.tableRecord.setBytes(columnsBytes, 0, columnsBytes.length);
            if (currentRow.hasMeasures()) {
                ByteBuffer buffer = currentRow.getMeasures().asReadOnlyByteBuffer();
                this.measureValues = pushedDownAggregators.deserializeMetricValues(buffer);
            }

            index++;

            return tupleConverter.makeTuple(this.tableRecord, this.measureValues, this.tuple);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }

    }
}
