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

package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import java.io.IOException;
import java.util.*;

import com.google.common.collect.Sets;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.metadata.measure.fixedlen.FixedPointLongCodec;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorProjector;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorRowType;
import org.apache.kylin.storage.hbase.coprocessor.FilterDecorator;
import org.apache.kylin.storage.hbase.coprocessor.endpoint.generated.IIProtos;
import org.apache.kylin.storage.tuple.Tuple;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.index.TableRecord;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorFilter;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;


/**
 * Created by Hongbin Ma(Binmahone) on 12/2/14.
 */
public class EndpointTupleIterator implements ITupleIterator {

    private final static Logger logger = LoggerFactory.getLogger(EndpointTupleIterator.class);

    private final IISegment seg;
    private final StorageContext context;
    private final List<FunctionDesc> measures;

    private final String factTableName;
    private final List<TblColRef> columns;
    private final List<String> columnNames;
    private final TupleInfo tupleInfo;
    private final TableRecordInfo tableRecordInfo;

    private final CoprocessorRowType pushedDownRowType;
    private final CoprocessorFilter pushedDownFilter;
    private final CoprocessorProjector pushedDownProjector;
    private final EndpointAggregators pushedDownAggregators;

    Iterator<List<IIProtos.IIResponse.IIRow>> regionResponsesIterator = null;
    ITupleIterator tupleIterator = null;
    HTableInterface table = null;

    int rowsInAllMetric = 0;

    public EndpointTupleIterator(IISegment segment, TupleFilter rootFilter, Collection<TblColRef> groupBy, List<FunctionDesc> measures, StorageContext context, HConnection conn) throws Throwable {

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
        this.context = context;
        this.measures = measures;

        this.columns = segment.getColumns();
        this.columnNames = getColumnNames(columns);

        this.tupleInfo = buildTupleInfo();
        this.tableRecordInfo = new TableRecordInfo(this.seg);

        this.pushedDownRowType = CoprocessorRowType.fromTableRecordInfo(tableRecordInfo, this.columns);
        this.pushedDownFilter = CoprocessorFilter.fromFilter(new ClearTextDictionary(this.tableRecordInfo), rootFilter, FilterDecorator.FilterConstantsTreatment.AS_IT_IS);

        for (TblColRef column : this.pushedDownFilter.getUnstrictlyFilteredColumns()) {
            groupBy.add(column);
        }

        this.pushedDownProjector = CoprocessorProjector.makeForEndpoint(tableRecordInfo, groupBy);
        this.pushedDownAggregators = EndpointAggregators.fromFunctions(tableRecordInfo, measures);

        IIProtos.IIRequest endpointRequest = prepareRequest();
        regionResponsesIterator = getResults(endpointRequest, table);

        if (this.regionResponsesIterator.hasNext()) {
            this.tupleIterator = new SingleRegionTupleIterator(this.regionResponsesIterator.next());
        } else {
            this.tupleIterator = ITupleIterator.EMPTY_TUPLE_ITERATOR;
        }
    }

    /**
     * measure comes from query engine, does not contain enough information
     *
     * @param measures
     * @param columns
     */
    private void rewriteMeasureParameters(List<FunctionDesc> measures, List<TblColRef> columns) {
        for (FunctionDesc functionDesc : measures) {
            if (functionDesc.isCount()) {
                functionDesc.setReturnType("bigint");
                functionDesc.setReturnDataType(DataType.getInstance(functionDesc.getReturnType()));
            } else {
                boolean updated = false;
                for (TblColRef column : columns) {
                    if (column.isSameAs(factTableName, functionDesc.getParameter().getValue())) {
                        if (functionDesc.isCountDistinct()) {
                            //TODO: default precision might need be configurable
                            String iiDefaultHLLC = "hllc10";
                            functionDesc.setReturnType(iiDefaultHLLC);
                            functionDesc.setReturnDataType(DataType.getInstance(iiDefaultHLLC));
                        } else {
                            functionDesc.setReturnType(column.getColumn().getType().toString());
                            functionDesc.setReturnDataType(DataType.getInstance(functionDesc.getReturnType()));
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

        return this.tupleIterator.next();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(table);
        logger.info("Closed after " + rowsInAllMetric + " rows are fetched");
    }

    private IIProtos.IIRequest prepareRequest() throws IOException {
        IIProtos.IIRequest request = IIProtos.IIRequest.newBuilder() //
                .setType(ByteString.copyFrom(CoprocessorRowType.serialize(pushedDownRowType))) //
                .setFilter(ByteString.copyFrom(CoprocessorFilter.serialize(pushedDownFilter))) //
                .setProjector(ByteString.copyFrom(CoprocessorProjector.serialize(pushedDownProjector))) //
                .setAggregator(ByteString.copyFrom(EndpointAggregators.serialize(pushedDownAggregators))).build();

        return request;
    }

    //TODO : async callback
    private Iterator<List<IIProtos.IIResponse.IIRow>> getResults(final IIProtos.IIRequest request, HTableInterface table) throws Throwable {
        Map<byte[], List<IIProtos.IIResponse.IIRow>> results = table.coprocessorService(IIProtos.RowsService.class, null, null, new Batch.Call<IIProtos.RowsService, List<IIProtos.IIResponse.IIRow>>() {
            public List<IIProtos.IIResponse.IIRow> call(IIProtos.RowsService rowsService) throws IOException {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<IIProtos.IIResponse> rpcCallback = new BlockingRpcCallback<>();
                rowsService.getRows(controller, request, rpcCallback);
                IIProtos.IIResponse response = rpcCallback.get();
                if (controller.failedOnException()) {
                    throw controller.getFailedOn();
                }

                return response.getRowsList();
            }
        });

        return results.values().iterator();
    }

    private TupleInfo buildTupleInfo() {
        TupleInfo info = new TupleInfo();
        int index = 0;

        for (int i = 0; i < columns.size(); i++) {
            TblColRef column = columns.get(i);
            //            if (!dimensions.contains(column)) {
            //                continue;
            //            }
            info.setField(columnNames.get(i), columns.get(i), columns.get(i).getType().getName(), index++);
        }

        for (FunctionDesc measure : measures) {
            info.setField(measure.getRewriteFieldName(), null, measure.getSQLType(), index++);
        }

        return info;
    }

    private List<String> getColumnNames(List<TblColRef> dimensionColumns) {
        Map<TblColRef, String> aliasMap = context.getAliasMap();
        List<String> result = new ArrayList<String>(dimensionColumns.size());
        for (TblColRef col : dimensionColumns)
            result.add(findName(col, aliasMap));
        return result;
    }

    private String findName(TblColRef column, Map<TblColRef, String> aliasMap) {
        String name = null;
        if (aliasMap != null) {
            name = aliasMap.get(column);
        }
        if (name == null) {
            name = column.getName();
        }
        return name;

    }

    /**
     * Internal class to handle iterators for a single region's returned rows
     */
    class SingleRegionTupleIterator implements ITupleIterator {
        private List<IIProtos.IIResponse.IIRow> rows;
        private int index = 0;

        //not thread safe!
        private TableRecord tableRecord;
        private List<Object> measureValues;
        private Tuple tuple;
        private RowKeyColumnIO rowKeyColumnIO;

        public SingleRegionTupleIterator(List<IIProtos.IIResponse.IIRow> rows) {
            this.rows = rows;
            this.index = 0;
            this.tableRecord = tableRecordInfo.createTableRecord();
            this.tuple = new Tuple(tupleInfo);
            rowKeyColumnIO = new RowKeyColumnIO(new ClearTextDictionary(tableRecordInfo));
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

            IIProtos.IIResponse.IIRow currentRow = rows.get(index);
            byte[] columnsBytes = currentRow.getColumns().toByteArray();
            this.tableRecord.setBytes(columnsBytes, 0, columnsBytes.length);
            if (currentRow.hasMeasures()) {
                byte[] measuresBytes = currentRow.getMeasures().toByteArray();

                this.measureValues = pushedDownAggregators.deserializeMetricValues(measuresBytes, 0);
            }

            index++;

            return makeTuple(this.tableRecord, this.measureValues);
        }

        @Override
        public void close() {

        }

        private ITuple makeTuple(TableRecord tableRecord, List<Object> measureValues) {
            // groups
            List<String> columnValues = Lists.newArrayList();
            for (int i = 0; i < columns.size(); ++i) {
                final TblColRef tblColRef = columns.get(i);
                columnValues.add(rowKeyColumnIO.readColumnString(tblColRef, tableRecord.getBytes(), tableRecordInfo.getDigest().offset(i), tableRecordInfo.getDigest().length(i)));
            }
            for (int i = 0; i < columnNames.size(); i++) {
                TblColRef column = columns.get(i);
                if (!tuple.hasColumn(column)) {
                    continue;
                }
                final String columnName = columnNames.get(i);
                if (tableRecordInfo.getDigest().isMetrics(i)) {
                    tuple.setDimensionValue(columnName, tableRecord.getValueString(i));
                } else {
                    tuple.setDimensionValue(columnName, columnValues.get(i));
                }
            }

            if (measureValues != null) {
                for (int i = 0; i < measures.size(); ++i) {
                    if (!measures.get(i).isDimensionAsMetric()) {
                        String fieldName = measures.get(i).getRewriteFieldName();
                        Object value = measureValues.get(i);
                        String dataType = tuple.getDataType(fieldName);
                        //TODO: currently in II all metrics except HLLC is returned as String
                        if (value instanceof String) {
                            value = Tuple.convertOptiqCellValue((String) value, dataType);
                        }
                        tuple.setMeasureValue(fieldName, value);
                    }
                }
            }
            return tuple;
        }
    }
}
