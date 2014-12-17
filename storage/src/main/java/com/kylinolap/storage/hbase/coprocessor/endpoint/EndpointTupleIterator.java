package com.kylinolap.storage.hbase.coprocessor.endpoint;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.kylinolap.invertedindex.IISegment;
import com.kylinolap.invertedindex.index.TableRecord;
import com.kylinolap.invertedindex.index.TableRecordInfo;
import com.kylinolap.metadata.model.ColumnDesc;
import com.kylinolap.metadata.model.DataType;
import com.kylinolap.metadata.model.FunctionDesc;
import com.kylinolap.metadata.model.TblColRef;
import com.kylinolap.storage.StorageContext;
import com.kylinolap.storage.filter.ConstantTupleFilter;
import com.kylinolap.storage.filter.TupleFilter;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorFilter;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorProjector;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorRowType;
import com.kylinolap.storage.hbase.coprocessor.endpoint.generated.IIProtos;
import com.kylinolap.storage.hbase.coprocessor.endpoint.generated.IIProtos.IIResponse.IIRow;
import com.kylinolap.storage.tuple.ITuple;
import com.kylinolap.storage.tuple.ITupleIterator;
import com.kylinolap.storage.tuple.Tuple;
import com.kylinolap.storage.tuple.TupleInfo;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by Hongbin Ma(Binmahone) on 12/2/14.
 */
public class EndpointTupleIterator implements ITupleIterator {

    private final static Logger logger = LoggerFactory.getLogger(EndpointTupleIterator.class);

    private final IISegment seg;
    private final StorageContext context;
    private final List<FunctionDesc> measures;

    private final List<TblColRef> columns;
    private final List<String> columnNames;
    private final TupleInfo tupleInfo;
    private final TableRecordInfo tableRecordInfo;

    private final CoprocessorRowType pushedDownRowType;
    private final CoprocessorFilter pushedDownFilter;
    private final CoprocessorProjector pushedDownProjector;
    private final EndpointAggregators pushedDownAggregators;

    Iterator<List<IIRow>> regionResponsesIterator = null;
    ITupleIterator tupleIterator = null;
    HTableInterface table = null;

    int rowsInAllMetric = 0;


    public EndpointTupleIterator(IISegment cubeSegment, ColumnDesc[] columnDescs,
            TupleFilter rootFilter, Collection<TblColRef> groupBy, List<FunctionDesc> measures, StorageContext context, HConnection conn) throws Throwable {

        String tableName = cubeSegment.getStorageLocationIdentifier();
        table = conn.getTable(tableName);

        if (rootFilter == null) {
            rootFilter = ConstantTupleFilter.TRUE;
        }

        if (groupBy == null) {
            groupBy = Lists.newArrayList();
        }

        if (measures == null) {
            measures = Lists.newArrayList();
        }
        initMeaureParameters(measures, columnDescs);


        this.seg = cubeSegment;
        this.context = context;
        this.measures = measures;

        this.columns = Lists.newArrayList();
        for (ColumnDesc columnDesc : columnDescs) {
            columns.add(new TblColRef(columnDesc));
        }
        columnNames = getColumnNames(columns);

        this.tupleInfo = buildTupleInfo();
        this.tableRecordInfo = new TableRecordInfo(this.seg);

        this.pushedDownRowType = CoprocessorRowType.fromTableRecordInfo(tableRecordInfo, columnDescs);
        this.pushedDownFilter = CoprocessorFilter.fromFilter(this.seg, rootFilter);
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
    private void initMeaureParameters(List<FunctionDesc> measures, ColumnDesc[] columns) {
        for (FunctionDesc functionDesc : measures) {
            if (functionDesc.isCount()) {
                functionDesc.setReturnType("bigint");
                functionDesc.setReturnDataType(DataType.getInstance(functionDesc.getReturnType()));
            } else {
                for (ColumnDesc columnDesc : columns) {
                    if (functionDesc.getParameter().getValue().equalsIgnoreCase(columnDesc.getName())) {
                        functionDesc.setReturnType(columnDesc.getTypeName());
                        functionDesc.setReturnDataType(DataType.getInstance(functionDesc.getReturnType()));
                        functionDesc.getParameter().setColRefs(ImmutableList.of(new TblColRef(columnDesc)));
                        break;
                    }
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
        IIProtos.IIRequest request = IIProtos.IIRequest.newBuilder().
                setType(ByteString.copyFrom(CoprocessorRowType.serialize(pushedDownRowType))).
                setFilter(ByteString.copyFrom(CoprocessorFilter.serialize(pushedDownFilter))).
                setProjector(ByteString.copyFrom(CoprocessorProjector.serialize(pushedDownProjector))).
                setAggregator(ByteString.copyFrom(EndpointAggregators.serialize(pushedDownAggregators))).
                build();

        return request;
    }


    private Iterator<List<IIRow>> getResults(final IIProtos.IIRequest request, HTableInterface table) throws Throwable {
        Map<byte[], List<IIRow>> results = table.coprocessorService(IIProtos.RowsService.class,
                null, null,
                new Batch.Call<IIProtos.RowsService, List<IIRow>>() {
                    public List<IIProtos.IIResponse.IIRow> call(IIProtos.RowsService rowsService) throws IOException {
                        ServerRpcController controller = new ServerRpcController();
                        BlockingRpcCallback<IIProtos.IIResponse> rpcCallback =
                                new BlockingRpcCallback<>();
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
        private List<IIRow> rows;
        private int index = 0;

        //not thread safe!
        private TableRecord tableRecord;
        private List<String> measureValues;
        private Tuple tuple;

        public SingleRegionTupleIterator(List<IIProtos.IIResponse.IIRow> rows) {
            this.rows = rows;
            this.index = 0;
            this.tableRecord = new TableRecord(tableRecordInfo);
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

            IIRow currentRow = rows.get(index);
            //ByteBuffer columnsBuffer = currentRow.getColumns().asReadOnlyByteBuffer();//avoid creating byte[], if possible
            //this.tableRecord.setBytes(columnsBuffer.array(), columnsBuffer.position(), columnsBuffer.limit());
            byte[] columnsBytes = currentRow.getColumns().toByteArray();
            this.tableRecord.setBytes(columnsBytes, 0, columnsBytes.length);

//            ByteBuffer measuresBuffer = currentRow.getMeasures().asReadOnlyByteBuffer();
//            this.measureValues = pushedDownAggregators.deserializeMetricValues(measuresBuffer.array(), measuresBuffer.position());
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

        private ITuple makeTuple(TableRecord tableRecord, List<String> measureValues) {
            // groups
            List<String> columnValues = tableRecord.getValueList();
            for (int i = 0; i < columnNames.size(); i++) {
                TblColRef column = columns.get(i);
                if (!tuple.hasColumn(column)) {
                    continue;
                }
                tuple.setValue(columnNames.get(i), columnValues.get(i));
            }

            if (measureValues != null) {
                for (int i = 0; i < measures.size(); ++i) {
                    tuple.setValue(measures.get(i).getRewriteFieldName(), measureValues.get(i));
                }
            }
            return tuple;
        }


    }
}
