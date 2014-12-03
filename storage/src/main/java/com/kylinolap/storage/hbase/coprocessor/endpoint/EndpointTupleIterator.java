package com.kylinolap.storage.hbase.coprocessor.endpoint;

import com.google.protobuf.ByteString;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.cube.invertedindex.TableRecord;
import com.kylinolap.cube.invertedindex.TableRecordInfo;
import com.kylinolap.cube.invertedindex.TableRecordInfoDigest;
import com.kylinolap.cube.kv.RowValueDecoder;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.metadata.model.cube.HBaseColumnDesc;
import com.kylinolap.metadata.model.realization.FunctionDesc;
import com.kylinolap.metadata.model.realization.TblColRef;
import com.kylinolap.storage.StorageContext;
import com.kylinolap.storage.filter.TupleFilter;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorFilter;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorProjector;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorRowType;
import com.kylinolap.storage.hbase.coprocessor.endpoint.generated.IIProtos;
import com.kylinolap.storage.hbase.coprocessor.endpoint.generated.IIProtos.IIResponse;
import com.kylinolap.storage.hbase.coprocessor.endpoint.generated.IIProtos.IIResponse.IIRow;
import com.kylinolap.storage.tuple.ITuple;
import com.kylinolap.storage.tuple.ITupleIterator;
import com.kylinolap.storage.tuple.Tuple;
import com.kylinolap.storage.tuple.TupleInfo;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Created by Hongbin Ma(Binmahone) on 12/2/14.
 */
public class EndpointTupleIterator implements ITupleIterator {

    private final CubeSegment seg;
    private final CubeDesc cubeDesc;
    private final Collection<TblColRef> dimensions;
    private final TupleFilter filter;
    private final Collection<TblColRef> groupBy;
    private final StorageContext context;
    private final List<FunctionDesc> measures;

    Iterator<List<IIRow>> regionResponsesIterator = null;
    Iterator<ITuple> tupleIterator = null;

    //TODO what exactly is dimension here?
    public EndpointTupleIterator(CubeSegment cubeSegment, CubeDesc cubeDesc, Collection<TblColRef> dimensions,
            TupleFilter filter, Collection<TblColRef> groupBy, List<FunctionDesc> measures, StorageContext context, HTableInterface table) throws Throwable {
        this.seg = cubeSegment;
        this.cubeDesc = cubeDesc;
        this.dimensions = dimensions;
        this.filter = filter;
        this.groupBy = groupBy;
        this.context = context;
        this.measures = measures;

        IIProtos.IIRequest endpointRequest = prepareRequest(filter, dimensions, measures);
        regionResponsesIterator = getResults(endpointRequest, table);

        if (this.regionResponsesIterator.hasNext()) {
            this.tupleIterator = this.segmentIteratorIterator.next();
        } else {
            this.tupleIterator = ITupleIterator.EMPTY_TUPLE_ITERATOR;
        }
    }


    private IIProtos.IIRequest prepareRequest(TupleFilter rootFilter, final Collection<TblColRef> dimensionColumns, final List<FunctionDesc> metrics) throws IOException {

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        CoprocessorRowType type = CoprocessorRowType.fromCuboid(this.seg, Cuboid.findById(cubeDesc, baseCuboidId));
        CoprocessorFilter filter = CoprocessorFilter.fromFilter(this.seg, rootFilter);


        TableRecordInfo recordInfo = new TableRecordInfo(seg);
        CoprocessorProjector projector = CoprocessorProjector.makeForEndpoint(recordInfo, dimensionColumns);
        EndpointAggregators aggregators = EndpointAggregators.fromFunctions(metrics, recordInfo);

        IIProtos.IIRequest request = IIProtos.IIRequest.newBuilder().
                setTableInfo(ByteString.copyFrom(TableRecordInfoDigest.serialize(recordInfo))).
                setType(ByteString.copyFrom(CoprocessorRowType.serialize(type))).
                setFilter(ByteString.copyFrom(CoprocessorFilter.serialize(filter))).
                setProjector(ByteString.copyFrom(CoprocessorProjector.serialize(projector))).
                setAggregator(ByteString.copyFrom(EndpointAggregators.serialize(aggregators))).
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


    private void translateResult(TableRecord tableRecord, List<FunctionDesc> measures, List<String> measureValues, Tuple tuple) throws IOException {
        // groups
        List<TblColRef> columns = new ArrayList<>(this.cubeDesc.listDimensionColumnsIncludingDerived());
        List<String> columnNames = getColumnNames(columns);
        List<String> columnValues = tableRecord.getValueList();
        for (int i = 0; i < columnNames.size(); i++) {
            TblColRef column = columns.get(i);
            if (!tuple.hasColumn(column)) {
                continue;
            }
            tuple.setValue(columnNames.get(i), columnValues.get(i));
        }

        for (int i = 0; i < measures.size(); ++i) {
            tuple.setValue(measures.get(i).getRewriteFieldName(), measureValues.get(i));
        }

    }

    private TupleInfo buildTupleInfo() {
        TupleInfo info = new TupleInfo();
        int index = 0;
        List<TblColRef> columns = new ArrayList<>(this.cubeDesc.listDimensionColumnsIncludingDerived());
        List<String> columnNames = getColumnNames(columns);


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


    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public ITuple next() {
        return null;
    }

    @Override
    public void close() {

    }
}
