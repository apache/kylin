package com.kylinolap.storage.hbase.coprocessor.endpoint;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.kylinolap.invertedindex.index.Slice;
import com.kylinolap.invertedindex.index.RawTableRecord;
import com.kylinolap.invertedindex.index.TableRecordInfoDigest;
import com.kylinolap.invertedindex.model.IIKeyValueCodec;
import com.kylinolap.metadata.measure.MeasureAggregator;
import com.kylinolap.storage.filter.BitMapFilterEvaluator;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorConstants;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorProjector;
import com.kylinolap.storage.hbase.coprocessor.endpoint.generated.IIProtos;

import static com.kylinolap.storage.hbase.coprocessor.endpoint.generated.IIProtos.IIResponse.IIRow;

import com.kylinolap.storage.hbase.coprocessor.CoprocessorFilter;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorRowType;
import it.uniroma3.mat.extendedset.intset.ConciseSet;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by honma on 11/7/14.
 */
public class IIEndpoint extends IIProtos.RowsService implements Coprocessor, CoprocessorService {

    private RegionCoprocessorEnvironment env;

    public IIEndpoint() {
    }

    private Scan buildScan() {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c"));

        return scan;
    }

    //TODO: protobuf does not provide built-in compression
    @Override
    public void getRows(RpcController controller, IIProtos.IIRequest request, RpcCallback<IIProtos.IIResponse> done) {

        CoprocessorRowType type;
        CoprocessorProjector projector;
        EndpointAggregators aggregators;
        CoprocessorFilter filter;

        type = CoprocessorRowType.deserialize(request.getType().toByteArray());
        projector = CoprocessorProjector.deserialize(request.getProjector().toByteArray());
        aggregators = EndpointAggregators.deserialize(request.getAggregator().toByteArray());
        filter = CoprocessorFilter.deserialize(request.getFilter().toByteArray());

        TableRecordInfoDigest tableRecordInfoDigest = aggregators.getTableRecordInfo();

        IIProtos.IIResponse response = null;
        RegionScanner innerScanner = null;
        HRegion region = null;
        try {
            region = env.getRegion();
            innerScanner = region.getScanner(buildScan());
            region.startRegionOperation();

            synchronized (innerScanner) {
                IIKeyValueCodec codec = new IIKeyValueCodec(tableRecordInfoDigest);
                //TODO pass projector to codec to skip loading columns
                Iterable<Slice> slices = codec.decodeKeyValue(new HbaseServerKVIterator(innerScanner));

                if (aggregators.isEmpty()) {
                    response = getNonAggregatedResponse(slices, filter, type);
                } else {
                    response = getAggregatedResponse(slices, filter, type, projector, aggregators);
                }
            }
        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        } finally {
            IOUtils.closeQuietly(innerScanner);
            if (region != null) {
                try {
                    region.closeRegionOperation();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }

        done.run(response);
    }

    //TODO check current memory checking is good enough
    private IIProtos.IIResponse getAggregatedResponse(Iterable<Slice> slices, CoprocessorFilter filter, CoprocessorRowType type, CoprocessorProjector projector, EndpointAggregators aggregators) {
        EndpointAggregationCache aggCache = new EndpointAggregationCache(aggregators);
        IIProtos.IIResponse.Builder responseBuilder = IIProtos.IIResponse.newBuilder();
        for (Slice slice : slices) {
            ConciseSet result = null;
            if (filter != null) {
                result = new BitMapFilterEvaluator(new SliceBitMapProvider(slice, type)).evaluate(filter.getFilter());
            }

            Iterator<RawTableRecord> iterator = slice.iterateWithBitmap(result);
            while (iterator.hasNext()) {
                byte[] data = iterator.next().getBytes();
                CoprocessorProjector.AggrKey aggKey = projector.getAggrKey(data);
                MeasureAggregator[] bufs = aggCache.getBuffer(aggKey);
                aggregators.aggregate(bufs, data);
                aggCache.checkMemoryUsage();
            }
        }

        byte[] metricBuffer = new byte[CoprocessorConstants.METRIC_SERIALIZE_BUFFER_SIZE];
        for (Map.Entry<CoprocessorProjector.AggrKey, MeasureAggregator[]> entry : aggCache.getAllEntries()) {
            CoprocessorProjector.AggrKey aggrKey = entry.getKey();
            IIRow.Builder rowBuilder = IIRow.newBuilder().setColumns(ByteString.copyFrom(aggrKey.get(), aggrKey.offset(), aggrKey.length()));
            int length = aggregators.serializeMetricValues(entry.getValue(), metricBuffer);
            rowBuilder.setMeasures(ByteString.copyFrom(metricBuffer, 0, length));
            responseBuilder.addRows(rowBuilder.build());
        }

        return responseBuilder.build();
    }

    private IIProtos.IIResponse getNonAggregatedResponse(Iterable<Slice> slices, CoprocessorFilter filter, CoprocessorRowType type) {
        IIProtos.IIResponse.Builder responseBuilder = IIProtos.IIResponse.newBuilder();
        for (Slice slice : slices) {
            ConciseSet result = null;
            if (filter != null) {
                result = new BitMapFilterEvaluator(new SliceBitMapProvider(slice, type)).evaluate(filter.getFilter());
            }

            Iterator<RawTableRecord> iterator = slice.iterateWithBitmap(result);
            while (iterator.hasNext()) {
                byte[] data = iterator.next().getBytes();
                IIRow.Builder rowBuilder = IIRow.newBuilder().setColumns(ByteString.copyFrom(data));
                responseBuilder.addRows(rowBuilder.build());
            }
        }

        return responseBuilder.build();
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
    }

    @Override
    public Service getService() {
        return this;
    }
}
