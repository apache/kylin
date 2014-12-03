package com.kylinolap.storage.hbase.coprocessor.endpoint;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.kylinolap.cube.invertedindex.*;
import com.kylinolap.cube.measure.MeasureAggregator;
import com.kylinolap.storage.filter.BitMapFilterEvaluator;
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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by honma on 11/7/14.
 */
public class IIEndpoint extends IIProtos.RowsService
        implements Coprocessor, CoprocessorService {

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

        CoprocessorRowType type = null;
        CoprocessorProjector projector = null;
        EndpointAggregators aggregators = null;
        CoprocessorFilter filter = null;

        if (request.hasType()) {
            type = CoprocessorRowType.deserialize(request.getType().toByteArray());
        }
        if (request.hasProjector()) {
            projector = CoprocessorProjector.deserialize(request.getProjector().toByteArray());
        }
        if (request.hasAggregator()) {
            aggregators = EndpointAggregators.deserialize(request.getAggregator().toByteArray());
        }
        if (request.hasFilter()) {
            filter = CoprocessorFilter.deserialize(request.getFilter().toByteArray());
        }


        IIProtos.IIResponse response = null;
        RegionScanner innerScanner = null;
        HRegion region = null;
        try {
            ByteBuffer byteBuffer = request.getTableInfo().asReadOnlyByteBuffer();
            TableRecordInfoDigest tableInfo = TableRecordInfoDigest.deserialize(byteBuffer);

            region = env.getRegion();
            innerScanner = region.getScanner(buildScan());
            region.startRegionOperation();


            synchronized (innerScanner) {
                IIProtos.IIResponse.Builder responseBuilder = IIProtos.IIResponse.newBuilder();

                EndpointAggregationCache aggCache = new EndpointAggregationCache(aggregators);
                IIKeyValueCodec codec = new IIKeyValueCodec(tableInfo);
                for (Slice slice : codec.decodeKeyValue(new HbaseServerKVIterator(innerScanner))) {
                    ConciseSet result = null;
                    if (filter != null) {
                        result = new BitMapFilterEvaluator(new SliceBitMapProvider(slice, type)).evaluate(filter.getFilter());
                    }

                    Iterator<TableRecordBytes> iterator = slice.iterateWithBitmap(result);
                    while (iterator.hasNext()) {
                        byte[] data = iterator.next().getBytes();
                        CoprocessorProjector.AggrKey aggKey = projector.getAggrKey(data);
                        MeasureAggregator[] bufs = aggCache.getBuffer(aggKey);
                        aggregators.aggregate(bufs, data);
                        aggCache.checkMemoryUsage();
                    }
                }

                for (Map.Entry<CoprocessorProjector.AggrKey, MeasureAggregator[]> entry : aggCache.getAllEntries()) {
                    CoprocessorProjector.AggrKey aggrKey = entry.getKey();
                    IIRow.Builder rowBuilder = IIRow.newBuilder().
                            setColumns(ByteString.copyFrom(aggrKey.get(), aggrKey.offset(), aggrKey.length())).
                            setMeasures(ByteString.copyFrom(aggregators.serializeMetricValues(entry.getValue())));
                    responseBuilder.addRows(rowBuilder.build());
                }

                response = responseBuilder.build();
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
