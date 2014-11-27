package com.kylinolap.storage.hbase.endpoint;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.kylinolap.cube.invertedindex.*;
import com.kylinolap.cube.kv.RowValueDecoder;
import com.kylinolap.metadata.model.cube.HBaseColumnDesc;
import com.kylinolap.storage.filter.BitMapFilterEvaluator;
import com.kylinolap.storage.hbase.endpoint.generated.IIProtos;
import com.kylinolap.storage.hbase.observer.SRowAggregators;
import com.kylinolap.storage.hbase.observer.SRowFilter;
import com.kylinolap.storage.hbase.observer.SRowProjector;
import com.kylinolap.storage.hbase.observer.SRowType;
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

    @Override
    public void getRows(RpcController controller, IIProtos.IIRequest request, RpcCallback<IIProtos.IIResponse> done) {

        SRowType type = null;
        SRowProjector projector = null;
        SRowAggregators aggregators = null;
        SRowFilter filter = null;

        if (request.hasSRowType()) {
            type = SRowType.deserialize(request.getSRowType().toByteArray());
        }
        if (request.hasSRowProjector()) {
            projector = SRowProjector.deserialize(request.getSRowProjector().toByteArray());
        }
        if (request.hasSRowAggregator()) {
            aggregators = SRowAggregators.deserialize(request.getSRowAggregator().toByteArray());
        }
        if (request.hasSRowFilter()) {
            filter = SRowFilter.deserialize(request.getSRowFilter().toByteArray());
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

                IIKeyValueCodec codec = new IIKeyValueCodec(tableInfo);
                for (Slice slice : codec.decodeKeyValue(new HbaseServerKVIterator(innerScanner))) {
                    if (filter != null) {
                        ConciseSet result = new BitMapFilterEvaluator(new SliceBitMapProvider(slice, type)).evaluate(filter.getFilter());
                        int index = 0;
                        //TODO: should not use iterator mode for performance
                        for (TableRecordBytes recordBytes : slice) {
                            if (result.contains(index)) {
                                responseBuilder.addRows(ByteString.copyFrom(recordBytes.getBytes()));
                            }
                            index++;
                        }
                    } else {
                        for (TableRecordBytes recordBytes : slice) {
                            responseBuilder.addRows(ByteString.copyFrom(recordBytes.getBytes()));
                        }
                    }
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
