package com.kylinolap.storage.hbase.coprocessor;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.kylinolap.cube.invertedindex.*;
import com.kylinolap.storage.hbase.coprocessor.generated.IIProtos;
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

    @Override
    public void getRows(RpcController controller, IIProtos.IIRequest request, RpcCallback<IIProtos.IIResponse> done) {
        IIProtos.IIResponse response = null;
        RegionScanner innerScanner = null;
        HRegion region = null;
        try {
            ByteBuffer byteBuffer = request.getTableInfo().asReadOnlyByteBuffer();
            TableRecordInfoDigest tableInfo = TableRecordInfoDigest.deserialize(byteBuffer);

            region = env.getRegion();
            innerScanner = region.getScanner(new Scan());
            region.startRegionOperation();

            synchronized (innerScanner) {
                IIProtos.IIResponse.Builder responseBuilder = IIProtos.IIResponse.newBuilder();

                IIKeyValueCodec codec = new IIKeyValueCodec(tableInfo);
                for (Slice slice : codec.decodeKeyValue(new HbaseServerKVIterator(innerScanner))) {
                    for (TableRecordBytes recordBytes : slice) {
                        responseBuilder.addRows(ByteString.copyFrom(recordBytes.getBytes()));
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
