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
import java.util.Iterator;
import java.util.Map;

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
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.invertedindex.index.RawTableRecord;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecordInfoDigest;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.invertedindex.model.IIKeyValueCodec;
import org.apache.kylin.metadata.measure.MeasureAggregator;
import org.apache.kylin.storage.filter.BitMapFilterEvaluator;
import org.apache.kylin.storage.hbase.coprocessor.AggrKey;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorConstants;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorFilter;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorProjector;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorRowType;
import org.apache.kylin.storage.hbase.coprocessor.endpoint.generated.IIProtos;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import it.uniroma3.mat.extendedset.intset.ConciseSet;

/**
 * Created by honma on 11/7/14.
 */
public class IIEndpoint extends IIProtos.RowsService implements Coprocessor, CoprocessorService {

    private RegionCoprocessorEnvironment env;

    public IIEndpoint() {
    }

    private Scan buildScan() {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(IIDesc.HBASE_FAMILY), Bytes.toBytes(IIDesc.HBASE_QUALIFIER));

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

        TableRecordInfoDigest tableRecordInfoDigest = aggregators.getTableRecordInfoDigest();

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
            System.out.println(ioe.toString());
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
                AggrKey aggKey = projector.getAggrKey(data);
                MeasureAggregator[] bufs = aggCache.getBuffer(aggKey);
                aggregators.aggregate(bufs, data);
                aggCache.checkMemoryUsage();
            }
        }

        byte[] metricBuffer = new byte[CoprocessorConstants.METRIC_SERIALIZE_BUFFER_SIZE];
        for (Map.Entry<AggrKey, MeasureAggregator[]> entry : aggCache.getAllEntries()) {
            AggrKey aggrKey = entry.getKey();
            IIProtos.IIResponse.IIRow.Builder rowBuilder = IIProtos.IIResponse.IIRow.newBuilder().setColumns(ByteString.copyFrom(aggrKey.get(), aggrKey.offset(), aggrKey.length()));
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
                IIProtos.IIResponse.IIRow.Builder rowBuilder = IIProtos.IIResponse.IIRow.newBuilder().setColumns(ByteString.copyFrom(data));
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
