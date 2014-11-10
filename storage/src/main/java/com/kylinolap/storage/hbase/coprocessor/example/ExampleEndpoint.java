package com.kylinolap.storage.hbase.coprocessor.example;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.kylinolap.storage.hbase.coprocessor.example.generated.ExampleProtos;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by honma on 11/3/14.
 */

public class ExampleEndpoint extends ExampleProtos.RowCountService
        implements Coprocessor, CoprocessorService {
    private RegionCoprocessorEnvironment env;

    public ExampleEndpoint() {
    }

    /**
     * Just returns a reference to this object, which implements the RowCounterService interface.
     */
    @Override
    public Service getService() {
        return this;
    }

    /**
     * Returns a count of the rows in the region where this coprocessor is loaded.
     */
    @Override
    public void getRowCount(RpcController controller, ExampleProtos.CountRequest request,
            RpcCallback<ExampleProtos.CountResponse> done) {
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        ExampleProtos.CountResponse response = null;
        InternalScanner scanner = null;
        try {
            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<Cell>();
            boolean hasMore = false;
            byte[] lastRow = null;
            long count = 0;
            do {
                hasMore = scanner.next(results);
                for (Cell kv : results) {
                    byte[] currentRow = CellUtil.cloneRow(kv);
                    if (lastRow == null || !Bytes.equals(lastRow, currentRow)) {
                        lastRow = currentRow;
                        count++;
                    }
                }
                results.clear();
            } while (hasMore);

            response = ExampleProtos.CountResponse.newBuilder()
                    .setCount(count).build();
        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {}
            }
        }
        done.run(response);
    }

    /**
     * Returns a count of all KeyValues in the region where this coprocessor is loaded.
     */
    @Override
    public void getKeyValueCount(RpcController controller, ExampleProtos.CountRequest request,
            RpcCallback<ExampleProtos.CountResponse> done) {
        ExampleProtos.CountResponse response = null;
        InternalScanner scanner = null;
        try {
            scanner = env.getRegion().getScanner(new Scan());
            List<Cell> results = new ArrayList<Cell>();
            boolean hasMore = false;
            long count = 0;
            do {
                hasMore = scanner.next(results);
                for (Cell kv : results) {
                    count++;
                }
                results.clear();
            } while (hasMore);

            response = ExampleProtos.CountResponse.newBuilder()
                    .setCount(count).build();
        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {}
            }
        }
        done.run(response);
    }

    /**
     * Stores a reference to the coprocessor environment provided by the
     * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost} from the region where this
     * coprocessor is loaded.  Since this is a coprocessor endpoint, it always expects to be loaded
     * on a table region, so always expects this to be an instance of
     * {@link RegionCoprocessorEnvironment}.
     * @param env the environment provided by the coprocessor host
     * @throws IOException if the provided environment is not an instance of
     * {@code RegionCoprocessorEnvironment}
     */
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // nothing to do
    }
}
