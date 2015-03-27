package org.apache.kylin.job.hadoop.invertedindex;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.invertedindex.model.IIKeyValueCodec;
import org.apache.kylin.invertedindex.model.IIRow;
import org.apache.kylin.streaming.Stream;
import org.apache.kylin.streaming.invertedindex.IIStreamBuilder;

/**
 * Created by Hongbin Ma(Binmahone) on 3/26/15.
 *
 * A IIStreamBuilder that can hold all the built slices in form of IIRow
 * This is only for test use
 */
public class ToyIIStreamBuilder extends IIStreamBuilder {
    private List<IIRow> result;

    public ToyIIStreamBuilder(BlockingQueue<Stream> queue, IIDesc desc, int partitionId, List<IIRow> result) {
        super(queue, null, desc, partitionId);
        this.result = result;
    }

    protected void outputSlice(Slice slice, TableRecordInfo tableRecordInfo) throws IOException {
        IIKeyValueCodec codec = new IIKeyValueCodec(tableRecordInfo.getDigest());
        for (IIRow iiRow : codec.encodeKeyValue(slice)) {
            result.add(iiRow);
        }
    }

}
