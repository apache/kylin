package org.apache.kylin.engine.mr.steps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.ByteArrayWritable;

import java.io.IOException;

/**
 */
public class MapContextGTRecordWriter extends KVGTRecordWriter {

    private static final Log logger = LogFactory.getLog(MapContextGTRecordWriter.class);
    protected MapContext<?, ?, ByteArrayWritable, ByteArrayWritable> mapContext;

    public MapContextGTRecordWriter(MapContext<?, ?, ByteArrayWritable, ByteArrayWritable> mapContext, CubeDesc cubeDesc, CubeSegment cubeSegment) {
        super(cubeDesc, cubeSegment);
        this.mapContext = mapContext;
    }

    @Override
    protected void writeAsKeyValue(ByteArrayWritable key, ByteArrayWritable value) throws IOException {
        try {
            mapContext.write(key, value);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

}
