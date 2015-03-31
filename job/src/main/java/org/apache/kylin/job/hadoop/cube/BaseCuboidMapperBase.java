package org.apache.kylin.job.hadoop.cube;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mr.KylinMapper;
import org.apache.kylin.common.util.BytesSplitter;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Hongbin Ma(Binmahone) on 3/27/15.
 */
public class BaseCuboidMapperBase<KEYIN, VALUEIN> extends KylinMapper<KEYIN, VALUEIN, Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(HiveToBaseCuboidMapper.class);
    public static final byte[] HIVE_NULL = Bytes.toBytes("\\N");
    public static final byte[] ONE = Bytes.toBytes("1");
    protected String cubeName;
    protected String segmentName;
    protected Cuboid baseCuboid;
    protected CubeInstance cube;
    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;
    protected List<byte[]> nullBytes;
    protected CubeJoinedFlatTableDesc intermediateTableDesc;
    protected String intermediateTableRowDelimiter;
    protected byte byteRowDelimiter;
    protected int counter;
    protected Object[] measures;
    protected byte[][] keyBytesBuf;
    protected BytesSplitter bytesSplitter;
    protected AbstractRowKeyEncoder rowKeyEncoder;
    protected MeasureCodec measureCodec;
    private int errorRecordCounter;
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME);
        intermediateTableRowDelimiter = context.getConfiguration().get(BatchConstants.CFG_CUBE_INTERMEDIATE_TABLE_ROW_DELIMITER, Character.toString(BatchConstants.INTERMEDIATE_TABLE_ROW_DELIMITER));
        if (Bytes.toBytes(intermediateTableRowDelimiter).length > 1) {
            throw new RuntimeException("Expected delimiter byte length is 1, but got " + Bytes.toBytes(intermediateTableRowDelimiter).length);
        }

        byteRowDelimiter = Bytes.toBytes(intermediateTableRowDelimiter)[0];

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);

        intermediateTableDesc = new CubeJoinedFlatTableDesc(cube.getDescriptor(), cubeSegment);

        bytesSplitter = new BytesSplitter(200, 4096);
        rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cubeSegment, baseCuboid);

        measureCodec = new MeasureCodec(cubeDesc.getMeasures());
        measures = new Object[cubeDesc.getMeasures().size()];

        int colCount = cubeDesc.getRowkey().getRowKeyColumns().length;
        keyBytesBuf = new byte[colCount][];

        initNullBytes();
    }

    private void initNullBytes() {
        nullBytes = Lists.newArrayList();
        nullBytes.add(HIVE_NULL);
        String[] nullStrings = cubeDesc.getNullStrings();
        if (nullStrings != null) {
            for (String s : nullStrings) {
                nullBytes.add(Bytes.toBytes(s));
            }
        }
    }

    private boolean isNull(byte[] v) {
        for (byte[] nullByte : nullBytes) {
            if (Bytes.equals(v, nullByte))
                return true;
        }
        return false;
    }

    private byte[] buildKey(SplittedBytes[] splitBuffers) {
        int[] rowKeyColumnIndexes = intermediateTableDesc.getRowKeyColumnIndexes();
        for (int i = 0; i < baseCuboid.getColumns().size(); i++) {
            int index = rowKeyColumnIndexes[i];
            keyBytesBuf[i] = Arrays.copyOf(splitBuffers[index].value, splitBuffers[index].length);
            if (isNull(keyBytesBuf[i])) {
                keyBytesBuf[i] = null;
            }
        }
        return rowKeyEncoder.encode(keyBytesBuf);
    }

    private void buildValue(SplittedBytes[] splitBuffers) {

        for (int i = 0; i < measures.length; i++) {
            byte[] valueBytes = getValueBytes(splitBuffers, i);
            measures[i] = measureCodec.getSerializer(i).valueOf(valueBytes);
        }

        valueBuf.clear();
        measureCodec.encode(measures, valueBuf);
    }

    private byte[] getValueBytes(SplittedBytes[] splitBuffers, int measureIdx) {
        MeasureDesc desc = cubeDesc.getMeasures().get(measureIdx);
        FunctionDesc func = desc.getFunction();
        ParameterDesc paramDesc = func.getParameter();
        int[] flatTableIdx = intermediateTableDesc.getMeasureColumnIndexes()[measureIdx];

        byte[] result = null;

        // constant
        if (flatTableIdx == null) {
            result = Bytes.toBytes(paramDesc.getValue());
        }
        // column values
        else {
            // for multiple columns, their values are joined
            for (int i = 0; i < flatTableIdx.length; i++) {
                SplittedBytes split = splitBuffers[flatTableIdx[i]];
                if (result == null) {
                    result = Arrays.copyOf(split.value, split.length);
                } else {
                    byte[] newResult = new byte[result.length + split.length];
                    System.arraycopy(result, 0, newResult, 0, result.length);
                    System.arraycopy(split.value, 0, newResult, result.length, split.length);
                    result = newResult;
                }
            }
        }

        if (func.isCount() || func.isHolisticCountDistinct()) {
            // note for holistic count distinct, this value will be ignored
            result = ONE;
        }

        if (isNull(result)) {
            result = null;
        }

        return result;
    }

    protected void outputKV(Context context) throws IOException, InterruptedException {
        intermediateTableDesc.sanityCheck(bytesSplitter);

        byte[] rowKey = buildKey(bytesSplitter.getSplitBuffers());
        outputKey.set(rowKey, 0, rowKey.length);

        buildValue(bytesSplitter.getSplitBuffers());
        outputValue.set(valueBuf.array(), 0, valueBuf.position());
        context.write(outputKey, outputValue);
    }

    protected void handleErrorRecord(BytesSplitter bytesSplitter, Exception ex) throws IOException {

        System.err.println("Insane record: " + bytesSplitter);
        ex.printStackTrace(System.err);

        errorRecordCounter++;
        if (errorRecordCounter > BatchConstants.ERROR_RECORD_THRESHOLD) {
            if (ex instanceof IOException)
                throw (IOException) ex;
            else if (ex instanceof RuntimeException)
                throw (RuntimeException) ex;
            else
                throw new RuntimeException("", ex);
        }
    }
}
