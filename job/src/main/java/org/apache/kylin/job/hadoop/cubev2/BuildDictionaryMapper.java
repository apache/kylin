package org.apache.kylin.job.hadoop.cubev2;

import com.google.common.base.Function;
import com.google.common.collect.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.mr.KylinMapper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryInfoSerializer;
import org.apache.kylin.dict.lookup.HiveTableReader;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by shaoshi on 3/24/15.
 */
public class BuildDictionaryMapper<KEYIN> extends KylinMapper<KEYIN, HCatRecord, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(BuildDictionaryMapper.class);
    private String cubeName;
    private CubeInstance cube;
    private CubeSegment cubeSegment;
    private CubeDesc cubeDesc;

    private HCatSchema schema = null;
    private HyperLogLogPlusCounter hll;


    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private List<TblColRef> dimColumns;
    private SetMultimap<Integer, String> columnDistinctValueMap;
    private CuboidScheduler cuboidScheduler = null;
    private CubeJoinedFlatTableDesc intermediateTableDesc;
    private long baseCuboidId;
    private List<String> rowKeyValues = null;
    private int nRowKey;

    @Override
    protected void setup(Context context) throws IOException {
        super.publishConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata(conf);
        cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        String segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME).toUpperCase();
        cubeDesc = cube.getDescriptor();
        cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);
        dimColumns = cubeDesc.listDimensionColumnsExcludingDerived();
        hll = new HyperLogLogPlusCounter(16);
        columnDistinctValueMap = HashMultimap.create(); // key is col, value is a set of string values
        cuboidScheduler = new CuboidScheduler(cubeDesc);
        intermediateTableDesc = new CubeJoinedFlatTableDesc(cube.getDescriptor(), cubeSegment);
        baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        nRowKey = cubeDesc.getRowkey().getRowKeyColumns().length;

        rowKeyValues = Lists.newArrayList();
    }

    @Override
    public void map(KEYIN key, HCatRecord record, Context context) throws IOException, InterruptedException {
        String[] row = HiveTableReader.getRowAsStringArray(record);
        buildDictAndCount(row);
    }

    protected void buildDictAndCount(String[] row) {
        for (int i = 0; i < intermediateTableDesc.getRowKeyColumnIndexes().length; i++) {
            columnDistinctValueMap.put(i, row[intermediateTableDesc.getRowKeyColumnIndexes()[i]]);
        }

        putRowKeyToHLL(row, baseCuboidId); // recursively put all possible row keys to hll
    }

    protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
        Map<Integer, DictionaryInfo> dictionaries = buildDictionary();

        DictionaryInfoSerializer dictionaryInfoSerializer = new DictionaryInfoSerializer();
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, this.baseCuboidId);
        byte[] keyBuf;
        // output dictionary to reducer, key is the index of the col on row key;
        for (Integer rowKeyIndex : dictionaries.keySet()) {
            keyBuf = Bytes.toBytes(rowKeyIndex);
            outputKey.set(keyBuf);

            //serialize the dictionary to bytes;
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream dout = new DataOutputStream(buf);
            dictionaryInfoSerializer.serialize(dictionaries.get(rowKeyIndex), dout);
            dout.close();
            buf.close();
            byte[] dictionaryBytes = buf.toByteArray();
            outputValue.set(dictionaryBytes);

            context.write(outputKey, outputValue);
        }

        // output hll to reducer, key is -1
        keyBuf = Bytes.toBytes(-1);
        outputKey.set(keyBuf);
        ByteBuffer hllBuf = ByteBuffer.allocate(1024 * 1024);
        hll.writeRegisters(hllBuf);
        outputValue.set(hllBuf.array());
        outputKey.set(keyBuf, 0, keyBuf.length);
        context.write(outputKey, outputValue);
    }

    private void putRowKeyToHLL(String[] row, long cuboidId) {
        rowKeyValues.clear();
        long mask = Long.highestOneBit(baseCuboidId);
        // int actualLength = Long.SIZE - Long.numberOfLeadingZeros(baseCuboidId);
        for (int i = 0; i < nRowKey; i++) {
            if ((mask & cuboidId) == 1) {
                rowKeyValues.add(row[intermediateTableDesc.getRowKeyColumnIndexes()[i]]);
            }
            mask = mask >> 1;
        }

        String key = StringUtils.join(rowKeyValues, ",");
        hll.add(key);

        Collection<Long> children = cuboidScheduler.getSpanningCuboid(cuboidId);
        for (Long childId : children) {
            putRowKeyToHLL(row, childId);
        }

    }

    private Map<Integer, DictionaryInfo> buildDictionary() {
        Map<Integer, DictionaryInfo> dictionaryMap = Maps.newHashMap();
        for (int i = 0; i < intermediateTableDesc.getRowKeyColumnIndexes().length; i++) {
            // dictionary
            if (cubeDesc.getRowkey().isUseDictionary(i)) {
                TblColRef col = cubeDesc.getRowkey().getRowKeyColumns()[i].getColRef();
                Dictionary dict = DictionaryGenerator.buildDictionaryFromValueList(col.getType(), Collections2.transform(columnDistinctValueMap.get(i), new Function<String, byte[]>() {
                    @Nullable
                    @Override
                    public byte[] apply(String input) {
                        return input.getBytes();
                    }
                }));

                logger.info("Building dictionary for " + col);
                DictionaryInfo dictInfo = new DictionaryInfo(col.getTable(), col.getName(), 0, col.getDatatype(), null, "");
                dictInfo.setDictionaryObject(dict);
                dictInfo.setDictionaryClass(dict.getClass().getName());
                dictionaryMap.put(i, dictInfo);
            }
        }

        return dictionaryMap;
    }

}
