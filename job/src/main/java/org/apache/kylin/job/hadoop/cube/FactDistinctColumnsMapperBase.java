package org.apache.kylin.job.hadoop.cube;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mr.KylinMapper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * Created by Hongbin Ma(Binmahone) on 3/26/15.
 */
public class FactDistinctColumnsMapperBase<KEYIN, VALUEIN> extends KylinMapper<KEYIN, VALUEIN, LongWritable, Text> {

    protected String cubeName;
    protected CubeInstance cube;
    protected CubeDesc cubeDesc;
    protected long baseCuboidId;
    protected List<TblColRef> columns;
    protected ArrayList<Integer> factDictCols;

    protected LongWritable outputKey = new LongWritable();
    protected Text outputValue = new Text();
    protected int errorRecordCounter =0;

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        publishConfiguration(conf);
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        columns = Cuboid.findById(cubeDesc, baseCuboidId).getColumns();

        factDictCols = new ArrayList<Integer>();
        RowKeyDesc rowKey = cubeDesc.getRowkey();
        DictionaryManager dictMgr = DictionaryManager.getInstance(config);
        for (int i = 0; i < columns.size(); i++) {
            TblColRef col = columns.get(i);
            if (!rowKey.isUseDictionary(col))
                continue;

            String scanTable = (String) dictMgr.decideSourceData(cubeDesc.getModel(), cubeDesc.getRowkey().getDictionary(col), col, null)[0];
            if (cubeDesc.getModel().isFactTable(scanTable)) {
                factDictCols.add(i);
            }
        }
    }

    protected void handleErrorRecord(HCatRecord record, Exception ex) throws IOException {

        System.err.println("Insane record: " + record.getAll());
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
