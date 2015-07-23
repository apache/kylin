package org.apache.kylin.engine.mr;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.IMRInput.IMRBatchCubingInputSide;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.IMROutput.IMRBatchCubingOutputSide;
import org.apache.kylin.engine.mr.IMROutput.IMRBatchMergeOutputSide;
import org.apache.kylin.engine.mr.IMROutput2.IMRBatchCubingOutputSide2;
import org.apache.kylin.engine.mr.IMROutput2.IMRBatchMergeInputSide2;
import org.apache.kylin.engine.mr.IMROutput2.IMRBatchMergeOutputSide2;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.TableSourceFactory;
import org.apache.kylin.storage.StorageFactory2;

public class MRUtil {

    public static IMRBatchCubingInputSide getBatchCubingInputSide(CubeSegment seg) {
        return TableSourceFactory.createEngineAdapter(seg, IMRInput.class).getBatchCubingInputSide(seg);
    }

    public static IMRTableInputFormat getTableInputFormat(String tableName) {
        return getTableInputFormat(getTableDesc(tableName));
    }

    public static IMRTableInputFormat getTableInputFormat(TableDesc tableDesc) {
        return TableSourceFactory.createEngineAdapter(tableDesc, IMRInput.class).getTableInputFormat(tableDesc);
    }

    private static TableDesc getTableDesc(String tableName) {
        return MetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getTableDesc(tableName);
    }

    public static IMRBatchCubingOutputSide getBatchCubingOutputSide(CubeSegment seg) {
        return StorageFactory2.createEngineAdapter(seg, IMROutput.class).getBatchCubingOutputSide(seg);
    }

    public static IMRBatchMergeOutputSide getBatchMergeOutputSide(CubeSegment seg) {
        return StorageFactory2.createEngineAdapter(seg, IMROutput.class).getBatchMergeOutputSide(seg);
    }

    public static IMRBatchCubingOutputSide2 getBatchCubingOutputSide2(CubeSegment seg) {
        return StorageFactory2.createEngineAdapter(seg, IMROutput2.class).getBatchCubingOutputSide(seg);
    }
    
    public static IMRBatchMergeInputSide2 getBatchMergeInputSide2(CubeSegment seg) {
        return StorageFactory2.createEngineAdapter(seg, IMROutput2.class).getBatchMergeInputSide(seg);
    }
    
    public static IMRBatchMergeOutputSide2 getBatchMergeOutputSide2(CubeSegment seg) {
        return StorageFactory2.createEngineAdapter(seg, IMROutput2.class).getBatchMergeOutputSide(seg);
    }
    
}
