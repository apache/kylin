package org.apache.kylin.job.cube;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

/**
 * Created by liuze on 2016/1/14 0014.
 */
public class UpdateCubeInfoAfterIndex extends AbstractExecutable {

    private static final String SEGMENT_ID = "segmentId";
    private static final String CUBE_NAME = "cubeName";
    private static final String CUBING_JOB_ID = "cubingJobId";

    public UpdateCubeInfoAfterIndex() {
        super();
    }

    public void setCubeName(String cubeName) {
        this.setParam(CUBE_NAME, cubeName);
    }

    private String getCubeName() {
        return getParam(CUBE_NAME);
    }

    public void setSegmentId(String segmentId) {
        this.setParam(SEGMENT_ID, segmentId);
    }

    private String getSegmentId() {
        return getParam(SEGMENT_ID);
    }



    public void setCubingJobId(String id) {
        setParam(CUBING_JOB_ID, id);
    }

    private String getCubingJobId() {
        return getParam(CUBING_JOB_ID);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager cubeManager = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = cubeManager.getCube(getCubeName());
        final CubeSegment segment = cube.getSegmentById(getSegmentId());

        long sourceCount = 0l;

        long sourceSize = 0l;


        long size = 0;
        boolean segmentReady = true;

        segment.setLastBuildJobID(getCubingJobId());
        segment.setLastBuildTime(System.currentTimeMillis());
        segment.setSizeKB(size);
        segment.setInputRecords(sourceCount);
        segment.setInputRecordsSize(sourceSize);

        try {
            if (segmentReady) {
                cubeManager.promoteNewlyBuiltSegments(cube, segment);
            } else {
                cubeManager.updateCube(cube);
            }
            segment.setStatus(SegmentStatusEnum.READY);
            List<CubeSegment> tobe =new ArrayList<>();
            tobe.add(segment);
            cube.setSegments(tobe);
            cube.setStatus(RealizationStatusEnum.READY);
            cubeManager.saveResource(cube);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to update cube after build", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }
}
