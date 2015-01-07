package com.kylinolap.job2.cube;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.job2.constants.ExecutableConstants;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.ExecutableContext;
import com.kylinolap.job2.execution.ExecuteResult;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by qianzhou on 1/7/15.
 */
public class UpdateCubeInfoAfterMergeExecutable extends AbstractExecutable {

    private static final String CUBE_NAME = "cubeName";
    private static final String SEGMENT_ID = "segmentId";
    private static final String MERGING_SEGMENT_IDS = "mergingSegmentIds";
    private static final String CONVERT_TO_HFILE_STEP_ID = "convertToHFileStepId";

    private final CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeInstance cube = cubeManager.getCube(getCubeName());
        List<String> mergingSegmentIds = getMergingSegmentIds();
        if (mergingSegmentIds.isEmpty()) {
            return new ExecuteResult(ExecuteResult.State.FAILED, "there are no merging segments");
        }
        CubeSegment mergedSegment = cube.getSegmentById(getSegmentId());
        if (mergedSegment == null) {
            return new ExecuteResult(ExecuteResult.State.FAILED, "there is no segment with id:" + getSegmentId());
        }
        String cubeSizeString = jobService.getOutput(getConvertToHfileStepId()).getExtra().get(ExecutableConstants.HDFS_BYTES_WRITTEN);
        Preconditions.checkState(StringUtils.isNotEmpty(cubeSizeString), "Can't get cube segment size.");
        long cubeSize = Long.parseLong(cubeSizeString) / 1024;

        List<CubeSegment> toBeRemoved = Lists.newArrayListWithExpectedSize(mergingSegmentIds.size());
        for (CubeSegment segment : cube.getSegments()) {
            if (mergingSegmentIds.contains(segment.getUuid())) {
                toBeRemoved.add(segment);
            }
        }

        long sourceCount = 0L;
        long sourceSize = 0L;
        for (CubeSegment segment : toBeRemoved) {
            sourceCount += segment.getSourceRecords();
            sourceSize += segment.getSourceRecordsSize();
        }
        //update segment info
        mergedSegment.setSizeKB(cubeSize);
        mergedSegment.setSourceRecords(sourceCount);
        mergedSegment.setSourceRecordsSize(sourceSize);
        //remove old segment
        cube.getSegments().removeAll(toBeRemoved);
        try {
            cubeManager.updateCube(cube);
            return new ExecuteResult(ExecuteResult.State.SUCCEED);
        } catch (IOException e) {
            logger.error("fail to update cube after merge", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    public void setSegmentId(String segmentId) {
        this.setParam(SEGMENT_ID, segmentId);
    }

    private String getSegmentId() {
        return getParam(SEGMENT_ID);
    }

    public void setCubeName(String cubeName) {
        this.setParam(CUBE_NAME, cubeName);
    }

    private String getCubeName() {
        return getParam(CUBE_NAME);
    }

    public void setMergingSegmentIds(List<String> ids) {
        setParam(MERGING_SEGMENT_IDS, StringUtils.join(ids, ","));
    }

    private List<String> getMergingSegmentIds() {
        final String ids = getParam(MERGING_SEGMENT_IDS);
        if (ids != null) {
            final String[] splitted = StringUtils.split(ids, ",");
            ArrayList<String> result = Lists.newArrayListWithExpectedSize(splitted.length);
            for (String id: splitted) {
                result.add(id);
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    public void setConvertToHFileStepId(String id) {
        setParam(CONVERT_TO_HFILE_STEP_ID, id);
    }

    private String getConvertToHfileStepId() {
        return getParam(CONVERT_TO_HFILE_STEP_ID);
    }
}
