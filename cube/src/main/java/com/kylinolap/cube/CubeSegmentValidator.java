/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.cube;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.kylinolap.cube.exception.CubeIntegrityException;
import com.kylinolap.cube.model.CubeBuildTypeEnum;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.cube.model.CubePartitionDesc;
import com.kylinolap.cube.model.DimensionDesc;
import com.kylinolap.dict.DictionaryManager;
import com.kylinolap.metadata.model.SegmentStatusEnum;
import com.kylinolap.metadata.model.TblColRef;

/**
 * @author xduo
 */
public abstract class CubeSegmentValidator {

    private CubeSegmentValidator() {
    }

    public static CubeSegmentValidator getCubeSegmentValidator(CubeBuildTypeEnum buildType) {
        switch (buildType) {
        case MERGE:
            return new MergeOperationValidator();
        case BUILD:
            return new BuildOperationValidator();
        default:
            throw new RuntimeException("invalid build type:" + buildType);
        }
    }

    abstract void validate(CubeInstance cubeInstance, List<CubeSegment> newSegments) throws CubeIntegrityException;

    private static class MergeOperationValidator extends CubeSegmentValidator {
        private void checkContingency(CubeInstance cubeInstance, List<CubeSegment> newSegments) throws CubeIntegrityException {
            if (cubeInstance.getSegments().size() < 2) {
                throw new CubeIntegrityException("No segments to merge.");
            }
            if (newSegments.size() != 1) {
                throw new CubeIntegrityException("Invalid date range.");
            }

            CubeSegment newSegment = newSegments.get(0);
            CubeSegment startSeg = null;
            CubeSegment endSeg = null;
            for (CubeSegment segment : cubeInstance.getSegments()) {
                if (segment.getDateRangeStart() == newSegment.getDateRangeStart()) {
                    startSeg = segment;
                }
                if (segment.getDateRangeEnd() == newSegment.getDateRangeEnd()) {
                    endSeg = segment;
                }
            }

            if (null == startSeg || null == endSeg || startSeg.getDateRangeStart() >= endSeg.getDateRangeStart()) {
                throw new CubeIntegrityException("Invalid date range.");
            }
        }

        private void checkLoopTableConsistency(CubeInstance cube, List<CubeSegment> newSegments) throws CubeIntegrityException {

            CubeSegment cubeSeg = newSegments.get(0);
            DictionaryManager dictMgr = DictionaryManager.getInstance(cube.getConfig());
            List<CubeSegment> segmentList = cube.getMergingSegments(cubeSeg);

            HashSet<TblColRef> cols = new HashSet<TblColRef>();
            CubeDesc cubeDesc = cube.getDescriptor();
            for (DimensionDesc dim : cubeDesc.getDimensions()) {
                for (TblColRef col : dim.getColumnRefs()) {
                    // include those dictionaries that do not need mergning
                    try {
                        if (cubeSeg.getCubeDesc().getRowkey().isUseDictionary(col)) {
                            String dictTable = (String) dictMgr.decideSourceData(cubeDesc.getModel(), cubeDesc.getRowkey().getDictionary(col), col, null)[0];
                            if (!cubeDesc.getFactTable().equalsIgnoreCase(dictTable)) {
                                cols.add(col);
                            }
                        }
                    } catch (IOException e) {
                        throw new CubeIntegrityException("checkLoopTableConsistency not passed when allocating a new segment.");
                    }
                }
            }

            // check if all dictionaries on lookup table columns are identical
            for (TblColRef col : cols) {
                String dictOfFirstSegment = null;
                for (CubeSegment segment : segmentList) {
                    String temp = segment.getDictResPath(col);
                    if (temp == null) {
                        throw new CubeIntegrityException("Dictionary is null on column: " + col + " Segment: " + segment);
                    }

                    if (dictOfFirstSegment == null) {
                        dictOfFirstSegment = temp;
                    } else {
                        if (!dictOfFirstSegment.equalsIgnoreCase(temp)) {
                            throw new CubeIntegrityException("Segments with different dictionaries(on lookup table) cannot be merged");
                        }
                    }
                }
            }

            // check if all segments' snapshot are identical
            CubeSegment firstSegment = null;
            for (CubeSegment segment : segmentList) {
                if (firstSegment == null) {
                    firstSegment = segment;
                } else {
                    Collection<String> a = firstSegment.getSnapshots().values();
                    Collection<String> b = segment.getSnapshots().values();
                    if (!((a.size() == b.size()) && a.containsAll(b)))
                        throw new CubeIntegrityException("Segments with different snapshots cannot be merged");
                }
            }

        }

        @Override
        public void validate(CubeInstance cubeInstance, List<CubeSegment> newSegments) throws CubeIntegrityException {
            this.checkContingency(cubeInstance, newSegments);
            this.checkLoopTableConsistency(cubeInstance, newSegments);
        }
    }

    private static class BuildOperationValidator extends CubeSegmentValidator {

        @Override
        void validate(CubeInstance cubeInstance, List<CubeSegment> newSegments) throws CubeIntegrityException {
            List<CubeSegment> readySegments = cubeInstance.getSegments(SegmentStatusEnum.READY);
            CubePartitionDesc cubePartitionDesc = cubeInstance.getDescriptor().getCubePartitionDesc();
            final long initStartDate = cubePartitionDesc.getPartitionDateColumn() != null ? cubePartitionDesc.getPartitionDateStart() : 0;
            long startDate = initStartDate;
            for (CubeSegment readySegment: readySegments) {
                if (startDate == readySegment.getDateRangeStart() && startDate < readySegment.getDateRangeEnd()) {
                    startDate = readySegment.getDateRangeEnd();
                } else {
                    throw new CubeIntegrityException("there is gap in cube segments");
                }
            }
            if (newSegments.size() != 1) {
                throw new CubeIntegrityException("there are more than 2 segments");
            }
            final CubeSegment newSegment = newSegments.get(0);
            if (cubeInstance.appendOnHll()) {
                if (newSegment.getDateRangeStart() == initStartDate && startDate < newSegment.getDateRangeEnd()) {
                    return;
                }
            } else {
                if (newSegment.getDateRangeStart() == startDate) {
                    return;
                }
            }
            throw new CubeIntegrityException("invalid segment date range from " + newSegment.getDateRangeStart() + " to " + newSegment.getDateRangeEnd());
        }
    }

    private static class IncrementalBuildOperationValidator extends CubeSegmentValidator {
        /*
         * (non-Javadoc)
         *
         * @see
         * com.kylinolap.cube.CubeSegmentValidator#validate(com.kylinolap.cube
         * .CubeInstance, java.util.List)
         */
        @Override
        void validate(CubeInstance cubeInstance, List<CubeSegment> newSegments) throws CubeIntegrityException {
            if (newSegments.size() != 1) {
                throw new CubeIntegrityException("Invalid date range.");
            }
            CubeSegment newSegment = newSegments.get(0);
            if (cubeInstance.needMergeImmediatelyAfterBuild(newSegment)) {

            } else {
                // check if user will rebuild one specified segment
                boolean hasMatchSegment = false;
                for (CubeSegment segment : cubeInstance.getSegments()) {
                    if (segment.getDateRangeStart() == newSegment.getDateRangeStart()) {
                        if (segment.getDateRangeEnd() == newSegment.getDateRangeEnd()) {
                            hasMatchSegment = true;
                        } else {
                            throw new CubeIntegrityException("Invalid date range.");
                        }
                    }
                }

                if (!hasMatchSegment) {
                    if (cubeInstance.getSegments().size() == 0) {
                        if (cubeInstance.getDescriptor().getCubePartitionDesc().getPartitionDateStart() != newSegment.getDateRangeStart()) {
                            throw new CubeIntegrityException("Invalid start date.");
                        }
                    } else {
                        CubeSegment lastSegment = cubeInstance.getSegments().get(cubeInstance.getSegments().size() - 1);
                        if (newSegment.getDateRangeStart() != lastSegment.getDateRangeEnd()) {
                            throw new CubeIntegrityException("Invalid start date.");
                        }
                    }
                }
            }
        }

    }

    private static class UpdateBuildOperationValidator extends CubeSegmentValidator {

        /*
         * (non-Javadoc)
         *
         * @see
         * com.kylinolap.cube.CubeSegmentValidator#validate(com.kylinolap.cube
         * .CubeInstance, java.util.List)
         */
        @Override
        void validate(CubeInstance cubeInstance, List<CubeSegment> newSegments) throws CubeIntegrityException {
            if (newSegments.size() != 1 && newSegments.size() != 2) {
                throw new CubeIntegrityException("Invalid new segment count, got " + newSegments.size());
            }

            CubeSegment previousSeg = null;
            for (CubeSegment newSegment : newSegments) {
                if (null == previousSeg) {
                    previousSeg = newSegment;
                } else {
                    if (previousSeg.getDateRangeEnd() != newSegment.getDateRangeStart()) {
                        throw new CubeIntegrityException("Invalid date range.");
                    }
                }
            }

            if (cubeInstance.getSegments().size() == 0) {
                if (cubeInstance.getDescriptor().getCubePartitionDesc().getPartitionDateStart() != newSegments.get(0).getDateRangeStart()) {
                    throw new CubeIntegrityException("Invalid start date.");
                }
            } else {
                CubeSegment startSegment = newSegments.get(0);
                CubeSegment matchSeg = null;
                for (CubeSegment segment : cubeInstance.getSegments()) {
                    if (segment.getDateRangeStart() == startSegment.getDateRangeStart()) {
                        matchSeg = segment;
                    }
                }

                if (newSegments.size() == 2 && null == matchSeg) {
                    throw new CubeIntegrityException("Invalid date range.");
                }

                if (newSegments.size() == 2 && newSegments.get(newSegments.size() - 1).getDateRangeEnd() < matchSeg.getDateRangeEnd()) {
                    throw new CubeIntegrityException("Invalid date range.");
                }
            }
        }
    }

}
