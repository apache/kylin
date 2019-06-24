/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.service;

import com.google.common.collect.Maps;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.measure.MeasureInstance;
import org.apache.kylin.metadata.model.DateTimeRange;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.rest.exception.BadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

@Service("measureMgmtService")
public class MeasureService extends BasicService{
    private final static Logger LOG = LoggerFactory.getLogger(MeasureService.class);

    public List<DateTimeRange> getSegmentRanges(String cubeName, String measureName) {
        CubeDesc cube = getCubeDesc(cubeName);
        if (cube == null) {
            return Collections.EMPTY_LIST;
        }
        return getSegmentRanges(cube, measureName);
    }

    public Map<String, List<DateTimeRange>> getMeasureSegmentsPair(String cubeName) {
        Map<String, List<DateTimeRange>> ret = Maps.newHashMap();
        CubeDesc cube = getCubeDesc(cubeName);
        if (cube == null) {
            return Collections.EMPTY_MAP;
        }
        List<MeasureInstance> measuresInCache = getMeasureManager().getMeasuresInCube(cubeName);
        List<MeasureDesc> measuresInCube = cube.getMeasures();
        if (measuresInCache.size() != measuresInCube.size()) {
            throw new IllegalStateException(String.format(Locale.ROOT, "The size of measures not equal. in cache: [%s], in cube: [%s]",
                    measuresInCache.stream().map(m -> m.getName()).collect(Collectors.joining(", ")),
                    measuresInCube.stream().map(m -> m.getName()).collect(Collectors.joining(", "))));
        }
        for (MeasureDesc measureDesc : measuresInCube) {
            ret.put(measureDesc.getName(), getSegmentRanges(cube, measureDesc.getName()));
        }
        return ret;
    }

    public int getSegmentCountOf(String cubeName, String measureName) {
        MeasureInstance measure = getMeasureManager().getMeasure(cubeName, measureName);
        return null == measure ? 0 : measure.getSegments().size();
    }

    public List<DateTimeRange> getSegmentRanges(CubeDesc cube, String measureName) {
        MeasureDesc measure = findMeasureByName(cube, measureName);
        MeasureInstance measureInstance = getMeasureManager().getMeasure(cube.getName(), measure.getName());
        if (measureInstance == null) {
            return Collections.EMPTY_LIST;
        }
        return measureInstance.getDateTimeRanges();
    }

    public List<String> getMeasuresOnSegment(String cubeName, String segmentName) {
        return getMeasureManager()
                .getMeasuresOnSegment(cubeName, segmentName)
                .stream()
                .map(MeasureInstance::getName)
                .collect(Collectors.toList());
    }

    public Map<String, List<String>> getMeasuresOnSegment(String cubeName) {
        CubeDesc cubeDesc = getCubeDesc(cubeName);
        if (null == cubeDesc) {
            return Collections.EMPTY_MAP;
        }
        CubeInstance cubeInstance = getCubeManager().getCube(cubeDesc.getName());
        Map<String, List<String>> ret = Maps.newHashMap();
        for (ISegment seg : cubeInstance.getSegments(SegmentStatusEnum.READY)) {
            ret.put(seg.getName(), getMeasuresOnSegment(cubeName, seg.getName()));
        }
        return ret;
    }

    private CubeDesc getCubeDesc(String cubeName) {
        CubeDesc cube = getCubeDescManager().getCubeDesc(cubeName);
        if (null == cube) {
            LOG.warn("Can't find cube {}, may be it's designning?", cubeName);
            return null;
        }
        return cube;
    }

    private MeasureDesc findMeasureByName(CubeDesc cube, String measureName) {
        for (MeasureDesc measureDesc : cube.getMeasures()) {
            if (measureName.equals(measureDesc.getName())) {
                return measureDesc;
            }
        }
        throw new BadRequestException(format(Locale.ROOT, "Can't find measure[%s] in cube[%s]", measureName, cube.getName()));
    }
}
