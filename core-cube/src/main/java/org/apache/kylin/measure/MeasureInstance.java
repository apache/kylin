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

package org.apache.kylin.measure;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.model.DateTimeRange;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.apache.kylin.metadata.model.SegmentRange.TSRange;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class MeasureInstance extends RootPersistentEntity {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureInstance.class);

    public static MeasureInstance createMeasureInstance(MeasureDesc measureDesc, CubeInstance cube) {
        MeasureInstance measureInstance = new MeasureInstance();
        measureInstance.setName(measureDesc.getName());
        measureInstance.setMeasureDesc(measureDesc);
        measureInstance.setCubeName(cube.getName());
        measureInstance.setSegments(new Segments<>());
        measureInstance.setSegmentsName(Lists.newArrayList());
        measureInstance.setOnline(true);
        return measureInstance;
    }

    public static MeasureInstance copy(MeasureInstance needCopy) {
        MeasureInstance copy = new MeasureInstance();
        copy.setName(needCopy.getName());
        copy.setCubeName(needCopy.getCubeName());
        copy.setSegmentsName(Lists.newArrayList(needCopy.getSegmentsName()));
        Segments<ISegment> copySegs = new Segments<>();
        // just copy reference of segments, you can't modify any element in segments here.
        needCopy.getSegments().forEach(s -> copySegs.add(s));
        copy.setSegments(copySegs);
        copy.setOnline(copy.isOnline());
        copy.setLastModified(needCopy.getLastModified());
        copy.setUuid(needCopy.getUuid());
        copy.setDateTimeRanges(needCopy.getDateTimeRanges());
        return copy;
    }

    public static String getResourceName(String cubeName, String measureName) {
        return cubeName + "/" + measureName;
    }

    @JsonProperty("name")
    private String name;
    @JsonProperty("cube_name")
    private String cubeName;
    @JsonProperty("segments_name")
    private List<String> segmentsName;
    @JsonProperty("segments")
    @JsonIgnore
    private Segments<ISegment> segments;
    @JsonProperty("online")
    private boolean online = true;

    private MeasureDesc measureDesc;

    private List<DateTimeRange> dateTimeRanges;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCubeName() {
        return cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public Segments<ISegment> getSegments() {
        return segments;
    }

    public void setSegments(Segments<ISegment> segments) {
        this.segments = segments;
    }

    public List<String> getSegmentsName() {
        return segmentsName;
    }

    public void setSegmentsName(List<String> segmentsName) {
        this.segmentsName = segmentsName;
    }

    public void setOnline(boolean online) {
        this.online = online;
    }

    public boolean isOnline() {
        return online;
    }

    public List<DateTimeRange> getDateTimeRanges() {
        return dateTimeRanges;
    }

    public MeasureDesc getMeasureDesc() {
        return measureDesc;
    }

    public void setMeasureDesc(MeasureDesc measureDesc) {
        this.measureDesc = measureDesc;
    }

    public void setDateTimeRanges(List<DateTimeRange> dateTimeRanges) {
        this.dateTimeRanges = dateTimeRanges;
    }

    public void init(CubeInstance cube) {
        refreshSegments(cube);
        this.measureDesc = findMeasureByName(this.name, cube);
    }

    private MeasureDesc findMeasureByName(String measureName, CubeInstance cube) {
        for (MeasureDesc m : cube.getMeasures()) {
            if (m.getName().equals(measureName)) {
                return m;
            }
        }
        LOG.warn("Can't find measure by name: {}, maybe it has been deleted in CubeDesc: {} ", measureName, cube.getDescriptor().getName());
        return null;
    }

    public void refreshSegments(CubeInstance cube) {
        // refresh segments
        if (null == this.segments) {
            this.segments = new Segments<>();
        }
        this.segments.clear();
        Iterator<? extends ISegment> cubeSegIter = cube.getSegments(SegmentStatusEnum.READY).iterator();
        while (cubeSegIter.hasNext()) {
            ISegment seg = cubeSegIter.next();
            if (this.getSegmentsName().contains(seg.getName())) {
                this.segments.add(seg);
            }
        }
        if (this.segments.size() != this.segmentsName.size()) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Segment names size doesn't equal segments size. Segment names: %s, name in segments: %s, Cube: %s",
                    this.segmentsName,
                    this.segments.stream().map(ISegment::getName).collect(Collectors.toList()),
                    cubeName));
        }

        // resort segment name
        for (int i = 0; i < this.segments.size(); i++) {
            this.segmentsName.set(i, segments.get(i).getName());
        }

        // refresh date time ranges
        if (this.dateTimeRanges == null) {
            dateTimeRanges = Lists.newArrayListWithCapacity(4);
        }
        dateTimeRanges.clear();
        if (segments.size() == 0) {
            return;
        }


        long startTime = segments.getTSStart();

        for (int i = 1; i < segments.size(); i++) {
            TSRange lastTsRange = segments.get(i - 1).getTSRange();
            TSRange tsRange = segments.get(i).getTSRange();
            if (tsRange.start.v < lastTsRange.end.v) {
                throw new IllegalStateException(String.format(Locale.ROOT, "Overlap time range, [%s, %s]  [%s, %s]",
                        lastTsRange.start.v, lastTsRange.end.v, tsRange.start.v, tsRange.end.v));
            } else if (tsRange.start.v > lastTsRange.end.v) {
                // there have a hole
                dateTimeRanges.add(DateTimeRange.create(startTime, lastTsRange.end.v));
                // change cursor
                startTime = tsRange.start.v;
            } else {
                continue;
            }
        }
        long endTime = segments.getTSEnd();
        dateTimeRanges.add(DateTimeRange.create(startTime, endTime));
    }

    public String getKey(){
        return getResourceName(cubeName, name);
    }

    @Override
    public String resourceName() {
        return getKey();
    }

    public boolean isOnSegment(ISegment s) {
        return isOnSegment(s.getName());
    }

    public boolean isOnSegment(String segName) {
        return segmentsName.contains(segName);
    }

    public void updateRandomUuid() {
        setUuid(RandomUtil.randomUUID().toString());
    }

    @Override
    public String toString() {
        return getKey();
    }
}
