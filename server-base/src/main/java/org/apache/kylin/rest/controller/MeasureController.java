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

package org.apache.kylin.rest.controller;

import com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.DateTimeRange;
import org.apache.kylin.rest.response.SegmentResponse;
import org.apache.kylin.rest.service.MeasureService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Controller
@RequestMapping(value = "/measure")
public class MeasureController extends BasicController{
    private static final Logger LOG = LoggerFactory.getLogger(MeasureController.class);

    @Autowired
    @Qualifier("measureMgmtService")
    public MeasureService measureService;


    @GetMapping(value = "/segment/{cubeName}/{measureName}", produces = { "application/json" })
    @ResponseBody
    public SegmentResponse getSegmentRanges(@PathVariable String cubeName,
                                               @PathVariable String measureName) {
        if (!getConfig().isEditableMetricCube()) {
            return new SegmentResponse();
        }
        List<DateTimeRange> dtrs = measureService.getSegmentRanges(cubeName, measureName);
        int segCnt = measureService.getSegmentCountOf(cubeName, measureName);
        SegmentResponse ret = new SegmentResponse();
        ret.setSegmentCount(segCnt);
        ret.setTimeRanges(dtrs.stream().map(dr -> dr.toString()).collect(Collectors.toList()));
        return ret;
    }

    @GetMapping(value = "/segment/{cubeName}", produces = { "application/json" })
    @ResponseBody
    public Map<String, SegmentResponse> getSegmentRanges(@PathVariable String cubeName) {
        if (!getConfig().isEditableMetricCube()) {
            return Collections.EMPTY_MAP;
        }
        Map<String, List<DateTimeRange>> measureVsegment = measureService.getMeasureSegmentsPair(cubeName);
        Map<String, SegmentResponse> ret = Maps.newHashMapWithExpectedSize(measureVsegment.size());
        for (Map.Entry<String, List<DateTimeRange>> entry : measureVsegment.entrySet()) {
            SegmentResponse sr = new SegmentResponse();
            sr.setTimeRanges(entry.getValue().stream().map(dr -> dr.toString()).collect(Collectors.toList()));
            sr.setSegmentCount(measureService.getSegmentCountOf(cubeName, entry.getKey()));
            ret.put(entry.getKey(), sr);
        }
        return ret;
    }


    @GetMapping(value = "/{cubeName}", produces = { "application/json" })
    @ResponseBody
    public Map<String, List<String>> getMeasuresOnSegment(@PathVariable String cubeName) {
        if (!getConfig().isEditableMetricCube()) {
            return Collections.EMPTY_MAP;
        }
        Map<String, List<String>> segmentMeasurePair = measureService.getMeasuresOnSegment(cubeName);
        return segmentMeasurePair;
    }

    @GetMapping(value = "/{cubeName}/{segmentName}", produces = { "application/json" })
    @ResponseBody
    public List<String> getMeasuresOnSegment(@PathVariable String cubeName,
                                             @PathVariable String segmentName) {
        if (!getConfig().isEditableMetricCube()) {
            return Collections.EMPTY_LIST;
        }
        List<String> measureNames = measureService.getMeasuresOnSegment(cubeName, segmentName);
        return measureNames;
    }

    public KylinConfig getConfig() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (kylinConfig == null) {
            throw new IllegalArgumentException("Failed to load kylin config instance");
        }
        return kylinConfig;
    }

}
