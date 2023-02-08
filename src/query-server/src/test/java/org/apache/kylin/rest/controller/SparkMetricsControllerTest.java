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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.rest.service.MonitorService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class SparkMetricsControllerTest {

    @Mock
    private MonitorService monitorService;

    @InjectMocks
    private SparkMetricsController nMonitorController = Mockito.spy(new SparkMetricsController());

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetSpark3MetricsForPrometheus() {
        Mockito.when(monitorService.fetchAndMergeSparkMetrics()).thenReturn("");
        String spark3MetricsForPrometheus = nMonitorController.getSparkMetricsForPrometheus();
        Assert.assertTrue(StringUtils.isBlank(spark3MetricsForPrometheus));
    }
}
