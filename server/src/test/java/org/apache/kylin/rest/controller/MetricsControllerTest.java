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

import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public class MetricsControllerTest extends ServiceTestBase {

    @Autowired
    MetricsController metricsController;

    @Test
    public void testQuery() {
        System.setProperty("kylin.server.query-metrics2-enabled", "true");
        ResponseEntity metrics = metricsController.getMetrics(null);
        Assert.assertEquals(HttpStatus.OK, metrics.getStatusCode());
        Assert.assertTrue(metrics.getBody() instanceof String);
    }

}
