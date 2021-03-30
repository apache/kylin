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

import org.apache.kylin.rest.service.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/jmetrics")
public class MetricsController extends BasicController{

    private static final Logger LOG = LoggerFactory.getLogger(MetricsController.class);

    @Autowired
    private MetricsService metricsService;

    @RequestMapping(value = "", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public ResponseEntity getAllMetrics() {
        return this.getMetrics(null);
    }

    @RequestMapping(value = "/{type}", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public ResponseEntity getMetrics(@PathVariable String type) {

        String metrics = null;
        try {
            metrics = metricsService.getMetrics(type);
        } catch (IllegalStateException e) {
            LOG.error("get metrics error, type:{}", type, e);
            return new ResponseEntity(e.getMessage(), HttpStatus.BAD_REQUEST);
        } catch (Exception e) {
            LOG.error("get metrics error, type:{}", type, e);
            return new ResponseEntity("Internal error", HttpStatus.BAD_REQUEST);
        }

        return new ResponseEntity(metrics, HttpStatus.OK);
    }
}
