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

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.Set;

import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.rest.service.QueryResourceService;
import org.apache.kylin.rest.service.QueryResourceService.QueryResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/api/resource/query", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class QueryResourceController {

    @Autowired
    private QueryResourceService queryResourceService;

    @PutMapping(value = "/adjust")
    @ResponseBody
    public QueryResource adjustQueryResource(@RequestBody QueryResource resource) {
        if (queryResourceService.isAvailable()) {
            return queryResourceService.adjustQueryResource(resource);
        }
        return new QueryResource();
    }

    @GetMapping(value = "/queueNames")
    @ResponseBody
    public Set<String> getQueueNames() {
        Set<String> queues = Sets.newHashSet();
        if (queryResourceService.isAvailable()) {
            queues.add(queryResourceService.getQueueName());
        }
        return queues;
    }

}
