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

import java.io.IOException;

import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.common.restclient.Broadcaster.EVENT;
import org.apache.kylin.rest.service.CacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * CubeController is defined as Restful API entrance for UI.
 *
 * @author jianliu
 */
@Controller
@RequestMapping(value = "/cache")
public class CacheController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(CacheController.class);

    @Autowired
    private CacheService cacheService;

    /**
     * Wipe system cache
     *
     * @param type  {@link Broadcaster.TYPE}
     * @param event {@link Broadcaster.EVENT}
     * @param name
     * @return if the action success
     * @throws IOException
     */
    @RequestMapping(value = "/{type}/{name}/{event}", method = { RequestMethod.PUT })
    @ResponseBody
    public void wipeCache(@PathVariable String type, @PathVariable String event, @PathVariable String name) throws IOException {

        Broadcaster.TYPE wipeType = Broadcaster.TYPE.getType(type);
        EVENT wipeEvent = Broadcaster.EVENT.getEvent(event);

        logger.info("wipe cache type: " + wipeType + " event:" + wipeEvent + " name:" + name);

        switch (wipeEvent) {
        case CREATE:
        case UPDATE:
            cacheService.rebuildCache(wipeType, name);
            break;
        case DROP:
            cacheService.removeCache(wipeType, name);
            break;
        default:
            throw new RuntimeException("invalid type:" + wipeEvent);
        }
    }

    public void setCacheService(CacheService cacheService) {
        this.cacheService = cacheService;
    }
}
