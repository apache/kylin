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
package com.kylinolap.rest.controller;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.kylinolap.common.restclient.Broadcaster;
import com.kylinolap.common.restclient.Broadcaster.EVENT;
import com.kylinolap.metadata.MetadataConstances;
import com.kylinolap.rest.service.CubeService;
import com.kylinolap.rest.service.ProjectService;

/**
 * CubeController is defined as Restful API entrance for UI.
 * 
 * @author jianliu
 * 
 */
@Controller
@RequestMapping(value = "/cache")
public class CacheController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(CacheController.class);

    @Autowired
    private CubeService cubeMgmtService;

    @Autowired
    private ProjectService projectService;

    /**
     * Wipe system cache
     * 
     * @param type
     *            {@link MetadataConstances.TYPE}
     * @param event
     *            {@link MetadataConstances.EVENT}
     * @param name
     * @return if the action success
     * @throws IOException
     */
    @RequestMapping(value = "/{type}/{name}/{event}", method = { RequestMethod.PUT })
    @ResponseBody
    public void wipeCache(@PathVariable String type, @PathVariable String event, @PathVariable String name) throws IOException {
        Broadcaster.TYPE wipeType = Broadcaster.TYPE.getType(type);
        EVENT wipeEvent = Broadcaster.EVENT.getEvent(event);
        switch (wipeType) {
        case METADATA:
            logger.debug("Reload all metadata");
            cubeMgmtService.reloadMetadataCache();
            projectService.cleanDataCache();
            cubeMgmtService.cleanDataCache();
            break;
        case CUBE:
            logger.debug("Reload cube " + name + " with type:" + type + ", event type " + event);
            cubeMgmtService.reloadMetadataCache();
            if ("ALL".equalsIgnoreCase(name.toUpperCase())) {
                cubeMgmtService.cleanDataCache();
                break;
            }

            switch (wipeEvent) {
            case CREATE:
            case UPDATE:
                cubeMgmtService.reloadCubeCache(name);
                break;
            case DROP:
                cubeMgmtService.removeCubeCache(name);
                break;
            }
            break;
        case PROJECT:
            logger.debug("Reload project " + name + " with type:" + type + ", event type " + event);
            cubeMgmtService.reloadMetadataCache();
            if ("ALL".equalsIgnoreCase(name.toUpperCase())) {
                projectService.cleanDataCache();
                break;
            }

            switch (wipeEvent) {
            case CREATE:
            case UPDATE:
                projectService.reloadProjectCache(name);
                break;
            case DROP:
                projectService.removeProjectCache(name);
                break;
            }
            break;
        }
    }
}
