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

import com.kylinolap.cube.CubeDescManager;
import com.kylinolap.invertedindex.IIDescManager;
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
     *            {@link Broadcaster.TYPE}
     * @param event
     *            {@link Broadcaster.EVENT}
     * @param name
     * @return if the action success
     * @throws IOException
     */
    @RequestMapping(value = "/{type}/{name}/{event}", method = { RequestMethod.PUT })
    @ResponseBody
    public void wipeCache(@PathVariable String type, @PathVariable String event, @PathVariable String name) throws IOException {
        Broadcaster.TYPE wipeType = Broadcaster.TYPE.getType(type);
        EVENT wipeEvent = Broadcaster.EVENT.getEvent(event);
        final String log = "wipe cache type: " + wipeType + " event:" + wipeEvent + " name:" + name;
        logger.info(log);
        switch (wipeType) {
            case TABLE:
                switch (wipeEvent) {
                    case CREATE:
                    case UPDATE:
                        cubeMgmtService.getMetadataManager().reloadTableCache(name);
                        break;
                    case DROP:
                        throw new UnsupportedOperationException(log);
                    default:
                        break;
                }
                IIDescManager.clearCache();
                CubeDescManager.clearCache();
                break;
            case DATA_MODEL:
                switch (wipeEvent) {
                    case CREATE:
                    case UPDATE:
                        cubeMgmtService.getMetadataManager().reloadDataModelDesc(name);
                        break;
                    case DROP:
                        throw new UnsupportedOperationException(log);
                }
                IIDescManager.clearCache();
                CubeDescManager.clearCache();
                break;
            case CUBE:
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
            case INVERTED_INDEX_DESC:
                switch (wipeEvent) {
                    case CREATE:
                    case UPDATE:
                        cubeMgmtService.getIIDescManager().reloadIIDesc(name);
                        break;
                    case DROP:
                        cubeMgmtService.getIIDescManager().removeIIDescLocal(name);
                        break;
                }
                break;
            case CUBE_DESC:
                switch (wipeEvent) {
                    case CREATE:
                    case UPDATE:
                        cubeMgmtService.getCubeDescManager().reloadCubeDesc(name);
                        break;
                    case DROP:
                        cubeMgmtService.getCubeDescManager().removeLocalCubeDesc(name);
                        break;
                }
                break;
            case PROJECT:
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
            case INVERTED_INDEX:
                switch (wipeEvent) {
                    case CREATE:
                    case UPDATE:
                        cubeMgmtService.getIIManager().loadIICache(name);
                        break;
                    case DROP:
                        cubeMgmtService.getIIManager().removeIILocalCache(name);
                        break;
                }
                break;
            default:
                throw new UnsupportedOperationException(log);
        }
    }
}
