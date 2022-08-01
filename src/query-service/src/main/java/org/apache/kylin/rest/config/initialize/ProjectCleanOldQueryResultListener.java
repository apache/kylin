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
package org.apache.kylin.rest.config.initialize;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.event.ProjectCleanOldQueryResultEvent;
import org.apache.kylin.rest.service.AsyncQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;

@Component
public class ProjectCleanOldQueryResultListener {

    @Autowired
    AsyncQueryService asyncQueryService;

    @Subscribe
    public void onCleanOldQueryResult(ProjectCleanOldQueryResultEvent event) throws IOException {

        asyncQueryService.cleanOldQueryResult(event.getProject(),
                KylinConfig.getInstanceFromEnv().getAsyncQueryResultRetainDays());
    }
}
