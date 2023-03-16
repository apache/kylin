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

package org.apache.kylin.engine.spark.job;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kylin.metadata.cube.model.NBatchConstants;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

public class MockResumeBuildJob extends DFBuildJob {
    // for ut only
    private volatile Set<String> breakPointLayouts;

    public static void main(String[] args) {
        MockResumeBuildJob buildJob = new MockResumeBuildJob();
        buildJob.execute(args);
    }

    @Override
    protected void extraInit() {
        // for ut only
        if (Objects.isNull(config) || !config.isUTEnv()) {
            return;
        }
        final String layoutsStr = getParam(NBatchConstants.P_BREAK_POINT_LAYOUTS);
        breakPointLayouts = org.apache.commons.lang3.StringUtils.isBlank(layoutsStr) ? Sets.newHashSet()
                : Stream.of(org.apache.commons.lang3.StringUtils.split(layoutsStr, ",")).collect(Collectors.toSet());
    }

    @Override
    protected void onLayoutFinished(long layoutId) {
        // for ut only
        final int sleep_secs = 10;
        if (Objects.nonNull(config) && config.isUTEnv() && breakPointLayouts.contains(String.valueOf(layoutId))) {
            logger.info("breakpoint BUILD_LAYOUT, sleep {} secs, layout {}", sleep_secs, layoutId);
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(sleep_secs));
            } catch (InterruptedException ie) {
                logger.error("buildLayer sleeping interrupted", ie);
            } finally {
                Thread.currentThread().interrupt();
            }
        }
    }
}
