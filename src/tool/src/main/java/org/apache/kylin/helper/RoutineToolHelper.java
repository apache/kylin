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

package org.apache.kylin.helper;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.query.util.QueryHisStoreUtil;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.metadata.streaming.util.StreamingJobRecordStoreUtil;
import org.apache.kylin.metadata.streaming.util.StreamingJobStatsStoreUtil;
import org.apache.kylin.tool.garbage.GarbageCleaner;
import org.apache.kylin.tool.garbage.SourceUsageCleaner;
import org.apache.kylin.tool.garbage.StorageCleaner;

import java.util.List;
import java.util.stream.Collectors;

/*
 * this class is only for removing dependency of kylin-tool module, and should be refactor later
 */
@Slf4j
public class RoutineToolHelper {

    private RoutineToolHelper() {
    }

    public static void cleanQueryHistories() {
        QueryHisStoreUtil.cleanQueryHistory();
    }

    public static void cleanStreamingStats() {
        StreamingJobStatsStoreUtil.cleanStreamingJobStats();
        StreamingJobRecordStoreUtil.cleanStreamingJobRecord();
    }

    public static void deleteRawRecItems() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<ProjectInstance> projectInstances = NProjectManager.getInstance(config).listAllProjects().stream()
                .filter(projectInstance -> !projectInstance.isExpertMode()).collect(Collectors.toList());
        if (projectInstances.isEmpty()) {
            return;
        }
        try (SetThreadName ignored = new SetThreadName("DeleteRawRecItemsInDB")) {
            val jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
            jdbcRawRecStore.deleteOutdated();
        } catch (Exception e) {
            log.error("delete outdated advice fail: ", e);
        }
    }

    public static void cleanGlobalSourceUsage() {
        log.info("Start to clean up global meta");
        try {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                new SourceUsageCleaner().cleanup();
                return null;
            }, UnitOfWork.GLOBAL_UNIT);
        } catch (Exception e) {
            log.error("Failed to clean global meta", e);
        }
        log.info("Clean up global meta finished");

    }

    public static void cleanMetaByProject(String projectName) {
        log.info("Start to clean up {} meta", projectName);
        try {
            GarbageCleaner.cleanMetadata(projectName);
        } catch (Exception e) {
            log.error("Project[{}] cleanup Metadata failed", projectName, e);
        }
        log.info("Clean up {} meta finished", projectName);
    }

    public static void cleanMeta(List<String> projectsToCleanup) {
        try {
            cleanGlobalSourceUsage();
            for (String projName : projectsToCleanup) {
                cleanMetaByProject(projName);
            }
            cleanQueryHistories();
            cleanStreamingStats();
            deleteRawRecItems();
            System.out.println("Metadata cleanup finished");
        } catch (Exception e) {
            log.error("Metadata cleanup failed", e);
            System.out.println(StorageCleaner.ANSI_RED
                    + "Metadata cleanup failed. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr"
                    + StorageCleaner.ANSI_RESET);
        }

    }

}
