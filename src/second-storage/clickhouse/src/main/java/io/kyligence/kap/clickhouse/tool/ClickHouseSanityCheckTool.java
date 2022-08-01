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

package io.kyligence.kap.clickhouse.tool;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NamedThreadFactory;

import io.kyligence.kap.clickhouse.job.ClickHouse;
import org.apache.kylin.common.util.Unsafe;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickHouseSanityCheckTool {

    public static void main(String[] args) throws InterruptedException {
        execute(args);
    }

    public static void execute(String[] args) throws InterruptedException {
        log.info("{}", args);
        SecondStorage.init(true);
        int threadNum = Integer.parseInt(args[0]);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(threadNum, threadNum, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new NamedThreadFactory("CLICKHOUSE-SANITY-CHECK"));
        val nodes = SecondStorageNodeHelper.getAllNames();
        List<Future<Boolean>> results = nodes.stream().map(node -> {
            val tool = new CheckTool(node);
            return executor.submit(tool);
        }).collect(Collectors.toList());
        List<String> failedNodes = new ArrayList<>();
        val it = results.listIterator();
        while (it.hasNext()) {
            val idx = it.nextIndex();
            val result = it.next();
            try {
                if (!result.get()) {
                    failedNodes.add(nodes.get(idx));
                }
            } catch (ExecutionException e) {
                failedNodes.add(nodes.get(idx));
            }
        }
        if (failedNodes.isEmpty()) {
            exit(0);
        } else {
            log.error("Nodes {} connect failed. Please check ClickHouse status", failedNodes);
            exit(1);
        }

    }

    public static void exit(int status) {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
            Unsafe.systemExit(status);
        }
    }


    public static class CheckTool implements Callable<Boolean> {
        private final String node;

        public CheckTool(final String node) {
            this.node = node;
        }

        private Boolean checkSingleNode(String node) {
            try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(node))) {
                clickHouse.connect();
                log.info("node {} connect success", node);
            } catch (SQLException e) {
                log.error("node {} connect failed", node, e);
                return false;
            }
            return true;
        }

        @Override
        public Boolean call() throws Exception {
            return checkSingleNode(node);
        }
    }

}
