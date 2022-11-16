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

package io.kyligence.kap.secondstorage.test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.JdbcDatabaseContainer;

import io.kyligence.kap.newten.clickhouse.ClickHouseUtils;
import lombok.SneakyThrows;

public class ClickHouseClassRule extends ExternalResource {
    private final JdbcDatabaseContainer<?>[] clickhouse;
    public static int[] EXPOSED_PORTS = IntStream.range(24000, 25000).toArray();

    static {
        Testcontainers.exposeHostPorts(EXPOSED_PORTS);
    }

    public static int getAvailablePort() {
        synchronized (ClickHouseClassRule.class) {
            for (int port : EXPOSED_PORTS) {
                try {
                    ServerSocket s = new ServerSocket(port);
                    s.close();
                    return port;
                } catch (IOException e) {
                    // continue another port
                }
            }
        }
        throw new IllegalStateException("no available port found");
    }

    @SneakyThrows
    public ClickHouseClassRule(int n) {
        clickhouse = new JdbcDatabaseContainer<?>[n];
    }

    public JdbcDatabaseContainer<?>[] getClickhouse() {
        return clickhouse;
    }

    public JdbcDatabaseContainer<?> getClickhouse(int index) {
        Assert.assertTrue(index < clickhouse.length);
        return clickhouse[index];
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        //setup clickhouse
        for (int i = 0; i < clickhouse.length; i++) {
            clickhouse[i] = ClickHouseUtils.startClickHouse();
        }

    }

    @Override
    protected void after() {
        super.after();
        for (int i = 0; i < clickhouse.length; i++) {
            clickhouse[i].close();
            clickhouse[i] = null;
        }
    }
}
