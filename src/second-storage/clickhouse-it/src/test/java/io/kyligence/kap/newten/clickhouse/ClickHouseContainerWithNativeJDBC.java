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
package io.kyligence.kap.newten.clickhouse;

import java.time.Duration;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

/**
 *  Duplicated implementation of  {@link ClickHouseContainer} which uses {@link com.github.housepower.jdbc.ClickHouseDriver}
 *  <p/>
 *  We use native driver {@link com.github.housepower.jdbc.ClickHouseDriver} instead.
 */
public class ClickHouseContainerWithNativeJDBC extends JdbcDatabaseContainer<ClickHouseContainerWithNativeJDBC> {
    public static final String NAME = "clickhouse";

    public static final String DRIVER_CLASS_NAME = "com.github.housepower.jdbc.ClickHouseDriver";
    public static final String JDBC_URL_PREFIX = "jdbc:" + NAME + "://";
    public static final String TEST_QUERY = "SELECT version()";

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("clickhouse/clickhouse-server");

    public static final String IMAGE = DEFAULT_IMAGE_NAME.getUnversionedPart();

    public static final String DEFAULT_TAG = "20.10.3.30";

    public static final Integer HTTP_PORT = 8123;
    public static final Integer NATIVE_PORT = 9000;

    private String databaseName = "default";
    private String username = "default";
    private String password = "";

    public ClickHouseContainerWithNativeJDBC() {
        this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
    }

    public ClickHouseContainerWithNativeJDBC(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public ClickHouseContainerWithNativeJDBC(final DockerImageName dockerImageName) {
        super(dockerImageName);

        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

        withExposedPorts(HTTP_PORT, NATIVE_PORT);
        waitingFor(new HttpWaitStrategy().forStatusCode(200).forResponsePredicate("Ok."::equals)
                .withStartupTimeout(Duration.ofMinutes(1)));
    }

    @Override
    protected Integer getLivenessCheckPort() {
        return getMappedPort(NATIVE_PORT);
    }

    @Override
    public String getDriverClassName() {
        return DRIVER_CLASS_NAME;
    }

    @Override
    public String getJdbcUrl() {
        return JDBC_URL_PREFIX + getHost() + ":" + getMappedPort(NATIVE_PORT);
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getTestQueryString() {
        return TEST_QUERY;
    }

    @Override
    public ClickHouseContainerWithNativeJDBC withUrlParam(String paramName, String paramValue) {
        throw new UnsupportedOperationException("The ClickHouse does not support this");
    }
}
