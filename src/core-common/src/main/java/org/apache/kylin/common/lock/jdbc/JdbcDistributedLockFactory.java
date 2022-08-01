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

package org.apache.kylin.common.lock.jdbc;

import java.util.concurrent.locks.Lock;

import javax.sql.DataSource;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLockFactory;
import org.springframework.integration.jdbc.lock.DefaultLockRepository;
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcDistributedLockFactory extends DistributedLockFactory {

    @Override
    public Lock getLockForClient(String client, String key) {
        DataSource dataSource = null;
        try {
            dataSource = JdbcDistributedLockUtil.getDataSource();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        DefaultLockRepository lockRepository = new DefaultLockRepository(dataSource, client);
        lockRepository.setPrefix(JdbcDistributedLockUtil.getGlobalDictLockTablePrefix());
        lockRepository.afterPropertiesSet();
        return new JdbcLockRegistry(lockRepository).obtain(key);
    }

    @Override
    public void initialize() {
        try {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            config.setJDBCDistributedLockURL(config.getJDBCDistributedLockURL().toString());
            JdbcDistributedLockUtil.createDistributedLockTableIfNotExist();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
