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

package org.apache.kylin.rest.session;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.MapSession;
import org.springframework.session.MapSessionRepository;
import org.springframework.session.PrincipalNameIndexResolver;
import org.springframework.session.Session;

public class MapIndexedSessionRepository extends MapSessionRepository
        implements FindByIndexNameSessionRepository<MapSession> {

    private static PrincipalNameIndexResolver nameIndexResolver = new PrincipalNameIndexResolver();

    private Map<String, Map<String, MapSession>> indexesMap;

    public MapIndexedSessionRepository(Map<String, Session> sessions, Map<String, Map<String, MapSession>> indexesMap) {
        super(sessions);
        this.indexesMap = indexesMap;
    }

    @Override
    public void save(MapSession session) {
        super.save(session);
        String index = nameIndexResolver.resolveIndexValueFor(session);
        if (index != null) {
            Map<String, MapSession> sessionMap = this.indexesMap.get(index);
            if (!session.getId().equals(session.getOriginalId()) && sessionMap != null) {
                sessionMap.remove(session.getOriginalId());
            }
            if (sessionMap == null) {
                sessionMap = new ConcurrentHashMap<>();
                sessionMap.put(session.getId(), findById(session.getId()));
                indexesMap.put(index, sessionMap);
            } else {
                sessionMap.put(session.getId(), findById(session.getId()));
            }
        }
    }

    @Override
    public void deleteById(String id) {
        super.deleteById(id);
        this.indexesMap.values().forEach(entry -> entry.remove(id));
    }

    @Override
    public Map<String, MapSession> findByIndexNameAndIndexValue(String indexName, String indexValue) {
        if (!PRINCIPAL_NAME_INDEX_NAME.equals(indexName)) {
            return Collections.emptyMap();
        }
        return indexesMap.get(indexValue);
    }

    @Override
    public Map<String, MapSession> findByPrincipalName(String principalName) {
        return FindByIndexNameSessionRepository.super.findByPrincipalName(principalName);
    }
}
