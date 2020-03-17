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

import org.springframework.session.ExpiringSession;
import org.springframework.session.SessionRepository;
import org.springframework.session.web.http.SessionRepositoryFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class KylinSessionFilter<S extends ExpiringSession> extends SessionRepositoryFilter<S> {

    private boolean externalSessionRepositoryExists;

    public KylinSessionFilter(SessionRepository<S> sessionRepository) {
        super(sessionRepository != null ? sessionRepository : new NoopSessionRepository<>());
        this.externalSessionRepositoryExists = (sessionRepository != null);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        if (externalSessionRepositoryExists) {
            super.doFilterInternal(request, response, filterChain);
        } else {
            filterChain.doFilter(request, response);
        }
    }

    public boolean isExternalSessionRepositoryExists() {
        return externalSessionRepositoryExists;
    }

    private static class NoopSessionRepository<ES extends ExpiringSession> implements SessionRepository<ES> {
        @Override
        public ES createSession() {
            return null;
        }

        @Override
        public void save(ES session) {
            // do nothing
        }

        @Override
        public ES getSession(String id) {
            return null;
        }

        @Override
        public void delete(String id) {
            // do nothing
        }
    }
}
