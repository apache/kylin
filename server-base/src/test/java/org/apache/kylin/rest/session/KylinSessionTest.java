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

import org.junit.Test;
import org.springframework.session.ExpiringSession;
import org.springframework.session.MapSessionRepository;
import org.springframework.session.web.http.SessionRepositoryFilter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KylinSessionTest {
    @Test
    public void testSessionFilter() {
        KylinSessionConfig config = new KylinSessionConfig();
        SessionRepositoryFilter<? extends ExpiringSession> filter = config.springSessionRepositoryFilter(null);

        assertTrue("filter type should be KylinSessionFilter",
                filter instanceof KylinSessionFilter);
        assertFalse("filter should has no external repository",
                ((KylinSessionFilter<?>) filter).isExternalSessionRepositoryExists());

        filter = config.springSessionRepositoryFilter(new MapSessionRepository());
        assertTrue("filter type should be KylinSessionFilter",
                filter instanceof KylinSessionFilter);
        assertTrue("filter should has external repository",
                ((KylinSessionFilter<?>) filter).isExternalSessionRepositoryExists());
    }
}
