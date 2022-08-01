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

package org.apache.kylin.query.routing.rules;

import java.util.List;

import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.routing.RoutingRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class RealizationSortRule extends RoutingRule {
    private static final Logger logger = LoggerFactory.getLogger(RealizationSortRule.class);

    @Override
    public void apply(List<Candidate> candidates) {
        if (candidates.isEmpty())
            return;
        StringBuilder sb = new StringBuilder();
        for (Candidate candidate : candidates) {
            sb.append(candidate.getRealization().getCanonicalName()).append(" cost ")
                    .append(candidate.getCapability().cost).append(". ");
        }
        logger.debug(sb.toString());

        candidates.sort(Candidate.COMPARATOR);
    }
}
