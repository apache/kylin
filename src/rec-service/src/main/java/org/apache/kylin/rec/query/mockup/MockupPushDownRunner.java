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

package org.apache.kylin.rec.query.mockup;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.source.adhocquery.IPushDownRunner;

public class MockupPushDownRunner implements IPushDownRunner {
    @Override
    public void init(KylinConfig config) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void init(KylinConfig config, String project) {
        init(config);
    }

    @Override
    public void executeQuery(String query, List<List<String>> returnRows, List<SelectedColumnMeta> returnColumnMeta,
            String project) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void executeUpdate(String sql, String project) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return QueryContext.PUSHDOWN_MOCKUP;
    }
}
