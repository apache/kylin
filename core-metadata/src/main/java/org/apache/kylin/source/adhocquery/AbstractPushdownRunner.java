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

package org.apache.kylin.source.adhocquery;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractPushdownRunner implements IPushDownRunner {
    public static final Logger logger = LoggerFactory.getLogger(AbstractPushdownRunner.class);

    @Override
    public String convertSql(KylinConfig kylinConfig, String sql, String project, String defaultSchema,
                             boolean isPrepare) {
        String converted = sql;
        for (String converterName : kylinConfig.getPushDownConverterClassNames()) {
            IPushDownConverter converter = (IPushDownConverter) ClassUtil.newInstance(converterName);
            String tmp = converter.convert(converted, project, defaultSchema, isPrepare);
            if (!converted.equals(tmp)) {
                logger.info("the query is converted to {} after applying converter {}", tmp, converterName);
            }
            converted = tmp;
        }
        return converted;
    }
}
