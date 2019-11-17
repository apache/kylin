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

package org.apache.kylin.engine.spark;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.kylin.engine.mr.common.BatchConstants;

public class SparkCreatingFlatTable extends SparkSqlBatch {
    public static final int SQL_COUNT = 5;

    public SparkCreatingFlatTable() {
        super();

        for (int i = 0; i < SQL_COUNT; i++) {
            getOptions().addOption(getSqlOption(i));
        }
    }

    public static Option getSqlOption(int index) {
        return OptionBuilder.withArgName(BatchConstants.ARG_SQL_COUNT + String.valueOf(index))
                .hasArg()
                .isRequired(true)
                .withDescription("Sql0")
                .create(BatchConstants.ARG_BASE64_ENCODED_SQL + String.valueOf(index));
    }
}
