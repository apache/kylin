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

package org.apache.kylin.query.udf;

import java.sql.Timestamp;

import org.apache.calcite.sql.type.NotConstant;
import org.apache.kylin.common.exception.CalciteNotSupportException;

public class SparkLeafUDF implements NotConstant {
    public String UUID() throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String CURRENT_DATABASE() throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Long INPUT_FILE_BLOCK_LENGTH() throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Long INPUT_FILE_BLOCK_START() throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String INPUT_FILE_NAME() throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Long MONOTONICALLY_INCREASING_ID() throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Timestamp NOW() throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer SPARK_PARTITION_ID() throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }
}
