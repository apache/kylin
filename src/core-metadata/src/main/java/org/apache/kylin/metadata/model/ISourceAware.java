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

package org.apache.kylin.metadata.model;

import org.apache.kylin.common.KylinConfig;

public interface ISourceAware {

    int ID_HIVE = 0;
    int ID_STREAMING = 1;
    int ID_SPARKSQL = 5;
    int ID_EXTERNAL = 7;
    int ID_JDBC = 8;
    int ID_SPARK = 9;
    int ID_CSV = 11;
    int ID_FILE = 13;

    int getSourceType();

    KylinConfig getConfig();
}
