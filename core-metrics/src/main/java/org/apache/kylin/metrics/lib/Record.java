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

package org.apache.kylin.metrics.lib;

import java.util.Map;

/**
 * Metrics event in Metrics System
 */
public interface Record {

    /**
     *  For classification, will mapping to Hive Table or Kafka Topic
     */
    String getSubject();

    /**
     *  For keep ordering in the same category
     */
    byte[] getKey();

    /**
     *  For the contents will be used
     */
    byte[] getValue();

    /**
     *  For the raw contents will be used
     */
    Map<String, Object> getValueRaw();

    /**
     *  For the timestamp the record created
     */
    Long getTime();

    Record clone();
}
