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
package org.apache.spark.ddl;

import static org.apache.kylin.common.exception.ServerErrorCode.DDL_CHECK_ERROR;

import org.apache.kylin.common.exception.KylinException;

public interface DDLCheck extends Comparable<DDLCheck> {

  default String[] description(String project, String pageType) {
    return new String[] {"", ""};
  }

  void check(DDLCheckContext context);

  default void throwException(String msg) {
    throw new KylinException(DDL_CHECK_ERROR, msg);
  }

  default int priority() {
    return Integer.MAX_VALUE;
  }

  @Override
  default int compareTo(DDLCheck other) {
    return this.priority() - other.priority();
  }
}
