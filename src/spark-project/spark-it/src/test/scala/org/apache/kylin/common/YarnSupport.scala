/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.common

import org.apache.kylin.common.util.ClassUtil
import org.apache.spark.internal.Logging
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait YarnSupport
  extends BeforeAndAfterAll
    with BeforeAndAfterEach
    with Logging with SystemPropertyHelper {
  self: Suite =>

  override def beforeAll(): Unit = {
    ClassUtil.addClasspath(System.getenv("HADOOP_CONF_DIR"))
    changeSystemProp("kylin.env", "DEV")
    checkSystem("kylin.env.hdfs-working-dir")
    super.beforeAll()
    val env = KylinConfig.getInstanceFromEnv
  }

  override def afterAll(): Unit = {
    super.afterAll()
    restoreSystemProperty()
  }
}
