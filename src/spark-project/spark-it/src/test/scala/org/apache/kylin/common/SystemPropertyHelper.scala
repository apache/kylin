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

import com.google.common.collect.Maps
import org.apache.kylin.common.util.Unsafe

import scala.collection.JavaConverters._

trait SystemPropertyHelper {
  val propCache: java.util.HashMap[String, String] = Maps.newHashMap[String, String]()

  def changeSystemProp(key: String, value: String): Unit = {
    propCache.put(key, System.getProperty(key))
    Unsafe.setProperty(key, value)
  }

  def restoreSystemProperty(): Unit = {
    propCache.asScala.filter(_._2 != null).foreach {
      case (key, value) =>
        Unsafe.setProperty(key, value)
    }
  }

  def checkSystem(key: String, desc: String = ""): Unit = {
    if (System.getProperty(key) == null) {
      var errorMessage = s"Could not found system property : $key. "
      if (desc.nonEmpty) {
        errorMessage = errorMessage + desc
      }
      throw new IllegalArgumentException(errorMessage)
    }
  }

}
