/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.spark.utils

import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.{SPARK_VERSION, SparkContext}
import org.apache.spark.util.Utils

import scala.reflect.runtime._

object KylinReflectUtils {
  private val rm = universe.runtimeMirror(getClass.getClassLoader)

  def getSessionState(sparkContext: SparkContext, kylinSession: Object): Any = {
    if (SPARK_VERSION.startsWith("2.4")) {
      var className: String =
        "org.apache.spark.sql.hive.KylinHiveSessionStateBuilder"
      if (!"hive".equals(sparkContext.getConf
        .get(CATALOG_IMPLEMENTATION.key, "in-memory"))) {
        className = "org.apache.spark.sql.hive.KylinSessionStateBuilder"
      }
      val tuple = createObject(className, kylinSession, None)
      val method = tuple._2.getMethod("build")
      method.invoke(tuple._1)
    } else {
      throw new UnsupportedOperationException("Spark version not supported")
    }
  }

  def createObject(className: String, conArgs: Object*): (Any, Class[_]) = {
    val clazz = Utils.classForName(className)
    val ctor = clazz.getConstructors.head
    ctor.setAccessible(true)
    (ctor.newInstance(conArgs: _*), clazz)
  }

  def createObject(className: String): (Any, Class[_]) = {
    val clazz = Utils.classForName(className)
    val ctor = clazz.getConstructors.head
    ctor.setAccessible(true)
    (ctor.newInstance(), clazz)
  }
}
