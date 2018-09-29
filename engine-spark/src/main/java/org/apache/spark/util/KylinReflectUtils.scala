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

package org.apache.spark.util

import java.lang.reflect.Field

import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.{SPARK_VERSION, SparkContext}

import scala.reflect.runtime._
import scala.reflect.runtime.universe._

object KylinReflectUtils {
  private val rm = universe.runtimeMirror(getClass.getClassLoader)


  def getSessionState(sparkContext: SparkContext, kylinSession: Object): Any = {
    if (SPARK_VERSION.startsWith("2.2")) {
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

  /**
    * Returns the field val from a object through reflection.
    *
    * @param name - name of the field being retrieved.
    * @param obj  - Object from which the field has to be retrieved.
    * @tparam T
    * @return
    */
  def getField[T: TypeTag : reflect.ClassTag](name: String, obj: T): Any = {
    val im = rm.reflect(obj)

    im.symbol.typeSignature.members.find(_.name.toString.equals(name))
      .map(l => im.reflectField(l.asTerm).get).getOrElse(null)
  }


  def createObject(className: String, conArgs: Object*): (Any, Class[_]) = {
    val clazz = Utils.classForName(className)
    val ctor = clazz.getConstructors.head
    ctor.setAccessible(true)
    (ctor.newInstance(conArgs: _*), clazz)
  }

  def getObjectField(clazz: Class[_], filedName: String): Field = {
    val field = clazz.getDeclaredField(filedName)
    field.setAccessible(true)
    field

  }
}
