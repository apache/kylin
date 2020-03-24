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

package org.apache.spark.memory

import java.lang.reflect.{Field, Method}

/* For example, I want to do this:
 *
 * sqlContext.catalog.client.getTable("default", "blah").properties
 *
 * but none of that is public to me in the shell.  Using this, I can now do:
 *
 * sqlContext.reflectField("catalog").reflectField("client").reflectMethod("getTable", Seq("default", "blah")).reflectField("properties")
 *
 * not perfect, but usable.
 */
object Reflector {

  def methods(obj: Any): Seq[String] = {
    _methods(obj.getClass).map(_.getName()).sorted
  }

  def _methods(cls: Class[_]): Seq[Method] = {
    if (cls == null) Seq()
    else cls.getDeclaredMethods ++ _methods(cls.getSuperclass)
  }

  def fields(obj: Any): Seq[String] = {
    _fields(obj.getClass).map(_.getName()).sorted
  }

  def _fields(cls: Class[_]): Seq[Field] = {
    if (cls == null) Seq()
    else cls.getDeclaredFields ++ _fields(cls.getSuperclass)
  }

  def findMethod(obj: Any, name: String): Method = {
    val method = _methods(obj.getClass).find(_.getName == name).get
    method.setAccessible(true)
    method
  }

  def get(obj: Any, name: String): Any = {
    val clz = obj.getClass
    val fields = _fields(clz)
    fields.find(_.getName == name).orElse {
      // didn't find an exact match, try again with name munging that happens for private vars
      fields.find(_.getName.endsWith("$$" + name))
    } match {
      case Some(f) =>
        f.setAccessible(true)
        f.get(obj)
      case None =>
        // not a field, maybe its actually a method in byte code
        val m = findMethod(obj, name)
        m.invoke(obj)
    }
  }

  implicit class ReflectorConversions(obj: Any) {
    def reflectField(name: String): Any = {
      get(obj, name)
    }

    def reflectMethod(name: String, args: Seq[Object]): Any = {
      findMethod(obj, name).invoke(obj, args: _*)
    }
  }

}
