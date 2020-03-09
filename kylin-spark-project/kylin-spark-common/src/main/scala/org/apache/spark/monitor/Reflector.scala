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
