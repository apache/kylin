package org.apache.spark.util

import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.{SPARK_VERSION, SparkContext}

import scala.reflect.runtime.universe

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
