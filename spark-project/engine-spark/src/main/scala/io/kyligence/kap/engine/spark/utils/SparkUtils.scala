
package io.kyligence.kap.engine.spark.utils

import org.apache.spark.rdd.RDD

object SparkUtils {

  def leafNodes(rdd: RDD[_]): List[RDD[_]] = {

    if (rdd.dependencies.isEmpty) {
      List(rdd)
    } else {
      rdd.dependencies.flatMap { dependency =>
        leafNodes(dependency.rdd)
      }.toList
    }
  }

  def leafNodePartitionNums(rdd: RDD[_]): Int = {
    leafNodes(rdd).map(_.partitions.length).sum
  }
}
