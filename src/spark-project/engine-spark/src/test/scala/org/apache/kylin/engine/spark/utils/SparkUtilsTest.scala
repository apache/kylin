
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

package org.apache.kylin.engine.spark.utils

import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.reflect.ClassTag
import org.apache.spark._
import org.apache.spark.rdd.RDD

class SparkUtilsTest extends SparderBaseFunSuite with SharedSparkSession {

  case class EmptyPartition(index: Int) extends Partition

  private class MyRDD[T: ClassTag] extends RDD[T](sc, Nil) {
    private val mutableDependencies: ArrayBuffer[Dependency[_]] = ArrayBuffer.empty

    override def compute(p: Partition, c: TaskContext): Iterator[T] = Iterator.empty

    override def getPartitions: Array[Partition] = Array(EmptyPartition(0), EmptyPartition(1))

    override def getDependencies: Seq[Dependency[_]] = mutableDependencies

    def addDependency(dep: Dependency[_]) {
      mutableDependencies += dep
    }
  }

  test("test leafNodes and  leafNodePartitionNums") {
    val rdd1 = new MyRDD[Int]
    val rdd2 = new MyRDD[Int]
    val rdd3 = new MyRDD[Int]
    val rdd4 = new MyRDD[Int]
    val rdd5 = new MyRDD[Int]
    rdd1.addDependency(new OneToOneDependency[Int](rdd2))
    rdd1.addDependency(new OneToOneDependency[Int](rdd3))
    rdd2.addDependency(new OneToOneDependency[Int](rdd4))
    rdd2.addDependency(new OneToOneDependency[Int](rdd5))

    assert(SparkUtils.leafNodes(rdd1).contains(rdd3))
    assert(SparkUtils.leafNodes(rdd1).contains(rdd4))
    assert(SparkUtils.leafNodes(rdd1).contains(rdd5))

    assert(SparkUtils.leafNodePartitionNums(rdd1) == 6)
  }
}
