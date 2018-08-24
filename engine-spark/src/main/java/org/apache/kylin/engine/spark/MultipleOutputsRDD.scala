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

package org.apache.kylin.engine.spark

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.{DataInputBuffer, Writable}
import org.apache.hadoop.mapred.RawKeyValueIterator
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.counters.GenericCounter
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl.DummyReporter
import org.apache.hadoop.mapreduce.task.{ReduceContextImpl, TaskAttemptContextImpl}
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob, TaskAttemptID, TaskType}
import org.apache.hadoop.util.Progress
import org.apache.spark._
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class MultipleOutputsRDD[K, V](self: RDD[(String, (K, V, String))])
                              (implicit kt: ClassTag[K], vt: ClassTag[V]) extends Serializable {

  def saveAsNewAPIHadoopDatasetWithMultipleOutputs(conf: Configuration) {
    val hadoopConf = conf
    val job = NewAPIHadoopJob.getInstance(hadoopConf)
    val formatter = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
    val jobtrackerID = formatter.format(new Date())
    val stageId = self.id
    val jobConfiguration = job.getConfiguration
    val wrappedConf = new SerializableWritable(jobConfiguration)
    val outfmt = job.getOutputFormatClass
    val jobFormat = outfmt.newInstance

    if (conf.getBoolean("spark.hadoop.validateOutputSpecs", true)) {
      jobFormat.checkOutputSpecs(job)
    }

    val writeShard = (context: TaskContext, itr: Iterator[(String, (K, V, String))]) => {
      val config = wrappedConf.value

      val attemptId = new TaskAttemptID(jobtrackerID, stageId, TaskType.REDUCE, context.partitionId,
        context.attemptNumber)
      val hadoopContext = new TaskAttemptContextImpl(config, attemptId)
      val format = outfmt.newInstance

      format match {
        case c: Configurable => c.setConf(wrappedConf.value)
        case _ => ()
      }

      val committer = format.getOutputCommitter(hadoopContext)
      committer.setupTask(hadoopContext)

      val recordWriter = format.getRecordWriter(hadoopContext).asInstanceOf[RecordWriter[K, V]]

      val taskInputOutputContext = new ReduceContextImpl(wrappedConf.value, attemptId, new InputIterator(itr), new GenericCounter, new GenericCounter,
        recordWriter, committer, new DummyReporter, null, kt.runtimeClass, vt.runtimeClass)

      // use hadoop MultipleOutputs
      val writer = new MultipleOutputs(taskInputOutputContext)

      try {
        while (itr.hasNext) {
          val pair = itr.next()
          writer.write(pair._1, pair._2._1, pair._2._2, pair._2._3)
        }
      } finally {
        writer.close()
      }
      committer.commitTask(hadoopContext)
      1
    }: Int

    val jobAttemptId = new TaskAttemptID(jobtrackerID, stageId, TaskType.MAP, 0, 0)
    val jobTaskContext = new TaskAttemptContextImpl(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    self.context.runJob(self, writeShard)
    jobCommitter.commitJob(jobTaskContext)
  }

  class InputIterator(itr: Iterator[_]) extends RawKeyValueIterator {
    def getKey: DataInputBuffer = null
    def getValue: DataInputBuffer = null
    def getProgress: Progress = null
    def next = itr.hasNext
    def close() { }
  }
}

object MultipleOutputsRDD {
  def rddToMultipleOutputsRDD[K, V](rdd: JavaPairRDD[String, (Writable, Writable, String)]) = {
    new MultipleOutputsRDD(rdd)
  }
}
