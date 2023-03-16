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

package org.apache.kylin.cluster

import org.apache.kylin.guava30.shaded.common.collect.Sets
import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationReport, QueueInfo, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.sql.common.LocalMetadata
import org.junit.Assert
import org.mockito.Mockito.{doNothing, mock, mockStatic, when}
import org.scalatest.funsuite.AnyFunSuite

import java.util

class YarnClusterManagerTest extends AnyFunSuite with LocalMetadata {

  test("YarnClusterManager setYarnClient") {
    YarnClusterManager.setYarnClient(null)
  }

  test("YarnClusterManager destroyYarnClient") {
    YarnClusterManager.destroyYarnClient()
  }

  test("YarnClusterManager newYarnClient null") {
    val yarnClusterManager = mock(classOf[YarnClusterManager])
    when(yarnClusterManager.getApplicationNameById(7070)).thenReturn("stepIdName")
    Assert.assertEquals("stepIdName", yarnClusterManager.getApplicationNameById(7070))
  }

  test("YarnClusterManager getApplicationNameById 0") {
    val yarnClusterManager = YarnClusterManager.setYarnClient(mock(classOf[YarnClient]))
    Assert.assertEquals(0, yarnClusterManager.getApplicationNameById(1).length)
    YarnClusterManager.destroyYarnClient()
  }

  test("YarnClusterManager getApplicationNameById with false") {
    val yarnClient = mock(classOf[YarnClient])

    val report = mock(classOf[ApplicationReport])
    val types = Sets.newHashSet("SPARK")
    val states = util.EnumSet.of(YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
      YarnApplicationState.SUBMITTED, YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING)
    when(yarnClient.getApplications(types, states)).thenReturn(util.Collections.singletonList(report))
    when(report.getName).thenReturn("fakeStepIdName")
    when(report.getApplicationId).thenReturn(mock(classOf[ApplicationId]))
    when(report.getApplicationId.getId).thenReturn(2)

    val yarnClusterManager = YarnClusterManager.setYarnClient(yarnClient)
    Assert.assertEquals("", yarnClusterManager.getApplicationNameById(1))
    YarnClusterManager.destroyYarnClient()
  }

  test("YarnClusterManager getApplicationNameById with fakeStepIdName") {
    val yarnClient = mock(classOf[YarnClient])

    val report = mock(classOf[ApplicationReport])
    val types = Sets.newHashSet("SPARK")
    val states = util.EnumSet.of(YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
      YarnApplicationState.SUBMITTED, YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING)
    when(yarnClient.getApplications(types, states)).thenReturn(util.Collections.singletonList(report))
    when(report.getName).thenReturn("fakeStepIdName")
    when(report.getApplicationId).thenReturn(mock(classOf[ApplicationId]))
    when(report.getApplicationId.getId).thenReturn(1)

    val yarnClusterManager = YarnClusterManager.setYarnClient(yarnClient)
    Assert.assertEquals("fakeStepIdName", yarnClusterManager.getApplicationNameById(1))
    YarnClusterManager.destroyYarnClient()
  }

  test("YarnClusterManager getSpecifiedConfFromWriteCluster") {
    System.setProperty("spark.hadoop.123", "123")
    val configuration: YarnConfiguration = YarnClusterManager.getSpecifiedConfFromWriteCluster
    val result: String = configuration.get("123", "0")
    Assert.assertEquals("123", result)
    System.clearProperty("spark.hadoop.123")
  }

  test("YarnClusterManager withYarnClientFromWriteCluster") {
    System.setProperty("spark.hadoop.123", "123")
    YarnClusterManager.withYarnClientFromWriteCluster(client => {
      Assert.assertNotNull(client)
      val result = client.getConfig.get("123", "0")
      Assert.assertEquals("123", result)
    })
    System.clearProperty("spark.hadoop.123")
  }

  test("YarnClusterManager killApplication") {
    val yarnClientStatic = mockStatic(classOf[YarnClient])
    try {
      val ycm = new YarnClusterManager
      val jobStepId = "fakeStepId"
      val yarnClient = mock(classOf[YarnClient])
      val applicationId = mock(classOf[ApplicationId])
      yarnClientStatic.when(() => YarnClient.createYarnClient()).thenReturn(yarnClient)
      ycm.killApplication(jobStepId)

      val report = mock(classOf[ApplicationReport])
      val types = Sets.newHashSet("SPARK")
      val states = util.EnumSet.of(YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
        YarnApplicationState.SUBMITTED, YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING)
      when(yarnClient.getApplications(types, states)).thenReturn(new util.ArrayList[ApplicationReport]())
      ycm.killApplication(jobStepId)

      when(yarnClient.getApplications(types, states)).thenReturn(util.Collections.singletonList(report))
      when(report.getName).thenReturn("job_step_" + jobStepId)
      when(report.getApplicationId).thenReturn(applicationId)
      doNothing().when(yarnClient).killApplication(applicationId)
      ycm.killApplication(jobStepId)
    } finally {
      yarnClientStatic.close()
    }
  }

  test("YarnClusterManager getRunningJobs") {
    val yarnClientStatic = mockStatic(classOf[YarnClient])
    try {
      val ycm = new YarnClusterManager
      val jobStepId = "job_step_fakeStepId"
      val queue = "queue"
      val queues = org.apache.kylin.guava30.shaded.common.collect.Sets.newHashSet(queue)
      val yarnClient = mock(classOf[YarnClient])
      val applicationId = mock(classOf[ApplicationId])
      val queueInfo = mock(classOf[QueueInfo])
      yarnClientStatic.when(() => YarnClient.createYarnClient()).thenReturn(yarnClient)

      val report = mock(classOf[ApplicationReport])
      val states = util.EnumSet.of(YarnApplicationState.RUNNING)
      when(yarnClient.getApplications(states)).thenReturn(new util.ArrayList[ApplicationReport]())
      var jobNames = ycm.getRunningJobs(util.Collections.emptySet())
      Assert.assertTrue(jobNames.isEmpty)

      when(yarnClient.getApplications(states)).thenReturn(util.Collections.singletonList(report))
      when(report.getName).thenReturn(jobStepId)
      when(report.getApplicationId).thenReturn(applicationId)
      jobNames = ycm.getRunningJobs(util.Collections.emptySet())
      Assert.assertEquals(1, jobNames.size())
      Assert.assertEquals(jobStepId, jobNames.stream().findFirst().get())

      when(yarnClient.getQueueInfo(queue)).thenReturn(queueInfo)
      when(queueInfo.getApplications).thenReturn(new util.ArrayList[ApplicationReport]())
      jobNames = ycm.getRunningJobs(queues)
      Assert.assertTrue(jobNames.isEmpty)

      when(queueInfo.getApplications).thenReturn(util.Collections.singletonList(report))
      jobNames = ycm.getRunningJobs(queues)
      Assert.assertTrue(jobNames.isEmpty)

      when(report.getYarnApplicationState).thenReturn(YarnApplicationState.RUNNING)
      jobNames = ycm.getRunningJobs(queues)
      Assert.assertEquals(1, jobNames.size())
      Assert.assertEquals(jobStepId, jobNames.stream().findFirst().get())
    } finally {
      yarnClientStatic.close()
    }
  }
}
