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

package org.apache.spark.deploy

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.utils.RestService
import org.apache.spark.internal.Logging

import java.io.IOException
import scala.collection.mutable
import scala.util.parsing.json.JSON._


object StandaloneAppClient extends Logging {

  private val JOB_STEP_PREFIX = "job_step_"

  // appId -> (appName, state, starttime)
  private val cachedKylinJobMap: mutable.Map[String, (String, String, Long)] = new mutable.LinkedHashMap[String, (String, String, Long)]()

  private var jobInfoUpdateTime = System.currentTimeMillis()
  private val cacheTtl = 3600 * 1000 * 24 * 5
  private val cacheMaxSize = 30000

  private val masterUrlJson: String = KylinConfig.getInstanceFromEnv.getSparkStandaloneMasterWebUI + "/json"

  private val restService: RestService = new RestService(10000, 10000)


  /**
   * @see org.apache.spark.deploy.master.ApplicationInfo
   * @return Kylin's Build Job 's ApplicationInfo, update every 5 minutes
   */
  def getRunningJobs: mutable.Map[String, (String, String, Long)] = cachedKylinJobMap.synchronized {
    val currMills = System.currentTimeMillis
    if (cachedKylinJobMap.isEmpty || currMills - jobInfoUpdateTime >= 10000) {
      logDebug("Updating app status ...")
      try {
        val realResp = restService.getRequest(masterUrlJson)
        parseApplicationState(realResp)
      } catch {
        case ioe: IOException => logError("Can not connect to standalone master service.", ioe)
        case e: Exception => logError("Error .", e)
      }
      jobInfoUpdateTime = currMills
    }
    cachedKylinJobMap
  }

  def getAppState(stepId: String): String = {
    getRunningJobs

    val doNothing: PartialFunction[(String, String, Long), (String, String, Long)] = {
      case x => x
    }
    val res: Iterable[(String, String, Long)] = cachedKylinJobMap.values.filter(app => app._1.contains(stepId)).collect(doNothing)

    res.size match {
      case 0 => "SUBMITTED"
      case 1 => res.head._2
      case _ =>
        // find the recent submitted application
        res.maxBy(x => x._3)._2
    }
  }

  def getAppUrl(appId: String, standaloneMaster: String): String = {
    var sparkUI = KylinConfig.getInstanceFromEnv.getSparkStandaloneMasterWebUI
    if (sparkUI.isEmpty) {
      sparkUI = "http://" + getMasterHost(standaloneMaster) + ":8080/"
      logWarning("Parameter 'kylin.engine.spark.standalone.master.httpUrl' is not configured. Use " +
        sparkUI + " as the spark standalone Web UI address.")
    }
    if (!sparkUI.endsWith("/")) {
      sparkUI = sparkUI + "/"
    }
    val sparkApp = sparkUI + "app/?appId="
    sparkApp + appId
  }

  def getMasterHost(master: String): String = {
    master.split("(://|:)").tail.head
  }

  def parseApplicationState(responseStr: String): Unit = {
    val curr = System.currentTimeMillis()

    var respJson = Map.empty[String, Any]
    val tree = parseFull(responseStr)
    respJson = tree match {
      case Some(map: Map[String, Any]) => map
    }
    val app1 = respJson.getOrElse("completedapps", Array())
    val completedApps = app1.asInstanceOf[List[Map[String, Any]]]

    for (app <- completedApps) {
      val name: String = app.getOrElse("name", "").asInstanceOf[String]
      val id: String = app.getOrElse("id", "").asInstanceOf[String]
      val state: String = app.getOrElse("state", "").asInstanceOf[String]
      val startTime: Double = app.getOrElse("starttime", "0").asInstanceOf[Double]
      if (name.contains(JOB_STEP_PREFIX)) {
        cachedKylinJobMap(id) = (name, state, startTime.toLong)
      }
    }

    // Clean too old jobs
    if (cachedKylinJobMap.size > cacheMaxSize) {
      for (id <- cachedKylinJobMap.keys) {
        val app = cachedKylinJobMap.get(id)
        if (app.isDefined && curr - app.get._3 > cacheTtl) {
          cachedKylinJobMap.remove(id)
        }
      }
    }
  }
}