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

import org.apache.spark.internal.Logging

/**
 * A client which ask application's state from Yarn's resource manager of Standalone master.
 */
object SparkApplicationClient extends Logging {

  // Copied from org.apache.spark.deploy.master.ApplicationState
  val finalStates = Set("FINISHED", "FAILED", "KILLED", "UNKNOWN")

  // master is spark standalone and deployMode is cluster
  val STANDALONE_CLUSTER: String = "standalone_cluster"

  /**
   * Report the state of an application which name is stepId until it has exited,
   * either successfully or due to some failure, then return app state.
   *
   * Current only for spark job which its master is spark standalone and deployMode is cluster,
   * because standalone client lack property "spark.standalone.submit.waitAppCompletion".
   */
  def awaitAndCheckAppState(sparkMaster: String, stepId: String): String = {
    logInfo(s"AwaitAndCheckAppState $stepId{} ...")
    sparkMaster match {
      case STANDALONE_CLUSTER =>
        var appState = StandaloneAppClient.getAppState(stepId)
        while (true) {
          appState = StandaloneAppClient.getAppState(stepId)
          logInfo(s"$stepId state is $appState .")
          if (finalStates.contains(appState)) {
            return appState
          }
          Thread.sleep(10000)
        }
        appState
      case m => throw new UnsupportedOperationException("waitAndCheckAppState " + m)
    }
  }
}
