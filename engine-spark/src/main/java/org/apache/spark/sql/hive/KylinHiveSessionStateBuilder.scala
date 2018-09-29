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
package org.apache.spark.sql.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionState}

/**
  * hive session  hava some rule exp: find datasource table rule
  *
  * @param sparkSession
  * @param parentState
  */
class KylinHiveSessionStateBuilder(sparkSession: SparkSession, parentState: Option[SessionState] = None)
  extends HiveSessionStateBuilder(sparkSession, parentState) {
  experimentalMethods.extraOptimizations = {
      Seq()
  }

  private def externalCatalog: HiveExternalCatalog =
    session.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog]

  override protected def newBuilder: NewBuilder =
    new KylinHiveSessionStateBuilder(_, _)

}

/**
  * use for no hive mode
  *
  * @param sparkSession
  * @param parentState
  */
class KylinSessionStateBuilder(sparkSession: SparkSession, parentState: Option[SessionState] = None)
  extends BaseSessionStateBuilder(sparkSession, parentState) {
  experimentalMethods.extraOptimizations = {
      Seq()
  }

  override protected def newBuilder: NewBuilder =
    new KylinSessionStateBuilder(_, _)

}
