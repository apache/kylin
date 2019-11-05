/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */

package io.kyligence.kap.cluster.parser

import org.apache.kylin.common.util.JsonUtil
import org.apache.spark.internal.Logging

object SchedulerParserFactory extends Logging{
  def create(info: String): SchedulerParser = {
    try {
      val schedulerType = JsonUtil.readValueAsTree(info).findValue("type").toString
      // call contains rather than equals cause of value is surrounded with ", for example: {"type":"fairScheduler"}
      // do not support FifoScheduler for now
      if (schedulerType.contains("capacityScheduler")) {
        new CapacitySchedulerParser
      } else if (schedulerType.contains("fairScheduler")) {
        new FairSchedulerParser
      } else {
        throw new IllegalArgumentException(s"Unsupported scheduler type from scheduler info. $schedulerType")
      }
    } catch {
      case throwable: Throwable =>
        logError(s"Invalid scheduler info. $info")
        throw throwable
    }
  }
}
