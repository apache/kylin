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

package io.kyligence.kap.common


import org.apache.kylin.guava30.shaded.common.collect.Maps
import org.apache.kylin.common.util.Unsafe

import scala.collection.JavaConverters._

trait SystemPropertyHelper {
  val propCache: java.util.HashMap[String, String] = Maps.newHashMap[String, String]()

  def changeSystemProp(key: String, value: String): Unit = {
    propCache.put(key, System.getProperty(key))
    Unsafe.setProperty(key, value)
  }

  def restoreSystemProperty(): Unit = {
    propCache.asScala.filter(_._2 != null).foreach {
      case (key, value) =>
        Unsafe.setProperty(key, value)
    }
  }

  def checkSystem(key: String, desc: String = ""): Unit = {
    if (System.getProperty(key) == null) {
      var errorMessage = s"Could not found system property : $key. "
      if (desc.nonEmpty) {
        errorMessage = errorMessage + desc
      }
      throw new IllegalArgumentException(errorMessage)
    }
  }

}
