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

package org.apache.kylin.engine.spark.scheduler

sealed trait KylinJobEvent

sealed trait JobStatus extends KylinJobEvent

case class JobSucceeded() extends JobStatus

case class JobFailed(reason: String, throwable: Throwable) extends JobStatus

sealed trait JobEndReason extends KylinJobEvent

sealed trait JobFailedReason extends JobEndReason

case class ResourceLack(throwable: Throwable) extends JobFailedReason

case class ExceedMaxRetry(throwable: Throwable) extends JobFailedReason

case class UnknownThrowable(throwable: Throwable) extends JobFailedReason


sealed trait JobCommand extends KylinJobEvent

case class RunJob() extends JobCommand


