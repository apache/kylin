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
package org.apache.spark.sql.derived

import org.apache.kylin.metadata.model.DeriveInfo.DeriveType
import org.apache.kylin.metadata.model.JoinDesc

//  hostIdx and fkIdx may differ at composite key cases
//  fkIdx respects to cuboid gt table, rather than origin fact table!
//  hostFkIdx <-> pkIndex as join relationship
//  calciteIdx <-> lookupIdx
case class DerivedInfo(
    hostIdx: Array[Int],
    fkIdx: Array[Int],
    pkIdx: Array[Int],
    calciteIdx: Array[Int],
    derivedIndex: Array[Int],
    path: String,
    tableIdentity: String,
    aliasTableName: String,
    join: JoinDesc,
    deriveType: DeriveType) {
  require(fkIdx.length == pkIdx.length)
  require(calciteIdx.length == derivedIndex.length)
}
