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

package org.apache.kylin.common.util;

import org.codehaus.jackson.map.annotate.JsonRootName;

@JsonRootName("detail")
public class KylinInstanceDetail {
  private String id;
  private String listenAddress;

  public KylinInstanceDetail(String id, String listenAddress) {
    this.id = id;
    this.listenAddress = listenAddress;
  }

  public KylinInstanceDetail() {
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getListenAddress() {
    return listenAddress;
  }

  public void setListenAddress(String listenAddress) {
    this.listenAddress = listenAddress;
  }

  @Override
  public String toString() {
    return "KylinInstanceDetail{" +
        "id='" + id + '\'' +
        ", listenAddress='" + listenAddress + '\'' +
        '}';
  }
}