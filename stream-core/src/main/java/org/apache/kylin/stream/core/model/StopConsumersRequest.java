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

package org.apache.kylin.stream.core.model;

public class StopConsumersRequest {
    private String cube;
    private boolean removeData; // indicate remove the existing data or not

    public String getCube() {
        return cube;
    }

    public void setCube(String cube) {
        this.cube = cube;
    }

    public boolean isRemoveData() {
        return removeData;
    }

    public void setRemoveData(boolean removeData) {
        this.removeData = removeData;
    }

    @Override
    public String toString() {
        return "StopConsumersRequest{" + "cube='" + cube + '\'' + ", removeData=" + removeData + '}';
    }
}
