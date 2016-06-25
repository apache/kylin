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

package org.apache.kylin.rest.response;

import java.util.HashMap;

/**
 * @author xduo
 * 
 */
public class MetricsResponse extends HashMap<String, Float> {

    private static final long serialVersionUID = 1L;

    public void increase(String key) {
        increase(key, (float) 1);
    }

    public void increase(String key, Float increased) {
        if (this.containsKey(key)) {
            this.put(key, (this.get(key) + increased));
        } else {
            this.put(key, increased);
        }
    }

    public void decrease(String key) {
        decrease(key, (float) 1);
    }

    public void decrease(String key, Float decreased) {
        if (this.containsKey(key)) {
            this.put(key, (this.get(key) - decreased));
        } else {
            this.put(key, decreased);
        }
    }

}
