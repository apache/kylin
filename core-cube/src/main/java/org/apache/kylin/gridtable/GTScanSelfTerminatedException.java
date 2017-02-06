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

package org.apache.kylin.gridtable;

/**
 * Implementations of {@link IGTScanner} should throw {@link GTScanSelfTerminatedException} or its subclasses
 * in cases where the scan runs out of resources (time, memory, etc) and can not be continued.
 */
public class GTScanSelfTerminatedException extends RuntimeException {

    public GTScanSelfTerminatedException(String s) {
        super(s);
    }
}
