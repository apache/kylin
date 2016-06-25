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

package org.apache.kylin.rest.request;

/**
 * @author jianliu
 * 
 */
public class CardinalityRequest {
    private int delimiter;

    private int format;

    /**
     * @return the delimiter
     */
    public String getDelimiter() {
        switch (delimiter) {
        case 0:
            return null;
        case 1:
            return "177";
        case 2:
            return "t";
        default:
            return null;
        }
    }

    /**
     * @param delimiter
     *            the delimiter to set
     */
    public void setDelimiter(int delimiter) {
        this.delimiter = delimiter;
    }

    /**
     * @return the format
     */
    public String getFormat() {
        switch (format) {
        case 0:
            return null;
        case 1:
            return "text";
        case 2:
            return "sequence";
        default:
            return null;
        }
    }

    /**
     * @param format
     *            the format to set
     */
    public void setFormat(int format) {
        this.format = format;
    }
}
