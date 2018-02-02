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

public class ModelRequest {

    private String uuid;
    private String modelName;
    private String modelDescData;
    private boolean successful;
    private String message;
    private String project;

    private String ccInCheck;
    private boolean isSeekingExprAdvice;

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    /**
     * @return the message
     */
    public String getMessage() {
        return message;
    }

    /**
     * @param message
     *            the message to set
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * @return the status
     */
    public boolean getSuccessful() {
        return successful;
    }

    /**
     * @param status
     *            the status to set
     */
    public void setSuccessful(boolean status) {
        this.successful = status;
    }

    public ModelRequest() {
    }

    public ModelRequest(String modelName, String modelDescData) {
        this.modelName = modelName;
        this.modelDescData = modelDescData;
    }

    public String getModelDescData() {
        return modelDescData;
    }

    public void setModelDescData(String modelDescData) {
        this.modelDescData = modelDescData;
    }

    /**
     * @return the modelName
     */
    public String getModelName() {
        return modelName;
    }

    /**
     * @param modelName
     *            the cubeName to set
     */
    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public boolean isSeekingExprAdvice() {
        return isSeekingExprAdvice;
    }

    public void setIsSeekingExprAdvice(boolean seekingExprAdvice) {
        isSeekingExprAdvice = seekingExprAdvice;
    }

    public String getCcInCheck() {
        return ccInCheck;
    }

    public void setCcInCheck(String ccInCheck) {
        this.ccInCheck = ccInCheck;
    }
}
