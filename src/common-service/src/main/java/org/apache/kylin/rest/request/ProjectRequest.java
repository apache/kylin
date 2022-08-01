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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Pattern;

import javax.validation.constraints.AssertTrue;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.insensitive.ProjectInsensitiveRequest;
import org.springframework.validation.FieldError;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;
import lombok.val;

@Data
public class ProjectRequest implements Validation, ProjectInsensitiveRequest {
    @JsonProperty("name")
    private String name;
    @JsonProperty("description")
    private String description;
    @JsonProperty("override_kylin_properties")
    private LinkedHashMap<String, String> overrideKylinProps;

    @AssertTrue
    @JsonIgnore
    public boolean isNameValid() {
        val pattern = Pattern.compile("^(?![_])\\w+$");
        try {
            return pattern.matcher(name).matches();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String getErrorMessage(List<FieldError> errors) {
        val message = MsgPicker.getMsg();
        if (!CollectionUtils.isEmpty(errors) && errors.get(0).getField().equalsIgnoreCase("nameValid")) {
            return message.getInvalidProjectName();
        }
        return "";
    }

    @Override
    public List<String> inSensitiveFields() {
        return Arrays.asList("name");
    }
}
