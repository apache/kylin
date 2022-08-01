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
package org.apache.kylin.metadata.model;

import static org.apache.kylin.common.exception.CommonErrorCode.UNKNOWN_ERROR_CODE;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.code.ErrorCodeProducer;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BadModelException extends KylinException {

    public enum CauseType {
        WRONG_POSITION_DUE_TO_NAME, // another model is using this cc name on a different alias table
        WRONG_POSITION_DUE_TO_EXPR, // another model is using this cc's expression on a different alias table
        SAME_NAME_DIFF_EXPR, // another model already has defined same cc name but different expr
        SAME_EXPR_DIFF_NAME, // another model already has defined same expr but different cc name
        SELF_CONFLICT_WITH_SAME_NAME, // cc conflicts with self's other cc
        SELF_CONFLICT_WITH_SAME_EXPRESSION, // cc conflicts with self's other cc
        LOOKUP_CC_NOT_REFERENCING_ITSELF // see io.kyligence.kap.metadata.model.KapModel.initComputedColumns()
    }

    @JsonProperty
    private CauseType causeType;
    @JsonProperty
    private String advise;
    @JsonProperty
    private String conflictingModel;
    @JsonProperty
    private String badCC;//tell caller which cc is bad

    public BadModelException(org.apache.kylin.common.exception.ErrorCodeSupplier errorCodeSupplier, String message,
            CauseType causeType, String advise, String conflictingModel, String badCC) {
        super(errorCodeSupplier, message);
        this.causeType = causeType;
        this.advise = advise;
        this.conflictingModel = conflictingModel;
        this.badCC = badCC;
    }

    public BadModelException(ErrorCodeProducer nerrorCodeProducer, CauseType causeType, String advise,
            String conflictingModel, String badCC, Object... args) {
        super(nerrorCodeProducer, args);
        this.causeType = causeType;
        this.advise = advise;
        this.conflictingModel = conflictingModel;
        this.badCC = badCC;
    }

    public BadModelException(String message, CauseType causeType, String advise, String conflictingModel,
            String badCC) {
        this(UNKNOWN_ERROR_CODE, message, causeType, advise, conflictingModel, badCC);
    }

    public CauseType getCauseType() {
        return causeType;
    }

    public String getAdvise() {
        return advise;
    }

    public String getConflictingModel() {
        return conflictingModel;
    }

    public String getBadCC() {
        return badCC;
    }
}
