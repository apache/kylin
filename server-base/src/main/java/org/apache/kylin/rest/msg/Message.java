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

package org.apache.kylin.rest.msg;

/**
 * Created by luwei on 17-4-12.
 */
public class Message {

    private static Message instance = null;

    protected Message() {

    }

    public static Message getInstance() {
        if (instance == null) {
            instance = new Message();
        }
        return instance;
    }

    // Cube
    private final String CUBE_NOT_FOUND = "Cannot find cube '%s'.";
    private final String SEG_NOT_FOUND = "Cannot find segment '%s'.";
    private final String KAFKA_DEP_NOT_FOUND = "Could not find Kafka dependency.";
    private final String BUILD_DRAFT_CUBE = "Could not build draft cube.";
    private final String BUILD_BROKEN_CUBE = "Broken cube '%s' can't be built.";
    private final String INCONSISTENT_CUBE_DESC_SIGNATURE = "Inconsistent cube desc signature for '%s', if it's right after an upgrade, please try 'Edit CubeDesc' to delete the 'signature' field. Or use 'bin/metastore.sh refresh-cube-signature' to batch refresh all cubes' signatures, then reload metadata to take effect.";
    private final String DELETE_NOT_FIRST_LAST_SEG = "Cannot delete segment '%s' as it is neither the first nor the last segment.";
    private final String DELETE_NOT_READY_SEG = "Cannot delete segment '%s' as its status is not READY. Discard the on-going job for it.";
    private final String INVALID_BUILD_TYPE = "Invalid build type: '%s'.";
    private final String NO_ACL_ENTRY = "There should have been an Acl entry for ObjectIdentity '%s'.";
    private final String ACL_INFO_NOT_FOUND = "Unable to find ACL information for object identity '%s'.";
    private final String ACL_DOMAIN_NOT_FOUND = "Acl domain object required.";
    private final String PARENT_ACL_NOT_FOUND = "Parent acl required.";
    private final String DISABLE_NOT_READY_CUBE= "Only ready cube can be disabled, status of '%s' is %s.";
    private final String PURGE_NOT_DISABLED_CUBE = "Only disabled cube can be purged, status of '%s' is %s.";
    private final String CLONE_BROKEN_CUBE = "Broken cube '%s' can't be cloned.";
    private final String INVALID_CUBE_NAME = "Invalid Cube name '%s', only letters, numbers and underline supported.";
    private final String CUBE_ALREADY_EXIST = "The cube named '%s' already exists.";
    private final String CUBE_DESC_ALREADY_EXIST = "The cube desc named '%s' already exists.";
    private final String BROKEN_CUBE_DESC = "Broken cube desc named '%s'.";
    private final String ENABLE_NOT_DISABLED_CUBE= "Only disabled cube can be enabled, status of '%s' is %s.";
    private final String NO_READY_SEGMENT = "Cube '%s' doesn't contain any READY segment.";
    private final String ENABLE_WITH_RUNNING_JOB= "Enable is not allowed with a running job.";
    private final String DISCARD_JOB_FIRST = "The cube '%s' has running or failed job, please discard it and try again.";
    private final String IDENTITY_EXIST_CHILDREN = "Children exists for '%s'.";
    private final String INVALID_CUBE_DEFINITION = "The cube definition is invalid.";
    private final String EMPTY_CUBE_NAME = "Cube name should not be empty.";
    private final String USE_DRAFT_MODEL = "Cannot use draft model '%s'.";
    private final String UNEXPECTED_CUBE_DESC_STATUS = "CubeDesc status should not be %s.";
    private final String EXPECTED_CUBE_DESC_STATUS = "CubeDesc status should be %s.";
    private final String CUBE_DESC_RENAME = "Cube Desc renaming is not allowed: desc.getName(): '%s', cubeRequest.getCubeName(): '%s'.";
    private final String INCONSISTENT_CUBE_DESC = "CubeDesc '%s' is inconsistent with existing. Try purge that cube first or avoid updating key cube desc fields.";
    private final String UPDATE_CUBE_NO_RIGHT = "You don't have right to update this cube.";
    private final String NOT_STREAMING_CUBE = "Cube '%s' is not a Streaming Cube.";
    private final String NO_DRAFT_CUBE_TO_UPDATE = "Cube '%s' has no draft to update.";
    private final String NON_DRAFT_CUBE_ALREADY_EXIST = "A non-draft cube with name '%s' already exists.";
    private final String CUBE_RENAME = "Cube renaming is not allowed.";
    private final String ORIGIN_CUBE_NOT_FOUND = "Origin cube not found.";

    // Model
    private final String INVALID_MODEL_DEFINITION = "The data model definition is invalid.";
    private final String EMPTY_MODEL_NAME = "Model name should not be empty.";
    private final String INVALID_MODEL_NAME = "Invalid Model name '%s', only letters, numbers and underline supported.";
    private final String UNEXPECTED_MODEL_STATUS = "Model status should not be %s.";
    private final String EXPECTED_MODEL_STATUS = "Model status should be %s.";
    private final String DUPLICATE_MODEL_NAME = "Model name '%s' is duplicated, could not be created.";
    private final String DROP_REFERENCED_MODEL = "Model is referenced by Cube '%s' , could not dropped";
    private final String UPDATE_MODEL_KEY_FIELD = "Dimensions and measures in use and existing join tree cannot be modified.";
    private final String BROKEN_MODEL_DESC = "Broken model desc named '%s'.";
    private final String MODEL_NOT_FOUND = "Data Model with name '%s' not found.";
    private final String EMPTY_PROJECT_NAME = "Project name should not be empty.";
    private final String EMPTY_NEW_MODEL_NAME = "New model name should not be empty";
    private final String UPDATE_MODEL_NO_RIGHT = "You don't have right to update this model.";
    private final String NO_DRAFT_MODEL_TO_UPDATE = "Model '%s' has no draft to update.";
    private final String NON_DRAFT_MODEL_ALREADY_EXIST = "A non-draft model with name '%s' already exists.";
    private final String MODEL_RENAME = "Model renaming is not allowed.";
    private final String ORIGIN_MODEL_NOT_FOUND = "Origin model not found.";

    // Job
    private final String ILLEGAL_TIME_FILTER = "Illegal timeFilter for job history: %s.";
    private final String ILLEGAL_EXECUTABLE_STATE = "Illegal status: %s.";
    private final String INVALID_JOB_STATE = "Invalid state: %s.";
    private final String ILLEGAL_JOB_TYPE = "Illegal job type, id: %s.";
    private final String INVALID_JOB_STEP_STATE = "Invalid state: %s.";

    // Acl
    private final String USER_NOT_EXIST = "User '%s' does not exist. Please make sure the user has logged in before";

    // Project
    private final String INVALID_PROJECT_NAME = "Invalid Project name '%s', only letters, numbers and underline supported.";
    private final String PROJECT_ALREADY_EXIST = "The project named '%s' already exists.";
    private final String PROJECT_NOT_FOUND = "Cannot find project '%s'.";

    // Table
    private final String HIVE_TABLE_NOT_FOUND = "Cannot find Hive table '%s'. ";
    private final String TABLE_DESC_NOT_FOUND = "Cannot find table descriptor '%s'.";
    private final String TABLE_IN_USE_BY_MODEL = "Table is already in use by models '%s'.";

    // Cube Desc
    private final String CUBE_DESC_NOT_FOUND = "Cannot find cube desc '%s'.";

    // Streaming
    private final String INVALID_TABLE_DESC_DEFINITION = "The TableDesc definition is invalid.";
    private final String INVALID_STREAMING_CONFIG_DEFINITION = "The StreamingConfig definition is invalid.";
    private final String INVALID_KAFKA_CONFIG_DEFINITION = "The KafkaConfig definition is invalid.";
    private final String ADD_STREAMING_TABLE_FAIL = "Failed to add streaming table.";
    private final String EMPTY_STREAMING_CONFIG_NAME = "StreamingConfig name should not be empty.";
    private final String STREAMING_CONFIG_ALREADY_EXIST = "The streamingConfig named '%s' already exists.";
    private final String SAVE_STREAMING_CONFIG_FAIL = "Failed to save StreamingConfig.";
    private final String KAFKA_CONFIG_ALREADY_EXIST = "The kafkaConfig named '%s' already exists.";
    private final String CREATE_KAFKA_CONFIG_FAIL = "StreamingConfig is created, but failed to create KafkaConfig.";
    private final String SAVE_KAFKA_CONFIG_FAIL = "Failed to save KafkaConfig.";
    private final String ROLLBACK_STREAMING_CONFIG_FAIL = "Action failed and failed to rollback the created streaming config.";
    private final String ROLLBACK_KAFKA_CONFIG_FAIL = "Action failed and failed to rollback the created kafka config.";
    private final String UPDATE_STREAMING_CONFIG_NO_RIGHT = "You don't have right to update this StreamingConfig.";
    private final String UPDATE_KAFKA_CONFIG_NO_RIGHT = "You don't have right to update this KafkaConfig.";
    private final String STREAMING_CONFIG_NOT_FOUND = "StreamingConfig with name '%s' not found.";

    // Query
    private final String QUERY_NOT_ALLOWED = "Query is not allowed in '%s' mode.";
    private final String NOT_SUPPORTED_SQL = "Not Supported SQL.";
    private final String TABLE_META_INCONSISTENT = "Table metadata inconsistent with JDBC meta.";
    private final String COLUMN_META_INCONSISTENT = "Column metadata inconsistent with JDBC meta.";

    // Access
    private final String ACL_PERMISSION_REQUIRED = "Acl permission required.";
    private final String SID_REQUIRED = "Sid required.";
    private final String REVOKE_ADMIN_PERMISSION = "Can't revoke admin permission of owner.";
    private final String ACE_ID_REQUIRED = "Ace id required.";

    // Admin
    private final String GET_ENV_CONFIG_FAIL = "Failed to get Kylin env Config.";

    // User
    private final String AUTH_INFO_NOT_FOUND = "Can not find authentication information.";
    private final String USER_NOT_FOUND = "User '%s' not found.";

    // Diagnosis
    private final String DIAG_NOT_FOUND = "diag.sh not found at %s.";
    private final String GENERATE_DIAG_PACKAGE_FAIL = "Failed to generate diagnosis package.";
    private final String DIAG_PACKAGE_NOT_AVAILABLE = "Diagnosis package is not available in directory: %s.";
    private final String DIAG_PACKAGE_NOT_FOUND = "Diagnosis package not found in directory: %s.";

    // Encoding
    private final String VALID_ENCODING_NOT_AVAILABLE = "can't provide valid encodings for datatype: %s.";

    // ExternalFilter
    private final String FILTER_ALREADY_EXIST = "The filter named '%s' already exists.";
    private final String FILTER_NOT_FOUND = "The filter named '%s' does not exist.";



    public String getCUBE_NOT_FOUND() {
        return CUBE_NOT_FOUND;
    }

    public String getSEG_NOT_FOUND() {
        return SEG_NOT_FOUND;
    }

    public String getKAFKA_DEP_NOT_FOUND() {
        return KAFKA_DEP_NOT_FOUND;
    }

    public String getBUILD_DRAFT_CUBE() {
        return BUILD_DRAFT_CUBE;
    }

    public String getBUILD_BROKEN_CUBE() {
        return BUILD_BROKEN_CUBE;
    }

    public String getINCONSISTENT_CUBE_DESC_SIGNATURE() {
        return INCONSISTENT_CUBE_DESC_SIGNATURE;
    }

    public String getDELETE_NOT_FIRST_LAST_SEG() {
        return DELETE_NOT_FIRST_LAST_SEG;
    }

    public String getDELETE_NOT_READY_SEG() {
        return DELETE_NOT_READY_SEG;
    }

    public String getINVALID_BUILD_TYPE() {
        return INVALID_BUILD_TYPE;
    }

    public String getNO_ACL_ENTRY() {
        return NO_ACL_ENTRY;
    }

    public String getACL_INFO_NOT_FOUND() {
        return ACL_INFO_NOT_FOUND;
    }

    public String getACL_DOMAIN_NOT_FOUND() {
        return ACL_DOMAIN_NOT_FOUND;
    }

    public String getPARENT_ACL_NOT_FOUND() {
        return PARENT_ACL_NOT_FOUND;
    }

    public String getDISABLE_NOT_READY_CUBE() {
        return DISABLE_NOT_READY_CUBE;
    }

    public String getPURGE_NOT_DISABLED_CUBE() {
        return PURGE_NOT_DISABLED_CUBE;
    }

    public String getCLONE_BROKEN_CUBE() {
        return CLONE_BROKEN_CUBE;
    }

    public String getINVALID_CUBE_NAME() {
        return INVALID_CUBE_NAME;
    }

    public String getCUBE_ALREADY_EXIST() {
        return CUBE_ALREADY_EXIST;
    }

    public String getCUBE_DESC_ALREADY_EXIST() {
        return CUBE_DESC_ALREADY_EXIST;
    }

    public String getBROKEN_CUBE_DESC() {
        return BROKEN_CUBE_DESC;
    }

    public String getENABLE_NOT_DISABLED_CUBE() {
        return ENABLE_NOT_DISABLED_CUBE;
    }

    public String getNO_READY_SEGMENT() {
        return NO_READY_SEGMENT;
    }

    public String getENABLE_WITH_RUNNING_JOB() {
        return ENABLE_WITH_RUNNING_JOB;
    }

    public String getDISCARD_JOB_FIRST() {
        return DISCARD_JOB_FIRST;
    }

    public String getIDENTITY_EXIST_CHILDREN() {
        return IDENTITY_EXIST_CHILDREN;
    }

    public String getINVALID_CUBE_DEFINITION() {
        return INVALID_CUBE_DEFINITION;
    }

    public String getEMPTY_CUBE_NAME() {
        return EMPTY_CUBE_NAME;
    }

    public String getUSE_DRAFT_MODEL() {
        return USE_DRAFT_MODEL;
    }

    public String getUNEXPECTED_CUBE_DESC_STATUS() {
        return UNEXPECTED_CUBE_DESC_STATUS;
    }

    public String getEXPECTED_CUBE_DESC_STATUS() {
        return EXPECTED_CUBE_DESC_STATUS;
    }

    public String getCUBE_DESC_RENAME() {
        return CUBE_DESC_RENAME;
    }

    public String getINCONSISTENT_CUBE_DESC() {
        return INCONSISTENT_CUBE_DESC;
    }

    public String getUPDATE_CUBE_NO_RIGHT() {
        return UPDATE_CUBE_NO_RIGHT;
    }

    public String getNOT_STREAMING_CUBE() {
        return NOT_STREAMING_CUBE;
    }

    public String getNO_DRAFT_CUBE_TO_UPDATE() {
        return NO_DRAFT_CUBE_TO_UPDATE;
    }

    public String getNON_DRAFT_CUBE_ALREADY_EXIST() {
        return NON_DRAFT_CUBE_ALREADY_EXIST;
    }

    public String getCUBE_RENAME() {
        return CUBE_RENAME;
    }

    public String getORIGIN_CUBE_NOT_FOUND() {
        return ORIGIN_CUBE_NOT_FOUND;
    }


    public String getINVALID_MODEL_DEFINITION() {
        return INVALID_MODEL_DEFINITION;
    }

    public String getEMPTY_MODEL_NAME() {
        return EMPTY_MODEL_NAME;
    }

    public String getINVALID_MODEL_NAME() {
        return INVALID_MODEL_NAME;
    }

    public String getUNEXPECTED_MODEL_STATUS() {
        return UNEXPECTED_MODEL_STATUS;
    }

    public String getEXPECTED_MODEL_STATUS() {
        return EXPECTED_MODEL_STATUS;
    }

    public String getDUPLICATE_MODEL_NAME() {
        return DUPLICATE_MODEL_NAME;
    }

    public String getDROP_REFERENCED_MODEL() {
        return DROP_REFERENCED_MODEL;
    }

    public String getUPDATE_MODEL_KEY_FIELD() {
        return UPDATE_MODEL_KEY_FIELD;
    }

    public String getBROKEN_MODEL_DESC() {
        return BROKEN_MODEL_DESC;
    }

    public String getMODEL_NOT_FOUND() {
        return MODEL_NOT_FOUND;
    }

    public String getEMPTY_PROJECT_NAME() {
        return EMPTY_PROJECT_NAME;
    }

    public String getEMPTY_NEW_MODEL_NAME() {
        return EMPTY_NEW_MODEL_NAME;
    }

    public String getUPDATE_MODEL_NO_RIGHT() {
        return UPDATE_MODEL_NO_RIGHT;
    }

    public String getNO_DRAFT_MODEL_TO_UPDATE() {
        return NO_DRAFT_MODEL_TO_UPDATE;
    }

    public String getNON_DRAFT_MODEL_ALREADY_EXIST() {
        return NON_DRAFT_MODEL_ALREADY_EXIST;
    }

    public String getMODEL_RENAME() {
        return MODEL_RENAME;
    }

    public String getORIGIN_MODEL_NOT_FOUND() {
        return ORIGIN_MODEL_NOT_FOUND;
    }


    public String getILLEGAL_TIME_FILTER() {
        return ILLEGAL_TIME_FILTER;
    }

    public String getILLEGAL_EXECUTABLE_STATE() {
        return ILLEGAL_EXECUTABLE_STATE;
    }

    public String getINVALID_JOB_STATE() {
        return INVALID_JOB_STATE;
    }

    public String getILLEGAL_JOB_TYPE() {
        return ILLEGAL_JOB_TYPE;
    }

    public String getINVALID_JOB_STEP_STATE() {
        return INVALID_JOB_STEP_STATE;
    }


    public String getUSER_NOT_EXIST() {
        return USER_NOT_EXIST;
    }


    public String getINVALID_PROJECT_NAME() {
        return INVALID_PROJECT_NAME;
    }

    public String getPROJECT_ALREADY_EXIST() {
        return PROJECT_ALREADY_EXIST;
    }

    public String getPROJECT_NOT_FOUND() {
        return PROJECT_NOT_FOUND;
    }


    public String getHIVE_TABLE_NOT_FOUND() {
        return HIVE_TABLE_NOT_FOUND;
    }

    public String getTABLE_DESC_NOT_FOUND() {
        return TABLE_DESC_NOT_FOUND;
    }

    public String getTABLE_IN_USE_BY_MODEL() {
        return TABLE_IN_USE_BY_MODEL;
    }

    public String getCUBE_DESC_NOT_FOUND() {
        return CUBE_DESC_NOT_FOUND;
    }


    public String getINVALID_TABLE_DESC_DEFINITION() {
        return INVALID_TABLE_DESC_DEFINITION;
    }

    public String getINVALID_STREAMING_CONFIG_DEFINITION() {
        return INVALID_STREAMING_CONFIG_DEFINITION;
    }

    public String getINVALID_KAFKA_CONFIG_DEFINITION() {
        return INVALID_KAFKA_CONFIG_DEFINITION;
    }

    public String getADD_STREAMING_TABLE_FAIL() {
        return ADD_STREAMING_TABLE_FAIL;
    }

    public String getEMPTY_STREAMING_CONFIG_NAME() {
        return EMPTY_STREAMING_CONFIG_NAME;
    }

    public String getSTREAMING_CONFIG_ALREADY_EXIST() {
        return STREAMING_CONFIG_ALREADY_EXIST;
    }

    public String getSAVE_STREAMING_CONFIG_FAIL() {
        return SAVE_STREAMING_CONFIG_FAIL;
    }

    public String getKAFKA_CONFIG_ALREADY_EXIST() {
        return KAFKA_CONFIG_ALREADY_EXIST;
    }

    public String getCREATE_KAFKA_CONFIG_FAIL() {
        return CREATE_KAFKA_CONFIG_FAIL;
    }

    public String getSAVE_KAFKA_CONFIG_FAIL() {
        return SAVE_KAFKA_CONFIG_FAIL;
    }

    public String getROLLBACK_STREAMING_CONFIG_FAIL() {
        return ROLLBACK_STREAMING_CONFIG_FAIL;
    }

    public String getROLLBACK_KAFKA_CONFIG_FAIL() {
        return ROLLBACK_KAFKA_CONFIG_FAIL;
    }

    public String getUPDATE_STREAMING_CONFIG_NO_RIGHT() {
        return UPDATE_STREAMING_CONFIG_NO_RIGHT;
    }

    public String getUPDATE_KAFKA_CONFIG_NO_RIGHT() {
        return UPDATE_KAFKA_CONFIG_NO_RIGHT;
    }

    public String getSTREAMING_CONFIG_NOT_FOUND() {
        return STREAMING_CONFIG_NOT_FOUND;
    }


    public String getQUERY_NOT_ALLOWED() {
        return QUERY_NOT_ALLOWED;
    }

    public String getNOT_SUPPORTED_SQL() {
        return NOT_SUPPORTED_SQL;
    }

    public String getTABLE_META_INCONSISTENT() {
        return TABLE_META_INCONSISTENT;
    }

    public String getCOLUMN_META_INCONSISTENT() {
        return COLUMN_META_INCONSISTENT;
    }


    public String getACL_PERMISSION_REQUIRED() {
        return ACL_PERMISSION_REQUIRED;
    }

    public String getSID_REQUIRED() {
        return SID_REQUIRED;
    }

    public String getREVOKE_ADMIN_PERMISSION() {
        return REVOKE_ADMIN_PERMISSION;
    }

    public String getACE_ID_REQUIRED() {
        return ACE_ID_REQUIRED;
    }


    public String getGET_ENV_CONFIG_FAIL() {
        return GET_ENV_CONFIG_FAIL;
    }


    public String getAUTH_INFO_NOT_FOUND() {
        return AUTH_INFO_NOT_FOUND;
    }

    public String getUSER_NOT_FOUND() {
        return USER_NOT_FOUND;
    }


    public String getDIAG_NOT_FOUND() {
        return DIAG_NOT_FOUND;
    }

    public String getGENERATE_DIAG_PACKAGE_FAIL() {
        return GENERATE_DIAG_PACKAGE_FAIL;
    }

    public String getDIAG_PACKAGE_NOT_AVAILABLE() {
        return DIAG_PACKAGE_NOT_AVAILABLE;
    }

    public String getDIAG_PACKAGE_NOT_FOUND() {
        return DIAG_PACKAGE_NOT_FOUND;
    }


    public String getVALID_ENCODING_NOT_AVAILABLE() {
        return VALID_ENCODING_NOT_AVAILABLE;
    }


    public String getFILTER_ALREADY_EXIST() {
        return FILTER_ALREADY_EXIST;
    }

    public String getFILTER_NOT_FOUND() {
        return FILTER_NOT_FOUND;
    }
}