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
    public String getCUBE_NOT_FOUND() {
        return "Cannot find cube '%s'.";
    }

    public String getSEG_NOT_FOUND() {
        return "Cannot find segment '%s'.";
    }

    public String getKAFKA_DEP_NOT_FOUND() {
        return "Could not find Kafka dependency.";
    }

    public String getBUILD_DRAFT_CUBE() {
        return "Could not build draft cube.";
    }

    public String getBUILD_BROKEN_CUBE() {
        return "Broken cube '%s' can't be built.";
    }

    public String getINCONSISTENT_CUBE_DESC_SIGNATURE() {
        return "Inconsistent cube desc signature for '%s', if it's right after an upgrade, please try 'Edit CubeDesc' to delete the 'signature' field. Or use 'bin/metastore.sh refresh-cube-signature' to batch refresh all cubes' signatures, then reload metadata to take effect.";
    }

    public String getDELETE_NOT_READY_SEG() {
        return "Cannot delete segment '%s' as its status is not READY. Discard the on-going job for it.";
    }

    public String getDELETE_READY_SEG_BY_UUID() {
        return "Cannot delete segment by UUID '%s' as its status is READY or its Cube is READY.";
    }

    public String getDELETE_SEG_FROM_READY_CUBE() {
        return "Cannot delete segment '%s' from ready cube '%s'. Please disable the cube first.";
    }

    public String getINVALID_BUILD_TYPE() {
        return "Invalid build type: '%s'.";
    }

    public String getNO_ACL_ENTRY() {
        return "There should have been an Acl entry for ObjectIdentity '%s'.";
    }

    public String getACL_INFO_NOT_FOUND() {
        return "Unable to find ACL information for object identity '%s'.";
    }

    public String getACL_DOMAIN_NOT_FOUND() {
        return "Acl domain object required.";
    }

    public String getPARENT_ACL_NOT_FOUND() {
        return "Parent acl required.";
    }

    public String getDISABLE_NOT_READY_CUBE() {
        return "Only ready cube can be disabled, status of '%s' is %s.";
    }

    public String getPURGE_NOT_DISABLED_CUBE() {
        return "Only disabled cube can be purged, status of '%s' is %s.";
    }

    public String getCLONE_BROKEN_CUBE() {
        return "Broken cube '%s' can't be cloned.";
    }

    public String getINVALID_CUBE_NAME() {
        return "Invalid Cube name '%s', only letters, numbers and underscore supported.";
    }

    public String getCUBE_ALREADY_EXIST() {
        return "The cube named '%s' already exists.";
    }

    public String getCUBE_DESC_ALREADY_EXIST() {
        return "The cube desc named '%s' already exists.";
    }

    public String getBROKEN_CUBE_DESC() {
        return "Broken cube desc named '%s'.";
    }

    public String getENABLE_NOT_DISABLED_CUBE() {
        return "Only disabled cube can be enabled, status of '%s' is %s.";
    }

    public String getNO_READY_SEGMENT() {
        return "Cube '%s' doesn't contain any READY segment.";
    }

    public String getDELETE_SEGMENT_CAUSE_GAPS() {
        return "Cube '%s' has gaps caused by deleting segment '%s'.";
    }

    public String getENABLE_WITH_RUNNING_JOB() {
        return "Enable is not allowed with a running job.";
    }

    public String getDISCARD_JOB_FIRST() {
        return "The cube '%s' has running or failed job, please discard it and try again.";
    }

    public String getIDENTITY_EXIST_CHILDREN() {
        return "Children exists for '%s'.";
    }

    public String getINVALID_CUBE_DEFINITION() {
        return "The cube definition is invalid.";
    }

    public String getEMPTY_CUBE_NAME() {
        return "Cube name should not be empty.";
    }

    public String getUSE_DRAFT_MODEL() {
        return "Cannot use draft model '%s'.";
    }

    public String getINCONSISTENT_CUBE_DESC() {
        return "CubeDesc '%s' is inconsistent with existing. Try purge that cube first or avoid updating key cube desc fields.";
    }

    public String getUPDATE_CUBE_NO_RIGHT() {
        return "You don't have right to update this cube.";
    }

    public String getNOT_STREAMING_CUBE() {
        return "Cube '%s' is not a Streaming Cube.";
    }

    public String getCUBE_RENAME() {
        return "Cube renaming is not allowed.";
    }

    public String getREBUILD_SNAPSHOT_OF_VIEW() {
        return "Rebuild snapshot of hive view '%s' is not supported, please refresh segment of the cube";
    }

    // Model
    public String getINVALID_MODEL_DEFINITION() {
        return "The data model definition is invalid.";
    }

    public String getEMPTY_MODEL_NAME() {
        return "Model name should not be empty.";
    }

    public String getINVALID_MODEL_NAME() {
        return "Invalid Model name '%s', only letters, numbers and underline supported.";
    }

    public String getDUPLICATE_MODEL_NAME() {
        return "Model name '%s' is duplicated, could not be created.";
    }

    public String getDROP_REFERENCED_MODEL() {
        return "Model is referenced by Cube '%s' , could not dropped";
    }

    public String getUPDATE_MODEL_KEY_FIELD() {
        return "Model cannot save because there are dimensions, measures or join relations modified to be inconsistent with existing cube.";
    }

    public String getBROKEN_MODEL_DESC() {
        return "Broken model desc named '%s'.";
    }

    public String getMODEL_NOT_FOUND() {
        return "Data Model with name '%s' not found.";
    }

    public String getEMPTY_PROJECT_NAME() {
        return "Project name should not be empty.";
    }

    public String getNULL_EMPTY_SQL() {
        return "SQL should not be empty.";
    }

    public String getEMPTY_NEW_MODEL_NAME() {
        return "New model name should not be empty.";
    }

    public String getUPDATE_MODEL_NO_RIGHT() {
        return "You don't have right to update this model.";
    }

    public String getMODEL_RENAME() {
        return "Model renaming is not allowed.";
    }

    // Job
    public String getILLEGAL_TIME_FILTER() {
        return "Illegal timeFilter: %s.";
    }

    public String getILLEGAL_EXECUTABLE_STATE() {
        return "Illegal status: %s.";
    }

    public String getILLEGAL_JOB_TYPE() {
        return "Illegal job type, id: %s.";
    }

    // Acl
    public String getUSER_NOT_EXIST() {
        return "User '%s' does not exist. Please make sure the user exists.";
    }

    // Project
    public String getINVALID_PROJECT_NAME() {
        return "Invalid Project name '%s', only letters, numbers and underline supported.";
    }

    public String getPROJECT_ALREADY_EXIST() {
        return "The project named '%s' already exists.";
    }

    public String getPROJECT_NOT_FOUND() {
        return "Cannot find project '%s'.";
    }

    public String getDELETE_PROJECT_NOT_EMPTY() {
        return "Cannot modify non-empty project";
    }

    public String getPROJECT_RENAME() {
        return "Project renaming is not allowed.";
    }

    // Table
    public String getHIVE_TABLE_NOT_FOUND() {
        return "Cannot find Hive table '%s'.";
    }

    public String getTABLE_DESC_NOT_FOUND() {
        return "Cannot find table descriptor '%s'.";
    }

    public String getTABLE_IN_USE_BY_MODEL() {
        return "Table is already in use by models '%s'.";
    }

    // Cube Desc
    public String getCUBE_DESC_NOT_FOUND() {
        return "Cannot find cube desc '%s'.";
    }

    // Streaming
    public String getINVALID_TABLE_DESC_DEFINITION() {
        return "The TableDesc definition is invalid.";
    }

    public String getINVALID_STREAMING_CONFIG_DEFINITION() {
        return "The StreamingConfig definition is invalid.";
    }

    public String getINVALID_KAFKA_CONFIG_DEFINITION() {
        return "The KafkaConfig definition is invalid.";
    }

    public String getADD_STREAMING_TABLE_FAIL() {
        return "Failed to add streaming table.";
    }

    public String getEMPTY_STREAMING_CONFIG_NAME() {
        return "StreamingConfig name should not be empty.";
    }

    public String getSTREAMING_CONFIG_ALREADY_EXIST() {
        return "The streamingConfig named '%s' already exists.";
    }

    public String getSAVE_STREAMING_CONFIG_FAIL() {
        return "Failed to save StreamingConfig.";
    }

    public String getKAFKA_CONFIG_ALREADY_EXIST() {
        return "The kafkaConfig named '%s' already exists.";
    }

    public String getCREATE_KAFKA_CONFIG_FAIL() {
        return "StreamingConfig is created, but failed to create KafkaConfig.";
    }

    public String getSAVE_KAFKA_CONFIG_FAIL() {
        return "Failed to save KafkaConfig.";
    }

    public String getROLLBACK_STREAMING_CONFIG_FAIL() {
        return "Action failed and failed to rollback the created streaming config.";
    }

    public String getROLLBACK_KAFKA_CONFIG_FAIL() {
        return "Action failed and failed to rollback the created kafka config.";
    }

    public String getUPDATE_STREAMING_CONFIG_NO_RIGHT() {
        return "You don't have right to update this StreamingConfig.";
    }

    public String getUPDATE_KAFKA_CONFIG_NO_RIGHT() {
        return "You don't have right to update this KafkaConfig.";
    }

    public String getSTREAMING_CONFIG_NOT_FOUND() {
        return "StreamingConfig with name '%s' not found.";
    }

    // Query
    public String getQUERY_NOT_ALLOWED() {
        return "Query is not allowed in '%s' mode.";
    }

    public String getNOT_SUPPORTED_SQL() {
        return "Not Supported SQL.";
    }

    public String getQUERY_TOO_MANY_RUNNING() {
        return "Too many concurrent query requests.";
    }

    public String getTABLE_META_INCONSISTENT() {
        return "Table metadata inconsistent with JDBC meta.";
    }

    public String getCOLUMN_META_INCONSISTENT() {
        return "Column metadata inconsistent with JDBC meta.";
    }

    public String getEXPORT_RESULT_NOT_ALLOWED() {
        return "Current user is not allowed to export query result.";
    }

    // Access
    public String getACL_PERMISSION_REQUIRED() {
        return "Acl permission required.";
    }

    public String getSID_REQUIRED() {
        return "Sid required.";
    }

    public String getREVOKE_ADMIN_PERMISSION() {
        return "Can't revoke admin permission of owner.";
    }

    public String getACE_ID_REQUIRED() {
        return "Ace id required.";
    }

    // Admin
    public String getGET_ENV_CONFIG_FAIL() {
        return "Failed to get Kylin env Config.";
    }

    // User
    public String getAUTH_INFO_NOT_FOUND() {
        return "Can not find authentication information.";
    }

    public String getUSER_NOT_FOUND() {
        return "User '%s' not found.";
    }

    // Diagnosis
    public String getDIAG_NOT_FOUND() {
        return "diag.sh not found at %s.";
    }

    public String getGENERATE_DIAG_PACKAGE_FAIL() {
        return "Failed to generate diagnosis package.";
    }

    public String getDIAG_PACKAGE_NOT_AVAILABLE() {
        return "Diagnosis package is not available in directory: %s.";
    }

    public String getDIAG_PACKAGE_NOT_FOUND() {
        return "Diagnosis package not found in directory: %s.";
    }

    public String getDIAG_PROJECT_NOT_FOUND() {
        return "Can not find project: %s.";
    }

    public String getDIAG_JOBID_NOT_FOUND() {
        return "Can not find job id: %s.";
    }

    // Encoding
    public String getVALID_ENCODING_NOT_AVAILABLE() {
        return "Can not provide valid encodings for datatype: %s.";
    }

    // ExternalFilter
    public String getFILTER_ALREADY_EXIST() {
        return "The filter named '%s' already exists.";
    }

    public String getFILTER_NOT_FOUND() {
        return "The filter named '%s' does not exist.";
    }

    // Basic
    public String getHBASE_FAIL() {
        return "HBase failed: '%s'";
    }

    public String getHBASE_FAIL_WITHOUT_DETAIL() {
        return "HBase failed.";
    }
}