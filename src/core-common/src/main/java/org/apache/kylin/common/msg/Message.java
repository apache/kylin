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

package org.apache.kylin.common.msg;

import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.annotation.Clarification;

@Clarification(priority = Clarification.Priority.MAJOR, msg = "Part message is for enterprise.")
public class Message {
    private static final String SECOND_STORAGE_PROJECT_ENABLED = "The project %s does not have tiered storage enabled.";
    private static final String SECOND_STORAGE_MODEL_ENABLED = "The model %s does not have tiered storage enabled.";
    private static final String SECOND_STORAGE_SEGMENT_WITHOUT_BASE_INDEX = "The base table index is missing in the segments, please add and try again.";
    private static final String SECOND_STORAGE_DELETE_NODE_FAILED = "Node %s has data, size is %d bytes";
    private static final String FORCED_TO_TIERED_STORAGE_AND_FORCE_TO_INDEX = "When force_to_index=ture, the query cannot pushdown when using tiered storage fails, forcedToTieredStorage=1 or conf=1 is invalid, please modify and try again";
    private static final String FORCED_TO_TIERED_STORAGE_RETURN_ERROR = "Query failed. Tiered storage is unavailable, please fix and try again.";
    private static final String FORCED_TO_TIERED_STORAGE_INVALID_PARAMETER = "invalid parameters, please fix and try again.";
    private static final String PARAMETER_IS_REQUIRED = "'%s' is required.";
    private static final String DISABLE_PUSH_DOWN_PROMPT = "You should turn on pushdown button if you want to pushdown.";
    private static final String QUERY_NODE_INVALID = "Can’t execute this request on Query node. Please check and try again.";
    private static final String NON_EXISTED_MODEL = "Model %s doesn't exist. Please confirm and try again later.";
    private static final String LACK_PROJECT = "Please fill in the project parameters.";
    private static final String NON_EXIST_PROJECT = "Project %s doesn't exist. Please confirm and try again later.";

    protected Message() {

    }

    public static Message getInstance() {
        return Singletons.getInstance(Message.class);
    }

    // Cube
    public String getCheckCcAmbiguity() {
        return "The computed column name \"%s\" has been used in the current model. Please rename it.";
    }

    public String getSegNotFound() {
        return "Can‘t find segment \"%s\" in model \"%s\". Please try again.";
    }

    public String getAclInfoNotFound() {
        return "Unable to find ACL information for object identity '%s'.";
    }

    public String getAclDomainNotFound() {
        return "Can’t authorize at the moment due to unknown object. Please try again later, or contact technical support.";
    }

    public String getParentAclNotFound() {
        return "Can’t authorize at the moment due to unknown object. Please try again later, or contact technical support.";
    }

    public String getIdentityExistChildren() {
        return "Children exists for '%s'.";
    }

    // Model
    public String getInvalidModelDefinition() {
        return "Can’t find model. Please check and try again.";
    }

    public String getEmptyModelName() {
        return "The model name can’t be empty.";
    }

    public String getInitMeasureFailed() {
        return "Can’t initialize metadata at the moment. Please try restarting first. If the problem still exist, please contact technical support.";
    }

    public String getInvalidModelName() {
        return "The model name \"%s\" is invalid. Please use letters, numbers and underlines only.";
    }

    public String getInvalidDimensionName() {
        return "The dimension name \"%s\" is invalid. Please use only characters, numbers, spaces and symbol(_ -()%%?). %s characters at maximum are supported.";
    }

    public String getInvalidMeasureName() {
        return "The measure name \"%s\" is invalid. Please use Chinese or English characters, numbers, spaces or symbol(_ -()%%?.). %s characters at maximum are supported.";
    }

    public String getDuplicateDimensionName() {
        return "Dimension name \"%s\" already exists. Please rename it. ";
    }

    public String getDuplicateMeasureName() {
        return "Measure name \"%s\" already exists. Please rename it. ";
    }

    public String getDuplicateMeasureDefinition() {
        return "The definition of this measure  is the same as measure \"%s\". Please modify it.";
    }

    public String getDuplicateInternalMeasureDefinition() {
        return "The definition of this measure  is the same as internal measure \"%s\". Please modify it.";
    }

    public String getDuplicateJoinConditions() {
        return "Can’t create the join condition between \"%s\" and \"%s\", because a same one already exists.";
    }

    public String getDuplicateModelName() {
        return "Model name '%s' is duplicated, could not be created.";
    }

    public String getBrokenModelOperationDenied() {
        return "BROKEN model \"%s\" cannot be operated.";
    }

    public String getModelNotFound() {
        return "Can’t find model named \"%s\". Please check and try again.";
    }

    public String getModelModifyAbandon(String table) {
        return String.format(Locale.ROOT, "Model is not support to modify because you do not have permission of '%s'",
                table);
    }

    public String getEmptyProjectName() {
        return "Can’t find project information. Please select a project.";
    }

    public String getGrantTableWithSidHasNotProjectPermission() {
        return "Failed to add table-level permissions.  User  (group)  [%s] has no project [%s] permissions.  Please grant user (group) project-level permissions first.";
    }

    public String getCheckCCType() {
        return "The actual data type \"{1}\" of computed column \"{0}\" is inconsistent with the defined type \"{2}\". Please modify it.";
    }

    public String getCheckCCExpression() {
        return "Can’t validate the expression \"%s\" (computed column: %s). Please check the expression, or try again later.";
    }

    public String getModelMetadataPackageInvalid() {
        return "Can’t parse the file. Please ensure that the file is complete.";
    }

    public String getExportBrokenModel() {
        return "Can’t export model \"%s\"  as it’s in \"BROKEN\" status. Please re-select and try again.";
    }

    public String getImportBrokenModel() {
        return "Model [%s] is broken, can not export.";
    }

    public String getImportModelException() {
        return "Can’t import the model.";
    }

    public String getUnSuitableImportType(String optionalType) {
        if (optionalType == null) {
            return "Can’t select ImportType \"%s\" for the model \"%s\". Please select \"UN_IMPORT\".";
        } else {
            return "Can’t select ImportType \"%s\" for the model \"%s\". Please select \"UN_IMPORT\" (or \""
                    + optionalType + "\").";
        }
    }

    public String getCanNotOverwriteModel() {
        return "Can’t overwrite the model \"%s\", as it doesn’t exist. Please re-select and try again.";
    }

    public String getIllegalModelMetadataFile() {
        return "Can’t parse the metadata file. Please don’t modify the content or zip the file manually after unzip.";
    }

    public String getExportAtLeastOneModel() {
        return "Please select at least one model to export.";
    }

    public String getComputedColumnExpressionDuplicated() {
        return "The expression of computed column has already been used in model '%s' as '%s'. Please modify the name to keep consistent, or use a different expression.";
    }

    public String getComputedColumnExpressionDuplicatedSingleModel() {
        return "This expression has already been used by other computed columns in this model. Please modify it.";
    }

    public String getComputedColumnNameDuplicated() {
        return "The name of computed column '%s' has already been used in model '%s', and the expression is '%s'. Please modify the expression to keep consistent, or use a different name.";
    }

    public String getComputedColumnNameDuplicatedSingleModel() {
        return "This name has already been used by other computed columns in this model. Please modify it.";
    }

    public String getModelChangePermission() {
        return "Don’t have permission. The model’s owner could only be changed by system or project admin.";
    }

    public String getModelOwnerChangeInvalidUser() {
        return "This user can’t be set as the model’s owner. Please select system admin, project admin or management user.";
    }

    // index
    public String getIndexStatusTypeError() {
        return "The parameter \"status\" only supports “NO_BUILD”, “ONLINE”, “LOCKED”, “BUILDING”.";
    }

    public String getIndexSourceTypeError() {
        return "The parameter \"sources\" only supports “RECOMMENDED_AGG_INDEX”, “RECOMMENDED_TABLE_INDEX”, “CUSTOM_AGG_INDEX”, “CUSTOM_TABLE_INDEX”.";
    }

    public String getIndexSortByError() {
        return "The parameter \"sort_by\" only supports “last_modified”, “usage”, “data_size”.";
    }

    // Job
    public String getIllegalTimeFilter() {
        return "The selected time filter is invalid. Please select again.";
    }

    public String getIllegalExecutableState() {
        return "The job status is invalid. Please select again.";
    }

    public String getIllegalJobState() {
        return "The job status is invalid. The value must be “PENDING“, “RUNNING“, “FINISHED“, “ERROR” or “DISCARDED“. Please check and try again.";
    }

    public String getIllegalJobAction() {
        return "Invalid value in parameter “action“ or “statuses“ or “job_ids“,  " //
                + "The value of “statuses“ or the status of jobs specified by “job_ids“ contains “%s“, "
                + "this status of jobs can only perform the following actions: “%s“ .";
    }

    public String getIllegalStateTransfer() {
        return "An error occurred when updating the job status. Please refresh the job list and try again.";
    }

    public String getInvalidPriority() {
        return "The selected priority is invalid. Please select a value within the range from 0 to 4.";
    }

    // Acl
    public String getUserNotExist() {
        return "User '%s' does not exist. Please make sure the user exists.";
    }

    public String getUserGroupNotExist() {
        return "Invalid values in parameter “group_name“. The value %s doesn’t exist.";
    }

    public String getUserGroupExist() {
        return "The user group \"%s\" already exists. Please check and try again.";
    }

    // user group
    public String getEmptyGroupName() {
        return "User group name should not be empty.";
    }

    public String getEmptySid() {
        return "User/Group name should not be empty.";
    }

    public String getEmptyQueryName() {
        return "Query name should not be empty.";
    }

    public String getInvalidQueryName() {
        return "Query name should only contain alphanumerics and underscores.";
    }

    //user
    public String getEmptyUserName() {
        return "Username should not be empty.";
    }

    public String getShortPassword() {
        return "The password should contain more than 8 characters!";
    }

    public String getInvalidPassword() {
        return "The password should contain at least one number, letter and special character (~!@#$%^&*(){}|:\"<>?[];\\'\\,./`).";
    }

    public String getPermissionDenied() {
        return "Permission denied!";
    }

    public String getSelfDeleteForbidden() {
        return "Cannot delete yourself!";
    }

    public String getSelfDisableForbidden() {
        return "Cannot disable yourself!";
    }

    public String getSelfEditForbidden() {
        return "The object is invalid. Please check and try again.";
    }

    public String getUserEditNotAllowed() {
        return "User editing operations under the LDAP authentication mechanism are not supported at this time";
    }

    public String getUserEditNotAllowedForCustom() {
        return "User editing is not allowed unless in current custom profile, function '%s' not implemented";
    }

    public String getGroupEditNotAllowed() {
        return "Group editing is not allowed unless in testing profile, please go to LDAP/SAML provider instead";
    }

    public String getGroupEditNotAllowedForCustom() {
        return "Group editing is not allowed unless in current custom profile, function '%s' not implemented";
    }

    public String getOldPasswordWrong() {
        return "Old password is not correct!";
    }

    public String getNewPasswordSameAsOld() {
        return "New password should not be same as old one!";
    }

    public String getUserAuthFailed() {
        return "Invalid username or password. Please check and try again.";
    }

    public String getInvalidExecuteAsUser() {
        return "User [%s] in the executeAs field does not exist";
    }

    public String getServiceAccountNotAllowed() {
        return "User [%s] does not have permissions for all tables, rows, and columns in the project [%s] and cannot use the executeAs parameter";
    }

    public String getExecuteAsNotEnabled() {
        return "Configuration item \"kylin.query.query-with-execute-as\" is not enabled. So you cannot use the \"executeAs\" parameter now";
    }

    public String getUserBeLocked(long seconds) {
        return "Invalid username or password. Please try again after " + formatSeconds(seconds) + ".";
    }

    public String getUserInLockedStatus(long leftSeconds, long nextLockSeconds) {
        return "For security concern, account %s has been locked. Please try again in " + formatSeconds(leftSeconds)
                + ". " + formatNextLockDuration(nextLockSeconds) + ".";
    }

    protected String formatNextLockDuration(long nextLockSeconds) {
        if (Long.MAX_VALUE == nextLockSeconds) {
            return "Login failure again will be locked permanently.";
        }
        return "Login failure again will be locked for " + formatSeconds(nextLockSeconds) + ".";
    }

    protected String formatSeconds(long seconds) {
        long remainingSeconds = seconds % 60;
        long remainingMinutes = ((seconds - remainingSeconds) / 60) % 60;
        long remainingHour = ((seconds - remainingSeconds - remainingMinutes * 60) / 3600) % 24;
        long remainingDay = (seconds - remainingSeconds - remainingMinutes * 60 - remainingHour * 3600) / (3600 * 24);
        String formatTimeMessage = formatTime(remainingDay, remainingHour, remainingMinutes, remainingSeconds);
        return formatTimeMessage.length() > 0 ? formatTimeMessage.substring(0, formatTimeMessage.length() - 1)
                : formatTimeMessage;
    }

    protected String formatTime(long day, long hour, long min, long second) {
        StringBuilder stringBuilder = new StringBuilder();
        if (day > 0) {
            stringBuilder.append(day).append(" days ");
        }
        if (hour > 0) {
            stringBuilder.append(hour).append(" hours ");
        }
        if (min > 0) {
            stringBuilder.append(min).append(" minutes ");
        }
        if (second > 0) {
            stringBuilder.append(second).append(" seconds ");
        }
        return stringBuilder.toString();
    }

    public String getUserInPermanentlyLockedStatus() {
        return "User %s is locked permanently. Please contact to your administrator to reset.";
    }

    public String getOwnerChangeError() {
        return "The change failed. Please try again.";
    }

    // Project
    public String getInvalidProjectName() {
        return "Please use number, letter, and underline to name your project, and start with a number or a letter.";
    }

    public String getProjectNameIsIllegal() {
        return "The project name can’t exceed 50 characters. Please re-enter.";
    }

    public String getProjectAlreadyExist() {
        return "The project name \"%s\" already exists. Please rename it.";
    }

    public String getProjectNotFound() {
        return "Can't find project \"%s\". Please check and try again.";
    }

    public String getProjectDropFailedSecondStorageEnabled() {
        return "Can't delete project \"%s\", please disable tiered storage firstly.";
    }

    public String getProjectDropFailedJobsNotKilled() {
        return "Can't delete project \"%s\", please discard the related job and try again.";
    }

    public String getProjectUnmodifiableReason() {
        return "Model recommendation is not supported for this project at the moment. Please turn on the recommendation mode in project setting, and try again.";
    }

    public String getProjectOngoingOptimization() {
        return "System is optimizing historical queries at the moment. Please try again later. ";
    }

    public String getProjectChangePermission() {
        return "Don’t have permission. The owner of project could only be changed by system admin.";
    }

    public String getProjectOwnerChangeInvalidUser() {
        return "This user can’t be set as the project’s owner. Please select system admin, or the admin of this project.";
    }

    public String getProjectDisableMlp() {
        return "The multilevel partitioning is not enabled for this project. Please enable it in the project setting and try again.";
    }

    public String getTableParamEmpty() {
        return "Can’t find the table. Please check and try again";
    }

    // Table
    public String getTableNotFound() {
        return "Can’t find table \"%s\". Please check and try again.";
    }

    public String getBeyondMixSamplingRowshint() {
        return "The number of sampling rows should be greater than %d. Please modify it.";
    }

    public String getBeyondMaxSamplingRowsHint() {
        return "The number of sampling rows should be smaller than %d. Please modify it.";
    }

    public String getSamplingFailedForIllegalTableName() {
        return "The name of table for sampling is invalid. Please enter a table name like “database.table”. ";
    }

    public String getFailedForNoSamplingTable() {
        return "Can’t perform table sampling. Please select at least one table.";
    }

    public String getReloadTableCcRetry() {
        return "%1$s The data type of column %3$s in table %2$s has been changed. Please try deleting the computed column or changing the data type.";
    }

    public String getReloadTableModelRetry() {
        return "The data type of column %2$s from the source table %1$s has changed. Please remove the column from model %3$s, or modify the data type.";
    }

    public String getSameTableNameExist() {
        return "Table %s already exists, please choose a different name.";
    }

    public String getQueryNotAllowed() {
        return "Job node is unavailable for queries. Please select a query node.";
    }

    public String getnotSupportedSql() {
        return "This SQL is not supported at the moment. Please try a different SQL.";
    }

    public String getQueryTooManyRunning() {
        return "Can’t submit query at the moment as there are too many ongoing queries. Please try again later, or contact project admin to adjust configuration.";
    }

    public String getExportResultNotAllowed() {
        return "Don’t have permission to export the query result. Please contact admin if needed.";
    }

    public String getForbiddenExportAsyncQueryResult() {
        return "Access denied. Only task submitters or admin users can download the query results";
    }

    public String getDuplicateQueryName() {
        return "Query named \"%s\" already exists. Please check and try again.";
    }

    public String getNullEmptySql() {
        return "SQL can’t be empty. Please check and try again.";
    }

    public String getjobRepeatedStartFailure() {
        return "Can’t start the streaming job repeatedly.";
    }

    public String getJobStartFailure() {
        return "Can’t start the streaming job of model \"%s\", as it has an ongoing one at the moment. Please check and try again.";
    }

    public String getJobBrokenModelStartFailure() {
        return "Can’t start. Model \"%s\" is currently broken. ";
    }

    public String getJobStopFailure() {
        return "Can’t stop the streaming job of model \"%s\" at the moment. Please check the log or try again later.";
    }

    // Access
    public String getAclPermissionRequired() {
        return "Acl permission required.";
    }

    public String getSidRequired() {
        return "Sid required.";
    }

    public String getRevokeAdminPermission() {
        return "Can't revoke admin permission of owner.";
    }

    public String getEmptyPermission() {
        return "Permission should not be empty.";
    }

    public String getInvalidPermission() {
        return "Invalid values in parameter \"permission\". The value should either be \"ADMIN\", \"MANAGEMENT\", \"OPERATION\" or \"QUERY\".";
    }

    public String getInvalidParameterType() {
        return "Invalid value in parameter \"type\". The value should either be \"user\" or \"group\".";
    }

    public String getUnauthorizedSid() {
        return "The user/group doesn’t have access to this project.";
    }

    public String getAccessDeny() {
        return "The user doesn't have access.";
    }

    // Async Query
    public String getQueryResultNotFound() {
        return "Can’t find the query by this query ID in this project. Please check and try again.";
    }

    public String getQueryResultFileNotFound() {
        return "Can’t find the file of query results. Please check and try again.";
    }

    public String getQueryExceptionFileNotFound() {
        return "Can’t get the query status for the failed async query. Please check and try again.";
    }

    public String getCleanFolderFail() {
        return "Can’t clean file folder at the moment. Please ensure that the related file on HDFS could be accessed.";
    }

    public String getAsyncQueryTimeFormatError() {
        return "The time format is invalid. Please enter the date in the format “yyyy-MM-dd HH:mm:ss”.";
    }

    public String getAsyncQueryProjectNameEmpty() {
        return "The project name can’t be empty. Please check and try again.";
    }

    // User
    public String getAuthInfoNotFound() {
        return "Cannot find authentication information.";
    }

    public String getUserNotFound() {
        return "User '%s' not found.";
    }

    public String getDiagPackageNotAvailable() {
        return "Diagnostic package is not available in directory: %s.";
    }

    public String getDiagFailed() {
        return "Can’t generate diagnostic package. Please try regenerating it.";
    }

    // Basic
    public String getFrequencyThresholdCanNotEmpty() {
        return "The query frequency threshold cannot be empty";
    }

    public String getRecommendationLimitNotEmpty() {
        return "The limit of recommendations for adding index cannot be empty.";
    }

    public String getDelayThresholdCanNotEmpty() {
        return "The query delay threshold cannot be empty";
    }

    public String getMinHitCountNotEmpty() {
        return "The hits cannot be empty";
    }

    public String getEffectiveDaysNotEmpty() {
        return "The time frame cannot be empty";
    }

    public String getUpdateFrequencyNotEmpty() {
        return "The recommendation frequency cannot be empty";
    }

    public String getSqlNumberExceedsLimit() {
        return "Up to %s SQLs could be imported at a time";
    }

    public String getSqlListIsEmpty() {
        return "Please enter the parameter “sqls“.";
    }

    public String getSqlFileTypeMismatch() {
        return "The suffix of sql files must be 'txt' or 'sql'.";
    }

    public String getConfigNotSupportDelete() {
        return "Can’t delete this configuration. ";
    }

    public String getConfigNotSupportEdit() {
        return "Can’t edit this configuration. ";
    }

    public String getConfigMapEmpty() {
        return "The configuration list can’t be empty. Please check and try again.";
    }

    // Query statistics

    public String getNotSetInfluxdb() {
        return "Not set kap.metric.write-destination to 'INFLUX'";
    }

    // License
    public String getLicenseErrorPre() {
        return "The license couldn’t be updated:\n";
    }

    public String getLicenseErrorSuff() {
        return "\nPlease upload a new license, or contact Kyligence.";
    }

    public String getLicenseOverdueTrial() {
        return "The license has expired and the validity period is [%s - %s]. Please upload a new license or contact Kyligence.";
    }

    public String getLicenseNodesExceed() {
        return "The number of nodes which you are using is higher than the allowable number. Please contact your Kyligence account manager.";
    }

    public String getLicenseNodesNotMatch() {
        return "The cluster information dose not match the license. Please upload a new license or contact Kyligence.";
    }

    public String getLicenseOverVolume() {
        return "The current used system capacity exceeds the license’s limit. Please upload a new license, or contact Kyligence.";
    }

    public String getLicenseNoLicense() {
        return "No license file. Please contact Kyligence.";
    }

    public String getlicenseWrongCategory() {
        return "The current version of Kyligence Enterprise does not match the license. Please upload a new license or contact Kyligence.";
    }

    public String getLicenseInvalidLicense() {
        return "The license is invalid. Please upload a new license, or contact Kyligence.";
    }

    public String getLicenseMismatchLicense() {
        return "The license doesn’t match the current cluster information. Please upload a new license, or contact Kyligence.";
    }

    public String getLicenseNotEffective() {
        return "License is not effective yet, please apply for a new license.";
    }

    public String getLicenseExpired() {
        return "The license has expired. Please upload a new license, or contact Kyligence.";
    }

    public String getLicenseSourceOverCapacity() {
        return "The amount of data volume used（%s/%s) exceeds the license’s limit. Build index and load data is unavailable.\n"
                + "Please contact Kyligence, or try deleting some segments.";
    }

    public String getLicenseProjectSourceOverCapacity() {
        return "The amount of data volume used（%s/%s) exceeds the project’s limit. Build index and load data is unavailable.\n"
                + "Please contact Kyligence, or try deleting some segments.";
    }

    public String getLicenseNodesOverCapacity() {
        return "The amount of nodes used (%s/%s) exceeds the license’s limit. Build index and load data is unavailable.\n"
                + "Please contact Kyligence, or try stopping some nodes.";
    }

    public String getLicenseSourceNodesOverCapacity() {
        return "The amount of data volume used (%s/%s)  and nodes used (%s/%s) exceeds license’s limit.\n"
                + "Build index and load data is unavailable.\n"
                + "Please contact Kyligence, or try deleting some segments and stopping some nodes.";
    }

    public String getlicenseProjectSourceNodesOverCapacity() {
        return "The amount of data volume used (%s/%s)  and nodes used (%s/%s) exceeds project’s limit.\n"
                + "Build index and load data is unavailable.\n"
                + "Please contact Kyligence, or try deleting some segments and stopping some nodes.";
    }

    public String saveModelFail() {
        return "Can’t save model \"%s\". Please ensure that the used column \"%s\" exist in source table \"%s\".";
    }

    public String getViewDateFormatDetectionError() {
        return "It is not supported to obtain the time format of the partition column "
                + "or the data range of the view table, please manually select or enter.";
    }

    // Async push down get date format
    public String getPushdownPartitionFormatError() {
        return "Can’t detect at the moment. Please set the partition format manually.";
    }

    // Async push down get data range
    public String getPushdownDatarangeError() {
        return "Can’t detect at the moment. Please set the data range manually.";
    }

    public String getpushdownDatarangeTimeout() {
        return "Can’t detect at the moment. Please set the data range manually.";
    }

    public String getDimensionNotfound() {
        return "The dimension %s is referenced by indexes or aggregate groups. Please go to the Data Asset - Model - Index page to view, delete referenced aggregate groups and indexes.";
    }

    public String getMeasureNotfound() {
        return "The measure %s is referenced by indexes or aggregate groups. Please go to the Data Asset - Model - Index page to view, delete referenced aggregate groups and indexes.";
    }

    public String getNestedCcCascadeError() {
        return "Can’t modify computed column \"%s\". It’s been referenced by a nested computed column \"%s\" in the current model. Please remove it from the nested column first.";
    }

    public String getccOnAntiFlattenLookup() {
        return "Can’t use the columns from dimension table “%s“ for computed columns, as the join relationships of this table won’t be precomputed.";
    }

    public String getFilterConditionOnAntiFlattenLookup() {
        return "Can’t use the columns from dimension table “%s“ for data filter condition, as the join relationships of this table won’t be precomputed.";
    }

    public String getChangeGlobaladmin() {
        return "You cannot add,modify or remove the system administrator’s rights";
    }

    public String getChangeDegaultadmin() {
        return "Can’t modify the permission of user “ADMIN”, as this user is a built-in admin by default.";
    }

    //Query
    public String getInvalidUserTag() {
        return "Can’t add the tag, as the length exceeds the maximum 256 characters. Please modify it.";
    }

    public String getInvalidId() {
        return "Can’t find ID \"%s\". Please check and try again.";
    }

    public String getSegmentLocked() {
        return "Can’t remove, refresh or merge segment \"%s\", as it’s LOCKED. Please try again later.";
    }

    public String getSegmentStatus(String status) {
        return "Can’t refresh or merge segment \"%s\", as it’s in \"" + status + "\".Please try again later.";
    }

    //HA
    public String getNoActiveLeaders() {
        return "There is no active leader node. Please contact system admin to check and fix.";
    }

    //Kerberos
    public String getPrincipalEmpty() {
        return "Principal name cannot be null.";
    }

    public String getKeytabFileTypeMismatch() {
        return "The suffix of keytab file must be 'keytab'.";
    }

    public String getKerberosInfoError() {
        return "Invalid principal name or keytab file, please check and submit again.";
    }

    public String getProjectHivePermissionError() {
        return "Permission denied. Please confirm the Kerberos account can access all the loaded tables.";
    }

    public String getLeadersHandleOver() {
        return "System is trying to recover service. Please try again later.";
    }

    public String getTableRefreshError() {
        return "Can’t connect to data source due to unexpected error. Please refresh and try again.";
    }

    public String getTableRefreshNotfound() {
        return "Can’t connect to data source due to unexpected error. Please refresh and try again.";
    }

    public String getTableRefreshParamInvalid() {
        return "The “table“ parameter is invalid. Please check and try again.";
    }

    public String getTableRefreshParamMore() {
        return "There exists invalid filed(s) other than the expected “tables“. Please check and try again.";
    }

    public String getTransferFailed() {
        return "Can’t transfer the request at the moment. Please try again later.";
    }

    public String getUserExists() {
        return "Username:[%s] already exists";
    }

    public String getOperationFailedByUserNotExist() {
        return "Operation failed, user:[%s] not exists, please add it first";
    }

    public String getOperationFailedByGroupNotExist() {
        return "Operation failed, group:[%s] not exists, please add it first";
    }

    public String getColumuIsNotDimension() {
        return "Please add column \"%s\" as a dimension.";
    }

    public String getModelCanNotPurge() {
        return "Can’t purge data by specifying model \"%s\" under the current project settings.";
    }

    public String getModelSegmentCanNotRemove() {
        return "Can’t delete the segment(s) in model \"%s\" under the current project settings.";
    }

    public String getSegmentCanNotRefresh() {
        return "Can’t refresh some segments, as they are being built at the moment. Please try again later.";
    }

    public String getSegmentCanNotRefreshBySegmentChange() {
        return "Can’t refresh at the moment, as the segment range has changed. Please try again later.";
    }

    public String getCanNotBuildSegment() {
        return "Can’t build segment(s). Please add some indexes first.";
    }

    public String getCanNotBuildSegmentManually() {
        return "Can’t manually build segments in model \"%s\" under the current project settings.";
    }

    public String getCanNotBuildIndicesManually() {
        return "Can’t manually build indexes in model \"%s\" under the current project settings.";
    }

    public String getInvalidMergeSegment() {
        return "Can’t merge segments which are not ready yet.";
    }

    public String getInvalidSetTableIncLoading() {
        return "Can‘t set table \"%s\" as incremental loading. It’s been used as a dimension table in model \"%s\".";
    }

    public String getInvalidRefreshSegmentByNoSegment() {
        return "There is no ready segment to refresh at the moment. Please try again later.";
    }

    public String getInvalidRefreshSegmentByNotReady() {
        return "Can’t refresh at the moment. Please ensure that all segments within the refresh range is ready.";
    }

    public String getInvalidLoadHiveTableName() {
        return "Can’t operate now. Please set “kap.table.load-hive-tablename-cached.enabled=true”, and try again.";
    }

    public String getInvalidRemoveUserFromAllUser() {
        return "Can not remove user from ALL USERS group.";
    }

    public String getAccessDenyOnlyAdmin() {
        return "Access Denied, only system and project administrators can edit users' tables, columns, and rows permissions";
    }

    public String getAccessDenyOnlyAdminAndProjectAdmin() {
        return "Access Denied, only system administrators can edit users' tables, columns, and rows permissions";
    }

    public String getSegmentListIsEmpty() {
        return "Can’t find the segment(s). Please check and try again.";
    }

    public String getSegmentIdNotExist() {
        return "Can’t find the segment by ID \"%s\". Please check and try again.";
    }

    public String getSegmentNameNotExist() {
        return "Can’t find the segment by name \"%s\". Please check and try again.";
    }

    public String getLayoutListIsEmpty() {
        return "Can’t find the layout(s). Please check and try again.";
    }

    public String getLayoutNotExists() {
        return "Can’t find layout \"%s\". Please check and try again.";
    }

    public String getInvalidRefreshSegment() {
        return "Please select at least one segment to refresh.";
    }

    public String getEmptySegmentParameter() {
        return "Please enter segment ID or name.";
    }

    public String getConflictSegmentParameter() {
        return "Can’t enter segment ID and name at the same time. Please re-enter.";
    }

    public String getInvalidMergeSegmentByTooLess() {
        return "Please select at least two segments to merge.";
    }

    public String getContentIsEmpty() {
        return "license content is empty";
    }

    public String getIllegalEmail() {
        return "A personal email or illegal email is not allowed";
    }

    public String getLicenseError() {
        return "Get license error";
    }

    public String getEmailUsernameCompanyCanNotEmpty() {
        return "Email, username, company can not be empty";
    }

    public String getEmailUsernameCompanyIsIllegal() {
        return "Email, username, company length should be less or equal than 50";
    }

    public String getUsernameCompanyIsIllegal() {
        return "Username, company only supports Chinese, English, numbers, spaces";
    }

    public String getInvalidComputerColumnNameWithKeyword() {
        return "The computed column name \"%s\" is a SQL keyword. Please choose another name.";
    }

    public String getInvalidComputerColumnName() {
        return "The computed column name \"%s\" is invalid. Please starts with a letter, and use only letters, numbers, and underlines. Please rename it.";
    }

    public String getModelAliasDuplicated() {
        return "Model \"%s\" already exists. Please rename it.";
    }

    public String getInvalidRangeLessThanZero() {
        return "The start and end time should be greater than 0. Please modify it.";
    }

    public String getInvalidRangeNotFormat() {
        return "The format of start or end time is invalid. Only supports timestamp with time unit of millisecond (ms). Please modify it.";
    }

    public String getInvalidRangeEndLessthanStart() {
        return "The end time must be greater than the start time. Please modify it.";
    }

    public String getInvalidRangeNotConsistent() {
        return "The start and end time must exist or not exist at the same time. Please modify it.";
    }

    public String getIdCannotEmpty() {
        return "ID can’t be empty. Please check and try again.";
    }

    public String getInvalidCreateModel() {
        return "Can’t add model manually under this project.";
    }

    public String getSegmentInvalidRange() {
        return "Can’t refresh. The segment range \"%s\" exceeds the range of loaded data, which is \"%s\". Please modify and try again.";
    }

    public String getSegmentRangeOverlap() {
        return "Can’t build segment. The specified data range overlaps with the built segments from \"%s\" to \"%s\". Please modify and try again.";
    }

    public String getPartitionColumnNotExist() {
        return "Can’t find the partition column. Please check and try again.";
    }

    public String getPartitionColumnStartError() {
        return "Can’t start. Please ensure the time partition column is a timestamp column and the time format is valid.";
    }

    public String getPartitionColumnSaveError() {
        return "Can’t submit. Please ensure the time partition column is a timestamp column and the time format is valid.";
    }

    public String getTimestampColumnNotExist() {
        return "Can’t load. Please ensure the table has at least a timestamp column.";
    }

    public String getTimestampPartitionColumnNotExist() {
        return "Can’t save the model. For fusion model, the time partition column must be added as a dimension.";
    }

    public String getInvalidPartitionColumn() {
        return "Please select an original column (not a computed column) from the fact table as time partition column.";
    }

    public String getTableNameCannotEmpty() {
        return "Table name can’t be empty. Please check and try again.";
    }

    public String getTableSampleMaxRows() {
        return "The sampling range should between 10,000 and 20,000,000 rows.";
    }

    public String getFileNotExist() {
        return "Cannot find file [%s]";
    }

    public String getDatabaseNotExist() {
        return "Can’t find database \"%s\". Please check and try again.";
    }

    public String getBrokenModelCannotOnoffline() {
        return "Can’t get model \"%s\" online or offline, as it’s in BROKEN state.";
    }

    public String getInvalidNameStartWithDot() {
        return "The user / group names cannot start with a period (.).";
    }

    public String getInvalidNameStartOrEndWithBlank() {
        return "User / group names cannot start or end with a space.";
    }

    public String getInvalidNameLength() {
        return "The username should contain less than 180 characters. Please check and try again.";
    }

    public String getInvalidNameContainsOtherCharacter() {
        return "Only alphanumeric characters can be used in user / group names";
    }

    public String getInvalidNameContainsInlegalCharacter() {
        return "The user / group names cannot contain the following symbols: backslash (\\), "
                + " slash mark (/), colon (:), asterisk (*), question mark (?),  quotation mark (\"), less than sign (<), greater than sign (>), vertical bar (|).";

    }

    public String getHiveTableNotFound() {
        return "Can’t load table \"%s\". Please ensure that the table(s) could be found in the data source.";
    }

    public String getDuplicateLayout() {
        return "Can’t add this index, as the same index already exists. Please modify.";
    }

    public String getDefaultReason() {
        return "Something went wrong. %s";
    }

    public String getDefaultSuggest() {
        return "Please contact Kyligence technical support for more details.";
    }

    public String getUnexpectedToken() {
        return "Syntax error occurred at line %s, column %s: \"%s\". Please modify it.";
    }

    public String getBadSqlReason() {
        return "The SQL has syntax error: %s ";
    }

    public String getBadSqlSuggest() {
        return "Please modify it.";
    }

    public String getBadSqlTableNotFoundReason() {
        return "Can’t find table \"%s\". Please check and try again.";
    }

    public String getBadSqlTableNotFoundSuggest() {
        return "Please add table \"%s\" to data source. If this table does exist, mention it as \"DATABASE.TABLE\".";
    }

    public String getBadSqlColumnNotFoundReason() {
        return "Can’t find column \"%s\". Please check if it exists in the source table. If exists, please try reloading the table; if not exist, please contact admin to add it.";
    }

    public String getBadSqlColumnNotFoundSuggest() {
        return getBadSqlColumnNotFoundReason();
    }

    public String getBadSqlColumnNotFoundInTableReason() {
        return getBadSqlColumnNotFoundReason();
    }

    public String getBadSqlColumnNotFoundInTableSuggestion() {
        return getBadSqlColumnNotFoundReason();
    }

    public String getProjectNumOverThreshold() {
        return "Failed to create a new project. The number of projects exceeds the maximum: {%s}. Please delete other abandoned projects before trying to create new ones or contact the administrator to adjust the maximum number of projects.";
    }

    public String getModelNumOverThreshold() {
        return "Failed to create a new model. The number of models exceeds the maximum: {%s}. Please delete other abandoned models before trying to create new ones or contact the administrator to adjust the maximum number of models.";
    }

    public String getQueryRowNumOverThreshold() {
        return "Can’t get query result. The rows of query result exceeds the maximum limit \"%s\". Please add filters, or contact admin to adjust the maximum limit.";
    }

    public String getCCExpressionConflict(String newCCExpression, String newCCName, String existedCCName) {
        return String.format(Locale.ROOT,
                "The expression \"%s\" of computed column \"%s\" is same as computed column \"%s\". Please modify it.",
                newCCExpression, newCCName, existedCCName);
    }

    public String getCCNameConflict(String ccName) {
        return String.format(Locale.ROOT, "Computed column \"%s\" already exists. Please modify it.", ccName);
    }

    public String getAliasConflictOfApprovingRecommendation() {
        return "The name already exists. Please rename and try again.";
    }

    public String getDimensionConflict(String dimensionName) {
        return String.format(Locale.ROOT, "Dimension \"%s\" already exists. Please modify it.", dimensionName);
    }

    public String getMeasureConflict(String measureName) {
        return String.format(Locale.ROOT, "Measure \"%s\" already exists. Please modify it.", measureName);
    }

    public String getJobNodeInvalid() {
        return "Can’t execute this request on job node. Please check and try again.";
    }

    public String getQueryNodeInvalid() {
        return QUERY_NODE_INVALID;
    }

    public String getInvalidTimeFormat() {
        return "Can’t set the time partition column. The values of the selected column is not time formatted. Please select again.";
    }

    public String getInvalidCustomizeFormat() {
        return "Unsupported format. Please check and re-enter.";
    }

    public String getSegmentContainsGaps() {
        return "Can’t merge the selected segments, as there are gap(s) in between. Please check and try again.";
    }

    public String getSegmentMergeLayoutConflictError() {
        return "The indexes included in the selected segments are not fully identical. Please build index first and try merging again.";
    }

    public String getSegmentMergePartitionConflictError() {
        return "The subpartitions included in the selected segments are not fully aligned. Please build the subpartitions first and try merging again.";
    }

    public String getSegmentMergeStorageCheckError() {
        return "During segment merging, the HDFS storage space may exceed the threshold limit, and the system actively terminates the merging job. If you need to remove the above restrictions, please refer to the user manual to adjust the parameter kylin.cube.merge-segment-storage-threshold.";
    }

    public String getDimensionTableUsedInThisModel() {
        return "Can’t set the dimension table of this model, as it has been used as fact table in this model. Please modify and try again.";
    }

    public String getNoDataInTable() {
        return "Can’t get data from table \"%s\". Please check and try again.";
    }

    public String getEffectiveDimensionNotFind() {
        return "The following columns are not added as dimensions to the model. Please delete them before saving or add them to the model.\nColumn ID: %s";
    }

    public String getInvalidPasswordEncoder() {
        return "Illegal PASSWORD ENCODER, please check configuration item kylin.security.user-password-encoder";
    }

    public String getFailedInitPasswordEncoder() {
        return "PASSWORD ENCODER init failed, please check configuration item kylin.security.user-password-encoder";
    }

    public String getInvalidNullValue() {
        return "Failed to rewrite the model settings, %s parameter value is null.";
    }

    public String getInvalidIntegerFormat() {
        return "Can’t rewrite the model settings. The value of  \"%s\" must be non-negative integer. Please modify and try again.";
    }

    public String getInvalidMemorySize() {
        return "Can’t rewrite the model settings. The value of \"spark-conf.spark.executor.memory\" must be non-negative integer, with the unit of GB. Please modify and try again.";
    }

    public String getInvalidBooleanFormat() {
        return "Can’t rewrite the model settings. The value of \"%s\" must be either “true” or “false”. Please modify and try again.";
    }

    public String getInvalidAutoMergeConfig() {
        return "Can’t rewrite the model settings. The automatic merge range can’t be empty. Please modify and try again.";
    }

    public String getInvalidVolatileRangeConfig() {
        return "Can’t rewrite the model settings. The unit of the dynamic interval parameter must be either \"day\", \"week\", \"month\", or \"year\", and the value must be non-negative integer. Please modify and try again.";
    }

    public String getInvalidRetentionRangeConfig() {
        return "Failed to rewrite the model settings, parameter value must be non-negative integer and the "
                + "unit of parameter must be the coarsest granularity unit in the unit selected for automatic merge.";
    }

    public String getInsufficientAuthentication() {
        return "Unable to authenticate. Please login again.";
    }

    public String getDisabledUser() {
        return "This user is disabled. Please contact admin.";
    }

    public String getWriteInMaintenanceMode() {
        return "System is currently undergoing maintenance. Metadata related operations are temporarily unavailable.";
    }

    public String getInvalidSidType() {
        return "Invalid value for parameter ‘type’ which should only be ‘user’ or ‘group’(case-insensitive).";
    }

    public String getEmptyDatabaseName() {
        return "Invalid value for parameter ‘database_name’ which should not be empty.";
    }

    public String getEmptyDatabase() {
        return "Please enter the value for the parameter \"Database\". ";
    }

    public String getEmptyTableList() {
        return "Please enter the value for the parameter \"Table\". ";
    }

    public String getEmptyTableName() {
        return "Invalid value for parameter ‘table_name’ which should not be empty.";
    }

    public String getEmptyColumnList() {
        return "Invalid value for parameter ‘columns’ which should not be empty.";
    }

    public String getEmptyRowList() {
        return "Invalid value for parameter ‘rows’ which should not be empty.";
    }

    public String getEmptyColumnName() {
        return "Invalid value for parameter ‘column_name’ which should not be empty.";
    }

    public String getEmptyItems() {
        return "Invalid value for parameter ‘items’ which should not be empty.";
    }

    public String getColumnNotExist() {
        return "Column:[%s] is not exist.";
    }

    public String getDatabaseParameterMissing() {
        return "All the databases should be defined and the database below are missing: (%s).";
    }

    public String getTableParameterMissing() {
        return "All the tables should be defined and the table below are missing: (%s).";
    }

    public String getColumnParameterMissing() {
        return "All the columns should be defined and the column below are missing: (%s).";
    }

    public String getColumnParameterInvalid(String column) {
        return String.format(Locale.ROOT,
                "Can’t assign value(s) for the column \"%s\", as the value(s) doesn’t match the column’s data type. Please check and try again.",
                column);
    }

    public String getDatabaseParameterDuplicate() {
        return "Database [%s] is duplicated in API requests.";
    }

    public String getTableParameterDuplicate() {
        return "Table [%s] is duplicated in API requests.";
    }

    public String getColumnParameterDuplicate() {
        return "Column [%s] is duplicated in API requests.";
    }

    public String getAddJobCheckFail() {
        return "Can’t submit the job at the moment, as a building job for the same object already exists. Please try again later.";
    }

    public String getAddExportJobFail() {
        return "Can’t submit the job at the moment. There is an ongoing load data job of tiered storage for the same segment(s). Please try again later.";
    }

    public String getAddJobCheckFailWithoutBaseIndex() {
        return "Can’t submit the job at the moment. The segment “%s” doesn’t have base index. Please refresh this segment.";
    }

    public String getAddJobException() {
        return "Can’t find executable jobs at the moment. Please try again later.";
    }

    public String getAddJobAbandon() {
        return "Can’t submit the job to this node, as it’s not a job node. Please check and try again.";
    }

    public String getStorageQuotaLimit() {
        return "No storage quota available. The system failed to submit building job, while the query engine will still be available. "
                + "Please clean up low-efficient storage in time, increase the low-efficient storage threshold, or notify the administrator to increase the storage quota for this project.";
    }

    public String getAddJobCheckSegmentFail() {
        return "Can’t submit the job, as the indexes are not identical in the selected segments. Please check and try again.";
    }

    public String getAddJobCheckSegmentReadyFail() {
        return "Can’t submit the job, as there are no segments in READY status. Please try again later.";
    }

    public String getAddJobCheckIndexFail() {
        return "Can’t submit the job, as no index is included in the segment. Please check and try again.";
    }

    public String getRefreshJobCheckIndexFail() {
        return "No index is available to be refreshed. Please check and try again.";
    }

    public String getAddJobCheckMultiPartitionAbandon() {
        return "Can’t add the job. Please ensure that the operation is valid for the current object.";
    }

    public String getAddJobCheckMultiPartitionEmpty() {
        return "Can’t add the job, as the subpartition value is empty. Please check and try again.";
    }

    public String getAddJobCheckMultiPartitionDuplicate() {
        return "Can’t add the job. Please ensure that the subpartitions are unique.";
    }

    public String getTableReloadAddColumnExist(String table, String column) {
        return String.format(Locale.ROOT,
                "Can’t reload the table at the moment. Column \"%s\" already exists in table \"%s\". Please modify and try again.",
                table, column);
    }

    public String getTableReloadHavingNotFinalJob() {
        return "The table metadata can’t be reloaded now. There are ongoing jobs with the following target subjects(s): %s. Please try reloading until all the jobs are completed, or manually discard the jobs.";
    }

    public String getColumnUnrecognized() {
        return "Can’t recognize column \"%s\". Please use \"TABLE_ALIAS.COLUMN\" to reference a column.";
    }

    public String getInvalidJobStatusTransaction() {
        return "Can’t %s job \"%s\", as it is in %s status.";
    }

    // Punctuations
    public String getCOMMA() {
        return ", ";
    }

    public String getRecListOutOfDate() {
        return "The recommendation is invalid, as some of the related content was deleted. Please refresh the page and try again.";
    }

    public String getGroupUuidNotExist() {
        return "Can’t operate user group (UUID: %s). Please check and try again.";
    }

    public String getModelOnlineWithEmptySeg() {
        return "This model can’t go online as it doesn’t have segments. Models with no segment couldn’t serve queries. Please add a segment.";
    }

    public String getScd2ModelOnlineWithScd2ConfigOff() {
        return "This model can’t go online as it includes non-equal join conditions(≥, <)."
                + " Please delete those join conditions,"
                + " or turn on 'Show non-equal join conditions for History table' in project settings.";
    }

    public String getConnectDatabaseError() {
        return "Can’t connect to the RDBMS metastore. Please check if the metastore is working properly.";
    }

    // acl
    public String getInvalidColumnAccess() {
        return "The current user or user group doesn’t have access to the column \"%s\".";
    }

    public String getInvalidSensitiveDataMaskColumnType() {
        return "Can’t do data masking for the data with type of boolean, map or array.";
    }

    public String getNotSupportNestedDependentCol() {
        return "Can’t set association rules on the column \"%s\". This column has been associated with another column.";
    }

    public String getInvalidRowACLUpdate() {
        return "The parameter “rows” or “like_rows” is invalid. Please use the parameter “row_filter” to update the row ACL.";
    }

    // Snapshots
    public String getSnapshotOperationPermissionDenied() {
        return "Don’t have permission. Please ensure that you have required permission to the table which this snapshot is associated with.";
    }

    public String getSnapshotNotFound() {
        return "Can't find the snapshot \"%s\". Please check and try again.";
    }

    public String getSnapshotManagementNotEnabled() {
        return "Snapshot management is not enabled in the settings. Please check and try again.";
    }

    public String getInvalidDiagTimeParameter() {
        return "The end time must be greater than the start time. Please modify it.";
    }

    public String getPartitionsToBuildCannotBeEmpty(List<String> tableDescNames) {
        return "Please select at least one partition for the following snapshots when conducting custom partition value refresh: "
                + tableDescNames.toString();
    }

    // Resource Group
    public String getResourceGroupFieldIsNull() {
        return "Can’t execute this request. Please ensure that all the parameters for the resource group request are included.";
    }

    public String getResourceCanNotBeEmpty() {
        return "Please ensure that at least one resource group exists once the resource group mode is enabled.";
    }

    public String getEmptyResourceGroupId() {
        return "Resource group ID can’t be empty. Please check and try again.";
    }

    public String getdDuplicatedResourceGroupId(String entityId) {
        return String.format(Locale.ROOT, "The resource group ID \"%s\" already exists. Please check and try again.",
                entityId);
    }

    public String getResourceGroupDisabledWithInvliadParam() {
        return "To disable the resource group mode, please remove all the instances and projects for the existing resource groups.";
    }

    public String getProjectWithoutResourceGroup() {
        return "Can’t use this project properly as no resource group has been allocated yet. Please contact admin.";
    }

    public String getEmptyKylinInstanceIdentity() {
        return "Please enter a value for the parameter \"instance\".";
    }

    public String getEmptyKylinInstanceResourceGroupId() {
        return "Please enter a value for the parameter \"resource_group_id\".";
    }

    public String getResourceGroupIdNotExistInKylinInstance(String id) {
        return String.format(Locale.ROOT,
                "Can’t find the parameter \"resource_group_id\" in the instance, which value is \"%s\". Please check and try again.",
                id);
    }

    public String getDuplicatedKylinInstance() {
        return "The same instance already exists. Please check and try again.";
    }

    public String getEmptyProjectInMappingInfo() {
        return "The project can’t be empty in the mapping_info. Please check and try again.";
    }

    public String getEmptyResourceGroupIdInMappingInfo() {
        return "The parameter \"resource_group_id\" can’t be empty in the mapping_info. Please check and try again.";
    }

    public String getProjectBindingResourceGroupInvalid() {
        return "Can’t allocate resource group for project \"%s\". Please ensure that the project is allocated to two resource groups at most. Meanwhile, each request (query or build) is allocated to one resource group.";
    }

    public String getResourceGroupIdNotExistInMappingInfo(String id) {
        return String.format(Locale.ROOT,
                "Can’t find the parameter \"resource_group_id\" (\"%s\") in the mapping_info. Please check and try again.",
                id);
    }

    public String getModelOnlineForbidden() {
        return "Can’t get the model online. Please set the configuration “kylin.model.offline“ as false first.";
    }

    // multi level partition mapping
    public String getMultiPartitionMappingReqeustNotValid() {
        return "Can’t update the mapping relationships of the partition column. The value for the parameter “multi_partition_columns“ doesn’t match the partition column defined in the model. Please check and try again.";
    }

    public String getConcurrentSubmitJobLimit() {
        return "Can't submit building jobs, as it exceeds the concurrency limit (%s).  Please try submitting fewer jobs at a time.";
    }

    public String getModelIsNotMlp() {
        return "\"%s\" is not a multilevel partitioning model. Please check and try again.";
    }

    public String getInvalidPartitionValue() {
        return "The subpartition(s) “%s“ doesn’t exist. Please check and try again.";
    }

    public String getPartitionValueNotSupport() {
        return "Model \"%s\" hasn’t set a partition column yet. Please set it first and try again.";
    }

    public String getAdminPermissionUpdateAbandon() {
        return "Admin is not supported to update permission.";
    }

    public String getModelIdNotExist() {
        return "The model with ID “%s” is not found.";
    }

    public String getNotInEffectiveCollection() {
        return "“%s“ is not a valid value. This parameter only supports “ONLINE”, “OFFLINE”, “WARNING”, “BROKEN“.";
    }

    public String getRowAclNotStringType() {
        return "The LIKE operator could only be used for the char or varchar data type. Please check and try again.";
    }

    public String getRowFilterExceedLimit() {
        return "The number of filters exceeds the upper limit (%s/%s). Please check and try again.";
    }

    public String getRowFilterItemExceedLimit() {
        return "The number of the included values of a single filter exceeds the upper limit (%s/%s). Please check and try again.";
    }

    public String getStopByUserErrorMessage() {
        return "Stopped by user.";
    }

    public String getExceedMaxAllowedPacket() {
        return "The result packet of MySQL exceeds the limit. Please contact the admin to adjust the value of “max_allowed_packet“ as 256M in MySQL. ";
    }

    public String getQueryHistoryColumnMeta() {
        return "Start Time,Duration,Query ID,SQL Statement,Answered by,Query Status,Query Node,Submitter,Query Message\n";
    }

    public String getSecondStorageJobExists() {
        return "Can’t turn off the tiered storage at the moment. Model “%s” has an ongoing job, Please try again later.\\n";
    }

    public String getSecondStorageConcurrentOperate() {
        return "Another tiered storage task is running. Please try again later.";
    }

    public String getSecondStorageProjectJobExists() {
        return "Can’t turn off the tiered storage at the moment. Project “%s” has an ongoing job, Please try again later.\\n";
    }

    public String getSecondStorageProjectEnabled() {
        return SECOND_STORAGE_PROJECT_ENABLED;
    }

    public String getSecondStorageModelEnabled() {
        return SECOND_STORAGE_MODEL_ENABLED;
    }

    public String getSecondStorageSegmentWithoutBaseIndex() {
        return SECOND_STORAGE_SEGMENT_WITHOUT_BASE_INDEX;
    }

    public String getSecondStorageDeleteNodeFailed() {
        return SECOND_STORAGE_DELETE_NODE_FAILED;
    }

    public String getJobRestartFailed() {
        return "Tiered storage task doesn't support restart.\n";
    }

    public String getSegmentDropFailed() {
        return "Segment can't remove. There is an ongoing load data job of tiered storage. Please try again later.\n";
    }

    public String getJobResumeFailed() {
        return "Tiered storage task can't resume. Please try again later.\n";
    }

    public String getInvalidBrokerDefinition() {
        return "The broker filed can’t be empty. Please check and try again.";
    }

    public String getBrokerTimeoutMessage() {
        return "Can’t get the cluster information. Please check whether the broker information is correct, or confirm whether the Kafka server status is normal.";
    }

    public String getStreamingTimeoutMessage() {
        return "Can’t get sample data. Please check and try again.";
    }

    public String getEmptyStreamingMessage() {
        return "This topic has no sample data. Please select another one.";
    }

    public String getInvalidStreamingMessageType() {
        return "The format is invalid. Only support json or binary at the moment. Please check and try again.";
    }

    public String getParseStreamingMessageError() {
        return "The parser cannot parse the sample data. Please check the options or modify the parser, and parse again.";
    }

    public String getReadKafkaJaasFileError() {
        return "Can't read Kafka authentication file correctly. Please check and try again.";
    }

    public String getBatchStreamTableNotMatch() {
        return "The columns from table “%s“ and the Kafka table are not identical. Please check and try again.";
    }

    public String getStreamingIndexesDelete() {
        return "Can’t delete the streaming indexes. Please stop the streaming job and then delete all the streaming segments.";
    }

    public String getStreamingIndexesEdit() {
        return "Can’t edit the streaming indexes. Please stop the streaming job and then delete all the streaming segments.";
    }

    public String getStreamingIndexesAdd() {
        return "Can’t add the streaming indexes. Please stop the streaming job and then delete all the streaming segments.";
    }

    public String getStreamingIndexesApprove() {
        return "Streaming model can’t accept recommendations at the moment.";
    }

    public String getStreamingIndexesConvert() {
        return "Streaming model can’t convert to recommendations at the moment.";
    }

    public String getCannotForceToBothPushdodwnAndIndex() {
        return "Cannot force the query to pushdown and index at the same time. Only one of the parameter “forcedToPushDown“ and “forced_to_index” could be used. Please check and try again.";
    }

    public String getForcedToTieredstorageAndForceToIndex() {
        return FORCED_TO_TIERED_STORAGE_AND_FORCE_TO_INDEX;
    }

    public String getForcedToTieredstorageReturnError() {
        return FORCED_TO_TIERED_STORAGE_RETURN_ERROR;
    }

    public String getForcedToTieredstorageInvalidParameter() {
        return FORCED_TO_TIERED_STORAGE_INVALID_PARAMETER;
    }

    public String getSecondStorageNodeNotAvailable() {
        return "Can't add node. The node does not exist or has been used by other project, please modify and try again.";
    }

    public String getBaseTableIndexNotAvailable() {
        return "Can’t turn on the tiered storage at the moment. Please add base table index first.";
    }

    public String getPartitionColumnNotAvailable() {
        return "Can’t turn on the tiered storage at the moment. Please add the time partition column as dimension, and update the base table index.";
    }

    public String getProjectLocked() {
        return "Data migration is in progress in the current project's tiered storage, please try again later.";
    }

    public String getFixStreamingSegment() {
        return "Can’t fix segment in streaming model.";
    }

    public String getStreamingDisabled() {
        return "The Real-time functions can only be used under Kyligence Premium Version, "
                + "please contact Kyligence customer manager to upgrade your license.";
    }

    public String getNoStreamingModelFound() {
        return "Can't be queried. As streaming data must be queried through indexes, please ensure there is an index for the query. ";
    }

    public String getStreamingTableNotSupportAutoModeling() {
        return "No support streaming table for auto modeling.";
    }

    public String getSparkFailure() {
        return "Can't complete the operation. Please check the Spark environment and try again. ";
    }

    public String getDownloadQueryHistoryTimeout() {
        return "Export SQL timeout, please try again later.";
    }

    public String getStreamingOperationNotSupport() {
        return "Can’t call this API. API calls related to the streaming data is not supported at the moment.";
    }

    public String getJdbcConnectionInfoWrong() {
        return "Invalid connection info.Please check and try again.";
    }

    public String getJdbcNotSupportPartitionColumnInSnapshot() {
        return "Snapshot can’t use partition column for the current data source.";
    }

    public String getParamTooLarge() {
        return "The parameter '%s' is too large, maximum %s byte.";
    }

    // KAP query sql blacklist
    public String getSqlBlacklistItemIdEmpty() {
        return "The id of blacklist item can not be empty.";
    }

    public String getSqlBlacklistItemRegexAndSqlEmpty() {
        return "The regex and sql of blacklist item can not all be empty.";
    }

    public String getSqlBlacklistItemProjectEmpty() {
        return "The project of blacklist item can not be empty.";
    }

    public String getSqlBlacklistItemIdExists() {
        return "Sql blacklist item id already exist.";
    }

    public String getSqlBlacklistItemIdNotExists() {
        return "Sql blacklist item id not exists.";
    }

    public String getSqlBlacklistItemRegexExists() {
        return "Sql blacklist item regex already exist. Blacklist item id: %s .";
    }

    public String getSqlBlacklistItemSqlExists() {
        return "Sql blacklist item sql already exist. Blacklist item id: %s .";
    }

    public String getSqlBlacklistItemIdToDeleteEmpty() {
        return "The id of sql blacklist item to delete can not be empty.";
    }

    public String getSqlBlacklistQueryRejected() {
        return "Query is rejected by blacklist, blacklist item id: %s.";
    }

    public String getSqlBlackListQueryConcurrentLimitExceeded() {
        return "Query is rejected by blacklist because concurrent limit is exceeded, blacklist item id: %s, concurrent limit: {%s}";
    }

    public String getInvalidRange() {
        return "%s is not integer in range [%s - %s] ";
    }

    public String getlDapUserDataSourceConnectionFailed() {
        return "The LDAP server is abnormal. Please check the user data source and try again.";
    }

    public String getLdapUserDataSourceConfigError() {
        return "LDAP connection error, please check LDAP configuration!";
    }

    public String getTableNoColumnsPermission() {
        return "Please add permissions to columns in the table!";
    }

    public String getParameterIsRequired() {
        return PARAMETER_IS_REQUIRED;
    }

    public String getDisablePushDownPrompt() {
        return DISABLE_PUSH_DOWN_PROMPT;
    }

    public String getNonExistedModel() {
        return NON_EXISTED_MODEL;
    }

    public String getLackProject() {
        return LACK_PROJECT;
    }

    public String getNonExistProject() {
        return NON_EXIST_PROJECT;
    }
}
