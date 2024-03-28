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
public class CnMessage extends Message {

    private static final String PARAMETER_NOT_ENABLED = "功能参数未启用，请检查参数 '%s'。";
    private static final String PROFILING_STATUS_ERROR = "构建火焰图任务状态异常，";
    private static final String TASK_TIMEOUT = "执行超时";

    private static final String PARAMETER_EMPTY = "请输入参数 “%s” 的值。";
    private static final String PARAMETER_MUST_BE_POSITIVE_NUMBER= "参数 %s 的值必须为非负数。";

    protected CnMessage() {

    }

    public static CnMessage getInstance() {
        return Singletons.getInstance(CnMessage.class);
    }

    // Cube
    @Override
    public String getCheckCcAmbiguity() {
        return "可计算列名 “%s” 在当前模型下已存在。请重新命名。";
    }

    @Override
    public String getSegNotFound() {
        return "Segment “%s” 在模型 “%s” 内不存在。请重试。";
    }

    @Override
    public String getAclInfoNotFound() {
        return "找不到对象 '%s' 的授权信息";
    }

    @Override
    public String getAclDomainNotFound() {
        return "由于未知对象，当前无法赋权。请稍后再试，或联系技术支持。";
    }

    @Override
    public String getParentAclNotFound() {
        return "由于未知对象，当前无法赋权。请稍后再试，或联系技术支持。";
    }

    @Override
    public String getIdentityExistChildren() {
        return "'%s' 存在下级授权";
    }

    @Override
    public String getInitMeasureFailed() {
        return "无法初始化元数据。请尝试重新启动。若问题依然存在，请联系技术支持。";
    }

    @Override
    public String getInvalidDimensionName() {
        return "无效的维度名称 “%s”。请使用中文、英文、数字、空格、特殊字符（_ -()%%?）。最多支持%s 个字符。";
    }

    @Override
    public String getInvalidMeasureName() {
        return "无效的度量名称 “%s”。请使用中文、英文、数字、空格、特殊字符（_ -()%%?）。最多支持%s 个字符。";
    }

    @Override
    public String getDuplicateModelName() {
        return "模型名称 '%s' 已存在, 不能被创建";
    }

    @Override
    public String getBrokenModelOperationDenied() {
        return "无法操作 BROKEN 模型 “%s”。";
    }

    @Override
    public String getModelModifyAbandon(String table) {
        return String.format(Locale.ROOT, "模型不支持被修改，因为没有 ['%s'] 权限", table);
    }

    @Override
    public String getEmptyProjectName() {
        return "没有项目信息，请指定一个项目。";
    }

    @Override
    public String getGrantTableWithSidHasNotProjectPermission() {
        return "添加表级权限失败。用户（组） [%s] 无项目 [%s] 权限。请先授予用户（组）项目级权限。";
    }

    @Override
    public String getProjectUnmodifiableReason() {
        return "当前项目暂不支持模型推荐及优化。请在设置中启用智能推荐后重试。";
    }

    @Override
    public String getProjectOngoingOptimization() {
        return "当前有进行中的优化任务。请稍后再试。";
    }

    @Override
    public String getDuplicateDimensionName() {
        return "维度名称 “%s” 已存在。请重新命名。";
    }

    @Override
    public String getDuplicateMeasureName() {
        return "度量名称 “%s” 已存在。请重新命名。";
    }

    @Override
    public String getDuplicateMeasureDefinition() {
        return "该度量的定义和度量 “%s” 相同。请修改。";
    }

    @Override
    public String getDuplicateInternalMeasureDefinition() {
        return "该度量的定义和隐藏度量 “%s” 相同。请修改。";
    }

    @Override
    public String getDuplicateJoinConditions() {
        return "“%s” 和 “%s” 已存在联接条件，不能被创建。请修改。";
    }

    @Override
    public String getCheckCCType() {
        return "可计算列 “{0}” 定义的数据类型 “{2}” 与实际类型 “{1}” 不符。请修改。";
    }

    @Override
    public String getCheckCCExpression() {
        return "无法校验表达式 “%s” (可计算列：%s)。请检查表达式的正确性，或稍后重试。";
    }

    @Override
    public String getModelMetadataPackageInvalid() {
        return "无法解析文件。请检查该文件的完整性。";
    }

    @Override
    public String getExportBrokenModel() {
        return "无法导出模型 “%s”，因为该模型状态为 “BROKEN” 。请重新选择并重试。";
    }

    @Override
    public String getImportBrokenModel() {
        return "无法导入 Broken 的模型 [%s]。";
    }

    @Override
    public String getImportModelException() {
        return "无法导入模型。";
    }

    @Override
    public String getUnSuitableImportType(String optionalType) {
        if (optionalType == null) {
            return "导入类型 “%s“ 对模型 “%s” 不可用，仅可选择 “UN_IMPORT”。";
        } else {
            return "导入类型 “%s“ 对模型 “%s” 不可用，仅可选择 “UN_IMPORT” (或 “" + optionalType + "”)。";
        }
    }

    @Override
    public String getCanNotOverwriteModel() {
        return "无法覆盖模型 “%s“，因为该模型不存在。请重新选择后重试。";
    }

    @Override
    public String getIllegalModelMetadataFile() {
        return "无法解析元数据文件。请勿修改或重新压缩导出的文件。";
    }

    @Override
    public String getExportAtLeastOneModel() {
        return "请至少选择一个模型进行导出。";
    }

    @Override
    public String getComputedColumnExpressionDuplicated() {
        return "该可计算列的表达式已被用于模型 '%s'，名称为 '%s'。请修改名称以保持一致，或使用其他的表达式。";
    }

    @Override
    public String getComputedColumnExpressionDuplicatedSingleModel() {
        return "该可计算列表达式在模型内已存在。请修改。";
    }

    @Override
    public String getComputedColumnNameDuplicated() {
        return "可计算列名称 '%s' 已被用于模型 '%s'，表达式为 '%s'。请修改表达式以保持一致，或使用其他的名称。";
    }

    @Override
    public String getComputedColumnNameDuplicatedSingleModel() {
        return "该可计算列名在模型内已存在。请修改。";
    }

    @Override
    public String getModelChangePermission() {
        return "没有权限操作。仅系统管理员和项目管理员可以更改模型的所有者。";
    }

    @Override
    public String getModelOwnerChangeInvalidUser() {
        return "该用户无法被设置成模型所有者。请选择系统管理员、项目管理员、或模型管理员。";
    }

    // Job
    @Override
    public String getIllegalTimeFilter() {
        return "选择的时间范围无效。请重新选择";
    }

    @Override
    public String getIllegalSortByFilter() {
        return "选择的排序条件\"%s\"无效。请重新选择";
    }

    @Override
    public String getIllegalExecutableState() {
        return "选择的任务状态无效。请重新选择";
    }

    // Acl
    @Override
    public String getUserNotExist() {
        return "用户 '%s' 不存在, 请确认用户是否存在。";
    }

    @Override
    public String getUserGroupExist() {
        return "用户组 “%s” 已存在。请检查后重试。";
    }

    // Project
    @Override
    public String getInvalidProjectName() {
        return "项目名称只支持数字、字母和下划线，并且需要用数字或者字母开头。";
    }

    @Override
    public String getProjectNameIsIllegal() {
        return "项目名称不得超过50个字符。请重新输入。";
    }

    @Override
    public String getProjectAlreadyExist() {
        return "项目名 \"%s\" 已存在。请重新命名。";
    }

    @Override
    public String getProjectDropFailedSecondStorageEnabled() {
        return "无法删除项目 \"%s\"，请先关闭分层存储。";
    }

    @Override
    public String getProjectDropFailedJobsNotKilled() {
        return "无法删除项目 \"%s\"，请终止相关任务后再试。";
    }

    @Override
    public String getSqlListIsEmpty() {
        return "请输入参数 “sqls“。";
    }

    @Override
    public String getProjectChangePermission() {
        return "没有权限操作。仅系统管理员可以更改项目的所有者。";
    }

    @Override
    public String getProjectOwnerChangeInvalidUser() {
        return "该用户无法被设置成项目所有者。请选择系统管理员，或该项目的管理员。";
    }

    @Override
    public String getSamplingFailedForIllegalTableName() {
        return "无效的采样表名称。请以 “database.table” 为格式命名。";
    }

    @Override
    public String getFailedForNoSamplingTable() {
        return "无法执行表采样。请选择至少一张表。";
    }

    @Override
    public String getReloadTableCcRetry() {
        return "%s源表 %s 中列 %s 的数据类型发生了变更。请删除可计算列或修改数据类型后再试。";
    }

    @Override
    public String getSameTableNameExist() {
        return "表 %s 已经存在，请选择其他名称。";
    }

    @Override
    public String getnotSupportedSql() {
        return "该 SQL 暂不支持。请尝试不同的 SQL。";
    }

    @Override
    public String getDuplicateQueryName() {
        return "名为 \"%s\" 的查询已存在。请检查后重试。";
    }

    @Override
    public String getNullEmptySql() {
        return "SQL 不能为空。请检查后重试。";
    }

    @Override
    public String getJobStartFailure() {
        return "模型 \"%s\" 当前已有运行中的流数据任务，无法重复启动。请检查后重试。";
    }

    @Override
    public String getJobBrokenModelStartFailure() {
        return "无法启动，模型 \"%s\" 当前为 Broken 状态。";
    }

    // Access
    @Override
    public String getAclPermissionRequired() {
        return "需要授权";
    }

    @Override
    public String getSidRequired() {
        return "找不到 Sid";
    }

    @Override
    public String getEmptyPermission() {
        return "权限不能为空";
    }

    @Override
    public String getInvalidPermission() {
        return "参数 \"permission\" 的值无效，请使用 \"ADMIN\"、\"MANAGEMENT\"、\"OPERATION\" 或 \"QUERY\"";
    }

    @Override
    public String getUnauthorizedSid() {
        return "用户/组没有当前项目访问权限";
    }

    @Override
    public String getAccessDeny() {
        return "当前用户无访问权限。";
    }

    // user group

    @Override
    public String getEmptySid() {
        return "用户名/用户组名不能为空";
    }

    @Override
    public String getInvalidQueryName() {
        return "查询名称只能包含字母，数字和下划线";
    }

    @Override
    public String getRevokeAdminPermission() {
        return "不能取消创建者的管理员权限";
    }

    @Override
    public String getGroupEditNotAllowed() {
        return "暂不支持LDAP认证机制下的用户组编辑操作";
    }

    @Override
    public String getGroupEditNotAllowedForCustom() {
        return "暂不支持客户认证接入机制下的用户组编辑操作, 方法 '%s' 未被实现";
    }

    // Async Query
    @Override
    public String getQueryResultNotFound() {
        return "该项目下无法找到该 Query ID 对应的异步查询。请检查后重试。";
    }

    @Override
    public String getQueryResultFileNotFound() {
        return "无法找到查询结果文件。请检查后重试。";
    }

    @Override
    public String getQueryExceptionFileNotFound() {
        return "无法获取失败异步查询的查询状态。请检查后重试。";
    }

    @Override
    public String getCleanFolderFail() {
        return "当前无法清理文件夹。请确保相关 HDFS 文件可以正常访问。";
    }

    @Override
    public String getUserNotFound() {
        return "找不到用户 '%s'";
    }

    @Override
    public String getUserBeLocked(long seconds) {
        return "用户名或密码错误，请在 " + formatSeconds(seconds) + "后再次重试。";
    }

    @Override
    public String getUserInLockedStatus(long leftSeconds, long nextLockSeconds) {
        return "为了账号安全，用户 %s 被锁定。请在 " + formatSeconds(leftSeconds) + "后再试。" + formatNextLockDuration(nextLockSeconds);
    }

    @Override
    protected String formatNextLockDuration(long nextLockSeconds) {
        if (Long.MAX_VALUE == nextLockSeconds) {
            return "如登录再次错误，将被永久锁定。";
        }
        return "如登录再次错误将会被继续锁定 " + formatSeconds(nextLockSeconds) + "。";
    }

    @Override
    protected String formatTime(long day, long hour, long min, long second) {
        StringBuilder stringBuilder = new StringBuilder();
        if (day > 0) {
            stringBuilder.append(day).append(" 天 ");
        }
        if (hour > 0) {
            stringBuilder.append(hour).append(" 小时 ");
        }
        if (min > 0) {
            stringBuilder.append(min).append(" 分 ");
        }
        if (second > 0) {
            stringBuilder.append(second).append(" 秒 ");
        }
        return stringBuilder.toString();
    }

    @Override
    public String getUserInPermanentlyLockedStatus() {
        return "用户 %s 已被永久锁定，请联系您的系统管理员进行重置。";
    }

    @Override
    public String getNewPasswordSameAsOld() {
        return "新密码与旧密码一致，请输入一个不同的新密码";
    }

    @Override
    public String getUserEditNotAllowed() {
        return "暂不支持LDAP认证机制下的用户编辑操作";
    }

    @Override
    public String getUserEditNotAllowedForCustom() {
        return "暂不支持客户认证接入机制下的用户编辑操作, 方法 '%s' 未被实现";
    }

    @Override
    public String getOwnerChangeError() {
        return "更改失败，请重试。";
    }

    @Override
    public String getDiagPackageNotAvailable() {
        return "诊断包不可用, 路径: %s";
    }

    @Override
    public String getDiagFailed() {
        return "生成诊断包失败。请尝试重新生成。";
    }

    // Basic
    @Override
    public String getFrequencyThresholdCanNotEmpty() {
        return "查询频率阈值不能为空";
    }

    @Override
    public String getRecommendationLimitNotEmpty() {
        return "新增索引上限不能为空";
    }

    @Override
    public String getDelayThresholdCanNotEmpty() {
        return "查询延迟阈值不能为空";
    }

    @Override
    public String getMinHitCountNotEmpty() {
        return "命中次数不能为空";
    }

    @Override
    public String getEffectiveDaysNotEmpty() {
        return "时间范围不能为空";
    }

    @Override
    public String getUpdateFrequencyNotEmpty() {
        return "推荐频率不能为空";
    }

    @Override
    public String getSqlNumberExceedsLimit() {
        return "最多可同时导入 %s 条 SQL";
    }

    @Override
    public String getConfigMapEmpty() {
        return "配置列表不能为空。请检查后重试。";
    }

    @Override
    public String getLowLevelLicenseMessage() {
        return "无法使用分层存储，如需使用请先升级为 Kyligence 高级版";
    }

    @Override
    public String getRestartNoticeMessage() {
        return "部分功能需重启后再试";
    }

    @Override
    public String saveModelFail() {
        return "模型 “%s” 保存失败。请确保模型中使用的列 “%s” 在源表 “%s” 中存在。";
    }

    @Override
    public String getViewDateFormatDetectionError() {
        return "暂不支持获取视图表的分区列时间格式或数据范围，请手动选择或输入。";
    }

    // Async push down get date format
    @Override
    public String getPushdownPartitionFormatError() {
        return "自动探测失败，请手动选择分区格式。";
    }

    // Async push down get data range
    @Override
    public String getPushdownDatarangeError() {
        return "自动探测失败，请手动选择数据范围。";
    }

    @Override
    public String getpushdownDatarangeTimeout() {
        return "自动探测失败，请手动选择数据范围。";
    }

    @Override
    public String getDimensionNotfound() {
        return "维度 %s 正在被索引、聚合组引用。请到”数据资产-模型-索引”查看，删除引用的聚合组、索引。";
    }

    @Override
    public String getMeasureNotfound() {
        return "度量 %s 正在被索引、聚合组引用。请到”数据资产-模型-索引”查看，删除引用的聚合组、索引。";
    }

    @Override
    public String getNestedCcCascadeError() {
        return "无法修改可计算列 “%s”。当前模型中存在嵌套可计算列 “%s” 依赖于当前可计算列。请先解除引用关系后再进行修改。";
    }

    @Override
    public String getccOnAntiFlattenLookup() {
        return "无法在可计算列中使用维度表 “%s” 中的列，因为该表的关联关系不进行预计算。";
    }

    @Override
    public String getFilterConditionOnAntiFlattenLookup() {
        return "无法在数据筛选条件中使用维度表 “%s” 中的列，因为该表的关联关系不进行预计算。";
    }

    @Override
    public String getChangeGlobaladmin() {
        return "您不可以添加，修改，删除系统管理员的权限。";
    }

    @Override
    public String getChangeDefaultadmin() {
        return "无法修改系统默认内置管理员 ADMIN 用户的权限。";
    }

    //Query
    @Override
    public String getInvalidUserTag() {
        return "无法添加标签，因为长度超出了256 个字符。请修改。";
    }

    @Override
    public String getInvalidId() {
        return "无法找到 ID \"%s\"。请检查后重试。";
    }

    //Kerberos
    @Override
    public String getPrincipalEmpty() {
        return "Principal 名称不能为空.";
    }

    @Override
    public String getKeytabFileTypeMismatch() {
        return "keytab 文件后缀必须是 'keytab'";
    }

    @Override
    public String getKerberosInfoError() {
        return "无效的 Principal 名称或者 Keytab 文件，请检查后重试.";
    }

    @Override
    public String getProjectHivePermissionError() {
        return "权限不足，请确保提交的 Kerberos 用户信息包含所有已加载表的访问权限.";
    }

    //HA
    @Override
    public String getNoActiveLeaders() {
        return "系统中暂无活跃的任务节点。请联系系统管理员进行检查并修复。";
    }

    @Override
    public String getLeadersHandleOver() {
        return "系统正在尝试恢复服务。请稍后重试。";
    }

    @Override
    public String getTableRefreshNotfound() {
        return "连接数据源异常。请尝试重新刷新。";
    }

    @Override
    public String getTableRefreshError() {
        return "连接数据源异常。请尝试重新刷新。";
    }

    @Override
    public String getTableRefreshParamInvalid() {
        return "请求中的 “tables” 字段无效。请检查后重试。";
    }

    @Override
    public String getTableRefreshParamMore() {
        return "请求中包含非 “tables“ 的多余字段。请检查后重试。";
    }

    @Override
    public String getTransferFailed() {
        return "请求转发失败。请稍后重试。";
    }

    @Override
    public String getUserExists() {
        return "用户名:[%s] 已存在。";
    }

    @Override
    public String getOperationFailedByUserNotExist() {
        return "操作失败，用户[%s]不存在，请先添加";
    }

    @Override
    public String getPermissionDenied() {
        return "拒绝访问";
    }

    @Override
    public String getModifyPermissionOfSuperAdminFailed() {
        return "超级管理员的权限不能被编辑。";
    }

    @Override
    public String getGrantPermissionFailedByIllegalAuthorizingUser() {
        return "编辑失败，只有超级管理员或有查询权限的系统管理员可以编辑用户的查询权限。";
    }

    @Override
    public String getGrantPermissionFailedByNonSystemAdmin() {
        return "编辑失败，你只能更改系统管理员的权限。";
    }

    @Override
    public String getModifyOwnPermissionFailed() {
        return "不能编辑自己的权限。";
    }

    @Override
    public String getColumuIsNotDimension() {
        return "请先添加列 “%s” 为维度。";
    }

    @Override
    public String getModelCanNotPurge() {
        return "当前项目设置下，不支持指定模型 “%s” 清除数据。";
    }

    @Override
    public String getModelSegmentCanNotRemove() {
        return "当前项目设置下，无法手动删除模型 “%s” 中的 Segment。";
    }

    @Override
    public String getSegmentCanNotRefresh() {
        return "有部分 Segment 正在构建，无法刷新。请稍后重试。";
    }

    @Override
    public String getSegmentCanNotRefreshBySegmentChange() {
        return "当前无法刷新 Segment，因为范围已更改。请稍后重试。";
    }

    @Override
    public String getCanNotBuildSegment() {
        return "无法构建 Segment。请先添加索引。";
    }

    @Override
    public String getCanNotBuildIndicesManually() {
        return "当前项目设置下，无法手动构建模型 “%s” 的索引。";
    }

    @Override
    public String getInvalidMergeSegment() {
        return "无法合并暂不可用的 Segment。";
    }

    @Override
    public String getInvalidSetTableIncLoading() {
        return "无法设置表 “％s” 的增量加载，因为其已在模型 “％s” 中作为维表使用。";
    }

    @Override
    public String getInvalidRefreshSegmentByNoSegment() {
        return "当前没有可用的 Segment 可以刷新，请稍后重试。";
    }

    @Override
    public String getInvalidRefreshSegmentByNotReady() {
        return "当前无法刷新，请确保刷新范围内的所有 Segment 均已就绪。";
    }

    @Override
    public String getInvalidLoadHiveTableName() {
        return "无法执行该操作。请设置 ”kap.table.load-hive-tablename-cached.enabled=true”，然后重试。";
    }

    @Override
    public String getInvalidRemoveUserFromAllUser() {
        return "无法从ALL USERS组中删除用户。";
    }

    @Override
    public String getAccessDenyOnlyAdmin() {
        return "拒绝访问，只有系统和项目管理员才能编辑用户的表，列和行权限";
    }

    @Override
    public String getAccessDenyOnlyAdminAndProjectAdmin() {
        return "拒绝访问，只有系统管理员才能编辑用户的表，列和行权限";
    }

    @Override
    public String getQueryTooManyRunning() {
        return "查询请求数量超过上限，无法提交。请稍后再试，或联系项目管理员修改设置。";
    }

    @Override
    public String getAsyncQueryTooManyRunning() {
        return "查询失败，异步查询总数已达到管理员设置的上限，请等候并重试。";
    }

    @Override
    public String getSelfDisableForbidden() {
        return "您不可以禁用您自己";
    }

    @Override
    public String getSelfDeleteForbidden() {
        return "您不可以删除您自己";
    }

    @Override
    public String getSelfEditForbidden() {
        return "无效的操作对象，请检查后重试。";
    }

    @Override
    public String getOldPasswordWrong() {
        return "原密码不正确";
    }

    @Override
    public String getInvalidPassword() {
        return "密码应至少包含一个数字，字母和特殊字符（〜！@＃$％^＆*（）{} |：\\“ <>？[]; \\'\\，。/`）。";
    }

    @Override
    public String getShortPassword() {
        return "密码应包含8个以上的字符！";
    }

    @Override
    public String getInvalidRefreshSegment() {
        return "请至少选一个 Segment 刷新。";
    }

    @Override
    public String getEmptySegmentParameter() {
        return "请输入 Segment ID 或名称。";
    }

    @Override
    public String getEmailUsernameCompanyCanNotEmpty() {
        return "邮箱, 用户名, 公司不能为空";
    }

    @Override
    public String getEmailUsernameCompanyIsIllegal() {
        return "邮箱, 用户名, 公司的长度要小于等于50";
    }

    @Override
    public String getInvalidComputerColumnNameWithKeyword() {
        return "可计算列 \"%s\" 的名称是 SQL 关键字。请使用其他名称。";
    }

    @Override
    public String getInvalidComputerColumnName() {
        return "无效的计算列名称 “%s”。请以字母开头，并只使用字母、数字、下划线。请重新命名。";
    }

    @Override
    public String getModelAliasDuplicated() {
        return "模型 “%s” 已存在。请重新命名。";
    }

    @Override
    public String getIdCannotEmpty() {
        return "ID 不能为空。请检查后重试。";
    }

    @Override
    public String getInvalidCreateModel() {
        return "无法在此项目中手动添加模型。";
    }

    @Override
    public String getPartitionColumnNotExist() {
        return "无法找到分区列。请检查后重试。";
    }

    @Override
    public String getPartitionColumnStartError() {
        return "无法启动。请确保模型的时间分区列为时间戳类型，且时间格式有效。";
    }

    @Override
    public String getPartitionColumnSaveError() {
        return "无法提交。请确保模型的时间分区列为时间戳类型，且时间格式有效。";
    }

    @Override
    public String getTimestampColumnNotExist() {
        return "无法加载。请确保表中有 timestamp 类型的列。";
    }

    @Override
    public String getTimestampPartitionColumnNotExist() {
        return "无法保存模型。融合模型必须将时间分区列加入模型维度。";
    }

    @Override
    public String getInvalidPartitionColumn() {
        return "请选择事实表上的原始列（而非可计算列）作为时间分区列。";
    }

    @Override
    public String getTableNameCannotEmpty() {
        return "表名不能为空。请检查后重试。";
    }

    @Override
    public String getTableNotFound() {
        return "无法找到表 \"%s\" 。请检查后重试。";
    }

    @Override
    public String getTableParamEmpty() {
        return "无法找到该表，请检查后重试。";
    }

    @Override
    public String getFileNotExist() {
        return "找不到文件[%s]";
    }

    @Override
    public String getDatabaseNotExist() {
        return "无法找到数据库 \"%s\" 。请检查后重试。";
    }

    @Override
    public String getBrokenModelCannotOnoffline() {
        return "模型 “%s” 无法上线或下线，因为其处于 BROKEN 状态。";
    }

    @Override
    public String getInvalidNameStartWithDot() {
        return "用户名/用户组名不能以英文句号开头(.)";
    }

    @Override
    public String getInvalidNameStartOrEndWithBlank() {
        return "用户名/用户组名不能以空格开头或结尾";
    }

    @Override
    public String getInvalidNameLength() {
        return "用户名需要小于180字符，请检查后重试。";
    }

    @Override
    public String getInvalidNameContainsOtherCharacter() {
        return "用户名/用户组中仅支持英文字符";
    }

    @Override
    public String getInvalidNameContainsInlegalCharacter() {
        return "用户名/用户组名中不能包含如下符号: 反斜杠(\\), 斜杠(/), 冒号(:), 星号(*), 问号(?), 引号(“), 小于号(<), 大于号(>), 垂直线(|)";
    }

    @Override
    public String getHiveTableNotFound() {
        return "无法加载表 \"%s\"。请确保以上表在数据源中存在。";
    }

    @Override
    public String getDuplicateLayout() {
        return "无法添加该索引，因为已存在相同的索引。请修改。";
    }

    @Override
    public String getDefaultReason() {
        return "遇到了一些问题。%s";
    }

    @Override
    public String getDefaultSuggest() {
        return "更多详情请联系 Kylin 5 技术支持。";
    }

    @Override
    public String getUnexpectedToken() {
        return "以下内容存在语法错误（%s 列，%s 行）：\"%s\" 。请修改。";
    }

    @Override
    public String getBadSqlReason() {
        return "SQL 存在语法错误：%s";
    }

    @Override
    public String getBadSqlSuggest() {
        return "请修改。";
    }

    @Override
    public String getBadSqlTableNotFoundReason() {
        return "无法找到表 \"%s\" 。请检查后重试。";
    }

    @Override
    public String getBadSqlTableNotFoundSuggest() {
        return "请在数据源中导入表 \"%s\"。如果该表已经存在，请在查询中以\"数据库名.表名\"的形式进行引用。";
    }

    @Override
    public String getBadSqlColumnNotFoundReason() {
        return "无法找到列 \"%s\"。请检查此列是否在源表或可计算列中存在。若存在，可尝试重载表；若不存在，请联系管理员添加。";
    }

    @Override
    public String getProjectNumOverThreshold() {
        return "新建项目失败，项目数超过最大值：{%s}，请删除其他废弃项目后再尝试新建或联系管理员调整最大项目数。";
    }

    @Override
    public String getModelNumOverThreshold() {
        return "新建模型失败。模型超过最大值：{%s}。请删除其他废弃模型后再尝试新建或联系管理员调整最大模型数。";
    }

    @Override
    public String getAliasConflictOfApprovingRecommendation() {
        return "该名称已存在，请重新命名。";
    }

    @Override
    public String getInvalidTimeFormat() {
        return "无法设置时间分区列，选取的时间分区列不符合时间格式。请重新选择。";
    }

    @Override
    public String getSegmentMergeStorageCheckError() {
        return "合并 Segment 过程中 HDFS 存储空间可能超过阈值限制，系统主动终止合并任务。如需解除上述限制，请参照用户手册对参数 kylin.cube.merge-segment-storage-threshold 进行调整。";
    }

    @Override
    public String getDimensionTableUsedInThisModel() {
        return "无法设置此模型的维度表，因为其已被用作当前模型的事实表。请修改后重试。";
    }

    @Override
    public String getNoDataInTable() {
        return "无法从表 “%s” 中获取数据。请检查后重试。";
    }

    @Override
    public String getEffectiveDimensionNotFind() {
        return "以下列未作为维度添加到模型中，请删除后再保存或添加到模型中。\nColumn ID: %s";
    }

    @Override
    public String getInvalidIntegerFormat() {
        return "无法重写模型设置，“%s” 参数值必须为非负整数。请修改后重试。";
    }

    @Override
    public String getInvalidMemorySize() {
        return "无法重写模型设置，“spark-conf.spark.executor.memory” 参数值必须为非负整数，且单位为 GB。请修改后重试。";
    }

    @Override
    public String getInvalidBooleanFormat() {
        return "无法重写模型设置，“%s” 参数值必须为 “true” 或 “false”。请修改后重试。";
    }

    @Override
    public String getInvalidAutoMergeConfig() {
        return "无法重写模型设置，自动合并范围不能为空。请修改后重试。";
    }

    @Override
    public String getColumnNotExist() {
        return "列:[%s] 不存在.";
    }

    @Override
    public String getColumnParameterInvalid(String column) {
        return String.format(Locale.ROOT, "无法给列 ”%s” 赋值，值和列的类型不匹配。请检查后重试。", column);
    }

    @Override
    public String getInvalidVolatileRangeConfig() {
        return "无法重写模型设置，动态区间参数单位必须为“天”、“周”、“月”、“年”其中一个，且值必须为非负整数。请修改后重试。";
    }

    @Override
    public String getInvalidRetentionRangeConfig() {
        return "重写模型设置失败，留存设置值必须为非负整数，单位必须为自动合并选中单位中的最粗粒度单位.";
    }

    @Override
    public String getDisabledUser() {
        return "该用户已被禁用，请联系管理员。";
    }

    @Override
    public String getAddJobCheckFail() {
        return "当前无法提交任务，因为已有相同对象的构建任务正在进行。请稍后再试。";
    }

    @Override
    public String getAddJobCheckFailWithoutBaseIndex() {
        return "当前无法提交任务，Segment “%s” 不包含基础索引。请刷新此 Segment。";
    }

    @Override
    public String getAddExportJobFail() {
        return "无法提交任务。模型当前已有相同 Segment 的加载数据任务正在进行。请稍后重试。";
    }

    @Override
    public String getEmptyDatabase() {
        return "请输入参数 “Database” 的值。";
    }

    @Override
    public String getEmptyTableList() {
        return "请输入参数 “Table” 的值。";
    }

    @Override
    public String getAddJobCheckSegmentReadyFail() {
        return "无法添加任务，当前没有 READY 状态的 Segment。请稍后重试。";
    }

    @Override
    public String getTableReloadAddColumnExist(String table, String column) {
        return String.format(Locale.ROOT, "当前无法重载表。列 “%s” 在表 “%s” 中已存在。请修改后重试。", column, table);
    }

    @Override
    public String getColumnUnrecognized() {
        return "无法识别表达式中的列名 “%s”。 请使用 “TABLE_ALIAS.COLUMN“ 来命名。";
    }

    // Punctuations
    @Override
    public String getCOMMA() {
        return "，";
    }

    @Override
    public String getRecListOutOfDate() {
        return "由于优化建议所依赖的内容被删除，该优化建议已失效。请刷新页面后再试。";
    }

    @Override
    public String getGroupUuidNotExist() {
        return "无法操作用户组 (UUID:%s)。请检查后重试。";
    }

    @Override
    public String getModelOnlineWithEmptySeg() {
        return "该模型尚未添加 Segment，不可服务于查询。请先添加 Segment 后再上线。";
    }

    @Override
    public String getModelOnlineForbidden() {
        return "无法上线该模型。若需上线，请将配置项 “kylin.model.offline“ 设为 false。";
    }

    // multi level partition mapping
    @Override
    public String getMultiPartitionMappingReqeustNotValid() {
        return "无法更新多级分区列映射关系，参数 “multi_partition_columns“ 的值和模型中定义的多级分区列不一致。请检查后重试。";
    }

    @Override
    public String getScd2ModelOnlineWithScd2ConfigOff() {
        return "该模型因存在 ≥ 或 < 的连接条件，当前不可上线。请删除相应连接条件，或在项目设置中开启支持拉链表开关";
    }

    @Override
    public String getConnectDatabaseError() {
        return "当前无法连接 RDBMS 元数据库。请检查元数据库是否工作正常。";
    }

    // acl
    @Override
    public String getInvalidColumnAccess() {
        return "当前用户或用户组没有权限访问列 “%s“ 。";
    }

    @Override
    public String getInvalidSensitiveDataMaskColumnType() {
        return "暂不支持对 Boolean, Map, Array 类型的数据进行脱敏。";
    }

    @Override
    public String getNotSupportNestedDependentCol() {
        return "无法对列 “%s” 设置关联规则，因为该列已被其他列关联。";
    }

    @Override
    public String getInvalidRowACLUpdate() {
        return "请求中包含无效的 “rows” 或 “like_rows” 参数。请使用参数 “row_filter” 进行行级权限的更新。";
    }

    // Snapshots
    @Override
    public String getSnapshotOperationPermissionDenied() {
        return "没有权限操作此快照。请确保您有该快照对应的表的相关权限。";
    }

    @Override
    public String getSnapshotNotFound() {
        return "无法找到快照 “%s”。请检查后重试。";
    }

    @Override
    public String getSnapshotManagementNotEnabled() {
        return "快照管理模式未开启。请检查后重试。";
    }

    @Override
    public String getPartitionsToBuildCannotBeEmpty(List<String> tableDescNames) {
        return "在执行自定义分区刷新时，请为以下快照选取至少一个分区值： " + tableDescNames.toString();
    }

    @Override
    public String getProjectWithoutResourceGroup() {
        return "当前项目未绑定资源组，无法正常使用。请联系管理员进行绑定。";
    }

    @Override
    public String getModelIsNotMlp() {
        return "模型 “%s“ 未设置多级分区。请检查后重试。";
    }

    @Override
    public String getInvalidPartitionValue() {
        return "子分区值 “%s” 不存在，请检查后重试。";
    }

    @Override
    public String getPartitionValueNotSupport() {
        return "模型 “%s” 未设置子分区列。请设置后重试。";
    }

    @Override
    public String getAdminPermissionUpdateAbandon() {
        return "项目管理员不支持被更新权限。";
    }

    @Override
    public String getRowAclNotStringType() {
        return "LIKE 行级权限仅支持 char 或 varchar 类型维度，请检查后重试。";
    }

    @Override
    public String getExceedMaxAllowedPacket() {
        return "MySQL 元数据库返回结果超过配置限制。请联系管理员在 MySQL 中将配置 “max_allowed_packet” 调整至 256M。";
    }

    @Override
    public String getRowFilterExceedLimit() {
        return "过滤器总数超过上限 (%s/%s)，请检查后重试。";
    }

    @Override
    public String getRowFilterItemExceedLimit() {
        return "过滤器包含的值超过上限 (%s/%s)，请检查后重试。";
    }

    @Override
    public String getQueryHistoryColumnMeta() {
        return "查询开始时间,查询耗时,查询 ID,SQL 语句,查询对象,查询状态,查询节点,查询用户,查询信息\n";
    }

    @Override
    public String getSecondStorageJobExists() {
        return "当前无法关闭分层存储。模型 “%s” 存在正在运行的任务，请检查后再试。\n";
    }

    @Override
    public String getSecondStorageConcurrentOperate() {
        return "存在相关的分层存储的任务正在运行，请稍后重试。";
    }

    @Override
    public String getSecondStorageProjectJobExists() {
        return "当前无法关闭分层存储。项目 “%s” 存在正在运行的任务，请检查后再试。\n";
    }

    @Override
    public String getSecondStorageProjectEnabled() {
        return "项目 %s 未开启分层存储。";
    }

    @Override
    public String getSecondStorageModelEnabled() {
        return "模型 %s 未开启分层存储。";
    }

    @Override
    public String getSecondStorageLayoutNotExist() {
        return "不存在 Layout id %s。";
    }

    @Override
    public String getSecondStorageLayoutNotBaseTableIndex() {
        return "Layout id %s 不是和分层存储一致的基础明细索引。";
    }

    @Override
    public String getSecondStorageNotSupportType(String dataType) {
        return String.format(Locale.ROOT, "Skipping Index不支持'%s'列类型。", dataType);
    }

    @Override
    public String getSecondStorageSegmentWithoutBaseIndex() {
        return "Segment 中缺少基础明细索引，请添加后重试。";
    }

    @Override
    public String getSecondStorageDeleteNodeFailed() {
        return "节点%s存在数据，大小为%d bytes";
    }

    @Override
    public String getInvalidLowCardinalityDataType() {
        return "无效的数据类型，当前仅支持 LowCardinality(Nullable(String)) 或 Nullable(String) 的数据类型。";
    }

    @Override
    public String getJobRestartFailed() {
        return "分层存储任务不支持重启操作。\n";
    }

    @Override
    public String getJobResumeFailed() {
        return "分层存储任务暂时不能恢复，请稍后再试。\n";
    }

    @Override
    public String getJobPauseFailed() {
        return "该类型任务不支持暂停操作";
    }

    @Override
    public String getSegmentDropFailed() {
        return "Segment 正在导入分层存储中。请稍后重试。\n";
    }

    @Override
    public String getInvalidBrokerDefinition() {
        return "Broker 信息不可为空，请检查后重试。";
    }

    @Override
    public String getBrokerTimeoutMessage() {
        return "无法获取集群信息，请检查 Broker 信息是否正确，或确认 Kafka 服务器状态是否正常。";
    }

    @Override
    public String getStreamingTimeoutMessage() {
        return "无法获取样例数据，请检查后重试";
    }

    @Override
    public String getEmptyStreamingMessage() {
        return "该 Topic 无可展示的样例数据，请尝试换一个。";
    }

    @Override
    public String getInvalidStreamingMessageType() {
        return "无效的消息类型，当前仅支持 Json 或 Binary 格式的消息。请检查后重试。";
    }

    @Override
    public String getParseStreamingMessageError() {
        return "解析器无法解析样例数据，建议检查选项设置或者修改解析器后再尝试解析。";
    }

    @Override
    public String getReadKafkaJaasFileError() {
        return "无法正确读取 Kafka 认证文件，请检查后再试。";
    }

    @Override
    public String getBatchStreamTableNotMatch() {
        return "表 “%s” 与 Kafka 表的列不一致，请确认两者的列完全一致后重试。";
    }

    @Override
    public String getStreamingIndexesDelete() {
        return "无法删除流数据索引。请先停止流数据任务，再清空流数据 Segment。";
    }

    @Override
    public String getStreamingIndexesEdit() {
        return "无法编辑流数据索引。请先停止流数据任务，再清空流数据 Segment。";
    }

    @Override
    public String getStreamingIndexesAdd() {
        return "无法添加流数据索引。请先停止流数据任务，再清空流数据 Segment。";
    }

    @Override
    public String getStreamingIndexesApprove() {
        return "流数据模型暂无法通过优化建议。";
    }

    @Override
    public String getStreamingIndexesConvert() {
        return "流数据模型暂无法转换为优化建议。";
    }

    @Override
    public String getForcedToTieredstorageAndForceToIndex() {
        return "“force_to_index=ture“ 时，查询使用分层存储失败时不能下压，“forcedToTieredStorage“=1 或者 conf=1 无效，请修改后重试";
    }

    @Override
    public String getForcedToTieredstorageReturnError() {
        return "查询失败。分层存储不可用，请修复后重试查询";
    }

    @Override
    public String getForcedToTieredstorageInvalidParameter() {
        return "无效的参数值，请修改后重试";
    }

    @Override
    public String getParameterEmpty() {
        return PARAMETER_EMPTY;
    }

    @Override
    public String getCannotForceToBothPushdodwnAndIndex() {
        return "不能同时强制下推和击中模型，参数 “forcedToPushDown” 和 “forced_to_index” 不能同时使用。请检查后重试。";
    }

    @Override
    public String getSecondStorageNodeNotAvailable() {
        return "无法添加节点。节点不存在或被其他项目占用，请修改后重试";
    }

    @Override
    public String getBaseTableIndexNotAvailable() {
        return "当前无法开启分层存储。请先创建基础所明细索引。";
    }

    @Override
    public String getPartitionColumnNotAvailable() {
        return "当前无法开启分层存储。请将时间分区列添加到维度，并更新基础明细索引。";
    }

    @Override
    public String getProjectLocked() {
        return "当前项目的分层存储中正在进行数据迁移，请稍后重试。";
    }

    @Override
    public String getFixStreamingSegment() {
        return "无法修复流数据模型的 Segment。";
    }

    @Override
    public String getStreamingDisabled() {
        return "开发中。";
    }

    @Override
    public String getNoStreamingModelFound() {
        return "无法查询。由于流数据必须通过索引查询，请确保有相应的索引。";
    }

    @Override
    public String getStreamingTableNotSupportAutoModeling() {
        return "不支持流数据表进行自动建模。";
    }

    @Override
    public String getSparkFailure() {
        return "无法完成操作，请检查 Spark 环境后重试。";
    }

    @Override
    public String getDownloadQueryHistoryTimeout() {
        return "导出超时，请稍后重试。";
    }

    @Override
    public String getStreamingOperationNotSupport() {
        return "API调用失败，暂不支持调用流数据相关的API。";
    }

    @Override
    public String getJdbcConnectionInfoWrong() {
        return "连接信息错误，请检查后重试。";
    }

    @Override
    public String getJdbcNotSupportPartitionColumnInSnapshot() {
        return "当前数据源的 Snapshot 无法使用分区设置。";
    }

    @Override
    public String getParamTooLarge() {
        return "参数 '%s' 太长， 最大 %s KB。";
    }

    @Override
    public String getSqlBlacklistItemIdEmpty() {
        return "黑名单条目 id 不能为空";
    }

    @Override
    public String getSqlBlacklistItemRegexAndSqlEmpty() {
        return "黑名单条目正则表达式和 sql 不能都为空";
    }

    @Override
    public String getSqlBlacklistItemProjectEmpty() {
        return "黑名单所属项目不能为空";
    }

    @Override
    public String getSqlBlacklistItemIdExists() {
        return "黑名单条目 id 不能为空";
    }

    @Override
    public String getSqlBlacklistItemIdNotExists() {
        return "黑名单条目 id 已存在";
    }

    @Override
    public String getSqlBlacklistItemRegexExists() {
        return "黑名单条目正则表达式已存在";
    }

    @Override
    public String getSqlBlacklistItemSqlExists() {
        return "黑名单条目 sql 已存在";
    }

    @Override
    public String getSqlBlacklistItemIdToDeleteEmpty() {
        return "待删除的黑名单条目 id 不能为空";
    }

    @Override
    public String getSqlBlacklistQueryRejected() {
        return "查询被黑名单终止, 黑名单条目 id: %s.";
    }

    @Override
    public String getSqlBlackListQueryConcurrentLimitExceeded() {
        return "查询被黑名单终止，因为超出了并发限制, 黑名单条目 id: %s, 并发限制: {%s}";
    }

    @Override
    public String getInvalidRange() {
        return "%s 不是 [%s - %s] 范围内整数";
    }

    @Override
    public String getlDapUserDataSourceConnectionFailed() {
        return "LDAP服务异常，请检查用户数据源。";
    }

    @Override
    public String getLdapUserDataSourceConfigError() {
        return "LDAP 连接错误，请检查 LDAP 配置信息！";
    }

    @Override
    public String getTableNoColumnsPermission() {
        return "请向表中的列添加权限! ";
    }

    @Override
    public String getDisablePushDownPrompt() {
        return "当您需要查询下压时，需要开启下压开关。";
    }

    @Override
    public String getNonExistedModel() {
        return "模型 %s 不存在，请检查后再重试.";
    }

    @Override
    public String getLackProject() {
        return "请填写项目参数.";
    }

    @Override
    public String getNonExistProject() {
        return "项目 %s 不存在，请检查后再重试.";
    }

    @Override
    public String getInvalidMergeSegmentWithoutDFS() {
        return "当前 Segments 所包含的索引未加载至 HDFS 存储/对象存储，请确保索引加载至 HDFS 存储后再合并。";
    }

    @Override
    public String getProfilingNotEnabled() {
        return "构建火焰图" + PARAMETER_NOT_ENABLED;
    }

    @Override
    public String getProfilingNotStartError() {
        return PROFILING_STATUS_ERROR + "收集任务未开始。";
    }

    @Override
    public String getProfilingStartedError() {
        return PROFILING_STATUS_ERROR + "收集任务正在执行。";
    }

    @Override
    public String getProfilingJobNotStartError() {
        return PROFILING_STATUS_ERROR + "Spark job 构建任务未开始。";
    }

    @Override
    public String getProfilingJobFinishedError() {
        return PROFILING_STATUS_ERROR + "Spark job 构建任务已结束。";
    }

    @Override
    public String getProfilingYarnAppIdError() {
        return PROFILING_STATUS_ERROR + "yarnAppId 输入有误, 请重试。";
    }

    @Override
    public String getProfilingCollectTimeout() {
        return "构建火焰图任务" + TASK_TIMEOUT;
    }

    @Override
    public String getSecondStorageIndexNotSupport() {
        return "Order by列和Skipping Index列不支持使用分区列";
    }

    @Override
    public String getSecondStorageIndexNotAllowNullable() {
        return "Order by和Skipping Index不允许列类型为Nullable。";
    }

    @Override
    public String getSecondStorageOrderByHasData() {
        return "在线索引已加载分层存储数据，不支持修改Order by列。如必须修改，请先清空分层存储数据";
    }

    @Override
    public String getSecondStorageNodeNotAvailable(String nodeName) {
        return String.format(Locale.ROOT, "分层存储节点'%s'不可用。", nodeName);
    }

    @Override
    public String getDDLUnSupported() {
        return "不支持的 DDL 语法，仅支持 `create view`, `drop view`, `alter view`, `show create table` 语法";
    }

    @Override
    public String getDDLViewNameError() {
        return "视图名需要以 KYLIN_ 开头";
    }

    @Override
    public String getDDLViewNameDuplicateError() {
        return "逻辑视图名称和已有视图重复";
    }

    @Override
    public String getDDLDropError() {
        return "仅支持删除 view 类型表且 view 名称需要以 KYLIN_ 开头";
    }

    @Override
    public String getDDLTableNotLoad(String table) {
        return String.format(Locale.ROOT, "'%s' 没有加载到数据源", table);
    }

    @Override
    public String getDDLTableNotSupport(String table) {
        return String.format(Locale.ROOT, "仅支持 hive 数据表，但 '%s' 不是 hive 表", table);
    }

    @Override
    public String getDDLDatabaseAccessnDenied() {
        return "用户没有视图所在数据库的权限";
    }

    @Override
    public String getDDLLogicalViewHasUsed(String table, String project) {
        return String.format(Locale.ROOT, "表 %s 已经在项目 %s 中加载过，请先卸载后再删除该表.", table, project);
    }

    @Override
    public String getDDLLogicalViewSourceTableError(String table) {
        return String.format(Locale.ROOT, "来源表 %s 是 Logical View，不能在 SQL 中使用", table);
    }

    @Override
    public String getDDLRestrictError(String syntax) {
        return String.format(Locale.ROOT, "仅支持 %s 语法", syntax);
    }

    @Override
    public String getLoadLogicalViewError(String tableName, String project) {
        return String.format(Locale.ROOT, "无法加载表: %s , 仅支持在项目 %s 中加载此表", tableName, project);
    }

    @Override
    public String getAsyncQueryCancel(String queryId) {
        return String.format(Locale.ROOT, "ID为%s的异步查询正被执行取消。", queryId);

    }

    @Override
    public String getQueryNotRunningError() {
        return "该查询没有在运行，请检查";
    }

    @Override
    public String getIllegalNegative(String parameter) {
        return String.format(PARAMETER_MUST_BE_POSITIVE_NUMBER, parameter);
    }
}
