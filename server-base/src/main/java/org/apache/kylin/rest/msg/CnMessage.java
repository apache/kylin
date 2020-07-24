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
public class CnMessage extends Message {

    private static CnMessage instance = null;

    protected CnMessage() {

    }

    public static CnMessage getInstance() {
        if (instance == null) {
            instance = new CnMessage();
        }
        return instance;
    }

    // Cube
    public String getCUBE_NOT_FOUND() {
        return "找不到 Cube '%s'";
    }

    public String getSEG_NOT_FOUND() {
        return "找不到 Segment '%s'";
    }

    public String getKAFKA_DEP_NOT_FOUND() {
        return "找不到 Kafka 依赖";
    }

    public String getBUILD_DRAFT_CUBE() {
        return "Cube 草稿不能被构建";
    }

    public String getBUILD_BROKEN_CUBE() {
        return "损坏的 cube '%s' 不能被构建";
    }

    public String getINCONSISTENT_CUBE_DESC_SIGNATURE() {
        return "Inconsistent cube desc signature for '%s', if it's right after an upgrade, please try 'Edit CubeDesc' to delete the 'signature' field. Or use 'bin/metastore.sh refresh-cube-signature' to batch refresh all cubes' signatures, then reload metadata to take effect.";
    }

    public String getDELETE_NOT_FIRST_LAST_SEG() {
        return "非首尾 segment '%s' 不能被删除";
    }

    public String getDELETE_NOT_READY_SEG() {
        return "非 READY 状态 segment '%s' 不能被删除, 请先抛弃它正在运行的任务";
    }

    public String getDELETE_SEG_FROM_READY_CUBE() {
        return "segment '%s' 不能从 READY 状态的 cube '%s' 中删除, 请先disable cube";
    }

    public String getINVALID_BUILD_TYPE() {
        return "非法构建类型: '%s'";
    }

    public String getNO_ACL_ENTRY() {
        return "找不到对象 '%s' 的授权记录";
    }

    public String getACL_INFO_NOT_FOUND() {
        return "找不到对象 '%s' 的授权信息";
    }

    public String getACL_DOMAIN_NOT_FOUND() {
        return "找不到授权对象";
    }

    public String getPARENT_ACL_NOT_FOUND() {
        return "找不到上级授权";
    }

    public String getDISABLE_NOT_READY_CUBE() {
        return "仅 ready 状态的 cube 可以被禁用, '%s' 的状态是 %s";
    }

    public String getPURGE_NOT_DISABLED_CUBE() {
        return "仅 disabled 状态的 cube 可以被清空, '%s' 的状态是 %s";
    }

    public String getCLONE_BROKEN_CUBE() {
        return "损坏的 cube '%s' 不能被克隆";
    }

    public String getINVALID_CUBE_NAME() {
        return "非法 cube 名称 '%s', 仅支持字母, 数字和下划线";
    }

    public String getCUBE_ALREADY_EXIST() {
        return "Cube 名称 '%s' 已存在";
    }

    public String getCUBE_DESC_ALREADY_EXIST() {
        return "Cube '%s' 已存在";
    }

    public String getBROKEN_CUBE_DESC() {
        return "损坏的 Cube 描述 '%s'";
    }

    public String getENABLE_NOT_DISABLED_CUBE() {
        return "仅 disabled 状态的 cube 可以被启用, '%s' 的状态是 %s";
    }

    public String getNO_READY_SEGMENT() {
        return "Cube '%s' 不包含任何 READY 状态的 segment";
    }

    public String getENABLE_WITH_RUNNING_JOB() {
        return "Cube 存在正在运行的任务, 不能被启用";
    }

    public String getDISCARD_JOB_FIRST() {
        return "Cube '%s' 存在正在运行或失败的任务, 请抛弃它们后重试";
    }

    public String getIDENTITY_EXIST_CHILDREN() {
        return "'%s' 存在下级授权";
    }

    public String getINVALID_CUBE_DEFINITION() {
        return "非法 cube 定义";
    }

    public String getEMPTY_CUBE_NAME() {
        return "Cube 名称不可为空";
    }

    public String getUSE_DRAFT_MODEL() {
        return "不能使用模型草稿 '%s'";
    }

    public String getINCONSISTENT_CUBE_DESC() {
        return "Cube 描述 '%s' 与现有不一致， 请清理 cube 或避免更新 cube 描述的关键字段";
    }

    public String getUPDATE_CUBE_NO_RIGHT() {
        return "无权限更新此 cube";
    }

    public String getNOT_STREAMING_CUBE() {
        return "Cube '%s' 不是实时 cube";
    }

    public String getCUBE_RENAME() {
        return "Cube 不能被重命名";
    }

    public String getREBUILD_SNAPSHOT_OF_VIEW() {
        return "不支持重新构建 Hive view '%s' 的 snapshot, 请刷新 Cube 的 segment";
    }

    // Model
    public String getINVALID_MODEL_DEFINITION() {
        return "非法模型定义";
    }

    public String getEMPTY_MODEL_NAME() {
        return "模型名称不可为空";
    }

    public String getINVALID_MODEL_NAME() {
        return "非法模型名称 '%s', 仅支持字母, 数字和下划线";
    }

    public String getDUPLICATE_MODEL_NAME() {
        return "模型名称 '%s' 已存在, 不能被创建";
    }

    public String getDROP_REFERENCED_MODEL() {
        return "模型被 Cube '%s' 引用, 不能被删除";
    }

    public String getUPDATE_MODEL_KEY_FIELD() {
        return "由于维度、度量或者连接关系被修改导致与存在的cube定义不一致，因而当前模型无法保存。";
    }

    public String getBROKEN_MODEL_DESC() {
        return "损坏的模型描述 '%s'";
    }

    public String getMODEL_NOT_FOUND() {
        return "找不到模型 '%s'";
    }

    public String getEMPTY_PROJECT_NAME() {
        return "项目名称不可为空";
    }

    public String getEMPTY_NEW_MODEL_NAME() {
        return "新模型名称不可为空";
    }

    public String getUPDATE_MODEL_NO_RIGHT() {
        return "无权限更新此模型";
    }

    public String getMODEL_RENAME() {
        return "模型不能被重命名";
    }

    // Job
    public String getILLEGAL_TIME_FILTER() {
        return "非法时间条件: %s";
    }

    public String getILLEGAL_EXECUTABLE_STATE() {
        return "非法状态: %s";
    }

    public String getILLEGAL_JOB_TYPE() {
        return "非法任务类型, id: %s.";
    }

    // Acl
    public String getUSER_NOT_EXIST() {
        return "用户 '%s' 不存在, 请确认用户是否存在。";
    }

    // Project
    public String getINVALID_PROJECT_NAME() {
        return "非法项目名词 '%s', 仅支持字母, 数字和下划线";
    }

    public String getPROJECT_ALREADY_EXIST() {
        return "项目 '%s' 已存在";
    }

    public String getPROJECT_NOT_FOUND() {
        return "找不到项目 '%s'";
    }

    public String getDELETE_PROJECT_NOT_EMPTY() {
        return "不能修改该项目，如需要修改请先清空其中的Cube和Model";
    }

    public String getRENAME_PROJECT_NOT_EMPTY() {
        return "不能重命名该项目，如果要重命名请先清空其中的Cube和Model";
    }
    // Table
    public String getHIVE_TABLE_NOT_FOUND() {
        return "找不到 Hive 表 '%s'";
    }

    public String getTABLE_DESC_NOT_FOUND() {
        return "找不到表 '%s'";
    }

    public String getTABLE_IN_USE_BY_MODEL() {
        return "表已被模型 '%s' 使用";
    }

    // Cube Desc
    public String getCUBE_DESC_NOT_FOUND() {
        return "找不到 cube '%s'";
    }

    // Streaming
    public String getINVALID_TABLE_DESC_DEFINITION() {
        return "非法表定义";
    }

    public String getINVALID_STREAMING_CONFIG_DEFINITION() {
        return "非法 StreamingConfig 定义";
    }

    public String getINVALID_KAFKA_CONFIG_DEFINITION() {
        return "非法 KafkaConfig 定义";
    }

    public String getADD_STREAMING_TABLE_FAIL() {
        return "添加流式表失败";
    }

    public String getEMPTY_STREAMING_CONFIG_NAME() {
        return "StreamingConfig 名称不可为空";
    }

    public String getSTREAMING_CONFIG_ALREADY_EXIST() {
        return "StreamingConfig '%s' 已存在";
    }

    public String getSAVE_STREAMING_CONFIG_FAIL() {
        return "保存 StreamingConfig 失败";
    }

    public String getKAFKA_CONFIG_ALREADY_EXIST() {
        return "KafkaConfig '%s' 已存在";
    }

    public String getCREATE_KAFKA_CONFIG_FAIL() {
        return "StreamingConfig 已创建, 但 KafkaConfig 创建失败";
    }

    public String getSAVE_KAFKA_CONFIG_FAIL() {
        return "KafkaConfig 保存失败";
    }

    public String getROLLBACK_STREAMING_CONFIG_FAIL() {
        return "操作失败, 并且回滚已创建的 StreamingConfig 失败";
    }

    public String getROLLBACK_KAFKA_CONFIG_FAIL() {
        return "操作失败, 并且回滚已创建的 KafkaConfig 失败";
    }

    public String getUPDATE_STREAMING_CONFIG_NO_RIGHT() {
        return "无权限更新此 StreamingConfig";
    }

    public String getUPDATE_KAFKA_CONFIG_NO_RIGHT() {
        return "无权限更新此 KafkaConfig";
    }

    public String getSTREAMING_CONFIG_NOT_FOUND() {
        return "找不到 StreamingConfig '%s'";
    }

    // Query
    public String getQUERY_NOT_ALLOWED() {
        return "'%s' 模式不支持查询";
    }

    public String getNOT_SUPPORTED_SQL() {
        return "不支持的 SQL";
    }

    public String getTABLE_META_INCONSISTENT() {
        return "表元数据与JDBC 元数据不一致";
    }

    public String getCOLUMN_META_INCONSISTENT() {
        return "列元数据与JDBC 元数据不一致";
    }

    // Access
    public String getACL_PERMISSION_REQUIRED() {
        return "需要授权";
    }

    public String getSID_REQUIRED() {
        return "找不到 Sid";
    }

    public String getREVOKE_ADMIN_PERMISSION() {
        return "不能取消创建者的管理员权限";
    }

    public String getACE_ID_REQUIRED() {
        return "找不到 Ace id";
    }

    // Admin
    public String getGET_ENV_CONFIG_FAIL() {
        return "无法获取 Kylin env Config";
    }

    // User
    public String getAUTH_INFO_NOT_FOUND() {
        return "找不到权限信息";
    }

    public String getUSER_NOT_FOUND() {
        return "找不到用户 '%s'";
    }

    // Diagnosis
    public String getDIAG_NOT_FOUND() {
        return "在 %s 找不到 diag.sh";
    }

    public String getGENERATE_DIAG_PACKAGE_FAIL() {
        return "无法生成诊断包";
    }

    public String getDIAG_PACKAGE_NOT_AVAILABLE() {
        return "诊断包不可用, 路径: %s";
    }

    public String getDIAG_PACKAGE_NOT_FOUND() {
        return "找不到诊断包, 路径: %s";
    }

    public String getDIAG_PROJECT_NOT_FOUND() {
        return "找不到项目: %s.";
    }

    public String getDIAG_JOBID_NOT_FOUND() {
        return "找不到任务ID: %s.";
    }

    // Encoding
    public String getVALID_ENCODING_NOT_AVAILABLE() {
        return "无法为数据类型: %s 提供合法的编码";
    }

    // ExternalFilter
    public String getFILTER_ALREADY_EXIST() {
        return "Filter '%s' 已存在";
    }

    public String getFILTER_NOT_FOUND() {
        return "找不到 filter '%s'";
    }

    // Basic
    public String getHBASE_FAIL() {
        return "HBase 遇到错误: '%s'";
    }

    public String getHBASE_FAIL_WITHOUT_DETAIL() {
        return "HBase 遇到错误";
    }
}
