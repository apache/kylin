<template>
  <div class="basic-setting">
    <!-- 项目基本设置 -->
    <EditableBlock
      :header-content="$t('basicInfo')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'basic-info')"
      :is-reset="false"
      @submit="(scb, ecb) => handleSubmit('basic-info', scb, ecb)">
      <div class="setting-item">
        <div class="setting-label font-medium">{{$t('projectName')}}</div>
        <div class="setting-value fixed">{{project.alias || project.project}}</div>
      </div>
      <div class="setting-item">
        <div class="setting-label font-medium">Developer Info</div>
        <div class="setting-value fixed"> {{project.package_version}} [{{project.package_timestamp}}, {{project.git_commit}}]</div>
      </div>
      <div class="setting-item">
        <span class="setting-label font-medium">
          {{$t('enableSemiAutomatic')}}
          <!-- <span class="beta-label">BETA</span> -->
        </span>
        <span class="setting-value fixed">
          <el-switch
            v-model="form.semi_automatic_mode"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('enableSemiAutomaticDesc')}}</div>
      </div>
      <div class="setting-item clearfix">
        <div class="setting-label font-medium">{{$t('description')}}</div>
        <div class="setting-value">{{project.description}}</div>
        <el-input class="setting-input" :rows="3" type="textarea" size="small" v-model="form.description"></el-input>
      </div>
    </EditableBlock>
    <!-- 低效存储设置 -->
    <EditableBlock
      :header-content="$t('indexOptimizationSettings')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'index-optimization')"
      @submit="(scb, ecb) => handleSubmit('index-optimization', scb, ecb)"
      @cancel="(scb, ecb) => handleResetForm('index-optimization', scb, ecb)">
      <el-form ref="setting-index-optimization" :model="form" :rules="indexOptimization">
        <div class="setting-item">
          <span class="setting-label font-medium">{{$t('storageGarbage')}}</span>
          <div class="setting-desc large">
            <span>{{$t('storageGarbageDesc1')}}</span>
            <el-select
              class="setting-input"
              size="small"
              style="width: 100px;"
              v-model="form.frequency_time_window"
              :placeholder="$t('kylinLang.common.pleaseChoose')">
              <el-option
                v-for="lowUsageStorageType in lowUsageStorageTypes"
                :key="lowUsageStorageType"
                :label="$t(lowUsageStorageType+'1')"
                :value="lowUsageStorageType">
              </el-option>
            </el-select>
            <span>{{$t('storageGarbageDesc2')}}</span>
            <el-form-item class="setting-input" prop="low_frequency_threshold">
              <el-input-number size="small" style="width: 100px;" :max="9999" input-enabled v-number="form.low_frequency_threshold" v-model="form.low_frequency_threshold" :controls="false"></el-input-number>
            </el-form-item>
            <span>{{$store.state.project.isSemiAutomatic ? $t('storageGarbageDesc3ForSemiAutomatic') : $t('storageGarbageDesc3')}}</span>
          </div>
        </div>
      </el-form>
    </EditableBlock>
    <!-- 下压查询设置 -->
    <EditableBlock
      :header-content="$t('pushdownSettings')"
      :isEditable="false">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('pushdownEngine')}}</span><span class="setting-value fixed">
          <el-switch
            v-model="form.push_down_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')"
            @input="value => handleSwitch('pushdown-engine', value)">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('pushdownEngineDesc')}}</div>
      </div>
    </EditableBlock>
    <!-- Segment设置 -->
    <EditableBlock
      :header-content="$t('segmentSettings')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'segment-settings')"
      @submit="(scb, ecb) => handleSubmit('segment-settings', scb, ecb)"
      @cancel="(scb, ecb) => handleResetForm('segment-settings', scb, ecb)">
      <el-form ref="segment-setting-form" :model="form" :rules="rules">
        <div class="setting-item">
          <span class="setting-label font-medium">{{$t('segmentMerge')}}</span><span class="setting-value fixed">
            <el-switch
              v-model="form.auto_merge_enabled"
              :active-text="$t('kylinLang.common.OFF')"
              :inactive-text="$t('kylinLang.common.ON')">
            </el-switch>
          </span>
          <div class="setting-desc">{{$t('segmentMergeDesc')}}</div>
          <div class="field-item" :class="{ disabled: !form.auto_merge_enabled }">
            <span class="setting-label font-medium">{{$t('autoMerge')}}</span>
            <span class="setting-value">
              {{form.auto_merge_time_ranges.map(autoMergeConfig => $t(autoMergeConfig)).join(', ')}}
            </span>
            <el-checkbox-group class="setting-input" :value="form.auto_merge_time_ranges" @input="handleCheckMergeRanges" :disabled="!form.auto_merge_enabled">
              <el-checkbox
                v-for="autoMergeType in autoMergeTypes"
                :key="autoMergeType"
                :label="autoMergeType">
                {{$t(autoMergeType)}}
              </el-checkbox>
            </el-checkbox-group>
          </div>
          <div class="field-item" :class="{ disabled: !form.auto_merge_enabled }">
            <span class="setting-label font-medium">{{$t('volatile')}}</span>
            <span class="setting-value">
              {{form.volatile_range.volatile_range_number}} {{$t(form.volatile_range.volatile_range_type.toLowerCase())}}
            </span>
            <el-form-item class="setting-input" prop="volatile_range.volatile_range_number">
              <el-input size="small" style="width: 100px;" v-number="form.volatile_range.volatile_range_number" v-model="form.volatile_range.volatile_range_number" :disabled="!form.auto_merge_enabled"></el-input>
            </el-form-item><el-select
              class="setting-input"
              size="small"
              style="width: 100px;"
              v-model="form.volatile_range.volatile_range_type"
              :disabled="!form.auto_merge_enabled"
              :placeholder="$t('kylinLang.common.pleaseChoose')">
              <el-option
                v-for="volatileType in volatileTypes"
                :key="volatileType"
                :label="$t(volatileType.toLowerCase())"
                :value="volatileType">
              </el-option>
            </el-select>
            <div class="setting-desc">{{$t('volatileTip')}}</div>
          </div>
        </div>
        <div class="setting-item">
          <span class="setting-label font-medium">{{$t('retentionThreshold')}}</span><span class="setting-value fixed ksd-fs-12">
            <el-switch
              v-model="form.retention_range.retention_range_enabled"
              :active-text="$t('kylinLang.common.OFF')"
              :inactive-text="$t('kylinLang.common.ON')">
            </el-switch>
          </span>
          <div class="setting-desc">{{$t('retentionThresholdDesc')}}</div>
          <div class="field-item" :class="{ disabled: !form.retention_range.retention_range_enabled }">
            <span class="setting-label font-medium">{{$t('retentionThreshold')}}</span>
            <span class="setting-value">
              {{form.retention_range.retention_range_number}} {{$t(form.retention_range.retention_range_type.toLowerCase())}}
            </span>
            <el-form-item class="setting-input" prop="retention_range.retention_range_number">
              <el-input size="small" style="width: 100px;" v-number="form.retention_range.retention_range_number" v-model="form.retention_range.retention_range_number" :disabled="!form.retention_range.retention_range_enabled"></el-input>
            </el-form-item>
            <el-select
              class="setting-input"
              size="small"
              :disabled="!form.retention_range.retention_range_enabled"
              style="width: 100px;"
              v-model="form.retention_range.retention_range_type"
              :placeholder="$t('kylinLang.common.pleaseChoose')">
              <el-option
                v-for="type in retentionTypes.filter(it => it !== 'WEEK')"
                :key="type"
                :label="$t(type.toLowerCase())"
                :value="type">
              </el-option>
            </el-select>
          </div>
        </div>
        <div class="setting-item">
          <span class="setting-label font-medium">{{$t('emptySegmentEnable')}} <span class="beta-label">BETA</span></span><span class="setting-value fixed ksd-fs-12">
            <el-switch
              v-model="form.create_empty_segment_enabled"
              :active-text="$t('kylinLang.common.OFF')"
              :inactive-text="$t('kylinLang.common.ON')">
            </el-switch>
          </span>
          <div class="setting-desc">{{$t('emptySegmentEnableDesc')}}</div>
        </div>
      </el-form>
    </EditableBlock>
    <EditableBlock
      v-if="$store.state.project.isSemiAutomatic"
      ref="accelerationRuleSettings"
      :header-content="$t('accelerationRuleSettings')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'acceleration-rule-settings')"
      @submit="(scb, ecb) => handleSubmit('acceleration-rule-settings', scb, ecb)"
      @cancel="(scb, ecb) => handleResetForm('acceleration-rule-settings', scb, ecb)">
      <el-form ref="rulesForm" :rules="rulesSettingRules" :model="rulesObj" size="medium" class="ruleSetting">
        <div class="conds">
          <div class="conds-title">
            <span class="setting-label font-medium">{{$t('querySubmitter')}}</span><span class="ksd-fs-12">
            <el-switch v-model="rulesObj.submitter_enable" :active-text="$t('kylinLang.common.OFF')" :inactive-text="$t('kylinLang.common.ON')"></el-switch></span>
          </div>
          <div class="conds-content">
            <div class="ksd-fs-12 ksd-mt-5">{{$t('querySubmitterTips')}}</div>
            <div class="vip-users-block">
              <el-form-item prop="users">
                <div class="ksd-mt-8 conds-title"><i class="el-icon-ksd-table_admin"></i> User</div>
                <el-select v-model="rulesObj.users" v-event-stop :popper-append-to-body="false" filterable remote :remote-method="remoteMethod" :loading="loading" size="medium" :placeholder="rulesObj.users.length ? '' : $t('kylinLang.common.pleaseSelectOrSearch')" class="ksd-mt-5" multiple style="width:100%">
                  <span slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!rulesObj.users.length"></span>
                  <el-option v-for="item in filterSubmitterUserOptions" :key="item" :label="item" :value="item"></el-option>
                </el-select>
              </el-form-item>
              <el-form-item prop="user_groups">
                <div class="ksd-mt-8 conds-title"><i class="el-icon-ksd-table_group"></i> User Group</div>
                <el-select v-model="rulesObj.user_groups" v-event-stop :popper-append-to-body="false" filterable size="medium" :placeholder="rulesObj.user_groups.length ? '' : $t('kylinLang.common.pleaseSelectOrSearch')" class="ksd-mt-5" multiple style="width:100%">
                  <span slot="prefix" class="el-input__icon el-ksd-icon-search_22" v-if="!rulesObj.user_groups.length"></span>
                  <el-option v-for="item in allSubmittersOptions.group" :key="item" :label="item" :value="item"></el-option>
                </el-select>
              </el-form-item>
            </div>
          </div>
        </div>
        <div class="conds">
          <div class="conds-title">
            <span class="setting-label font-medium">{{$t('queryDuration')}}</span>
          </div>
          <div class="conds-content clearfix">
            <div class="duration-rule ksd-mt-8 ksd-fs-12">
              {{$t('from')}}
              <el-form-item prop="min_duration" style="display: inline-block;">
                <el-input v-model.trim="rulesObj.min_duration" v-number="rulesObj.min_duration" size="small" :class="['rule-setting-input', durationError && 'is-error']" @blur="$refs.rulesForm.validateField('min_duration')"></el-input>
              </el-form-item>
              {{$t('to')}}
              <el-form-item prop="max_duration" style="display: inline-block;">
                <el-input v-model.trim="rulesObj.max_duration" v-number="rulesObj.max_duration" size="small" :class="['rule-setting-input', durationError && 'is-error']" @blur="$refs.rulesForm.validateField('max_duration')"></el-input>
              </el-form-item>
              {{$t('secondes')}}
            </div>
            <span class="error-msg" v-if="durationError && durationErrorMsg">{{durationErrorMsg}}</span>
          </div>
        </div>
        <div class="conds">
          <div class="conds-title">
            <span class="setting-label font-medium">{{$t('hitRules')}}</span>
          </div>
          <div class="conds-content clearfix">
            <div class="ksd-fs-12 ksd-mt-8">
              {{$t('ruleTips')}}
              <el-tooltip effect="dark" :content="$t('ruleTooltips')" placement="top"><i class="el-ksd-icon-more_info_16 icon ksd-fs-16"></i></el-tooltip>
            </div>
            <div class="ksd-fs-12 ksd-mt-8">
              {{$t('timeFrame')}}
              <el-form-item prop="effective_days" class="inline-form-item">
                <el-input-number style="width:100px;" input-enabled v-model="rulesObj.effective_days" size="small" :min="1" :max="30" ></el-input-number>
              </el-form-item>
              {{$t('days')}}
            </div>
            <div class="ksd-fs-12 ksd-mt-8">
              {{$t('hitNums')}}
              <el-form-item prop="min_hit_count" class="inline-form-item">
                <el-input style="width:100px;" v-model.trim="rulesObj.min_hit_count" v-number4="rulesObj.min_hit_count" size="small"></el-input>
              </el-form-item>
              {{$t('hitNumTips')}}
            </div>
          </div>
        </div>
        <div class="conds">
          <div class="conds-title">
            <span class="setting-label font-medium">{{$t('optimizationSuggestions')}}</span>
          </div>
          <div class="conds-content clearfix">
            <div class="conds-content clearfix">
              <div class="ksd-mt-8 ksd-fs-12">
                {{$t('suggestionTip1')}}
                <el-form-item prop="recommendations_value" style="display: inline-block;">
                  <el-select v-model="rulesObj.recommendations_value" v-event-stop size="mini" :disabled="!rulesObj.recommendation_enable" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" class="ksd-mt-5" style="width:70px">
                    <el-option v-for="item in [10, 20, 50, 100]" :key="item" :label="item" :value="item"></el-option>
                  </el-select>
                </el-form-item>
                <span v-html="$t('suggestionTip2')"></span>
              </div>
            </div>
            <div class="ksd-fs-12 ksd-mt-12">
              {{$t('recommendationFrequency')}}
              <el-form-item prop="update_frequency" class="inline-form-item">
                <el-input-number style="width:100px;" input-enabled v-model="rulesObj.update_frequency" size="small" :min="1"></el-input-number>
              </el-form-item>
              <span v-html="$t('recommendationFrequencyTips')"></span>
            </div>
          </div>
        </div>
      </el-form>
    </EditableBlock>
    <!-- 屏蔽列设置 -->
    <EditableBlock
      :header-content="$t('excludeRuleSettings')"
      :isEditable="false">
      <div class="setting-item">
        <div class="conds-title">
          <span class="setting-label font-medium">{{$t('excludeRule')}}</span><span class="ksd-fs-12">
          <el-switch
            :value="form.table_exclusion_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')"
            @input="value => handleSwitch('exclude-rule', value)"></el-switch></span>
        </div>
        <div class="conds-content clearfix">
          <div class="ksd-mt-8 ksd-fs-14">
            <div class="exclude-rule-msg">
              <p class="tips"><span v-html="$t('excludeRuleTip')"></span><span class="review-details" @click="showExcludeRuleDetails = !showExcludeRuleDetails">{{$t('moreDetails')}}<i class="" :class="['arrow', 'ksd-fs-16', showExcludeRuleDetails ? 'el-ksd-n-icon-arrow-down-outlined' : 'el-ksd-n-icon-arrow-right-outlined']"></i></span></p>
              <div class="details" v-if="showExcludeRuleDetails">
                <ol>
                  <li><i class="point ksd-mr-2">•</i>{{$t('excludeRuleDetailMsg1')}}</li>
                  <li><i class="point ksd-mr-2">•</i>{{$t('excludeRuleDetailMsg2')}}</li>
                  <li><i class="point ksd-mr-2">•</i>{{$t('excludeRuleDetailMsg3')}}</li>
                </ol>
              </div>
            </div>
            <div v-if="form.table_exclusion_enabled">
              <el-button class="ksd-mt-8 ksd-mb-4" icon="el-ksd-icon-add_22" @click="addExcludeColumns()">{{$t('addExcludeColumns')}}</el-button>
              <el-table
                :data="excludeColumnsTables"
                class="exclude-column-table"
                style="width: 100%">
                <el-table-column width="230px" show-overflow-tooltip prop="table" :label="$t('table')"></el-table-column>
                <el-table-column show-overflow-tooltip prop="excluded_columns" :label="$t('columns')">
                  <template slot-scope="scope">
                    <span v-if="scope.row.excluded">{{$t('allColumns')}}</span>
                    <span v-else>({{scope.row.excluded_col_size}} {{$t('columns')}}) {{scope.row.excluded_columns.join(', ')}}</span>
                  </template>
                </el-table-column>
                <el-table-column
                  width="90px"
                  :label="$t('kylinLang.common.action')">
                    <template slot-scope="scope">
                      <common-tip>
                        <div slot="content">{{$t('addExcludeColumns')}}</div>
                        <i class="el-ksd-n-icon-plus-outlined ksd-fs-18" @click="addExcludeColumns(scope.row.table)"></i>
                      </common-tip>
                      <common-tip>
                        <div slot="content">{{$t('delExcludeColumns')}}</div>
                        <i class="el-icon-ksd-table_delete ksd-fs-14" @click="delExcludeColumns(scope.row.table)"></i>
                      </common-tip>
                    </template>
                </el-table-column>
              </el-table>
              <kylin-pager :totalSize="excludeColumnsTablesSize" :perPageSize="filter.page_size" :curPage="filter.page_offset+1" v-on:handleCurrentChange='currentChange' ref="excludeColumnsTablesPager" :refTag="pageRefTags.excludeColumnsTablesPager" class="ksd-mtb-10 ksd-center" ></kylin-pager>
            </div>
          </div>
        </div>
      </div>
    </EditableBlock>
    <ExcludeColumnsDialog />
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions, mapGetters, mapMutations, mapState } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import locales from './locales'
import { handleError, handleSuccess, handleSuccessAsync, objectClone, ArrayFlat, kylinConfirm } from '../../../util'
import { lowUsageStorageTypes, autoMergeTypes, volatileTypes, validate, initialFormValue, _getProjectGeneralInfo, _getSegmentSettings, _getPushdownConfig, _getExcludeColumnConfig, _getStorageQuota, _getIndexOptimization, _getRetentionRangeScale } from './handler'
import { retentionTypes } from '../handler'
import { pageCount, pageRefTags } from '../../../config'
import EditableBlock from '../../common/EditableBlock/EditableBlock.vue'
import ExcludeColumnsDialog from '../../common/EditExcludeColumnsDialog/EditExcludeColumnsDialog.vue'
import SourceAuthorityForm from '../../common/DataSourceModal/SourceJDBC/SourceAuthorityForm/SourceAuthorityForm.vue'

@Component({
  props: {
    project: {
      type: Object,
      default: () => ({})
    }
  },
  components: {
    EditableBlock,
    SourceAuthorityForm,
    ExcludeColumnsDialog
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'currentProjectData'
    ]),
    ...mapState({
      currentProject: state => state.project.selected_project,
      dataSource: state => state.datasource.dataSource
    })
  },
  methods: {
    ...mapActions({
      updateProjectGeneralInfo: 'UPDATE_PROJECT_GENERAL_INFO',
      updateSegmentConfig: 'UPDATE_SEGMENT_CONFIG',
      updatePushdownConfig: 'UPDATE_PUSHDOWN_CONFIG',
      updateStorageQuota: 'UPDATE_STORAGE_QUOTA',
      updateIndexOptimization: 'UPDATE_INDEX_OPTIMIZATION',
      resetConfig: 'RESET_PROJECT_CONFIG',
      getFavoriteRules: 'GET_FAVORITE_RULES',
      getUserAndGroups: 'GET_USER_AND_GROUPS',
      updateFavoriteRules: 'UPDATE_FAVORITE_RULES',
      fetchDBandTables: 'FETCH_DB_AND_TABLES',
      checkConnectByGbase: 'CHECK_BASE_CONFIG',
      loadExcludeTables: 'LOAD_EXCLUDE_TABLES',
      updateExcludeColumnConfig: 'UPDATE_EXCLUDE_COLUMN_CONFIG'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    }),
    ...mapActions('ExcludeColumnsDialog', {
      callExcludeColumnsDialog: 'CALL_MODAL'
    }),
    ...mapMutations({
      updateProject: 'UPDATE_PROJECT'
    })
  },
  locales
})
export default class SettingBasic extends Vue {
  lowUsageStorageTypes = lowUsageStorageTypes
  autoMergeTypes = autoMergeTypes
  volatileTypes = volatileTypes
  retentionTypes = retentionTypes
  form = initialFormValue
  storageQuotaSize = 0
  rulesSettingRules = {
    count_value: [{ validator: this.validatePass, trigger: 'blur' }],
    min_duration: [{ validator: this.validatePass, trigger: 'blur' }],
    max_duration: [{ validator: this.validatePass, trigger: 'blur' }],
    effective_days: [{ validator: this.validatePass, trigger: 'blur' }],
    min_hit_count: [{ validator: this.validatePass, trigger: 'blur' }],
    update_frequency: [{ validator: this.validatePass, trigger: 'blur' }]
  }

  allSubmittersOptions = {
    user: [],
    group: []
  }
  loading = false
  filterUsers = []
  rulesObj = {
    count_enable: true,
    count_value: 0,
    submitter_enable: true,
    users: [],
    user_groups: [],
    duration_enable: false,
    min_duration: 0,
    max_duration: 0,
    recommendation_enable: true,
    recommendations_value: 20,
    effective_days: 2,
    min_hit_count: 30,
    update_frequency: 2
  }

  rulesAccelerationDefault = {}
  durationError = false
  durationErrorMsg = ''
  dbInfoFilter = {
    project_name: '',
    source_type: 9,
    page_offset: 0,
    page_size: 10,
    table: ''
  }
  JDBCConnectSettingBackup = []
  jdbcDatasourceEnabled = false
  allDatasourceTables = []
  showExcludeRuleDetails = false
  pageRefTags = pageRefTags
  excludeColumnsTables = []
  filter = {
    page_offset: 0,
    page_size: +localStorage.getItem(this.pageRefTags.excludeColumnsTablesPager) || pageCount
  }
  excludeColumnsTablesSize = 1

  created () {
    this.rulesAccelerationDefault = { ...this.rulesObj }
  }

  get retentionRangeScale () {
    return _getRetentionRangeScale(this.form).toLowerCase()
  }
  get rules () {
    return {
      'volatile_range.volatile_range_number': [{ validator: (rule, value, callback) => validate['positiveNumber'].call(this, rule, value, callback), trigger: 'change' }],
      'retention_range.retention_range_number': [{ validator: (rule, value, callback) => validate['positiveNumber'].call(this, rule, value, callback), trigger: 'change' }]
    }
  }
  get storageQuota () {
    return {
      'storage_quota_tb_size': [{ validator: (rule, value, callback) => validate['storageQuotaSize'].call(this, rule, value, callback), trigger: 'change' }]
    }
  }
  get indexOptimization () {
    return {
      'low_frequency_threshold': [{ validator: (rule, value, callback) => validate['storageQuotaNum'].call(this, rule, value, callback), trigger: 'change' }]
    }
  }

  get haveHiveOrKafKa () {
    if (this.dataSource[this.currentProject] && this.dataSource[this.currentProject].length) {
      return this.dataSource[this.currentProject].filter(it => it.source_type === 1 || it.source_type === 9).length
    } else {
      return false
    }
  }

  get haveJDBCDatasource () {
    return this.allDatasourceTables.filter(it => it.source_type !== 1 && it.source_type !== 9).length > 0
  }

  validatePass (rule, value, callback) {
    if (rule.field.indexOf('duration') !== -1 && this.rulesObj.duration_enable) {
      if (rule.field === 'min_duration') {
        if (!value && value !== 0) {
          callback(new Error(this.$t('emptyTips')))
        }
        this.$refs.rulesForm.validateField('max_duration')
        callback()
      } else if (rule.field === 'max_duration') {
        if (!value && value !== 0) {
          callback(new Error(this.$t('emptyTips')))
        } else if (+this.rulesObj.min_duration > +this.rulesObj.max_duration) {
          callback(new Error(this.$t('prevGreaterThanNext')))
        } else if (+this.rulesObj.max_duration > 3600) {
          callback(new Error(this.$t('overTimeLimitTip')))
        } else {
          callback()
        }
      } else {
        callback()
      }
    } else if (rule.field === 'count_value' && this.rulesObj.count_enable) {
      if (!value && value !== 0) {
        callback(new Error(null))
      } else {
        callback()
      }
    } else if (rule.field === 'effective_days') {
      if (!value && value !== 0) {
        callback(new Error(this.$t('effectiveDaysEmptyTips')))
      } else {
        callback()
      }
    } else if (rule.field === 'min_hit_count') {
      if (!value && value !== 0) {
        callback(new Error(this.$t('emptyTips')))
      } else {
        callback()
      }
    } else if (rule.field === 'update_frequency') {
      if (!value && value !== 0) {
        callback(new Error(this.$t('upadateFreEmptyTips')))
      } else {
        callback()
      }
    } else {
      this.durationError = false
      callback()
    }
  }
  @Watch('form', { deep: true })
  @Watch('project', { deep: true })
  @Watch('rulesObj', { deep: true })
  @Watch('rulesAccelerationDefault', { deep: true })
  onFormChange () {
    const basicSetting = this.isFormEdited(this.form, 'basic-info') || this.isFormEdited(this.form, 'datasource-info') || this.isFormEdited(this.form, 'segment-settings')
    this.$emit('form-changed', { basicSetting })
  }
  initForm () {
    this.handleInit('basic-info')
    this.handleInit('datasource-info')
    this.handleInit('segment-settings')
    this.handleInit('pushdown-settings')
    this.handleInit('storage-quota')
    this.handleInit('index-optimization')
    this.handleInit('exclude-rule')
  }
  async mounted () {
    this.initForm()
    this.getAllDatasourceTables()
    if (this.$store.state.project.projectExcludeTableConfig) {
      this.getExcludeColumns()
    }
    if (this.$store.state.project.isSemiAutomatic) {
      this.getAccelerationRules()
      // this.getDbAndTablesInfo()
    }
    if ('moveTo' in this.$route.query && this.$route.query.moveTo === 'index-suggest-setting') {
      this.$refs.accelerationRuleSettings && this.$refs.accelerationRuleSettings.$el.scrollIntoView && this.$refs.accelerationRuleSettings.$el.scrollIntoView()
    }
  }
  handleCheckMergeRanges (value) {
    if (value.length > 0) {
      this.form.auto_merge_time_ranges = value
    }
  }
  async handleSwitch (type, value) {
    try {
      switch (type) {
        case 'auto-merge': {
          const submitData = _getSegmentSettings(this.project)
          submitData.auto_merge_enabled = value
          await this.updateSegmentConfig(submitData); break
        }
        case 'auto-retention': {
          const submitData = _getSegmentSettings(this.project)
          submitData.retention_range.retention_range_enabled = value
          await this.updateSegmentConfig(submitData); break
        }
        case 'pushdown-range': {
          const submitData = _getPushdownConfig(this.project)
          submitData.push_down_range_limited = value
          await this.updatePushdownConfig(submitData); break
        }
        case 'pushdown-engine': {
          const submitData = _getPushdownConfig(this.project)
          submitData.push_down_enabled = value
          await this.updatePushdownConfig(submitData); break
        }
        case 'exclude-rule': {
          if (!value) {
            try {
              await kylinConfirm(this.$t('confirmCloseExcludeRule'), {type: 'warning'}, this.$t('closeExcludeRuleTitle'))
            } catch (e) {
              return
            }
          }
          const submitData = _getExcludeColumnConfig(this.project)
          submitData.table_exclusion_enabled = value
          await this.updateExcludeColumnConfig(submitData)
          this.form.table_exclusion_enabled = value
          if (value) {
            this.getExcludeColumns()
          }
          break
        }
      }
      this.$emit('reload-setting')
      this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
    } catch (e) {
      handleError(e)
    }
  }
  async handleSubmit (type, successCallback, errorCallback) {
    try {
      switch (type) {
        case 'basic-info': {
          const submitData = _getProjectGeneralInfo(this.form)
          if (!submitData.semi_automatic_mode) {
            await this.callGlobalDetailDialog({
              msg: this.$t('turnOffTips'),
              title: this.$t('turnOff') + this.$t('enableSemiAutomatic'),
              dialogType: 'warning',
              isBeta: false,
              wid: '600px',
              showDetailBtn: false,
              dangerouslyUseHTMLString: true,
              needConcelReject: true,
              submitText: this.$t('confirmClose')
            })
            await this.updateProjectGeneralInfo(submitData); break
          } else {
            await this.callGlobalDetailDialog({
              msg: this.$t('turnOnTips'),
              title: this.$t('turnOn') + this.$t('enableSemiAutomatic'),
              dialogType: 'warning',
              isBeta: false,
              wid: '400px',
              isCenterBtn: true,
              showDetailBtn: false,
              dangerouslyUseHTMLString: true,
              needConcelReject: true,
              submitText: this.$t('confirmOpen')
            })
            await this.updateProjectGeneralInfo(submitData)
            this.getAccelerationRules()
            // this.getDbAndTablesInfo() // 开启智能推荐模式，拉取数据源表
            break
          }
        }
        case 'datasource-info': {
          if (!this.form.JDBCConnectSetting.length) return errorCallback()
          const { connectionString, username, password, name, driver } = this.form.JDBCConnectSetting[0]
          const currentProject = this.currentProjectData
          const data = {
            jdbc_source_connection_url: connectionString,
            jdbc_source_user: username,
            jdbc_source_enable: this.form.jdbc_datasource_enabled,
            jdbc_source_name: name,
            jdbc_source_driver: driver
          }
          password && (data['jdbc_source_pass'] = password)
          if (!connectionString || !username) return errorCallback()
          if (this.JDBCConnectSettingBackup.length && JSON.stringify(this.JDBCConnectSettingBackup) !== JSON.stringify(this.form.JDBCConnectSetting)) {
            await this.$msgbox({
              width: '400px',
              type: 'warning',
              centerButton: true,
              showCancelButton: true,
              title: this.$t('saveDatasourceTitle'),
              message: this.$t('saveDatasourceContent'),
              confirmButtonText: this.$t('kylinLang.common.save')
            })
          }
          await this.checkConnectByGbase({...data, project: this.currentSelectedProject})
          if ('override_kylin_properties' in currentProject) {
            currentProject['override_kylin_properties']['kylin.source.jdbc.connection-url'] = connectionString
            currentProject['override_kylin_properties']['kylin.source.jdbc.pass'] = password
            currentProject['override_kylin_properties']['kylin.source.jdbc.source.enable'] = this.form.jdbc_datasource_enabled.toString()
            currentProject['override_kylin_properties']['kylin.source.jdbc.source.name'] = name
            currentProject['override_kylin_properties']['kylin.source.jdbc.user'] = username
          }
          this.JDBCConnectSettingBackup = JSON.parse(JSON.stringify(this.form.JDBCConnectSetting))
          this.jdbcDatasourceEnabled = this.form.jdbc_datasource_enabled
          this.updateProject({project: currentProject})
          break
        }
        case 'segment-settings': {
          if (await this.$refs['segment-setting-form'].validate()) {
            const submitData = _getSegmentSettings(this.form, this.project)
            await this.updateSegmentConfig(submitData); break
          } else {
            return errorCallback()
          }
        }
        case 'storage-quota': {
          if (await this.$refs['setting-storage-quota'].validate()) {
            const submitData = _getStorageQuota(this.form, this.project)
            // TB转byte
            this.form.storage_quota_size = submitData.storage_quota_size = +(submitData.storage_quota_tb_size * 1024 * 1024 * 1024 * 1024).toFixed(0)
            await this.updateStorageQuota(submitData); break
          } else {
            return errorCallback()
          }
        }
        case 'index-optimization': {
          if (await this.$refs['setting-index-optimization'].validate()) {
            const submitData = _getIndexOptimization(this.form, this.project)
            await this.updateIndexOptimization(submitData); break
          } else {
            return errorCallback()
          }
        }
        case 'acceleration-rule-settings': {
          if (await this.$refs.rulesForm.validate() && !this.durationError) {
            await this.saveAccelerationRule()
          } else {
            return errorCallback()
          }
        }
      }
      successCallback()
      await this.$emit('reload-setting')
      if (type === 'storage-quota') {
        setTimeout(() => {
          this.handleInit('storage-quota')
        }, 500)
      }
      this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
    } catch (e) {
      errorCallback()
      handleError(e)
    }
  }
  handleInit (type) {
    switch (type) {
      case 'basic-info': {
        this.form = { ...this.form, ..._getProjectGeneralInfo(this.project) }; break
      }
      case 'datasource-info': {
        const { override_kylin_properties: projectConfig } = this.currentProjectData
        this.form.JDBCConnectSetting = []
        if (!projectConfig['kylin.source.jdbc.connection-url']) return
        this.form.JDBCConnectSetting.push({
          connectionString: projectConfig['kylin.source.jdbc.connection-url'] || '',
          username: projectConfig['kylin.source.jdbc.user'] || '',
          password: projectConfig['kylin.source.jdbc.pass'] ? projectConfig['kylin.source.jdbc.pass'] === '*****' ? '' : projectConfig['kylin.source.jdbc.pass'] : '',
          sourceType: +projectConfig['kylin.source.default'] || 9,
          name: projectConfig['kylin.source.jdbc.source.name'] || '',
          driver: projectConfig['kylin.source.jdbc.driver'] || ''
        })
        this.form.jdbc_datasource_enabled = projectConfig['kylin.source.jdbc.source.enable'] ? projectConfig['kylin.source.jdbc.source.enable'] === 'true' : false
        this.JDBCConnectSettingBackup = JSON.parse(JSON.stringify(this.form.JDBCConnectSetting))
        this.jdbcDatasourceEnabled = this.form.jdbc_datasource_enabled
        break
      }
      case 'segment-settings': {
        this.form = { ...this.form, ..._getSegmentSettings(this.project) }
        this.$refs['segment-setting-form'] && this.$refs['segment-setting-form'].clearValidate && this.$refs['segment-setting-form'].clearValidate()
        break
      }
      case 'pushdown-settings': {
        this.form = { ...this.form, ..._getPushdownConfig(this.project) }; break
      }
      case 'storage-quota': {
        this.form = { ...this.form, ..._getStorageQuota(this.project) }; break
      }
      case 'index-optimization': {
        this.form = { ...this.form, ..._getIndexOptimization(this.project) }; break
      }
      case 'exclude-rule': {
        this.form = { ...this.form, ..._getExcludeColumnConfig(this.project) }; break
      }
    }
  }
  async handleResetForm (type, successCallback, errorCallback) {
    try {
      switch (type) {
        case 'segment-settings': {
          const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'segment_config'})
          const data = await handleSuccessAsync(res)
          this.form = { ...this.form, ..._getSegmentSettings(data) }
          this.$refs['segment-setting-form'].clearValidate()
          break
        }
        case 'storage-quota': {
          const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'storage_quota_config'})
          let data = await handleSuccessAsync(res)
          const quotaSize = data.storage_quota_size / 1024 / 1024 / 1024 / 1024
          data = {...data, storage_quota_tb_size: quotaSize.toFixed(2)}
          this.form = { ...this.form, ..._getStorageQuota(data) }
          this.$refs['setting-storage-quota'].clearValidate()
          break
        }
        case 'index-optimization': {
          const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'garbage_cleanup_config'})
          const data = await handleSuccessAsync(res)
          this.form = { ...this.form, ..._getIndexOptimization(data) }
          this.$refs['setting-index-optimization'].clearValidate()
          break
        }
        case 'acceleration-rule-settings': {
          const res = await this.resetConfig({ project: this.currentSelectedProject, reset_item: 'favorite_rule_config' })
          const data = await handleSuccessAsync(res)
          const { favorite_rules } = data
          this.rulesObj = { ...this.rulesObj, ...favorite_rules }
          this.rulesAccelerationDefault = { ...this.rulesAccelerationDefault, ...favorite_rules }
          this.$refs.rulesForm.resetFields()
        }
      }
      successCallback()
      this.$emit('reload-setting')
      this.$message({ type: 'success', message: this.$t('kylinLang.common.resetSuccess') })
    } catch (e) {
      errorCallback()
      handleError(e)
    }
  }
  isFormEdited (form, type) {
    const project = { ...this.project, alias: this.project.alias || this.project.project }
    switch (type) {
      case 'basic-info':
        return JSON.stringify(_getProjectGeneralInfo(form)) !== JSON.stringify(_getProjectGeneralInfo(project))
      case 'datasource-info':
        return form.jdbc_datasource_enabled
          ? form.JDBCConnectSetting.length > 0
            ? this.jdbcDatasourceEnabled !== form.jdbc_datasource_enabled || JSON.stringify(this.JDBCConnectSettingBackup) !== JSON.stringify(form.JDBCConnectSetting)
            : this.jdbcDatasourceEnabled !== form.jdbc_datasource_enabled && JSON.stringify(this.JDBCConnectSettingBackup) !== JSON.stringify(form.JDBCConnectSetting)
          : this.jdbcDatasourceEnabled !== form.jdbc_datasource_enabled || JSON.stringify(this.JDBCConnectSettingBackup) !== JSON.stringify(form.JDBCConnectSetting)
      case 'segment-settings':
        return JSON.stringify(_getSegmentSettings(form)) !== JSON.stringify(_getSegmentSettings(project))
      case 'storage-quota':
        // form.storage_quota_size = +(form.storage_quota_tb_size * 1024 * 1024 * 1024 * 1024).toFixed(0)
        return JSON.stringify(_getStorageQuota(form)) !== JSON.stringify(_getStorageQuota(project))
      case 'index-optimization':
        return JSON.stringify(_getIndexOptimization(form)) !== JSON.stringify(_getIndexOptimization(project))
      case 'acceleration-rule-settings':
        return JSON.stringify(this.rulesAccelerationDefault) !== JSON.stringify(this.rulesObj)
    }
  }

  // 获取优化建议规则
  getAccelerationRules () {
    if (this.currentSelectedProject) {
      this.getFavoriteRules({ project: this.currentSelectedProject }).then((res) => {
        handleSuccess(res, (data) => {
          this.rulesObj = { ...data }
          this.rulesAccelerationDefault = { ...data }
        })
      }).catch((res) => {
        handleError(res)
      })
      this.getUserAndGroups().then((res) => {
        handleSuccess(res, (data) => {
          this.allSubmittersOptions = data
          this.filterUsers = this.allSubmittersOptions.user
        }, (res) => {
          handleError(res)
        })
      })
    }
  }

  remoteMethod (query) {
    if (query !== '') {
      this.loading = true
      setTimeout(() => {
        this.loading = false
        this.filterUsers = this.allSubmittersOptions.user.filter(item => {
          return item.toLowerCase()
            .indexOf(query.toLowerCase()) > -1
        })
      }, 0)
    } else {
      this.filterUsers = this.allSubmittersOptions.user
    }
  }

  get filterSubmitterUserOptions () {
    const filterUsers = objectClone(this.filterUsers)
    return filterUsers.splice(0, 500)
  }

  // 保存优化建议规则
  saveAccelerationRule () {
    return new Promise((resolve, reject) => {
      const submitData = objectClone(this.rulesObj)
      submitData.min_duration = +submitData.min_duration
      submitData.max_duration = +submitData.max_duration
      // 换成次数字段了，不需要除于 100
      // submitData.freqValue = submitData.freqValue / 100
      this.updateFavoriteRules({ ...submitData, ...{ project: this.currentSelectedProject } }).then((res) => {
        handleSuccess(res, (data) => {
          this.rulesAccelerationDefault = { ...this.rulesAccelerationDefault, ...this.rulesObj }
        })
        resolve()
      }, (res) => {
        handleError(res)
        reject(res)
      })
    })
  }

  changeDurationEnable (val) {
    !val && (this.durationError = val)
  }

  async getAllDatasourceTables () {
    try {
      this.dbInfoFilter.project_name = this.currentSelectedProject
      const response = await this.fetchDBandTables({
        project_name: this.currentSelectedProject,
        source_type: '1,9,8',
        page_offset: 0,
        page_size: 10,
        table: ''
      })
      const results = await handleSuccessAsync(response)
      const { databases } = results
      databases && databases.length > 0 && (this.allDatasourceTables = ArrayFlat(databases.map(it => 'tables' in it ? it.tables : [])))
    } catch (e) {
      handleError(e)
    }
  }

  async addExcludeColumns (excludeTable) {
    const isSubmit = await this.callExcludeColumnsDialog({ excludeColumntitle: !excludeTable ? 'addExcludeColumns' : 'appendExcludeColumns', excludeTable: excludeTable })
    if (isSubmit) {
      this.getExcludeColumns()
    }
  }

  async delExcludeColumns (excludeTable) {
    const isSubmit = await this.callExcludeColumnsDialog({ excludeColumntitle: 'delExcludeColumns', excludeTable: excludeTable })
    if (isSubmit) {
      this.getExcludeColumns()
    }
  }

  currentChange (size, count) {
    this.filter.page_offset = size
    this.filter.page_size = count
    this.getExcludeColumns()
  }

  async getExcludeColumns () {
    try {
      const res = await this.loadExcludeTables({ ...this.filter, project: this.currentSelectedProject })
      const { value, total_size } = await handleSuccessAsync(res)
      this.excludeColumnsTables = value
      this.excludeColumnsTablesSize = total_size
    } catch (e) {
      handleError(e)
    }
  }

  // 更改数据源设置
  modifyDataSourceSetting (index) {
    const key = Date.now().toString(32)
    const form = objectClone(this.form.JDBCConnectSetting[index])
    this.$msgbox({
      width: '600px',
      title: this.$t('modifyDatasourceConnect'),
      message: <source-authority-form ref='datasourceAuthorityForm' key={key} form={form}/>,
      confirmButtonText: this.$t('kylinLang.common.save'),
      showCancelButton: true,
      closeOnClickModal: false,
      closeOnPressEscape: false,
      callback: (action) => {
        return new Promise((resolve, reject) => {
          if (action === 'confirm') {
            this.$set(this.form.JDBCConnectSetting, index, form)
            resolve()
          } else {
            reject()
          }
        })
      },
      beforeClose: async (action, instance, done) => {
        try {
          if (action === 'confirm') {
            this.$refs.datasourceAuthorityForm && await this.$refs.datasourceAuthorityForm.$refs.form.validate()
            done()
          } else {
            this.$refs.datasourceAuthorityForm && this.$refs.datasourceAuthorityForm.$refs.form.clearValidate()
            done()
          }
        } catch (e) {
          console.error(e)
        }
      }
    })
  }

  // 添加数据源设置
  addDataSourceSetting () {
    const key = Date.now().toString(32)
    const form = {
      connectionString: '',
      username: '',
      password: '',
      name: ''
    }
    this.$msgbox({
      width: '600px',
      title: this.$t('addDatasourceConnect'),
      message: <source-authority-form ref='datasourceAuthorityForm' key={key} form={form}/>,
      confirmButtonText: this.$t('kylinLang.common.save'),
      showCancelButton: true,
      closeOnClickModal: false,
      closeOnPressEscape: false,
      callback: (action) => {
        return new Promise((resolve, reject) => {
          if (action === 'confirm') {
            this.form.JDBCConnectSetting.push(form)
            resolve()
          } else {
            reject()
          }
        })
      },
      beforeClose: async (action, instance, done) => {
        try {
          if (action === 'confirm') {
            this.$refs.datasourceAuthorityForm && await this.$refs.datasourceAuthorityForm.$refs.form.validate()
            done()
          } else {
            if (this.$refs.datasourceAuthorityForm) {
              this.$refs.datasourceAuthorityForm.$refs.form.clearValidate()
            }
            done()
          }
        } catch (e) {
          console.error(e)
        }
      }
    })
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.basic-setting {
  .beta-label {
    display: inline-block;
    height: 18px;
    line-height: 9px;
    background: #EFDBFF;
    color: #531DAB;
    font-size: 11px;
    font-family: Lato-Bold, Lato;
    font-weight: bold;
    border-radius: 2px;
    padding: 5px;
    box-sizing: border-box;
  }
  .clearfix .setting-value,
  .clearfix .setting-input {
    width: calc(~'100% - 114px');
  }
  .ksd-switch {
    transform: scale(0.91);
    transform-origin: left;
  }
  .el-icon-ksd-expert_mode_small,
  .el-icon-ksd-smart_mode_small {
    margin-right: 5px;
  }
  .form-item {
    display: flex;
    .setting-label {
      padding-top: 6px;
    }
  }
  .el-form-item {
    &.is-error {
      vertical-align: top;
      margin-top: -4px;
    }
    .el-form-item__error {
      width: 100px;
    }
  }
  .duration-rule {
    .el-form-item {
      &.is-error {
        vertical-align: text-top;
        margin-top: -8px;
      }
    }
  }
  .exclude-rule-msg {
    font-size: 12px;
    .tips {
      .review-details {
        color: @base-color;
        cursor: pointer !important;
        position: relative;
      }
    }
    .details {
      background: @base-background-color-1;
      padding: 10px 10px;
      margin-top: 5px;
      box-sizing: border-box;
      .point {
        font-style: inherit;
        margin-right: 5px;
      }
    }
  }
  .ruleSetting {
    padding: 15px 20px;
    .conds-title {
      font-weight: @font-medium;
    }
    .conds > .conds-title {
      height: 21px;
      line-height: 22px;
      display: flex;
      align-items: center;
    }
    .el-form-item--medium .el-form-item__content, .el-form-item--medium .el-form-item__label {
      line-height: 1;
    }
    .el-form-item {
      margin-bottom: 0;
      &.inline-form-item {
        display: inline-block;
        vertical-align: top;
        margin-top: -4px;
      }
    }
    .conds {
      margin-bottom: 16px;
    }
  }
  .rule-setting-input {
    display: inline-block;
    width: 100px;
    &.count-input{
      width: 80px;
      &.el-input-number.is-without-controls .el-input__inner{
        text-align: left;
      }
    }
    &.is-error {
      .el-input__inner {
        border: 1px solid @error-color-1;
      }
    }
  }
  .error-msg {
    color: #ff0000;
    font-size: 12px;
  }
}

</style>
