<template>
  <div class="accelerate-setting">
    <!-- 默认数据库设置 -->
    <EditableBlock
      class="js_defautDB_block"
      :header-content="$t('defaultDBTitle')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'defaultDB-settings')"
      :is-reset="false"
      @submit="(scb, ecb) => handleSubmit('defaultDB-settings', scb, ecb)">
      <el-form ref="setDefaultDB" :model="form" :rules="setDefaultDBRules">
        <div class="setting-item">
          <div class="setting-label font-medium">{{$t('defaultDB')}}</div>
          <el-form-item prop="default_database">
            <el-select v-model="form.default_database" size="small" class="js_select" popper-class="js_defautDB_select" :placeholder="$t('kylinLang.common.pleaseChoose')">
              <el-option
                v-for="item in dbList"
                :key="item"
                :label="item"
                :value="item">
              </el-option>
            </el-select>
          </el-form-item>
          <div class="setting-desc">
            <p>{{$t('defaultDBNote1')}}</p>
            <p class="warning"><i class="el-icon-ksd-alert"></i>{{$t('defaultDBNote2')}}</p>
          </div>
        </div>
      </el-form>
    </EditableBlock>
    <!-- 任务邮件通知设置 -->
    <EditableBlock
      :header-content="$t('jobAlert')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'job-alert')"
      @submit="(scb, ecb) => handleSubmit('job-alert', scb, ecb)"
      @cancel="(scb, ecb) => handleResetForm('job-alert', scb, ecb)">
      <!-- 空任务邮件通知 -->
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('emptyDataLoad')}}</span><span class="setting-value fixed">
          <el-switch
            v-model="form.data_load_empty_notification_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('emptyDataLoadDesc')}}</div>
        <div class="split"></div>
        <span class="setting-label font-medium">{{$t('errorJob')}}</span><span class="setting-value fixed">
          <el-switch
            v-model="form.job_error_notification_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('errorJobDesc')}}</div>
      </div>
      <div class="setting-item">
        <span class="setting-value">
          <span class="setting-label font-medium">{{$t('emails')}}</span>
          <span v-for="(email, index) in form.job_notification_emails" :key="index">
            <span class="notice-email" v-if="email">{{email}}</span>
            <span v-else-if="index === 0 && !email">{{$t('noData')}}</span>
          </span>
        </span>
        <div class="setting-input">
          <el-form ref="job-alert" :model="form" :rules="emailRules" size="small">
            <div class="item-value" v-for="(email, index) in form.job_notification_emails" :key="index">
              <span class="setting-label font-medium email-fix-top">{{$t('emails')}}</span>
              <el-form-item :prop="`job_notification_emails.${index}`">
                <el-input size="small" v-model="form.job_notification_emails[index]" :placeholder="$t('pleaseInputEmail')"></el-input><el-button
                 icon="el-ksd-icon-add_16" circle size="mini" @click="handleAddItem('job_notification_emails', index)"></el-button><el-button
                  icon="el-ksd-icon-minus_16" class="ksd-ml-5" circle size="mini" @click="handleRemoveItem('job_notification_emails', index)" :disabled="form.job_notification_emails.length < 2"></el-button>
              </el-form-item>
            </div>
          </el-form>
        </div>
      </div>
    </EditableBlock>
    <!-- Kerberos 账户设置 -->
    <EditableBlock
      class="kerberosAccBlock"
      :header-content="$t('kerberosAcc')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'kerberos-acc') && form.fileList.length > 0"
      v-if="kerberosEnabled==='true' && settingActions.includes('kerberosAcc')"
      @submit="(scb, ecb) => handleSubmit('kerberos-acc', scb, ecb)"
      @cancel="(scb, ecb) => handleResetForm('kerberos-acc', scb, ecb)">
      <div class="setting-item">
        <el-form ref="kerberos-setting-form" :model="form" :rules="kerberosRules" label-position="left">
          <el-form-item prop="principal" :label="$t('principalName')" :show-message="false">
            <el-input
              size="small"
              class="yarn-name-input"
              v-model.trim="form.principal">
            </el-input>
          </el-form-item>
          <el-form-item prop="fileList" :label="$t('keytabFile')">
            <!-- <input type="hidden" v-model="form.fileList" :show-message="false" /> -->
            <el-upload
              name="file"
              ref="kerberosFileUpload"
              :action="kerberosActionUrl"
              :auto-upload="false"
              :multiple="false"
              :file-list="form.fileList"
              :on-change="changeFile"
              :on-remove="handleRemove"
              :show-file-list="true">
              <el-button size="small" type="primary">{{$t('selectFile')}}</el-button>
              <span slot="tip" class="ksd-ml-10 ksd-fs-12">{{$t('uploadFileTips')}}</span>
            </el-upload>
          </el-form-item>
          <div class="setting-desc">
            <p>{{$t('kerberosTips')}}</p>
          </div>
        </el-form>
      </div>
    </EditableBlock>
    <!-- YARN 资源队列 -->
    <EditableBlock
      class="yarn-queue-config"
      :header-content="$t('yarnQueue')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'yarn-name')"
      :is-editable="userType"
      :is-reset="false"
      v-if="settingActions.includes('yarnQueue')"
      @submit="(scb, ecb) => handleSubmit('yarn-name', scb, ecb)">
      <el-form ref="yarn-setting-form" :model="form" :rules="yarnQueueRules">
        <div class="setting-item">
          <div class="setting-label font-medium">{{$t('yarnQueue')}}</div>
          <el-form-item prop="yarn_queue">
            <el-input
              size="small"
              class="yarn-name-input"
              :disabled="!userType"
              v-model.trim="form.yarn_queue">
            </el-input>
          </el-form-item>
          <div class="setting-desc">
            <p>{{$t('yarnQueueTip')}}</p>
            <p class="warning"><i class="el-icon-ksd-alert"></i>{{$t('yarnQueueWarn')}}</p>
          </div>
        </div>
      </el-form>
    </EditableBlock>
    <!-- 分层存储 -->
    <!-- <EditableBlock
      :header-content="$t('secondaryStorage')"
      :is-keep-editing="true"
      :is-edited="!form.second_storage_enabled&&isFormEdited(form, 'sec-storage') || (form.second_storage_enabled && new_nodes.length > 0)"
      :is-reset="false"
      v-if="isShowSecondStorage"
      @submit="(scb, ecb) => handleSubmit('sec-storage', scb, ecb)">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('supportSecStorage')}}</span><span class="setting-value fixed ksd-fs-12">
          <el-switch
            v-model="form.second_storage_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('supportSecStorageDesc')}}</div>
      </div>
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('storageNode')}}</span>
        <ul class="sec-nodes" v-if="form.second_storage_nodes&&form.second_storage_nodes.length">
          <li v-for="n in form.second_storage_nodes" :key="n.name">
            <span class="node-name">{{n.name}}</span>
            <span class="node-ip">{{n.ip}}:{{n.port}}</span>
          </li>
        </ul>
        <el-select class="setting-desc secondary-storage-nodes" value-key="name" multiple v-model="new_nodes" :placeholder="$t('chooseNode')">
          <el-option v-for="item in nodes" :label="`${item.name} ${item.ip}:${item.port}`" :key="item.name" :value="item">
            <span class="node-name">{{item.name}}</span>
            <span class="node-ip">{{item.ip}}:{{item.port}}</span>
          </el-option>
        </el-select>
      </div>
    </EditableBlock> -->
    <!-- 多级分区 -->
    <EditableBlock
      :header-content="$t('mulPartitionSettings')"
      :isEditable="false">
      <template slot="header">
        <span class="beta-label">BETA</span>
      </template>
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('mulPartition')}}</span><span class="setting-value fixed">
          <el-switch
            v-model="form.multi_partition_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')"
            @change="handleMulPartitionSetting">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('mulPartitionDecription')}}</div>
      </div>
    </EditableBlock>
    <!-- Snapshot -->
    <EditableBlock
      :header-content="$t('snapshotTitle')"
      :isEditable="false">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('snapshotManagment')}}</span><span class="setting-value fixed ksd-fs-12">
          <el-switch
            v-model="form.snapshot_manual_management_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')"
            @input="value => handleSnapshotSwitch(value)">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('snapshotDesc')}}</div>
      </div>
    </EditableBlock>
    <!-- 可计算列 -->
    <EditableBlock
      :header-content="$t('computedColumns')"
      :isEditable="false">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('exposingCC')}}</span><span class="setting-value fixed">
          <el-switch
            v-model="form.expose_computed_column"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')"
            @input="value => handleSwitch(value)">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('exposingCCDesc')}}</div>
      </div>
    </EditableBlock>
    <!-- 支持拉链表 -->
    <EditableBlock
      :header-content="$t('SCD2Settings')"
      :isEditable="false">
      <!-- <template slot="header">
        <span class="beta-label">BETA</span>
      </template> -->
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('nonEqualJoin')}}</span><span class="setting-value fixed ksd-fs-12">
          <el-switch
            v-model="form.scd2_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')"
            @change="handleScdSetting">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('noEqualDecription')}}</div>
      </div>
    </EditableBlock>

    <EditableBlock
      :header-content="$t('projectConfig')"
      :isEditable="false">
      <div class="setting-item">
        <el-button icon="el-ksd-icon-add_22" @click="editConfig()">{{$t('configuration')}}</el-button>
        <el-table
          class="ksd-mt-10"
          :data="convertedProperties"
          size="medium"
          style="width: 100%">
          <el-table-column
            show-overflow-tooltip
            prop="key"
            label="Key">
          </el-table-column>
          <el-table-column
            show-overflow-tooltip
            prop="value"
            label="Value">
            <template slot-scope="scope">{{scope.row.key === 'kylin.source.jdbc.pass' ? '••••••' : scope.row.value}}</template>
          </el-table-column>
          <el-table-column
            :label="$t('kylinLang.common.action')"
            width="100">
            <template slot-scope="scope">
              <common-tip :content="$t('kylinLang.common.edit')">
                <i class="el-icon-ksd-table_edit" @click="editConfig(scope.row)"></i>
              </common-tip>
              <common-tip :content="$t('kylinLang.common.delete')">
                <i class="el-icon-ksd-table_delete ksd-ml-10" @click="deleteConfig(scope.row)"></i>
              </common-tip>
            </template>
          </el-table-column>
        </el-table>
        <kylin-pager
          class="ksd-center ksd-mt-10" ref="pager"
          :refTag="pageRefTags.basicPorjectConfigPager"
          layout="prev, pager, next"
          :background="false"
          :curPage="currentPage+1"
          :totalSize="configList.length"
          :perPageSize="pageSize"
          @handleCurrentChange="handleCurrentChange">
        </kylin-pager>
      </div>
    </EditableBlock>
  </div>
</template>

<script>
import Vue from 'vue'
import locales from './locales'
import { mapActions, mapGetters, mapState, mapMutations } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import { handleError, handleSuccessAsync, objectArraySort } from '../../../util'
import { kylinConfirm } from 'util/business'
import { apiUrl } from '../../../config'
import { validate, _getJobAlertSettings, _getDefaultDBSettings, _getYarnNameSetting, _getSecStorageSetting, _getExposeCCSetting, _getSnapshotSetting, _getKerberosSettings } from './handler'
import EditableBlock from '../../common/EditableBlock/EditableBlock.vue'
import { pageRefTags, pageCount } from 'config'

@Component({
  components: {
    EditableBlock
  },
  props: {
    project: {
      type: Object,
      default: () => ({})
    }
  },
  methods: {
    ...mapActions({
      updateJobAlertSettings: 'UPDATE_JOB_ALERT_SETTINGS',
      resetConfig: 'RESET_PROJECT_CONFIG',
      updateDefaultDBSettings: 'UPDATE_DEFAULT_DB_SETTINGS',
      fetchAvailableNodes: 'FETCH_AVAILABLE_NODES',
      fetchDatabases: 'FETCH_DATABASES',
      updateYarnQueue: 'UPDATE_YARN_QUEUE',
      updateExposeCCConfig: 'UPDATE_EXPOSE_CC_CONFIG',
      updateSnapshotConfig: 'UPDATE_SNAPSHOT_CONFIG',
      updateKerberosConfig: 'UPDATE_KERBEROS_CONFIG',
      reloadHiveDBAndTables: 'RELOAD_HIVE_DB_TABLES',
      toggleEnableSCD: 'TOGGLE_ENABLE_SCD',
      getSCDModels: 'GET_SCD2_MODEL',
      toggleMultiPartition: 'TOGGLE_MULTI_PARTITION',
      getMultiPartitionModels: 'GET_MULTI_PARTITION_MODEL',
      getProjectConfigList: 'LOAD_CONFIG_BY_PROJEECT',
      deleteProjectConfig: 'DELETE_PROJECT_CONFIG'
    }),
    ...mapMutations({
      updateSCD2Enable: 'UPDATE_SCD2_ENABLE',
      updateMultiPartitionEnable: 'UPDATE_MULTI_PARTITION_ENABLE'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'currentProjectData',
      'settingActions',
      'configList',
      'isShowSecondStorage'
    ]),
    ...mapState({
      platform: state => state.config.platform,
      currentUser: state => state.user.currentUser,
      kerberosEnabled: state => state.system.kerberosEnabled
    })
  },
  locales
})
export default class SettingAdvanced extends Vue {
  filterData = {
    page_offset: 0,
    page_size: 1,
    exact: true,
    project: '',
    permission: 'ADMINISTRATION'
  }
  currentPage = 0
  pageSize = pageCount
  convertedProperties = []
  pageRefTags = pageRefTags

  dbList = []
  nodes = []
  new_nodes = []
  form = {
    project: '',
    // tips_enabled: true,
    // threshold: 20,
    job_error_notification_enabled: true,
    data_load_empty_notification_enabled: true,
    job_notification_emails: [],
    default_database: this.$store.state.project.projectDefaultDB || '',
    yarn_queue: this.$store.state.project.yarn_queue || '',
    expose_computed_column: true,
    principal: '',
    fileList: [],
    file: null,
    snapshot_manual_management_enabled: this.project.snapshot_manual_management_enabled,
    multi_partition_enabled: this.project.multi_partition_enabled,
    scd2_enabled: this.project.scd2_enabled,
    second_storage_enabled: true,
    second_storage_nodes: []
  }
  get setDefaultDBRules () {
    return {
      'default_database': { required: true, message: this.$t('kylinLang.common.pleaseSelect'), trigger: 'change' }
    }
  }
  get emailRules () {
    const rules = this.form.job_notification_emails.reduce((t, e, index) => {
      return Object.assign(t, {[`job_notification_emails.${index}`]: [
        { required: true, message: this.$t('pleaseInputEmail'), trigger: 'blur' },
        { type: 'email', message: this.$t('pleaseInputVaildEmail'), trigger: 'blur' }
      ]})
    }, {})
    return rules
  }
  get yarnQueueRules () {
    return {
      'yarn_queue': [{ validator: validate['validateYarnName'], message: [this.$t('emptyTips'), this.$t('yarnFormat')], trigger: 'blur' }]
    }
  }
  get userType () {
    return this.currentUser.authorities.map(user => user.authority).some((u) => u === 'ROLE_ADMIN')
  }
  get kerberosRules () {
    return {
      'principal': [{ validator: validate['principalName'], trigger: 'blur' }]
    }
  }
  get kerberosActionUrl () {
    return apiUrl + 'projects/' + this.currentSelectedProject + '/project_kerberos_info'
  }
  @Watch('form', { deep: true })
  @Watch('project', { deep: true })
  onFormChange () {
    const advanceSetting = this.isFormEdited(this.form, 'job-alert') || this.isFormEdited(this.form, 'defaultDB-settings') || this.isFormEdited(this.form, 'yarn-name') || this.isFormEdited(this.form, 'kerberos-acc') || this.isFormEdited(this.form, 'sec-storage')
    this.$emit('form-changed', { advanceSetting })
  }

  @Watch('configList', { deep: true })
  onConfigListChange () {
    this.currentPage = 0
    this.handleCurrentChange(0, this.pageSize)
  }
  async loadDataBases () {
    // 按单数据源来处理
    const { override_kylin_properties: overrideKylinProperties } = this.currentProjectData || {}
    let currentSourceTypes = overrideKylinProperties && overrideKylinProperties['kylin.source.default']
      ? [+overrideKylinProperties['kylin.source.default']]
      : []
    const responses = await this.fetchDatabases({ projectName: this.currentSelectedProject, sourceType: currentSourceTypes[0] })
    this.dbList = await handleSuccessAsync(responses)
    if (this.dbList.indexOf('DEFAULT') === -1) {
      this.dbList.push('DEFAULT')
    }
    this.form.default_database = this.$store.state.project.projectDefaultDB
  }
  mounted () {
    this.loadDataBases()
    this.initForm()
    this.getConfigList()
    // 系统配置分层存储集群节点时可调用
    // if (this.$store.state.system.isShowSecondStorage) {
    //   this.loadAvailableNodes()
    // }
  }
  async loadAvailableNodes () {
    const res = await this.fetchAvailableNodes()
    const data = await handleSuccessAsync(res)
    this.nodes = objectArraySort(data, true, 'name')
  }
  initForm () {
    this.handleInit('job-alert')
    this.handleInit('defaultDB-settings')
    this.handleInit('yarn-name')
    this.handleInit('sec-storage')
    this.handleInit('expose_computed_column')
    this.handleInit('kerberos-acc')
  }
  handleInit (type) {
    switch (type) {
      case 'job-alert': {
        this.form = { ...this.form, ..._getJobAlertSettings(this.project, true) }; break
      }
      case 'defaultDB-settings': {
        this.form = { ...this.form, ..._getDefaultDBSettings(this.project) }; break
      }
      case 'yarn-name': {
        this.form = { ...this.form, ..._getYarnNameSetting(this.project) }; break
      }
      case 'sec-storage': {
        this.form = { ...this.form, ..._getSecStorageSetting(this.project) }; break
      }
      case 'expose_computed_column': {
        this.form = { ...this.form, ..._getExposeCCSetting(this.project) }; break
      }
      case 'kerberos-acc': {
        this.form = { ...this.form, ..._getKerberosSettings(this.project) }
        if (this.$refs['kerberos-setting-form']) {
          this.$refs['kerberos-setting-form'].clearValidate && this.$refs['kerberos-setting-form'].clearValidate()
        }
        break
      }
    }
  }
  async handleSnapshotSwitch (value) {
    try {
      const submitData = _getSnapshotSetting(this.project)
      const msg = value ? this.$t('openManualTips') : this.$t('closeManualTips')
      const confirmButtonText = value ? this.$t('confirmOpen') : this.$t('confirmClose')
      const title = value ? this.$t('openSnapshotTitle') : this.$t('closeSnapshotTitle')
      try {
        await kylinConfirm(msg, {confirmButtonText, dangerouslyUseHTMLString: true, centerButton: true, type: 'warning'}, title)
        submitData.snapshot_manual_management_enabled = value
        try {
          await this.updateSnapshotConfig(submitData)
          this.$emit('reload-setting')
          this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
        } catch (e) {
          this.form.snapshot_manual_management_enabled = !value
          handleError(e)
        }
      } catch (e) {
        this.form.snapshot_manual_management_enabled = !value
      }
    } catch (e) {
      handleError(e)
    }
  }
  async handleSwitch (value) {
    try {
      const submitData = _getExposeCCSetting(this.project)
      if (!value) {
        try {
          await kylinConfirm(this.$t('confirmCloseExposeCC'), {confirmButtonText: this.$t('kylinLang.common.confirmClose'), centerButton: true, type: 'warning'})
          submitData.expose_computed_column = value
          try {
            await this.updateExposeCCConfig(submitData)
            this.$emit('reload-setting')
            this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
          } catch (e) {
            handleError(e)
          }
        } catch (e) {
          this.form.expose_computed_column = !value
        }
      } else {
        submitData.expose_computed_column = value
        await this.updateExposeCCConfig(submitData)
        this.$emit('reload-setting')
        this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
      }
    } catch (e) {
      handleError(e)
    }
  }
  async handleSubmit (type, successCallback, errorCallback) {
    try {
      switch (type) {
        case 'defaultDB-settings': {
          // 需要二次确认
          this.$confirm(this.$t('confirmDefaultDBContent', {dbName: this.form.default_database}), this.$t('confirmDefaultDBTitle'), {
            confirmButtonText: this.$t('kylinLang.common.submit'),
            cancelButtonText: this.$t('kylinLang.common.cancel'),
            centerButton: true,
            type: 'warning'
          }).then(async () => {
            if (await this.$refs['setDefaultDB'].validate()) {
              try {
                const submitData = _getDefaultDBSettings(this.form)
                await this.updateDefaultDBSettings({project: submitData.project, default_database: submitData.defaultDatabase})
                successCallback()
                this.$emit('reload-setting')
                this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
              } catch (e) {
                errorCallback()
                handleError(e)
              }
            } else {
              return errorCallback()
            }
          }).catch(() => {
            return errorCallback()
          })
          break
        }
        case 'job-alert': {
          if (await this.$refs['job-alert'].validate()) {
            const submitData = _getJobAlertSettings(this.form)
            await this.updateJobAlertSettings(submitData); break
          } else {
            return errorCallback()
          }
        }
        case 'sec-storage': {
          if (this.form.second_storage_enabled) {
            // 需要二次确认
            this.$confirm(this.$t('openSecStorageTips'), this.$t('openSecStorageTitle'), {
              confirmButtonText: this.$t('openSecConfirmBtn'),
              cancelButtonText: this.$t('kylinLang.common.cancel'),
              dangerouslyUseHTMLString: true,
              centerButton: true,
              type: 'warning'
            }).then(async () => {
              try {
                await this.updateSecStorageSettings({project: this.currentSelectedProject, new_nodes: this.new_nodes.map(n => n.name), enabled: this.form.second_storage_enabled})
                successCallback()
                this.$emit('reload-setting')
                this.form.second_storage_nodes = objectArraySort([...this.form.second_storage_nodes, ...this.new_nodes], true, 'name')
                this.new_nodes = []
                this.loadAvailableNodes()
                this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
              } catch (e) {
                errorCallback()
                handleError(e)
              }
            }).catch(() => {
              return errorCallback()
            })
            break
          } else {
            const h = this.$createElement
            // 抓取下是分层存储 的model，如果存在，需要二次确认
            this.getSecStorageModels({project: this.currentSelectedProject}).then(async (modeldata) => {
              try {
                const data = await handleSuccessAsync(modeldata)
                if (data.length) {
                  this.$msgbox({
                    title: this.$t('closeSecStorageSetting'),
                    message: h('p', null, [
                      h('span', null, [
                        // h('i', { class: 'el-icon-ksd-alert', style: 'color: #F7BA2A;margin-right: 5px;' }),
                        h('span', null, this.$t('closeSecStorageTip'))
                      ]),
                      h('div', null, this.$t('affectedModels')),
                      h('div', {
                        style: 'color: #546174;padding:8px;background:#F8F9FB;margin-top:16px;font-size:12px;line-height:16px;max-height:100px;overflow-y:auto;'
                      }, data.map(d => {
                        return h('div', null, d)
                      })),
                      h('div', { class: 'ksd-mt-24' }, [
                        h('i', { style: 'color: #E03B3B;margin-right: 5px' }, '*'),
                        h('span', null, this.$t('secStorageInputTitle'))
                      ])
                    ]),
                    type: 'warning',
                    centerButton: true,
                    showInput: true,
                    showCancelButton: true,
                    showClose: false,
                    $type: 'prompt',
                    inputPattern: this.$lang === 'en' ? /^Turn Off Tiered Storage$/ : /^关闭分层存储$/,
                    inputErrorMessage: '',
                    confirmButtonText: this.$t('confirmClose'),
                    cancelButtonText: this.$t('kylinLang.common.cancel')
                  }).then(() => {
                    this.updateSecStorageSettings({project: this.currentSelectedProject, enabled: this.form.second_storage_enabled}).then(async (res) => {
                      try {
                        await handleSuccessAsync(res)
                        successCallback()
                        this.$emit('reload-setting')
                        this.form.second_storage_nodes = []
                        this.nodes = []
                        this.loadAvailableNodes()
                        this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
                      } catch (e) {
                        errorCallback()
                        handleError(e)
                      }
                    }).catch((e) => {
                      this.handleInit('sec-storage')
                      errorCallback()
                      handleError(e)
                    })
                  }).catch(() => {
                    errorCallback()
                    this.handleInit('sec-storage')
                  })
                } else {
                  this.updateSecStorageSettings({project: this.currentSelectedProject, enabled: this.form.second_storage_enabled}).then(async (res) => {
                    try {
                      await handleSuccessAsync(res)
                      successCallback()
                      this.$emit('reload-setting')
                      this.form.second_storage_nodes = []
                      this.nodes = []
                      this.loadAvailableNodes()
                      this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
                    } catch (e) {
                      errorCallback()
                      handleError(e)
                    }
                  }).catch((e) => {
                    errorCallback()
                    handleError(e)
                  })
                }
              } catch (e) {
                errorCallback()
                handleError(e)
              }
            }).catch((e) => {
              errorCallback()
              handleError(e)
            })
            break
          }
        }
        case 'yarn-name': {
          if (await this.$refs['yarn-setting-form'].validate()) {
            let submitData = _getYarnNameSetting(this.form)
            submitData = 'yarn_queue' in submitData && {project: submitData.project, queue_name: submitData.yarn_queue}
            await this.updateYarnQueue(submitData)
          } else {
            errorCallback()
          }
          break
        }
        case 'kerberos-acc': {
          if (this.form.fileList.length === 0) {
            this.$message({
              type: 'error',
              message: this.$t('kerberosFileRequied')
            })
            errorCallback()
          } else {
            // 利用H5 FORMDATA 同时传输多文件和数据
            if (await this.$refs['kerberos-setting-form'].validate()) {
              let formData = new FormData() // 利用H5 FORMDATA 同时传输多文件和数据
              formData.append('principal', this.form.principal)
              formData.append('file', this.form.file)
              let params = {
                project: this.form.project,
                body: formData
              }
              await this.updateKerberosConfig(params)
              // 提交完成后，要重置上传组件
              this.form.fileList = []
              this.form.file = null
              this.$refs['kerberos-setting-form'].clearValidate()
              // 刷新数据源确认弹窗
              this.$confirm(this.$t('refreshContent'), this.$t('refreshTitle'), {
                confirmButtonText: this.$t('refreshNow'),
                cancelButtonText: this.$t('refreshLater'),
                type: 'warning',
                centerButton: true,
                dangerouslyUseHTMLString: true
              }).then(() => {
                // 刷新数据源
                this.reloadHiveDBAndTables({ force: true, project: this.currentSelectedProject })
              })
            } else {
              errorCallback()
            }
          }
          break
        }
      }
      // 设置默认参数的有二次确认，所以成功的反馈不能在结尾调
      if (type !== 'defaultDB-settings' && type !== 'sec-storage') {
        successCallback()
        this.$emit('reload-setting')
        if (type === 'kerberos-acc') return
        this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
      }
    } catch (e) {
      errorCallback()
      handleError(e)
    }
  }
  async handleResetForm (type, successCallback, errorCallback) {
    try {
      switch (type) {
        case 'job-alert': {
          const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'job_notification_config'})
          const data = await handleSuccessAsync(res)
          this.form = { ...this.form, ..._getJobAlertSettings(data, true) }
          this.$refs['job-alert'].clearValidate()
          break
        }
        case 'defaultDB-settings': {
          // 不用重置
          /* const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'query_accelerate_threshold'})
          const data = await handleSuccessAsync(res)
          this.form = { ...this.form, ..._getDefaultDBSettings(data) }
          this.$refs['setDefaultDB'].clearValidate() */
          break
        }
        // case 'yarn-name': {
        //   const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'yarn_queue'})
        //   const data = await handleSuccessAsync(res)
        //   this.form = { ...this.form, ..._getYarnNameSetting(data) }
        //   this.$refs['yarn-setting-form'].clearValidate()
        //   break
        // }
        case 'kerberos-acc': {
          const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'kerberos_project_level_config'})
          const data = await handleSuccessAsync(res)
          this.form = { ...this.form, ..._getKerberosSettings(data, true) }
          this.form.fileList = []
          this.form.file = null
          this.$refs['kerberos-setting-form'].clearValidate()
          break
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
  handleAddItem (key, index) {
    this.form[key].splice(index + 1, 0, '')
  }
  handleRemoveItem (key, index) {
    if (this.form[key].length > 1) {
      this.form[key].splice(index, 1)
    }
  }
  handleRemove () {
    this.form.fileList = []
  }
  changeFile (file, fileList) {
    // 多次上传后清理之前上传的内容
    if (fileList.length > 1) {
      fileList.splice(0, 1)
    }
    // 输入内容后重新触发校验
    if (fileList.length > 0) {
      if (!/.keytab$/.test(fileList[0].name)) {
        this.$message({
          type: 'error',
          message: this.$t('fileTypeError')
        })
        // 清空列表
        fileList.splice(0, 1)
        this.form.fileList = []
        this.form.file = null
      } else if (fileList[0].size > 5 * 1024 * 1024) {
        this.$message({
          type: 'error',
          message: this.$t('maxSizeLimit')
        })
        // 清空列表
        fileList.splice(0, 1)
        this.form.fileList = []
        this.form.file = null
      } else {
        this.form.fileList = fileList
        this.form.file = file.raw ? file.raw : file
      }
    } else {
      this.form.fileList = []
      this.form.file = null
    }
  }
  isFormEdited (form, type) {
    const project = { ...this.project, alias: this.project.alias || this.project.project }
    switch (type) {
      case 'defaultDB-settings':
        return JSON.stringify(_getDefaultDBSettings(form)) !== JSON.stringify(_getDefaultDBSettings(project))
      case 'job-alert':
        return JSON.stringify(_getJobAlertSettings(form, true, true)) !== JSON.stringify(_getJobAlertSettings(project, true, true))
      case 'yarn-name':
        return JSON.stringify(_getYarnNameSetting(form)) !== JSON.stringify(_getYarnNameSetting(project))
      case 'sec-storage':
        return JSON.stringify(_getSecStorageSetting(form)) !== JSON.stringify(_getSecStorageSetting(project))
      case 'kerberos-acc':
        // 对比principal 或者有新上传的文件
        let name = this.form.principal !== project.principal
        let fileName = this.form.fileList.length > 0
        return name || fileName
    }
  }
  handleMulPartitionSetting (val) {
    const h = this.$createElement
    if (val) {
      this.$msgbox({
        title: this.$t('openMulPartitionSetting'),
        message: h('p', null, [
          h('span', null, [
            // h('i', { class: 'el-icon-ksd-alert', style: 'color: #F7BA2A;margin-right: 5px;' }),
            h('span', null, this.$t('openMulPartitionTip'))
          ]),
          h('a', {
            style: 'color: #0988DE',
            attrs: {
              href: `https://docs.kyligence.io/books/v4.5/${this.$lang === 'en' ? 'en' : 'zh-cn'}/Designers-Guide/model/model_design/advance_guide/multilevel_partitioning.${this.$lang === 'en' ? 'en' : 'cn'}.html`,
              target: '_blank'
            }
          }, this.$t('userManual')),
          this.$lang === 'en' && h('span', null, this.$t('openMulPartitionTip1')),
          h('p', null, this.$t('confirmOpenTip'))
        ]),
        type: 'warning',
        showCancelButton: true,
        centerButton: true,
        confirmButtonText: this.$t('confirmOpen'),
        cancelButtonText: this.$t('kylinLang.common.cancel')
      }).then(() => {
        this.toggleMultiPartition({multi_partition_enabled: val, project: this.currentSelectedProject}).then(async (res) => {
          try {
            await handleSuccessAsync(res)
            this.updateMultiPartitionEnable(val)
            this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
          } catch (e) {
            handleError(e)
          }
        }).catch((e) => {
          this.form.multi_partition_enabled = false
          handleError(e)
        })
      }).catch(() => {
        this.form.multi_partition_enabled = false
      })
    } else {
      // 抓取下是scd2 的model，如果存在，需要二次确认
      this.getMultiPartitionModels({project: this.currentSelectedProject}).then(async (modeldata) => {
        try {
          const data = await handleSuccessAsync(modeldata)
          if (data.length) {
            this.$msgbox({
              title: this.$t('closeMulPartitionSetting'),
              message: h('p', null, [
                h('span', null, [
                  // h('i', { class: 'el-icon-ksd-alert', style: 'color: #F7BA2A;margin-right: 5px;' }),
                  h('span', null, this.$t('closeMulPartitionTip'))
                ]),
                h('div', {
                  style: 'color: #546174;padding: 10px;background:#F8F9FB;'
                }, data.join(',')),
                h('p', null, this.$t('closeMulPartitionTip1'))
              ]),
              showCancelButton: true,
              type: 'warning',
              centerButton: true,
              confirmButtonText: this.$t('confirmClose'),
              cancelButtonText: this.$t('kylinLang.common.cancel')
            }).then(() => {
              this.toggleMultiPartition({multi_partition_enabled: val, project: this.currentSelectedProject}).then(async (res) => {
                try {
                  await handleSuccessAsync(res)
                  this.updateMultiPartitionEnable(val)
                  this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
                } catch (e) {
                  handleError(e)
                }
              }).catch((e) => {
                this.form.multi_partition_enabled = true
                handleError(e)
              })
            }).catch(() => {
              this.form.multi_partition_enabled = true
            })
          } else {
            this.toggleMultiPartition({multi_partition_enabled: val, project: this.currentSelectedProject}).then(async (res) => {
              try {
                await handleSuccessAsync(res)
                this.updateMultiPartitionEnable(val)
                this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
              } catch (e) {
                handleError(e)
              }
            }).catch((e) => {
              handleError(e)
            })
          }
        } catch (e) {
          handleError(e)
        }
      }).catch((e) => {
        handleError(e)
      })
    }
  }
  handleScdSetting (val) {
    const h = this.$createElement
    if (val) {
      this.$msgbox({
        title: this.$t('openSCDSetting'),
        message: h('p', null, [
          h('span', null, [
            // h('i', { class: 'el-icon-ksd-alert', style: 'color: #F7BA2A;margin-right: 5px;' }),
            h('span', null, this.$t('openSCDTip'))
          ]),
          h('a', {
            style: 'color: #0988DE',
            attrs: {
              href: `https://docs.kyligence.io/books/v4.5/${this.$lang === 'en' ? 'en' : 'zh-cn'}/Designers-Guide/model/model_design/slowly_changing_dimension.${this.$lang === 'en' ? 'en' : 'cn'}.html`,
              target: '_blank'
            }
          }, this.$t('userManual')),
          this.$lang === 'en' && h('span', null, this.$t('openSCDTip1')),
          h('p', null, this.$t('confirmOpenTip'))
        ]),
        showCancelButton: true,
        centerButton: true,
        type: 'warning',
        confirmButtonText: this.$t('confirmOpen'),
        cancelButtonText: this.$t('kylinLang.common.cancel')
      }).then(() => {
        this.toggleEnableSCD({scd2_enabled: val, project: this.currentSelectedProject}).then(async (res) => {
          try {
            await handleSuccessAsync(res)
            this.updateSCD2Enable(val)
            this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
          } catch (e) {
            handleError(e)
          }
        }).catch((e) => {
          this.form.scd2_enabled = false
          handleError(e)
        })
      }).catch(() => {
        this.form.scd2_enabled = false
      })
    } else {
      // 抓取下是scd2 的model，如果存在，需要二次确认
      this.getSCDModels({project: this.currentSelectedProject}).then(async (modeldata) => {
        try {
          const data = await handleSuccessAsync(modeldata)
          if (data.length) {
            this.$msgbox({
              title: this.$t('closeSCDSetting'),
              message: h('p', null, [
                h('span', null, [
                  // h('i', { class: 'el-icon-ksd-alert', style: 'color: #F7BA2A;margin-right: 5px;' }),
                  h('span', null, this.$t('closeSCDTip'))
                ]),
                h('div', {
                  style: 'color: #546174;padding: 10px;background:#F8F9FB;'
                }, data.join(',')),
                h('p', null, this.$t('closeSCDTip1'))
              ]),
              type: 'warning',
              showCancelButton: true,
              centerButton: true,
              confirmButtonText: this.$t('confirmClose'),
              cancelButtonText: this.$t('kylinLang.common.cancel')
            }).then(() => {
              this.toggleEnableSCD({scd2_enabled: val, project: this.currentSelectedProject}).then(async (res) => {
                try {
                  await handleSuccessAsync(res)
                  this.updateSCD2Enable(val)
                  this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
                } catch (e) {
                  handleError(e)
                }
              }).catch((e) => {
                this.form.scd2_enabled = true
                handleError(e)
              })
            }).catch(() => {
              this.form.scd2_enabled = true
            })
          } else {
            this.toggleEnableSCD({scd2_enabled: val, project: this.currentSelectedProject}).then(async (res) => {
              try {
                await handleSuccessAsync(res)
                this.updateSCD2Enable(val)
                this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
              } catch (e) {
                handleError(e)
              }
            }).catch((e) => {
              handleError(e)
            })
          }
        } catch (e) {
          handleError(e)
        }
      }).catch((e) => {
        handleError(e)
      })
    }
  }
  // 获取项目列表 新建，编辑，删除项目后重新拉取列表
  async getConfigList () {
    // 获取项目列表
    try {
      this.filterData.project = this.currentProjectData.name
      await this.getProjectConfigList(this.filterData)
      this.currentPage = 0
      this.handleCurrentChange(0, this.pageSize)
    } catch (e) {
      handleError(e)
    }
  }

  editConfig (item) {
    this.callProjectConfigModal({
      form: {
        key: item ? item.key : '',
        value: item ? item.value : ''
      },
      editType: item ? 'edit' : 'add'
    })
  }

  deleteConfig (item) {
    this.$confirm(this.$t('confirmDeleteConfig', {key: item.key}), this.$t('deleteConfig'), {
      confirmButtonText: this.$t('kylinLang.common.ok'),
      cancelButtonText: this.$t('kylinLang.common.cancel'),
      centerButton: true,
      type: 'warning'
    }).then(async () => {
      try {
        let params = {
          project: this.currentProjectData.name,
          config_name: item.key
        }
        await this.deleteProjectConfig(params)
        // 成功提示
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.actionSuccess')
        })
        this.getConfigList()
      } catch (e) {
        handleError(e)
      }
    })
  }

  handleCurrentChange (currentPage, pageSize) {
    this.currentPage = currentPage
    this.pageSize = pageSize
    this.convertedProperties = this.configList.slice(this.pageSize * currentPage, this.pageSize * (currentPage + 1))
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.el-message-box__input {
  padding-top: 8px;
}
.el-message-box__errormsg {
  display: none;
}
.accelerate-setting {
  .yarn-queue-config {
    .el-form-item.is-error {
      vertical-align: top;
      margin-top: -6px;
    }
  }
  .secondary-storage-nodes {
    display: block;
    width: 100%;
  }
  .beta-label {
    display: inline-block;
    height: 18px;
    line-height: 9px;
    background: #EFDBFF;
    color: #531DAB;
    font-size: 11px;
    font-family: Lato-Bold,Lato;
    font-weight: bold;
    border-radius: 2px;
    padding: 5px;
    box-sizing: border-box;
  }
  .item-value .el-input {
    width: 200px;
  }
  .notice-email {
    font-size: 14px;
    color: @text-title-color;
    line-height: 18px;
    background-color: @grey-4;
    margin-right: 10px;
  }
  .item-value .el-button {
    margin-left: 10px;
  }
  .item-value .el-button+.el-button {
    margin-left: 2px;
  }
  .item-value:not(:last-child) {
    margin-bottom: 5px;
  }
  .item-value:not(:first-child) .setting-label {
    visibility: hidden;
  }
  .acce-input {
    width: 64px;
  }
  .el-form-item {
    margin-bottom: 0;
    display: inline-block;
  }
  .ksd-switch {
    transform: scale(0.91);
    transform-origin: left;
  }
  .email-fix-top {
    position: relative;
    top: 7px;
    vertical-align: top;
  }
  .split {
    margin-top: 15px;
  }
  .el-button.is-disabled {
    * {
      cursor: not-allowed;
    }
  }
  .yarn-name-input {
    width: 120px;
  }
  .kerberosAccBlock{
    .el-form-item {
      margin-bottom: 15px;
      display: block;
    }
    .file-error {
      color: @error-color-1;
      font-size: 12px;
      height: 12px;
      line-height: 12px;
      margin-top: 5px;
    }
  }
}
</style>
