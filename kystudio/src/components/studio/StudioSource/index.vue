<template>
  <div class="studio-source">
    <div class="table-layout clearfix">
      <!-- 数据源导航栏 -->
      <div class="layout-left">
        <DataSourceBar
          :ignore-node-types="['column']"
          ref="datasource-bar"
          :project-name="currentSelectedProject"
          :is-show-load-source="true"
          :is-show-load-table="datasourceActions.includes('loadSource')"
          :is-show-settings="false"
          :is-show-selected="true"
          :hide-fact-icon="true"
          :is-expand-on-click-node="false"
          :is-first-select="true"
          :is-show-source-switch="datasourceActions.includes('sourceManagement')"
          :is-show-drag-width-bar="true"
          :expand-node-types="['datasource', 'database']"
          :searchable-node-types="['table', 'column']"
          @click="handleClick"
          @show-source="handleShowSourcePage"
          @tables-loaded="handleTablesLoaded">
        </DataSourceBar>
      </div>

      <!-- Source Table展示 -->
      <div class="layout-right">
        <template v-if="selectedTable">
          <!-- Source Table标题信息 -->
          <div class="table-header">
            <h1 class="table-name" :title="selectedTable.fullName">{{selectedTable.fullName}}</h1>
            <h2 class="table-update-at">{{$t('updateAt')}} {{selectedTable.updateAt | toGMTDate}}</h2>
            <div class="table-actions ky-no-br-space">
              <el-button type="primary" text icon="el-ksd-icon-sample_22" @click="sampleTable" v-if="datasourceActions.includes('sampleSourceTable') && [9, 8].includes(selectedTable.datasource)">{{$t('sample')}}</el-button>
              <el-button type="primary" class="ksd-ml-2" text icon="el-ksd-icon-resure_22" :loading="reloadBtnLoading" @click="handleReload" v-if="datasourceActions.includes('reloadSourceTable') && [9, 8].includes(selectedTable.datasource)">{{$t('reload')}}</el-button>
              <el-button type="primary" class="ksd-ml-2" text icon="el-ksd-icon-table_delete_22" :loading="delBtnLoading" v-if="datasourceActions.includes('delSourceTable')" @click="handleDelete">{{$t('delete')}}</el-button>
            </div>
          </div>
          <!-- Source Table详细信息 -->
          <el-tabs class="table-details" :class="{'is-global-alter': $store.state.system.isShowGlobalAlter}" v-model="viewType">
            <el-tab-pane :label="$t('columns')" :name="viewTypes.COLUMNS" >
              <TableColumns :table="selectedTable" v-if="viewType === viewTypes.COLUMNS"></TableColumns>
            </el-tab-pane>
            <el-tab-pane :label="$t('sampling')" :name="viewTypes.SAMPLE" v-if="selectedTable.datasource!==1">
              <TableSamples :table="selectedTable" v-if="viewType === viewTypes.SAMPLE"></TableSamples>
            </el-tab-pane>
            <el-tab-pane :label="$t('kafkaCluster')" :name="viewTypes.KAFKA" v-if="selectedTable.datasource===1">
              <KafkaCluster :streamingData="selectedTable"></KafkaCluster>
            </el-tab-pane>
          </el-tabs>
        </template>
        <!-- Table空页 -->
        <div class="empty-page" v-if="!selectedTable">
          <kylin-empty-data />
        </div>
        <transition name="slide">
          <SourceManagement v-if="isShowSourcePage" :project="currentProjectData" @fresh-tables="handleFreshTable"></SourceManagement>
        </transition>
      </div>
      <el-dialog
        class="sample-dialog"
        @close="resetSampling"
        limited-area
        :title="$t('sampleDialogTitle')" width="480px" :visible.sync="sampleVisible" :close-on-press-escape="false" :close-on-click-modal="false">
        <div class="sample-desc">{{sampleDesc}}</div>
        <div class="sample-desc" style="margin-top: 3px;">
          {{$t('sampleDesc1')}}<el-input size="small" class="ksd-mrl-5" style="width: 110px;" :class="{'is-error': errorMsg}" v-number="samplingRows" v-model="samplingRows" @input="handleSamplingRows"></el-input>{{$t('sampleDesc2')}}
          <div class="error-msg" v-if="errorMsg">{{errorMsg}}</div>
        </div>
        <span slot="footer" class="dialog-footer ky-no-br-space">
          <el-button plain @click="cancelSample" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" @click="submitSample" size="medium" :disabled="!!errorMsg" :loading="sampleLoading">{{$t('kylinLang.common.submit')}}</el-button>
        </span>
      </el-dialog>

      <el-dialog
        :visible.sync="isDelAllDepVisible"
        width="720px"
        status-icon="el-ksd-icon-warning_24"
        class="del-comfirm-dialog"
        :close-on-press-escape="false"
        :close-on-click-modal="false">
        <span slot="title">{{$t('unloadTableTitle')}}</span>
        <el-alert :show-background="false" :closable="false" type="warning" style="padding:0">
          <span slot="title" class="ksd-fs-14" v-html="delTabelConfirmMessage"></span>
        </el-alert>
        <span slot="footer" class="dialog-footer ky-no-br-space">
          <el-button type="primary" text size="medium" @click="isDelAllDepVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button v-if="isOnlySnapshot" type="primary" size="medium" :loading="delLoading" @click="handelDeleteTable(false)">{{$t('kylinLang.common.delete')}}</el-button>
          <template v-else>
            <el-button size="medium" :loading="delAllLoading" @click="handelDeleteTable(true)">{{$t('deleteAll')}}</el-button>
            <el-button type="primary" size="medium" :loading="delLoading" @click="handelDeleteTable(false)">{{$t('deleteTable')}}</el-button>
          </template>
        </span>
      </el-dialog>
    </div>
    <ReloadTable></ReloadTable>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions, mapMutations } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { viewTypes } from './handler'
import DataSourceBar from '../../common/DataSourceBar/index.vue'
import TableDataLoad from './TableDataLoad/TableDataLoad.vue'
import TableColumns from './TableColumns/TableColumns.vue'
import TableSamples from './TableSamples/TableSamples.vue'
import KafkaCluster from './KafkaCluster/KafkaCluster.vue'
import SourceManagement from './SourceManagement/SourceManagement.vue'
import ReloadTable from './TableReload/reload.vue'
import { handleSuccessAsync, handleError } from '../../../util'
import { kylinConfirm } from '../../../util/business'
import { getFormattedTable } from '../../../util/UtilTable'

@Component({
  components: {
    DataSourceBar,
    TableDataLoad,
    TableColumns,
    TableSamples,
    KafkaCluster,
    SourceManagement,
    ReloadTable
  },
  computed: {
    ...mapGetters([
      'currentProjectData',
      'currentSelectedProject',
      'datasourceActions'
    ])
  },
  methods: {
    ...mapActions({
      fetchTables: 'FETCH_TABLES',
      importTable: 'LOAD_HIVE_IN_PROJECT',
      prepareUnload: 'PREPARE_UNLOAD',
      deleteTable: 'DELETE_TABLE',
      submitSampling: 'SUBMIT_SAMPLING',
      hasSamplingJob: 'HAS_SAMPLING_JOB',
      getReloadInfluence: 'GET_RELOAD_INFLUENCE'
    }),
    ...mapActions('ReloadTableModal', {
      callReloadModal: 'CALL_MODAL'
    }),
    ...mapMutations({
      refreshTableCache: 'REPLACE_TABLE_CACHE'
    })
  },
  locales
})
export default class StudioSource extends Vue {
  selectedTableData = null
  viewType = viewTypes.DATA_LOAD
  viewTypes = viewTypes
  isShowSourcePage = false
  reloadBtnLoading = false
  sampleVisible = false
  sampleLoading = false
  delBtnLoading = false
  samplingRows = 20000000
  errorMsg = ''
  isDelAllDepVisible = false
  delTabelConfirmMessage = ''
  delLoading = false
  delAllLoading = false
  isOnlySnapshot = false
  get selectedTable () {
    this.viewType = viewTypes.COLUMNS
    return this.selectedTableData ? getFormattedTable(this.selectedTableData) : null
  }
  get sampleDesc () {
    return this.$t('sampleDesc', {tableName: this.selectedTable && this.selectedTable.fullName})
  }
  handleShowSourcePage (value) {
    this.isShowSourcePage = value
  }
  handleSamplingRows (samplingRows) {
    if (samplingRows && samplingRows < 10000) {
      this.errorMsg = this.$t('minNumber')
    } else if (samplingRows && samplingRows > 20000000) {
      this.errorMsg = this.$t('maxNumber')
    } else if (!samplingRows) {
      this.errorMsg = this.$t('invalidType')
    } else {
      this.errorMsg = ''
    }
  }
  resetSampling () {
    this.samplingRows = 20000000
    this.sampleLoading = false
    this.errorMsg = ''
  }
  async handleClick (data = {}) {
    if (data.type !== 'table') return
    try {
      const tableName = data.label
      const databaseName = data.database
      this.handleShowSourcePage(false)
      await this.fetchTableDetail({ tableName, databaseName, sourceType: data.datasource })
    } catch (e) {
      handleError(e)
    }
  }
  showDeleteTableConfirm (hasModel, hasJob) {
    const tableName = this.selectedTable.name
    const contentVal = { tableName }
    const confirmTitle = this.$t('unloadTableTitle')
    const confirmMessage1 = this.$t('affactUnloadInfo', contentVal)
    const confirmMessage2 = this.$t('unloadTable', contentVal)
    let confirmMessage = ''
    if (hasJob && !hasModel) {
      confirmMessage = confirmMessage1
    } else if (!hasJob && !hasModel) {
      confirmMessage = confirmMessage2
    } else if (hasModel) { // 这种情况说明时有相关模型引用，且选择了全部删除
      if (this.selectedTable.datasource === 9) {
        confirmMessage = this.$t('delHiveTableAllTips')
      } else if (this.selectedTable.datasource === 1) {
        confirmMessage = this.$t('delKafkaTableAllTips')
      }
    }
    const confirmButtonText = this.$t('kylinLang.common.ok')
    const cancelButtonText = this.$t('kylinLang.common.cancel')
    const confirmParams = { confirmButtonText, cancelButtonText, type: 'warning', closeOnClickModal: false, closeOnPressEscape: false }
    return this.$confirm(confirmMessage, confirmTitle, confirmParams)
  }
  sampleTable () {
    this.sampleVisible = true
  }
  cancelSample () {
    this.sampleVisible = false
  }
  async submitSample () {
    this.sampleLoading = true
    try {
      const res = await this.hasSamplingJob({ project: this.currentSelectedProject, qualified_table_name: this.selectedTable.fullName })
      const isHasSamplingJob = await handleSuccessAsync(res)
      if (!isHasSamplingJob) {
        this.toSubmitSample()
      } else {
        this.sampleVisible = false
        this.sampleLoading = false
        kylinConfirm(this.$t('confirmSampling', {table_name: this.selectedTable.fullName})).then(() => {
          this.toSubmitSample()
        })
      }
    } catch (e) {
      handleError(e)
      this.sampleVisible = false
      this.sampleLoading = false
    }
  }
  async toSubmitSample () {
    try {
      const res = await this.submitSampling({ project: this.currentSelectedProject, qualified_table_name: this.selectedTable.fullName, rows: this.samplingRows })
      await handleSuccessAsync(res)
      this.$message({ type: 'success', message: this.$t('samplingTableJobBeginTips', {tableName: this.selectedTable.fullName}) })
      this.sampleVisible = false
      this.sampleLoading = false
    } catch (e) {
      handleError(e)
      this.sampleVisible = false
      this.sampleLoading = false
    }
  }
  async handleDelete () {
    const projectName = this.currentSelectedProject
    const databaseName = this.selectedTable.database
    const tableName = this.selectedTable.name
    this.delBtnLoading = true
    const { hasModel, hasJob, hasSnapshot, modelSize } = await this._getAffectedModelCountAndSize()
    if (!hasModel && !hasSnapshot) {
      try {
        await this.showDeleteTableConfirm(hasModel, hasJob)
        await this.deleteTable({ projectName, databaseName, tableName })
        this.refreshTableCache({data: this.selectedTable, project: projectName})
        this.$message({ type: 'success', message: this.$t('unloadSuccess') })
        await this.handleFreshTable({ isSetToDefault: true })
        this.delBtnLoading = false
      } catch (e) {
        this.delBtnLoading = false
        handleError(e)
      }
    } else {
      this.delBtnLoading = false
      const storageSize = Vue.filter('dataSize')(modelSize)
      const dropTabelDepenSub = this.selectedTable.datasource === 1 ? this.$t('dropTabelDepenSub') : ''
      const dropTabelDepenSub2 = this.selectedTable.datasource === 9 ? this.$t('dropTabelDepenSub2') : ''
      const dropTabelDepenSub3 = this.selectedTable.datasource === 1 ? this.$t('dropTabelDepenSub3') : ''
      const contentVal = { tableName, storageSize, dropTabelDepenSub, dropTabelDepenSub2, dropTabelDepenSub3 }
      if (hasModel && !hasSnapshot) {
        this.delTabelConfirmMessage = this.$t('dropTabelDepen', contentVal)
      } else if (hasModel && hasSnapshot) {
        this.delTabelConfirmMessage = this.$t('dropTabelDepen2', contentVal)
      } else if (!hasModel && hasSnapshot) {
        this.isOnlySnapshot = true
        this.delTabelConfirmMessage = this.$t('dropTabelDepen3', contentVal)
      }
      this.isDelAllDepVisible = true
    }
  }
  async handelDeleteTable (cascade) {
    if (cascade) {
      this.delAllLoading = true
    } else {
      this.delLoading = true
    }
    try {
      const projectName = this.currentSelectedProject
      const databaseName = this.selectedTable.database
      const tableName = this.selectedTable.name
      cascade && await this.showDeleteTableConfirm(true) // 选择全部删除时，再次comfirm
      await this.deleteTable({ projectName, databaseName, tableName, cascade })
      this.refreshTableCache({data: this.selectedTable, project: projectName})
      this.$message({ type: 'success', message: this.$t('unloadSuccess') })
      await this.handleFreshTable({ isSetToDefault: true })
      this.isDelAllDepVisible = false
      this.delAllLoading = false
      this.delLoading = false
    } catch (e) {
      this.isDelAllDepVisible = false
      this.delAllLoading = false
      this.delLoading = false
      handleError(e)
    }
  }
  async handleReload () {
    try {
      const projectName = this.currentSelectedProject
      const databaseName = this.selectedTable.database
      const datasource = this.selectedTable.datasource
      const tableName = this.selectedTable.name
      let fullTableName = databaseName + '.' + tableName
      this.reloadBtnLoading = true
      const res = await this.getReloadInfluence({
        project: projectName,
        table: fullTableName
      })
      this.reloadBtnLoading = false
      const influenceDetail = await handleSuccessAsync(res)
      const isSubmit = await this.callReloadModal({
        checkData: influenceDetail,
        tableName: fullTableName
      })
      if (isSubmit) {
        this.fetchTableDetail({ tableName, databaseName, sourceType: datasource })
      }
    } catch (e) {
      this.reloadBtnLoading = false
      handleError(e)
    }
  }
  async handleFreshTable (options = {}) {
    try {
      const { isSetToDefault } = options
      const tableName = this.selectedTable.name
      const databaseName = this.selectedTable.database
      const datasource = this.selectedTable.datasource
      let isHaveFirstTable = true
      // 3016 临时修复方案，sprint1 修复后，以修复后的代码为准
      if (isSetToDefault) {
        await this.$refs['datasource-bar'].reloadTables(true) // true 表示不改变树开合情况
      } else {
        await this.$refs['datasource-bar'].refreshTables()
      }
      isSetToDefault
        ? isHaveFirstTable = this.$refs['datasource-bar'].selectFirstTable()
        : await this.fetchTableDetail({ tableName, databaseName, sourceType: datasource })
      if (!isHaveFirstTable) {
        this.selectedTableData = null
      }
    } catch (e) {
      handleError(e)
    }
  }
  async fetchTableDetail ({ tableName, databaseName, sourceType }) {
    try {
      const projectName = this.currentSelectedProject
      const res = await this.fetchTables({ projectName, databaseName, tableName, isExt: true, isFuzzy: false, sourceType })
      const tableDetail = await handleSuccessAsync(res)
      this.selectedTableData = tableDetail.tables[0]
    } catch (e) {
      handleError(e)
    }
  }
  handleTablesLoaded () {
  }
  _showDataRangeConfrim () {
    const confirmTitle = this.$t('remindLoadRangeTitle')
    const confirmMessage = this.$t('remindLoadRange')
    const confirmButtonText = this.$t('kylinLang.common.ok')
    const confirmButtonClass = 'guideTipSetPartitionConfitmBtn'
    const type = 'warning'
    return this.$alert(confirmMessage, confirmTitle, { confirmButtonText, type, confirmButtonClass })
  }
  async _getAffectedModelCountAndSize () {
    const projectName = this.currentSelectedProject
    const databaseName = this.selectedTable.database
    const tableName = this.selectedTable.name
    const response = await this.prepareUnload({projectName, databaseName, tableName})
    const result = await handleSuccessAsync(response)
    return { hasModel: result.has_model, hasJob: result.has_job, hasSnapshot: result.has_snapshot, modelSize: result.storage_size }
  }
  mounted () {
    this.viewType = viewTypes.COLUMNS
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.studio-source {
  height: 100%;
  background: white;
  .del-comfirm-dialog {
    .el-alert__title {
      line-height: 22px;
    }
  }
  .layout-left {
    z-index:8;
    .data-source-bar .el-tree {
      border: none;
    }
  }
  .layout-right {
    padding: 32px 24px 0;
    min-height: 100%;
    box-sizing: border-box;
    position: relative;
  }
  .table-name {
    font-size: 16px;
    line-height: 24px;
    color: #263238;
    margin-bottom: 2px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .table-details {
    padding-bottom: 20px;
    .el-tabs__content {
      min-height: calc(~'100vh - 204px');
    }
    &.is-global-alter {
      .el-tabs__content {
        min-height: calc(~'100vh - 257px');
      }
    }
  }
  .table-update-at {
    font-size: 12px;
    color: @text-normal-color;
    font-weight: normal;
  }
  .table-header {
    padding-right: 300px;
    position: relative;
    margin-bottom: 16px;
  }
  .table-actions {
    position: absolute;
    top: 50%;
    right: -12px;
    transform: translateY(-14px);
  }
  .el-tabs__nav {
    margin-left: 0;
  }
  .empty-page {
    position: absolute;
    top: 30%;
    left: 50%;
    transform: translate(-50%, -30%);
  }
  .slide-enter-active, .slide-leave-active {
    transition: transform .5s;
    transform: translateX(0);
  }
  .slide-enter, .slide-leave-to {
    transform: translateX(-100%);
  }
}
.sample-dialog {
  .sample-desc {
    color: @text-normal-color;
    word-break: break-word;
  }
  .error-msg {
    color: @color-danger;
    font-size: 12px;
  }
  .is-error .el-input__inner{
    border-color: @color-danger;
  }
}
</style>
