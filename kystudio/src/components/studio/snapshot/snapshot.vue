<template>
  <div id="snapshot">
    <div class="ksd-title-page">{{$t('snapshotList')}}
      <common-tip placement="bottom-start">
        <div slot="content" class="snapshot-desc">
          <p>*&nbsp;{{$t('snapshotDesc1')}}</p>
          <p>*&nbsp;{{$t('snapshotDesc2')}}</p>
          <p>*&nbsp;{{$t('snapshotDesc3')}}</p>
        </div>
        <i class="el-ksd-icon-more_info_22 snapshot-icon ksd-fs-22"></i>
      </common-tip>
    </div>
    <div class="ksd-mb-16 snapshot-desc">{{$t('snapshotDesc')}}</div>

    <div class="clearfix">
      <div class="ksd-fleft ky-no-br-space" v-if="datasourceActions.includes('snapshotAction')">
        <el-button type="primary" class="ksd-mr-8 ksd-fleft" icon="el-ksd-icon-add_22" @click="addSnapshot">{{$t('snapshot')}}</el-button>
        <div class="ke-it-other_actions ksd-fleft">
          <el-button type="primary" text icon="el-ksd-icon-refresh_22" :disabled="!multipleSelection.length || hasEventAuthority('refresh')" @click="refreshSnapshot">{{$t('kylinLang.common.refresh')}}</el-button>
          <el-button type="primary" text icon="el-ksd-icon-table_delete_22" :disabled="!multipleSelection.length || hasEventAuthority('delete')" @click="deleteSnap">{{$t('kylinLang.common.delete')}}</el-button>
          <el-button type="primary" text icon="el-ksd-icon-repair_22" :disabled="!multipleSelection.length || hasEventAuthority('repair')" @click="repairSnapshot">{{$t('kylinLang.common.repair')}}</el-button>
        </div>
      </div>
      <el-input class="ksd-fright search-input ke-it-search_snapshot" :disabled="loadingSnapshotTable" v-global-key-event.enter.debounce="onFilterChange" @clear="onFilterChange()" :value="filter.table" @input="handleFilterInput" prefix-icon="el-ksd-icon-search_22" :placeholder="$t('searchSnapshot')" size="medium"></el-input>
    </div>
    <el-table class="ksd-mt-16 snapshot-table ke-it-snapshot_table"
      v-scroll-shadow
      :data="snapshotTables"
      @sort-change="sortSnapshotList"
      :default-sort = "{prop: 'last_modified_time', order: 'descending'}"
      :empty-text="emptyText"
      :row-class-name="setRowClass"
      @selection-change="handleSelectionChange"
      v-loading="loadingSnapshotTable"
      ref="snapshotTableRef"
      style="width: 100%">
      <el-table-column type="selection" align="center" width="44"></el-table-column>
      <el-table-column
        prop="table"
        :label="$t('tableName')"
        sortable="custom"
        min-width="150"
        show-overflow-tooltip>
        <template slot-scope="scope">
          <div v-if="scope.row.forbidden_colunms&&scope.row.forbidden_colunms.length" style="height: 23px;">
            <span v-custom-tooltip="{text: scope.row.table, w: 20, tableClassName: 'snapshot-table'}">{{scope.row.table}}</span>
            <i class="el-icon-ksd-lock ksd-fs-16" @click="openAuthorityDetail(scope.row)"></i>
          </div>
          <span v-else>{{scope.row.table}}</span>
        </template>
      </el-table-column>
      <el-table-column
        prop="database"
        :label="$t('databaseName')"
        show-overflow-tooltip
        width="130">
      </el-table-column>
      <el-table-column
        :label="$t('partitionColumns')"
        show-overflow-tooltip
        :filters="partitionColumns"
        :filtered-value="filter.partitionColumns"
        filter-icon="el-ksd-icon-filter_22"
        :show-multiple-footer="false"
        :filter-change="(v) => filterContent(v, 'partition')"
        width="160">
        <template slot-scope="scope">
          <div class="partition-column" v-if="!loadingSnapshotTable">
            <span class="content" v-custom-tooltip="{text: scope.row.select_partition_col, w: 20, tableClassName: 'snapshot-table'}">{{scope.row.select_partition_col || $t('noPartition')}}</span>
            <i @click="editPartitionColumn(scope.row)" v-if="scope.row.source_type === 9" :class="['el-icon-ksd-table_edit', {'is-disabled': scope.row.status === 'BROKEN'}]"></i>
          </div>
        </template>
      </el-table-column>
      <el-table-column
        prop="usage"
        sortable="custom"
        :label="$t('usage')"
        header-align="right"
        align="right"
        width="120">
      </el-table-column>
      <el-table-column
        width="160"
        :filters="allStatus.map(item => ({text: $t(item), value: item}))"
        :filtered-value="filter.status"
        :label="$t('status')"
        filter-icon="el-ksd-icon-filter_22"
        :show-multiple-footer="false"
        :filter-change="(v) => filterContent(v, 'status')"
      >
        <template slot-scope="scope">
          <el-tag size="mini" :type="getTagType(scope.row)">{{scope.row.status}}</el-tag>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('rows')"
        prop="total_rows"
        sortable="custom"
        width="100"
        header-align="right"
        align="right"
      >
        <template slot-scope="scope">
          <div>{{sliceNumber(scope.row.total_rows)}}</div>
        </template>
      </el-table-column>
      <el-table-column
        header-align="right"
        align="right"
        prop="storage"
        sortable="custom"
        show-overflow-tooltip
        width="120px"
        :label="$t('storage')">
        <template slot-scope="scope">
          {{scope.row.storage | dataSize}}
        </template>
      </el-table-column>
      <el-table-column
        prop="lookup_table_count"
        info-icon="el-ksd-icon-more_info_22"
        :info-tooltip="$t('lookupModelsTip')"
        :label="$t('lookupModels')"
        header-align="right"
        align="right"
        width="200">
      </el-table-column>
      <el-table-column
        prop="fact_table_count"
        info-icon="el-ksd-icon-more_info_22"
        :info-tooltip="$t('factModelsTip')"
        :label="$t('factModels')"
        header-align="right"
        align="right"
        width="180">
      </el-table-column>
      <el-table-column prop="last_modified_time" sortable="custom" width="180" show-overflow-tooltip :label="$t('modifyTime')">
        <template slot-scope="scope">
          <span v-if="!scope.row.last_modified_time || scope.row.status === 'LOADING'">-</span>
          <span v-else>{{scope.row.last_modified_time | toServerGMTDate}}</span>
        </template>
      </el-table-column>
    </el-table>
    <kylin-pager :totalSize="snapshotTotal" :curPage="filter.page_offset+1"  v-on:handleCurrentChange='currentChange' ref="snapshotPager" :refTag="pageRefTags.snapshotPager" class="ksd-mtb-16 ksd-center" ></kylin-pager>

    <!-- 添加Snapshot -->
    <SnapshotModel v-on:reloadSnapshotList="getSnapshotList"/>

    <el-dialog
      :visible.sync="authorityVisible"
      width="480px"
      :close-on-click-modal="false"
      class="authority-dialog ke-it-authority_snapshot"
      @close="handleClose">
      <span slot="title" class="ksd-title-label">{{$t('authorityTitle')}}</span>
      <div class="ksd-m-b-10">{{$t('authorityTips', {snapshot: snapshotObj.table})}}</div>
      <div class="ksd-mb-5 ksd-mt-10">{{$t('columns')}}({{snapshotObj.forbidden_colunms.length}})</div>
      <div class="authority-block">
        <el-button class="copy-btn"
          v-clipboard:copy="snapshotObj.forbidden_colunms.join('\r\n')"
          v-clipboard:success="onCopy"
          v-clipboard:error="onError"
          size="mini">{{$t('kylinLang.common.copy')}}</el-button>
        <p v-for="c in snapshotObj.forbidden_colunms" :key="c">{{c}}</p>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button plain @click="authorityVisible = false">{{$t('kylinLang.common.close')}}</el-button>
      </span>
    </el-dialog>

    <!-- 分区设置 -->
    <el-dialog
      :visible.sync="partitionSetting"
      width="480px"
      :close-on-click-modal="false"
      class="partition-setting-dialog"
      @close="handleClosePartitionSetting">
      <span slot="title" class="ksd-title-label">{{$t('partitionTitle')}}</span>
      <!-- <div class="ksd-mb-5 ksd-mt-10">{{$t('columns')}}({{snapshotObj.forbidden_colunms.length}})</div> -->
      <el-alert class="ksd-mb-10" :title="currentPartitionRow && currentPartitionRow.prevPartitionColumn && !currentPartitionRow.config_partition_column ? $t('alertPartitionChangeTip') : $t('alertPartitionChangeToOthersTip')" type="warning" :closable="false" show-icon v-if="currentPartitionRow && (currentPartitionRow.prevPartitionColumn && !currentPartitionRow.config_partition_column || currentPartitionRow.prevPartitionColumn !== currentPartitionRow.config_partition_column)"></el-alert>
      <div class="ksd-mb-10">{{$t('partitionColumnTip')}}</div>
      <p class="title ksd-mb-10">{{$t('selectPartitionTitle')}}</p>
      <div class="select-partitions" v-if="currentPartitionRow">
        <el-row :gutter="5">
          <el-col :span="22">
            <el-select v-loading="loadingParition" v-model="currentPartitionRow.config_partition_column" @change="currentPartitionRow.fetchError = false" :placeholder="$t('selectPartitionPlaceholder')" style="width: 100%;" :disabled="currentPartitionRow.isLoadingPartition">
              <el-option :label="$t('noPartition')" value=""></el-option>
              <el-option :label="currentPartitionRow.partition_col" :value="currentPartitionRow.partition_col" v-if="currentPartitionRow.partition_col">
                <el-tooltip :content="currentPartitionRow.partition_col" effect="dark" placement="top"><span style="float: left">{{ currentPartitionRow.partition_col | omit(30, '...') }}</span></el-tooltip>
                <span class="ky-option-sub-info">{{ currentPartitionRow.partition_col_type.toLocaleLowerCase() }}</span>
              </el-option>
              <!-- <p class="more-partition" @click="showAllPartition" v-if="!currentPartitionRow.showMore">{{$t('viewAllPartition')}}</p> -->
              <template v-if="currentPartitionRow.showMore">
                <!-- <el-option :label="key" :value="key" v-for="(key, value) in item.other_column_and_type" :key="key"></el-option> -->
                <el-option :label="value" :value="value" v-for="(key, value) in currentPartitionRow.partition[0].other_column_and_type" :key="value">
                  <el-tooltip :content="value" effect="dark" placement="top"><span style="float: left">{{ value | omit(25, '...') }}</span></el-tooltip>
                  <span class="ky-option-sub-info">{{ key.toLocaleLowerCase() }}</span>
                </el-option>
              </template>
              <!-- <el-option label="" value="" v-if="partitionMeta.column && timeDataType.indexOf(getColumnInfo(partitionMeta.column).datatype)===-1"></el-option> -->
            </el-select>
            <p class="error-msg" v-if="currentPartitionRow.fetchError"><i class="el-icon-ksd-error_01 error-icon"></i>{{$t('fetchPartitionErrorTip')}}</p>
            <p class="alert-msg" v-if="currentPartitionRow.undefinedPartitionColErrorTip"><i class="el-icon-ksd-alert alert-icon"></i>{{$t('undefinedPartitionColErrorTip')}}</p>
          </el-col>
          <el-col :span="2">
            <el-tooltip effect="dark" :content="$t('detectPartition')" placement="top">
              <div style="display: inline-block;">
                <el-button
                  size="medium"
                  :loading="currentPartitionRow.isLoadingPartition"
                  icon="el-ksd-icon-data_range_search_old"
                  @click="handleLoadPartitionColumn">
                </el-button>
              </div>
            </el-tooltip>
          </el-col>
        </el-row>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button plain @click="partitionSetting = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button :loading="loadingSubmit" @click="savePartitionColumns">{{$t('kylinLang.common.save')}}</el-button>
      </span>
    </el-dialog>

    <!-- 刷新 snapshot -->
    <el-dialog
      v-if="refreshSnapshotDialog"
      :visible="true"
      width="800px"
      :close-on-click-modal="false"
      class="refresh-snapshot-dialog ke-it-refresh-snapshot"
      @close="handleCloseRefreshDialog">
      <span slot="title" class="ksd-title-label">{{$t('refreshTitle')}}</span>
      <el-alert
        type="tip"
        :closable="false"
        show-icon>
        {{$t('refreshTips', {snapshotNum: multipleSelection.length, partitionColNum: refreshSnapshotTables.length})}}
      </el-alert>
      <el-radio-group v-model="refreshType" class="refresh-table-types">
        <el-radio :label="item.label" v-for="item in refreshTableOptions" :key="item.label" :disabled="['incremental', 'custom'].includes(item.label) ? onlyFullBuild : false">{{item.text}}
          <el-tooltip v-if="item.tips" :content="item.tips" placement="top">
            <i class="tips el-ksd-icon-info_fill_16"></i>
          </el-tooltip>
        </el-radio>
      </el-radio-group>
      <el-table class="ksd-mt-16 snapshot-table ke-it-snapshot_table"
        v-loading="loadSnapshotValues"
        v-if="multipleSelection.length"
        :data="multipleSelection"
        :empty-text="emptyText"
        border
        style="width: 100%">
        <el-table-column
          prop="table"
          :label="$t('tableName')"
          :sortable="true"
          show-overflow-tooltip>
        </el-table-column>
        <el-table-column
          prop="database"
          :label="$t('databaseName')"
          show-overflow-tooltip
          width="130">
        </el-table-column>
        <el-table-column
          prop="select_partition_col"
          :label="$t('partitionColumns')"
          show-overflow-tooltip
          width="140">
          <template slot-scope="scope">
            <span>{{scope.row.select_partition_col || '-'}}</span>
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('partitionValues')"
          show-overflow-tooltip
          width="231">
          <template slot-scope="scope">
            <template v-if="!loadSnapshotValues">
              <template v-if="refreshType === 'custom'">
                <el-select
                  :class="['partition-value-select', {'is-error': incrementalBuildErrorList.includes(`${scope.row.database}.${scope.row.table}`)}]"
                  v-model="scope.row.partition_values"
                  :disabled="!scope.row.select_partition_col"
                  multiple
                  @change="changePartitionValues(scope)"
                  :placeholder="scope.row.select_partition_col && (scope.row.readyPartitions.length > 0 || scope.row.notReadyPartitions.length > 0) ? $t('kylinLang.common.pleaseSelect') : ''"
                  collapse-tags
                  filterable>
                  <el-option-group
                    class="group-partitions"
                    :label="$t('readyPartitions')">
                    <span class="partition-count">{{scope.row.readyPartitions.length}}</span>
                    <el-option
                      v-for="item in scope.row.readyPartitions.slice(0, scope.row.pageReadyPartitionsSize * scope.row.pageSize)"
                      :key="item.value"
                      :label="item.label"
                      :value="item.value">
                    </el-option>
                    <p class="page-value-more" v-show="scope.row.pageReadyPartitionsSize * scope.row.pageSize < scope.row.readyPartitions.length" @click.stop="scope.row.pageReadyPartitionsSize += 1">{{$t('kylinLang.common.loadMore')}}</p>
                  </el-option-group>
                  <el-option-group
                    class="group-partitions"
                    :label="$t('notReadyPartitions')">
                    <span class="partition-count">{{scope.row.notReadyPartitions.length}}</span>
                    <el-option
                      v-for="item in scope.row.notReadyPartitions.slice(0, scope.row.pageNotReadyPartitions * scope.row.pageSize)"
                      :key="item.value"
                      :label="item.label"
                      :value="item.value">
                    </el-option>
                    <p class="page-value-more" v-show="scope.row.pageNotReadyPartitions * scope.row.pageSize < scope.row.notReadyPartitions.length" @click.stop="scope.row.pageNotReadyPartitions += 1">{{$t('kylinLang.common.loadMore')}}</p>
                  </el-option-group>
                </el-select>
                <p class="error-tip" v-if="incrementalBuildErrorList.includes(`${scope.row.database}.${scope.row.table}`)">{{$t('noPartitionValuesError')}}</p>
              </template>
              <template v-else>
                <span v-if="!scope.row.select_partition_col">-</span>
                <div class="partition-values" v-else><el-tag class="partition-value-tag ksd-mr-2" :title="tag.value" size="small" v-for="tag in scope.row.readyPartitions" :key="tag.value">{{tag.value}}</el-tag></div>
              </template>
            </template>
          </template>
        </el-table-column>
      </el-table>
      <!-- <el-checkbox class="ksd-mt-10" v-model="refreshNewPartition" v-if="refreshSnapshotTables.length">
        {{$t('refreshNewPartitionTip')}}
        <el-tooltip :content="$t('refreshNewPartitionInfo')" effect="dark" placement="top">
          <i class="el-ksd-icon-more_info_22 ksd-fs-22"></i>
        </el-tooltip>
      </el-checkbox> -->
      <span slot="footer" class="dialog-footer">
        <el-button @click="handleCloseRefreshDialog">{{$t('kylinLang.common.close')}}</el-button>
        <el-button type="primary" @click="handleRefreshSnapshot" :loading="loadingRefresh">{{$t('kylinLang.common.refresh')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { handleSuccessAsync, handleError, kylinConfirm, sliceNumber } from '../../../util'
import { pageRefTags, bigPageCount } from 'config'
import SnapshotModel from './SnapshotModel/SnapshotModel.vue'

@Component({
  beforeRouteEnter (to, from, next) {
    next(vm => {
      if (to.params.table) {
        vm.filter.table = to.params.table
      }
      // vm.getSnapshotList()
    })
  },
  components: {
    SnapshotModel
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'datasourceActions'
    ])
  },
  methods: {
    ...mapActions({
      fetchSnapshotList: 'FETCH_SNAPSHOT_LIST',
      refreshSnapshotTable: 'REFRESH_SNAPSHOT_TABLE',
      deleteSnapshotCheck: 'DELETE_SNAPSHOT_CHECK',
      deleteSnapshot: 'DELETE_SNAPSHOT',
      fetchPartitionConfig: 'FETCH_PARTITION_CONFIG',
      reloadPartitionColumn: 'RELOAD_PARTITION_COLUMN',
      savePartitionColumn: 'SAVE_PARTITION_COLUMN',
      getSnapshotPartitionValues: 'GET_SNAPSHOT_PARTITION_VALUES'
    }),
    ...mapActions('SnapshotModel', {
      showSnapshotModelDialog: 'CALL_MODAL'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    })
  },
  locales
})
export default class Snapshot extends Vue {
  pageRefTags = pageRefTags
  snapshotTables = []
  allStatus = ['ONLINE', 'LOADING', 'REFRESHING', 'BROKEN']
  filter = {
    page_offset: 0,
    page_size: +localStorage.getItem(this.pageRefTags.snapshotPager) || bigPageCount,
    status: [],
    sort_by: 'last_modified_time',
    reverse: true,
    table: '',
    partition: []
  }
  snapshotTotal = 0
  multipleSelection = []
  authorityVisible = false
  snapshotObj = {
    table: '',
    forbidden_colunms: []
  }
  partitionSetting = false
  currentPartitionRow = null
  partitionColumnData = []
  loadingSubmit = false
  refreshSnapshotDialog = false
  refreshNewPartition = true
  loadingSnapshotTable = false
  filterTimer = null
  sliceNumber = sliceNumber
  refreshType = 'full'
  incrementalBuildErrorList = []
  loadSnapshotValues = false
  loadingRefresh = false

  get partitionColumns () {
    return [{text: this.$t('noPartition'), value: false}, {text: this.$t('hasPartition'), value: true}]
  }

  get refreshSnapshotTables () {
    return this.multipleSelection.filter(it => it.select_partition_col)
  }

  get refreshTableOptions () {
    return [
      { label: 'full', text: this.$t('fullRefresh'), tips: this.$t('fullFreshTip') },
      { label: 'incremental', text: this.$t('incrementalFresh'), tips: this.$t('incrementalFreshTip') },
      { label: 'custom', text: this.$t('customRefresh'), tips: this.$t('customRefreshTip') }
    ]
  }

  get onlyFullBuild () {
    return this.multipleSelection.filter(it => !it.select_partition_col).length === this.multipleSelection.length
  }

  handleClose () {
    this.authorityVisible = false
    this.snapshotObj = {
      table: '',
      forbidden_colunms: []
    }
  }

  handleCloseRefreshDialog () {
    this.refreshSnapshotDialog = false
    this.refreshNewPartition = true
    this.incrementalBuildErrorList = []
    this.loadSnapshotValues = false
  }

  // 刷新 snapshot
  async handleRefreshSnapshot () {
    try {
      this.loadingRefresh = true
      // await kylinConfirm(this.$t('refreshTips', {snapshotNum: this.multipleSelection.length}), {confirmButtonText: this.$t('kylinLang.common.refresh'), dangerouslyUseHTMLString: true, type: 'warning'}, this.$t('refreshTitle'))
      const tables = this.multipleSelection.map((s) => {
        return s.database + '.' + s.table
      })
      const options = {}
      this.multipleSelection.forEach(item => {
        if (!item.select_partition_col) return
        options[`${item.database}.${item.table}`] = {
          partition_col: item.select_partition_col,
          incremental_build: !!item.select_partition_col && this.refreshType !== 'full',
          partitions_to_build: this.refreshType === 'custom' && !!item.select_partition_col ? item.partition_values : null
        }
      })
      if (this.refreshType === 'custom') {
        const incrementalBuildErrorList = Object.keys(options).filter(it => options[it].partitions_to_build && options[it].partitions_to_build.length === 0 && options[it].partition_col)
        this.incrementalBuildErrorList = incrementalBuildErrorList
        if (incrementalBuildErrorList.length > 0) {
          this.loadingRefresh = false
          return
        }
      }
      await this.refreshSnapshotTable({ project: this.currentSelectedProject, tables, options })
      this.loadingRefresh = false
      this.$message({
        dangerouslyUseHTMLString: true,
        type: 'success',
        customClass: 'build-full-load-success',
        duration: 10000,
        showClose: true,
        message: (
          <div>
            <span>{this.$t('kylinLang.common.buildSuccess')}</span>
            <a href="javascript:void(0)" onClick={() => this.gotoJob()}>{this.$t('kylinLang.common.toJoblist')}</a>
          </div>
        )
      })
      this.handleCloseRefreshDialog()
      this.getSnapshotList()
    } catch (e) {
      this.loadingRefresh = false
      handleError(e)
    }
  }

  openAuthorityDetail (row) {
    this.snapshotObj = row
    this.authorityVisible = true
  }

  onCopy () {
    this.$message.success(this.$t('kylinLang.common.copySuccess'))
  }
  onError () {
    this.$message.error(this.$t('kylinLang.common.copyfail'))
  }

  get emptyText () {
    return this.filter.table ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  setRowClass (res) {
    const { row } = res
    return 'forbidden_colunms' in row && row.forbidden_colunms.length ? 'no-authority-model' : 'snapshot-row-layout'
  }
  handleFilterInput (val) {
    this.filter.table = val
  }
  onFilterChange () {
    this.filter.page_offset = 0
    this.getSnapshotList()
  }
  sortSnapshotList ({ column, prop, order }) {
    if (order === 'ascending') {
      this.filter.reverse = false
    } else {
      this.filter.reverse = true
    }
    this.filter.sort_by = prop
    this.filter.page_offset = 0
    this.getSnapshotList()
  }
  getTagType (row) {
    let map = {
      ONLINE: 'success',
      WARNING: 'warning',
      LOCKED: 'info',
      BROKEN: 'danger'
    }
    if (row.status in map) {
      return map[row.status]
    } else {
      return ''
    }
  }
  // 状态控制按钮的使用
  hasEventAuthority (type) {
    let typeList = (type) => {
      return this.multipleSelection.length && typeof this.multipleSelection[0] !== 'undefined' ? this.multipleSelection.filter(it => !type.includes(it.status)).length > 0 : false
    }
    if (type === 'refresh') {
      return typeList(['ONLINE'])
    } else if (type === 'delete') {
      return typeList(['ONLINE', 'LOADING', 'REFRESHING'])
    } else if (type === 'repair') {
      return typeList(['BROKEN'])
    }
  }
  filterContent (val, type) {
    clearTimeout(this.filterTimer)
    this.filterTimer = setTimeout(() => {
      this.filter[type] = val
      this.filter.page_offset = 0
      this.$refs.snapshotTableRef && this.$refs.snapshotTableRef.$el.click()
      this.getSnapshotList()
    }, 500)
  }

  // 编辑分区列
  async editPartitionColumn (row) {
    try {
      this.partitionSetting = true
      this.loadingParition = true
      const res = await this.fetchPartitionConfig({project: this.currentSelectedProject, tables: `${row.database}.${row.table}`})
      const results = await handleSuccessAsync(res)
      this.loadingParition = false
      this.currentPartitionRow = {
        ...row,
        config_partition_column: row.select_partition_col || '',
        prevPartitionColumn: row.select_partition_col || '',
        partition_col: results.value[0].partition_col,
        partition_col_type: results.value[0].partition_col_type,
        partitionColByKE: results.value[0].partition_col,
        showMore: false,
        isLoadingPartition: false,
        fetchError: false,
        partition: results.value,
        undefinedPartitionColErrorTip: false
      }
    } catch (e) {
      this.loadingParition = false
      handleError(e)
    }
  }

  showAllPartition (item) {
    this.currentPartitionRow.showMore = true
  }

  // 保存分区列
  savePartitionColumns () {
    const { database, table, config_partition_column } = this.currentPartitionRow
    this.loadingSubmit = true
    this.savePartitionColumn({
      project: this.currentSelectedProject,
      table_partition_col: {
        [`${database}.${table}`]: config_partition_column
      }
    }).then(() => {
      this.loadingSubmit = false
      this.handleClosePartitionSetting()
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.saveSuccess')
      })
      this.filter.page_offset = 0
      this.getSnapshotList()
    }).catch((e) => {
      this.loadingSubmit = false
      handleError(e)
    })
  }

  handleClosePartitionSetting () {
    this.partitionSetting = false
    this.currentPartitionRow = null
  }

  // 获取分区列
  handleLoadPartitionColumn () {
    const { database, table } = this.currentPartitionRow
    this.currentPartitionRow.isLoadingPartition = true
    this.currentPartitionRow.undefinedPartitionColErrorTip = false
    this.reloadPartitionColumn({project: this.currentSelectedProject, table: `${database}.${table}`}).then(async (res) => {
      this.currentPartitionRow.isLoadingPartition = false
      this.currentPartitionRow.fetchError = false
      try {
        const results = await handleSuccessAsync(res)
        const partitionColumns = [...Object.keys(this.currentPartitionRow.partition[0].other_column_and_type).map(it => ({key: it, datatype: this.currentPartitionRow.partition[0].other_column_and_type[it]})), {key: this.currentPartitionRow.partitionColByKE, datatype: this.currentPartitionRow.partition_col_type}]
        // this.currentPartitionRow.partition_column = results.partition_col || ''
        if (!results.partition_col) {
          this.currentPartitionRow.undefinedPartitionColErrorTip = true
        }
        if (results.partition_col && partitionColumns.filter(it => (it.key === results.partition_col && it.datatype === results.partition_col_type)).length === 0) {
          this.$confirm(this.$t('excludePartitionColumnTip', {partitionColumn: results.partition_col, tableName: table}), this.$t('kylinLang.common.tip'), {
            confirmButtonText: this.$t('jumpToDatasource'),
            cancelButtonText: this.$t('waitMoment'),
            type: 'error'
          }).then(() => {
            this.$router.push({path: '/studio/source'})
          })
          return
        }
        this.currentPartitionRow.config_partition_column = results.partition_col || ''
        this.currentPartitionRow.partition_col = results.partition_col || ''
        this.currentPartitionRow.partition_col_type = results.partition_col_type || ''
        if (Object.keys(results.other_column_and_type).length) {
          this.currentPartitionRow.partition[0].other_column_and_type = results.other_column_and_type
        }
      } catch (e) {
      }
    }).catch((e) => {
      this.currentPartitionRow.isLoadingPartition = false
      this.currentPartitionRow.fetchError = true
      handleError(e)
    })
  }
  handleSelectionChange (val) {
    this.multipleSelection = val.map(it => ({
      ...it,
      partition_values: [],
      readyPartitions: [],
      notReadyPartitions: [],
      pageReadyPartitionsSize: 1,
      pageNotReadyPartitions: 1,
      pageSize: 100,
      values: []
    }))
  }
  async refreshSnapshot () {
    if (!this.multipleSelection.length) return
    this.refreshSnapshotDialog = true
    this.loadSnapshotValues = true
    // 获取分区值
    try {
      const tableCols = {}
      this.multipleSelection.forEach(item => {
        tableCols[`${item.database}.${item.table}`] = item.select_partition_col
      })
      const results = await this.getSnapshotPartitionValues({
        project: this.currentSelectedProject,
        table_cols: tableCols
      })
      const partitionValues = results.data.data
      const snapshots = JSON.parse(JSON.stringify(this.multipleSelection))
      if (partitionValues) {
        snapshots.forEach(item => {
          if (`${item.database}.${item.table}` in partitionValues && partitionValues[`${item.database}.${item.table}`]) {
            item.partition_values = []
            item['readyPartitions'] = partitionValues[`${item.database}.${item.table}`].ready_partitions.map(it => ({label: it, value: it}))
            item['notReadyPartitions'] = partitionValues[`${item.database}.${item.table}`].not_ready_partitions.map(it => ({label: it, value: it}))
          }
        })
      }
      this.$set(this, 'multipleSelection', snapshots)
      this.loadSnapshotValues = false
    } catch (e) {
      this.loadSnapshotValues = false
      handleError(e)
    }
  }
  gotoJob () {
    this.$router.push('/monitor/job')
  }
  async deleteSnap () {
    if (!this.multipleSelection.length) return
    const tables = this.multipleSelection.map((s) => {
      return s.database + '.' + s.table
    })
    const res = await this.deleteSnapshotCheck({ project: this.currentSelectedProject, tables })
    const data = await handleSuccessAsync(res)
    if (data.affected_jobs && data.affected_jobs.length) {
      await this.callGlobalDetail(data.affected_jobs, this.$t('deleteTips', {snapshotNum: this.multipleSelection.length}) + this.$t('deleteTablesTitle'), this.$t('deleteTitle'), 'warning', this.$t('kylinLang.common.delete'))
    } else {
      await kylinConfirm(this.$t('deleteTips', {snapshotNum: this.multipleSelection.length}), {confirmButtonText: this.$t('kylinLang.common.delete'), dangerouslyUseHTMLString: true, type: 'warning'}, this.$t('deleteTitle'))
    }
    await this.deleteSnapshot({ project: this.currentSelectedProject, tables: tables.join(',') })
    this.$message({
      type: 'success',
      message: this.$t('kylinLang.common.delSuccess')
    })
    this.getSnapshotList()
  }

  async callGlobalDetail (targetTables, msg, title, type, submitText) {
    const tableData = []
    targetTables.forEach((t) => {
      const obj = {}
      obj['table'] = t.table
      obj['database'] = t.database
      tableData.push(obj)
    })
    await this.callGlobalDetailDialog({
      msg: msg,
      title: title,
      detailTableData: tableData,
      detailColumns: [
        {column: 'table', label: this.$t('tableName')},
        {column: 'database', label: this.$t('databaseName')}
      ],
      dialogType: type,
      showDetailBtn: false,
      submitText: submitText
    })
  }
  async addSnapshot (filterText) {
    const isSubmit = await this.showSnapshotModelDialog({})
    if (isSubmit) {
      this.getSnapshotList()
    }
  }
  async getSnapshotList () {
    try {
      this.loadingSnapshotTable = true
      this.filter.project = this.currentSelectedProject
      const res = await this.fetchSnapshotList({...this.filter, partition: this.filter.partition.join(',')})
      const { value, total_size } = await handleSuccessAsync(res)
      this.snapshotTables = value
      this.snapshotTotal = total_size
      this.loadingSnapshotTable = false
    } catch (e) {
      this.loadingSnapshotTable = false
      handleError(e)
    }
  }
  currentChange (size, count) {
    this.filter.page_offset = size
    this.filter.page_size = count
    this.getSnapshotList()
  }
  async repairSnapshot () {
    const data = this.multipleSelection
    const isSubmit = await this.showSnapshotModelDialog({type: 'repair', data})
    if (isSubmit) {
      this.getSnapshotList()
    }
  }

  // 更改刷新的分区列
  changePartitionValues (scope) {
    const index = this.incrementalBuildErrorList.findIndex(it => it === `${scope.row.database}.${scope.row.table}`)
    index >= 0 && this.incrementalBuildErrorList.splice(index, 1)
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
#snapshot {
  margin: 32px 24px 24px;
  .snapshot-icon {
    position: relative;
    bottom: 6px;
    left: -5px;
  }
  .snapshot-desc {
    color: @text-disabled-color;
  }
  .search-input {
    width: 248px;
  }
  .snapshot-table {
    .cell span:first-child {
      line-height: initial;
      line-height: 23px\0;
      vertical-align: middle;
    }
    .el-table__row.no-authority-model {
      background-color: @table-stripe-color;
      color: @text-disabled-color;
    }
    .ksd-nobr-text {
      width: calc(~'100% - 20px');
    }
    .el-icon-ksd-lock {
      position: absolute;
      right: 10px;
      top: 10px;
      color: @text-title-color;
    }
    .snapshot-row-layout {
      &:hover {
        .partition-column {
          i {
            display: inline-block;
          }
        }
      }
    }
    .partition-value-select {
      width: 100%;
      &.is-error {
        .el-input input {
          border: 1px solid @error-color-1;
        }
      }
      .el-input__inner {
        padding-left: 15px
      }
    }
    .partition-column {
      height: 24px;
      position: relative;
      .content {
        // width: calc(100% - 20px);
        // display: inline-block;
      }
      i {
        position: absolute;
        display: none;
        right: 0;
        vertical-align: top;
        margin-top: 5px;
        &.is-disabled {
          color: @text-disabled-color;
          pointer-events: none;
        }
      }
    }
    .partition-values {
      width: 100%;
      overflow: hidden;
      text-overflow: ellipsis;
      .partition-value-tag {
        display: inline-block;
        max-width: 94px;
        overflow-x: hidden;
        text-overflow: ellipsis;
        vertical-align: middle !important;
      }
    }
  }
}
.authority-dialog {
  .authority-block {
    border: 1px solid @line-border-color;
    background-color: @base-background-color;
    color: @text-normal-color;
    padding: 10px;
    border-radius: 2px;
    position: relative;
    .copy-btn {
      position: absolute;
      right: 10px;
    }
  }
}
.partition-setting-dialog {
  .title {
    font-weight: bold;
  }
  .select-partitions {
    .el-select {
      width: 100%;
    }
    .error-msg {
      // color: @color-danger;
      font-size: 12px;
      margin-top: 2px;
      .error-icon {
        margin-right: 5px;
        color: @color-danger;
      }
    }
    .alert-msg {
      font-size: 12px;
      margin-top: 2px;
      .alert-icon {
        margin-right: 5px;
        color: @color-warning;
      }
    }
  }
}
.refresh-snapshot-dialog {
  .alert-info {
    color: #F7BA2A;
  }
  .refresh-table-types {
    margin-top: 16px;
    .el-radio+.el-radio {
      margin-left: 16px;
    }
    .tips {
      vertical-align: baseline;
      color: @text-placeholder-color;
    }
  }
}
.error-tip {
  color: @error-color-1;
  font-size: 12px;
}
.group-partitions {
  .partition-count {
    position: absolute;
    top: 5px;
    right: 16px;
    font-size: 12px;
    color: @text-disabled-color;
  }
  .page-value-more {
    margin-top: 8px;
    text-align: center;
    cursor: pointer;
    font-size: 12px;
    color: @text-disabled-color;
  }
}
</style>
