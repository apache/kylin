<template>
  <div class="internal-table-container">
    <div class="ksd-title-page">
      {{$t('internalTable')}}
      <common-tip placement="bottom-start" popper-class="internal-table-tips">
        <div slot="content" class="snapshot-desc">
          <div class="ksd-mb-8 snapshot-desc">{{$t('internalTableDesc')}}</div>
          <p>*&nbsp;{{$t('internalTableDesc1')}}</p>
          <p>*&nbsp;{{$t('internalTableDesc2')}}</p>
          <p>*&nbsp;{{$t('internalTableDesc3')}}</p>
        </div>
        <i class="el-ksd-icon-more_info_22 internal-table-icon ksd-fs-22"></i>
      </common-tip>
      <!-- <el-button type="primary" class="ksd-fright" icon="el-ksd-icon-add_22">{{$t('kylinLang.common.add')}}</el-button> -->
    </div>

    <div>
      <div class="ksd-fleft ky-no-br-space">
        <div class="ke-it-other_actions ksd-fleft">
          <!-- <el-button type="primary" text icon="el-ksd-icon-refresh_22"
            :disabled="selectedRows.length <= 0"
            @click="handleRefreshInternalTable"
          >{{$t('kylinLang.common.refresh')}}</el-button> -->
          <!-- <el-button type="primary" text icon="el-ksd-icon-table_delete_22"
            :disabled="selectedRows.length > 1 || selectedRows.length === 0"
            @click="handleDeleteInternalTable"
          >{{$t('kylinLang.common.delete')}}</el-button> -->
          <!-- <el-button type="primary" text icon="el-ksd-icon-repair_22">{{$t('kylinLang.common.repair')}}</el-button> -->
        </div>
      </div>
      <!-- <el-input class="ksd-fright ke-it-search_internal-table" prefix-icon="el-ksd-icon-search_22" size="medium"></el-input> -->
    </div>
    <div style="margin-bottom: 10px;">
      <el-tag size="small" closable v-for="(item, index) in filterTags" :key="index" @close="handleClose(item)" style="margin-left: 5px;">{{item}}</el-tag>
    </div>
    <el-table
      :data="internalTableList"
      style="width: 100%"
      v-loading="somethingLoading"
      @selection-change="handleSelectionChange">
      <el-table-column
        type=""
        width="55">
      </el-table-column>
      <el-table-column
        :label="$t('tableNameTitle')"
        sortable
        sort-by="table_name"
        width="340"
      >
        <template slot-scope="scope">
          <span class="layout-with-actions">
            <span class="">{{scope.row.table_name}}</span>
            <span :disabled="scope.row.row_count > 0" class="hover-action">
              <i class="icon el-ksd-icon-edit_22 hover-action-icon" @click="handleEditPartation(scope.row)"></i>
            </span>
            <span class="hover-action">
              <i class="icon el-ksd-icon-build_index_22 hover-action-icon" @click="handleDataManagement(scope.row, true)"></i>
            </span>
            <el-dropdown @command="(command) => handleCommand(command, scope.row)" :id="scope.row.uuid" trigger="click" class="hover-action">
              <span class="el-dropdown-link" >
                <el-button type="primary" text icon="el-ksd-icon-more_16"></el-button>
              </span>

              <el-dropdown-menu slot="dropdown"  :uuid='scope.row.uuid' :append-to-body="true" :popper-container="'modelListPage'" class="specialDropdown">
                <el-dropdown-item command="viewDetails" :disabled="!scope.row.time_partition_col">{{$t('menuViewDetails')}}</el-dropdown-item>
                <el-dropdown-item command="deleteInternalTable">{{$t('menuDeleteInternalTable')}}</el-dropdown-item>
                <el-dropdown-item command="clearData">{{$t('menuClearData')}}</el-dropdown-item>
              </el-dropdown-menu>
            </el-dropdown>
          </span>
        </template>
      </el-table-column>
      <el-table-column
        prop="database_name"
        :label="$t('databaseTitle')"
        width="180">
      </el-table-column>
      <el-table-column
        :label="$t('partationTitle')"
        width="220">
        <template slot-scope="scope">
          <span>{{scope.row.time_partition_col}}</span>
        </template>
      </el-table-column>
      <el-table-column
        prop="hit_count"
        :label="$t('usedTimesTitle')"
        width="100">
      </el-table-column>
      <el-table-column
        prop="row_count"
        :label="$t('rowCountTitle')"
        width="100">
      </el-table-column>
      <el-table-column
        prop="storage_size"
        :label="$t('storageSizeTitle')">
      </el-table-column>
      <el-table-column
        prop="update_time"
        sortable
        sort-by="update_time"
        :label="$t('lastUpdateTitle')"
        :formatter="dateFormatter"
        width="180">
      </el-table-column>
    </el-table>
     <kap-pager :totalSize="internalTableListTotal" :perPageSize="internalTableListPageLimit" :curPage="internalTableListPageOffset + 1" @handleCurrentChange='handlePagerChange' class="ksd-mtb-16 ksd-center" ></kap-pager>

     <InternalTableSetting :open="openDialog === 'internalTable'" :tableInfo="currentEditTableInfo" :handleClose="handleInternalSettingClose"/>
     <DataManagement :open="openDialog === 'dataManagement' || openDialog === 'dataManagementLoadData'" :openAction="openDialog" :tableInfo="currentEditTableInfo" :handleClose="handleDataManagementClose"/>
  </div>
</template>
<script>
import Vue from 'vue'
import { mapActions, mapGetters, mapState } from 'vuex'
import { Component } from 'vue-property-decorator'
import { pageRefTags } from 'config'
import dayjs from 'dayjs'
import * as types from 'src/store/types'
import InternalTableSetting from '../Setting/InternalTableSetting.vue'
import DataManagement from '../DataManagement/DataManagement.vue'
import locales from './locales'

@Component({
  locales,
  components: {
    InternalTableSetting,
    DataManagement
  },
  methods: {
    ...mapActions({
      getInternalTables: types.GET_INTERNAL_TABLES,
      fetchTableDetails: types.FETCH_TABLES,
      deleteInternalTable: types.DELETE_INTERNAL_TABLE,
      batchLoadInternalTables: types.BATCH_LOAD_INTERNAL_TABLES,
      deleteAllData: types.DELETE_ALL_DATA_INTERNAL_TABLE
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    ...mapState({
      internalTableList: state => state.internalTable.internalTableList,
      internalTableListTotal: state => state.internalTable.internalTableListTotal,
      internalTableListPageOffset: state => state.internalTable.internalTableListPageOffset,
      internalTableListPageLimit: state => state.internalTable.internalTableListPageLimit
    })
  }
})
export default class InternalTableList extends Vue {
  pageRefTags = pageRefTags
  openDialog = ''
  somethingLoading = false

  currentEditTableInfo = {}

  selectedRows = []

  filterTags = []

  created () {
    this._getInternalTables(this.internalTableListPageOffset, this.internalTableListPageLimit)
  }

  _getInternalTables (page, pageSize) {
    if (!this.somethingLoading) {
      this.somethingLoading = true
      this.getInternalTables({
        project: this.currentSelectedProject,
        page_offset: page,
        page_size: pageSize
      }).finally(() => {
        this.somethingLoading = false
      })
    }
  }

  dateFormatter (row) {
    return dayjs(row.update_time).format('YYYY-MM-DD HH:mm:ss')
  }

  filterTag (value) {
    this.filterTags = value
  }

  filterHandler (value, row, column) {
    const property = column.property
    return row[property] === value
  }

  handlePagerChange (page, pageSize) {
    this._getInternalTables(page, pageSize)
  }

  handleInternalSettingClose () {
    this.openDialog = ''
    this._getInternalTables(this.internalTableListPageOffset, this.internalTableListPageLimit)
  }

  handleEditPartation (row) {
    if (!this.somethingLoading && !(row.row_count > 0)) {
      this.somethingLoading = true
      this._fetchTableDetails(row).then(() => {
        if (this.currentEditTableInfo) {
          this.openDialog = 'internalTable'
        }
      }).finally(() => {
        this.somethingLoading = false
      })
    }
  }

  _fetchTableDetails (row) {
    return this.fetchTableDetails(
      { projectName: this.currentSelectedProject, databaseName: row.database_name, tableName: row.table_name, isExt: false, pageSize: 1, sourceType: 9 }
    ).then(result => {
      if (result.data.data && result.data.data.tables[0]) {
        const tableInfo = result.data.data.tables[0]
        tableInfo.project = this.currentSelectedProject
        tableInfo.time_partition_col = row.time_partition_col
        tableInfo.date_partition_format = row.date_partition_format
        tableInfo.bucket_column = row.tbl_properties.bucketCol
        tableInfo.bucket_num = parseInt(row.tbl_properties.bucketNum)
        const orderByArray = row.tbl_properties.orderByKey ? row.tbl_properties.orderByKey.split(',') : []
        const primaryKeyArray = row.tbl_properties.primaryKey ? row.tbl_properties.primaryKey.split(',') : []
        const tableAttributes = (tableInfo && tableInfo.columns) ? tableInfo.columns.map(c => ({ name: c.name, primaryKey: primaryKeyArray.includes(c.name), sortByKey: orderByArray.includes(c.name) })) : []
        tableInfo.table_attributes = tableAttributes.sort((a, b) => {
          const indexA = orderByArray.indexOf(a.name)
          const indexB = orderByArray.indexOf(b.name)
          return (indexA === -1 ? Infinity : indexA) - (indexB === -1 ? Infinity : indexB)
        })
        this.currentEditTableInfo = tableInfo
      }
    })
  }

  handleDataManagementClose () {
    this.openDialog = ''
  }

  handleDataManagement (row, loadData = false) {
    this.somethingLoading = true
    return this._fetchTableDetails(row).then(() => {
      if (this.currentEditTableInfo) {
        this.openDialog = loadData ? 'dataManagementLoadData' : 'dataManagement'
      }
    }).finally(() => {
      this.somethingLoading = false
    })
  }

  handleSelectionChange (values) {
    this.selectedRows = values
  }

  handleDeleteInternalTable () {
    if (!this.somethingLoading) {
      this.$confirm(this.$t('confirmDeleteInternalTablePrompt', {}), this.$t('confirmDeleteTitle'), {
        confirmButtonText: this.$t('kylinLang.common.submit'),
        cancelButtonText: this.$t('kylinLang.common.cancel'),
        centerButton: true,
        type: 'warning'
      }).then(() => {
        this.somethingLoading = true
        Promise.all(this.selectedRows.map(row => this.deleteInternalTable({
          project: this.currentSelectedProject,
          database: row.database_name,
          table: row.table_name
        }))).finally(() => {
          this.somethingLoading = false
        }).then(results => {
          this._getInternalTables(this.internalTableListPageOffset, this.internalTableListPageLimit)
        })
      })
    }
  }

  handleDeleteSingleInternalTable (row) {
    if (!this.somethingLoading) {
      this.$confirm(this.$t('confirmDeleteInternalTablePrompt', {}), this.$t('confirmDeleteTitle'), {
        confirmButtonText: this.$t('kylinLang.common.submit'),
        cancelButtonText: this.$t('kylinLang.common.cancel'),
        centerButton: true,
        type: 'warning'
      }).then(() => {
        this.somethingLoading = true
        this.deleteInternalTable({
          project: this.currentSelectedProject,
          database: row.database_name,
          table: row.table_name
        }).finally(() => {
          this.somethingLoading = false
        }).then(results => {
          this._getInternalTables(this.internalTableListPageOffset, this.internalTableListPageLimit)
        })
      })
    }
  }

  handleRefreshInternalTable () {
    if (!this.somethingLoading) {
      this.$confirm(this.$t('confirmRefreshInternalTablePrompt', {}), this.$t('confirmRefreshTitle'), {
        confirmButtonText: this.$t('kylinLang.common.submit'),
        cancelButtonText: this.$t('kylinLang.common.cancel'),
        centerButton: true,
        type: 'warning'
      }).then(async () => {
        this.somethingLoading = true
        this.batchLoadInternalTables({
          project: this.currentSelectedProject,
          tables: this.selectedRows.map(row => `${row.database_name}.${row.table_name}`),
          incremental: false,
          refresh: true
        }).finally(() => {
          this.somethingLoading = false
        })
      })
    }
  }

  clearSingleTableData (row) {
    this.$confirm(this.$t('confirmDeleteAllDataPrompt', {}), this.$t('confirmDeleteAllDataTitle'), {
      confirmButtonText: this.$t('kylinLang.common.submit'),
      cancelButtonText: this.$t('kylinLang.common.cancel'),
      centerButton: true,
      type: 'warning'
    }).then(async () => {
      this.somethingLoading = true
      this.deleteAllData({
        table: `${row.database_name}.${row.table_name}`,
        project: this.currentSelectedProject
      }).finally(() => {
        this.somethingLoading = false
        this._getInternalTables(this.internalTableListPageOffset, this.internalTableListPageLimit)
      })
    })
  }

  handleCommand (command, row) {
    switch (command) {
      case 'viewDetails':
        this.handleDataManagement(row, false)
        break
      case 'deleteInternalTable':
        this.handleDeleteSingleInternalTable(row)
        break
      case 'clearData':
        this.clearSingleTableData(row)
        break
    }
  }
}
</script>
<style lang="less">
  .internal-table-container {
    margin: 32px 24px 24px;
  }
  .ke-it-search_internal-table {
    width: 248px;
  }
  .internal-table-icon {
    position: relative;
    bottom: 5px;
    left: -5px;
  }
  .layout-with-actions {
    display: flex;
    column-gap: 8px;
    justify-content: space-between;
    align-items: center;

    :first-child {
      flex: 1
    }
  }
  .el-table__row {
    .hover-action {
      visibility: hidden;
      cursor: pointer;
    }
    &:hover {
      .hover-action {
        visibility: visible;
      }
    }

    .hover-action-icon {
      vertical-align: middle;
      font-size: 22px;
      color: #2f374c;
    }

    .el-dropdown-link{
      top: 2px;
      position: relative;

      .el-button--medium {
        padding: 5px 0;
      }
    }

    [disabled] .icon {
      opacity: 0.3;
      cursor: not-allowed;
    }
  }
  .common-tip-layout {
    max-width: 420px !important;

    :first-child {
      max-height: 100% !important;
    }
  }
</style>
