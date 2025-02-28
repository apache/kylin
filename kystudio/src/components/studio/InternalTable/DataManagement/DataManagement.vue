<template>
  <el-dialog
    :visible="open"
    :title="dialogTitle"
    class='internal-table-data-management'
    :before-close="handleDataClose">
    <div v-show="!showLoadData" v-loading="internalTableDataListLoading">
      <div>
        <div class="ksd-fleft ky-no-br-space">
          <div class="ke-it-other_actions ksd-fleft">
            <!-- <el-button type="primary" text icon="el-ksd-icon-refresh_22" :disabled="selectedRows.length === 0" @click="handleRefreshData">{{$t('kylinLang.common.refresh')}}</el-button> -->
            <el-button type="primary" text icon="el-ksd-icon-table_delete_22" :disabled="selectedRows.length === 0" @click="handleDeletePartitions">{{$t('kylinLang.common.delete')}}</el-button>
            <el-button type="primary" text icon="el-ksd-icon-build_index_22" @click="handleShowLoadData">{{$t('loadData')}}</el-button>
          </div>
        </div>
      </div>

      <el-table
        :data="internalTableDataList"
        class="data-list-table"
        style="width: 100%"
        @selection-change="handleSelectionChange">
        <el-table-column
          type="selection"
          width="40">
        </el-table-column>

        <el-table-column
          prop="uuid"
          :label="$t('idColumnTitle')"
          width="80"
        >
          <template slot-scope="scope">
            <el-tooltip :content="'uuid: ' + scope.row.uuid" placement="follow-mouse">
              <span class="text-ellipsis">{{scope.row.uuid}}</span>
            </el-tooltip>
          </template>
        </el-table-column>

        <el-table-column
          prop="storage_path"
          :label="$t('partitionColumnTitle')"
          width="400"
        >
        </el-table-column>

        <el-table-column
          prop="size_in_bytes"
          :label="$t('storageVolumnColumnTitle')"
          width="130"
        >
        </el-table-column>

        <el-table-column
          prop="file_count"
          :label="$t('fileCountColumnTitle')"
          width="100"
        >
        </el-table-column>

        <el-table-column
          prop="last_modified"
          :formatter="dateFormatter"
          :label="$t('updateColumnTitle')"
          width="150"
        >
        </el-table-column>
      </el-table>
      <kylin-pager
        :totalSize="internalTableDataListTotal"
        :perPageSize="internalTableDataListPageLimit"
        :curPage="internalTableDataListPageOffset + 1"
        @handleCurrentChange='handlePagerChange'
        class="ksd-mtb-16 ksd-center">
      </kylin-pager>
    </div>
    <LoadData v-show="showLoadData" :tableInfo="tableInfo" :handleClose="handleCloseLoadData"/>
  </el-dialog>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import dayjs from 'dayjs'
import LoadData from './LoadData.vue'
import * as types from 'src/store/types'
import locales from './locales'

@Component({
  locales,
  components: {
    LoadData
  },
  props: {
    open: {
      type: Boolean,
      default: false
    },
    openAction: {
      type: String,
      default: ''
    },
    tableInfo: {
      type: Object
    },
    handleClose: {
      type: Function,
      default: () => {}
    }
  },

  methods: {
    ...mapActions({
      getInternalTableDetails: types.GET_INTERNAL_TABLE_DETAILS,
      loadSingleInternalTable: types.LOAD_SINGLE_INTERNAL_TABLE,
      deletePartitions: types.DELETE_INTERNAL_TABLE_PARTITIONS,
      deleteAllData: types.DELETE_ALL_DATA_INTERNAL_TABLE
    })
  },

  computed: {
    dialogTitle () {
      return this.showLoadData ? this.$t('dialogLoadDataTitle') : this.$t('dialogTitle', { tableName: this.tableInfo.name })
    }
  }
})
export default class DataManagement extends Vue {
  internalTableDataList = []
  internalTableDataListTotal = 0
  internalTableDataListPageOffset = 0
  internalTableDataListPageLimit = 20
  internalTableDataListLoading = false

  showLoadData = false

  selectedRows = []

  @Watch('tableInfo')
  tableInfoChanged (newVal, oldVal) {
    if (newVal.database) {
      this._getInternalTableDetails(this.internalTableDataListPageOffset, this.internalTableDataListPageLimit)
    }
  }

  _getInternalTableDetails (page, pageSize, afterLoadData = false) {
    if (!this.internalTableDataListLoading) {
      this.internalTableDataListLoading = true
      this.getInternalTableDetails({
        database: this.tableInfo.database,
        table: this.tableInfo.name,
        project: this.tableInfo.project,
        page_offset: page,
        page_size: pageSize
      }).then(result => {
        this.internalTableDataList = (result.data.data && result.data.data.value) || []
        this.internalTableDataListTotal = (result.data.data && result.data.data.total_size) || 0
        this.internalTableDataListPageOffset = (result.data.data && result.data.data.offset) || 0
        this.internalTableDataListPageLimit = (result.data.data && result.data.data.limit) || 0
      }).finally(() => {
        this.internalTableDataListLoading = false
        this.showLoadData = afterLoadData ? false : this.openAction === 'dataManagementLoadData'
      })
    }
  }

  dateFormatter (row) {
    return dayjs(row.last_modified).format('YYYY-MM-DD HH:mm:ss')
  }

  handlePagerChange (page, pageSize) {
    this._getInternalTableDetails(page, pageSize)
  }

  handleShowLoadData () {
    this.showLoadData = true
  }

  handleCloseLoadData () {
    this.showLoadData = false
    if (this.openAction === 'dataManagementLoadData') this.handleClose()
    else this._getInternalTableDetails(this.internalTableDataListPageOffset, this.internalTableDataListPageLimit, true)
  }

  handleDataClose () {
    if (this.showLoadData) {
      this.handleCloseLoadData()
    } else {
      this.handleClose()
    }
  }

  handleSelectionChange (values) {
    this.selectedRows = values
  }

  handleRefreshData () {
    !this.internalTableDataListLoading && this.$confirm(this.$t('confirmRefreshDataPrompt', {}), this.$t('confirmRefreshTitle'), {
      confirmButtonText: this.$t('kylinLang.common.submit'),
      cancelButtonText: this.$t('kylinLang.common.cancel'),
      centerButton: true,
      type: 'warning'
    }).then(() => {
      this.internalTableDataListLoading = true
      this.loadSingleInternalTable({
        database: this.tableInfo.database,
        table: this.tableInfo.name,
        project: this.tableInfo.project,

        incremental: false,
        refresh: true,
        yarn_queue: 'default'
      }).then(() => {
        this.$message({ type: 'success', message: this.$t('refreshDataSuccessTips', { tableName: this.tableInfo.name }) })
      }).finally(() => {
        this.internalTableDataListLoading = false
      })
    })
  }

  handleDeletePartitions () {
    if (this.internalTableDataListLoading) {
      return false
    }
    if (this.selectedRows.length === 0) {
      this.$confirm(this.$t('confirmDeleteAllDataPrompt', {}), this.$t('confirmDeleteAllDataTitle'), {
        confirmButtonText: this.$t('kylinLang.common.submit'),
        cancelButtonText: this.$t('kylinLang.common.cancel'),
        centerButton: true,
        type: 'warning'
      }).then(async () => {
        this.internalTableDataListLoading = true
        this.deleteAllData({
          table: `${this.tableInfo.database}.${this.tableInfo.name}`,
          project: this.tableInfo.project
        }).finally(() => {
          this.internalTableDataListLoading = false
        }).then(
          () => this._getInternalTableDetails(this.internalTableDataListPageOffset, this.internalTableDataListPageLimit)
        )
      })
    } else {
      this.$confirm(this.$t('confirmDeletePartitionsPrompt', {}), this.$t('confirmDeletePartionsTitle'), {
        confirmButtonText: this.$t('kylinLang.common.submit'),
        cancelButtonText: this.$t('kylinLang.common.cancel'),
        centerButton: true,
        type: 'warning'
      }).then(async () => {
        this.internalTableDataListLoading = true
        this.deletePartitions({
          table: `${this.tableInfo.database}.${this.tableInfo.name}`,
          project: this.tableInfo.project,
          partitions: this.selectedRows.map(row => row.partition_value).join(',')
        }).finally(() => {
          this.internalTableDataListLoading = false
        }).then(
          () => this._getInternalTableDetails(this.internalTableDataListPageOffset, this.internalTableDataListPageLimit)
        )
      })
    }
  }
}
</script>
<style lang="less">
  .internal-table-data-management {
    position: absolute;

    .el-dialog {
      width: 960px;
      padding-bottom: 24px;
    }
    .el-dialog__body {
      overflow: visible !important;
    }
    .data-list-table {
      max-height: 430px;
      overflow: auto;
    }
    .el-dialog__wrapper {
      overflow: hidden;
    }
    .text-ellipsis {
      text-overflow: ellipsis;
      white-space: nowrap;
    }
}
</style>
