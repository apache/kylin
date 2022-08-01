<template>
   <el-dialog
    :title="title"
    limited-area
    width="1250px"
    :visible="isShow"
    v-if="isShow"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @close="closeModal()">
      <div class="ksd-mb-10" v-html="subTitle"></div>
      <el-table border :data="segments" size="small" @selection-change="handleSelectSegments" :empty-text="emptyText" @sort-change="handleSortChange">
        <el-table-column type="selection" :selectable="selectable" width="44">
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.startTime')" show-overflow-tooltip prop="start_time" sortable="custom">
          <template slot-scope="scope">{{segmentTime(scope.row, scope.row.segRange.date_range_start) | toServerGMTDate}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.endTime')" show-overflow-tooltip prop="end_time" sortable="custom">
          <template slot-scope="scope">{{segmentTime(scope.row,scope.row.segRange.date_range_end) | toServerGMTDate}}</template>
        </el-table-column>
        <el-table-column
          header-align="right"
          align="right"
          sortable="custom"
          prop="indexAmount"
          width="185"
          v-if="$store.state.project.multi_partition_enabled && model.multi_partition_desc"
          show-overflow-tooltip
          :label="$t('subPratitionAmount')"
          :info-tooltip="$t('subPratitionAmountTip')"
          info-icon="el-ksd-icon-more_info_22"
        >
          <template slot-scope="scope">
            <span>{{scope.row.multi_partition_count}} / {{scope.row.multi_partition_count_total}}</span>
          </template>
        </el-table-column>
        <el-table-column
          width="165"
          header-align="right"
          align="right"
          sortable="custom"
          prop="indexAmount"
          show-overflow-tooltip
          :label="$t('kylinLang.common.indexAmount')"
          :info-tooltip="$t('kylinLang.common.indexAmountTip')"
          info-icon="el-ksd-icon-more_info_22"
        >
          <template slot-scope="scope">
              <span v-if="['LOADING', 'REFRESHING', 'MERGING'].indexOf(scope.row.status_to_display) !== -1">-/{{scope.row.index_count_total}}</span>
              <span v-else>{{scope.row.index_count}}/{{scope.row.index_count_total}}</span>
          </template>
        </el-table-column>
        <el-table-column width="114" prop="status" :label="$t('kylinLang.common.status')">
          <template slot-scope="scope">
            <el-tag size="mini" :type="getTagType(scope.row)">{{scope.row.status_to_display}}</el-tag>
          </template>
        </el-table-column>
        <el-table-column width="140" prop="last_modified_time" show-overflow-tooltip :label="$t('modifyTime')">
          <template slot-scope="scope">
            <span>{{scope.row.last_modified_time | toServerGMTDate}}</span>
          </template>
        </el-table-column>
        <el-table-column width="140" :label="$t('sourceRecords')" align="right" prop="source_count" sortable="custom">
        </el-table-column>
        <el-table-column width="130" :label="$t('storageSize')" align="right" prop="storage" sortable="custom">
          <template slot-scope="scope">{{scope.row.bytes_size | dataSize}}</template>
        </el-table-column>
      </el-table>
      <kylin-pager
        class="ksd-center ksd-mtb-10"
        :refTag="pageRefTags.confirmSegmentPager"
        :background="false"
        :curPage="pagination.page_offset+1"
        :perPageSize="pagination.pageSize"
        :totalSize="totalSegmentCount"
        @handleCurrentChange="handleCurrentChange">
      </kylin-pager>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <div class="ksd-fleft" v-if="!refrashWarningSegment&&!isRemoveIndex&&!isFullLoadModel">
          <el-checkbox v-model="parallel_build_by_segment" :disabled="selectedSegments.length < 2">
            <span>{{$t('parallelBuild')}}</span>
            <common-tip placement="top" :content="$t('parallelBuildTip')">
              <span class='el-icon-ksd-what'></span>
            </common-tip>
          </el-checkbox>
        </div>
        <el-button @click="closeModal" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" @click="submit" :disabled="!selectedSegments.length" :loading="btnLoading" size="medium">{{submitText}}</el-button>
      </div>
    </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
import vuex from 'store'
import { pageCount, pageRefTags } from 'config'
import { handleError, handleSuccessAsync, transToGmtTime, transToServerGmtTime } from 'util'
import locales from './locales'
import store, { types } from './store'
vuex.registerModule(['modals', 'ConfirmSegment'], store)

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    ...mapState('ConfirmSegment', {
      isShow: state => state.isShow,
      title: state => state.title,
      subTitle: state => state.subTitle,
      refrashWarningSegment: state => state.refrashWarningSegment, // 有值说明是刷新segment list
      indexes: state => state.indexes,
      isRemoveIndex: state => state.isRemoveIndex,
      submitText: state => state.submitText,
      isHybridBatch: state => state.isHybridBatch,
      model: state => state.model,
      callback: state => state.callback
    })
  },
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      fetchSegments: 'FETCH_SEGMENTS',
      refreshSegments: 'REFRESH_SEGMENTS',
      complementAllIndex: 'COMPLEMENT_ALL_INDEX',
      complementBatchIndex: 'COMPLEMENT_BATCH_INDEX',
      deleteBatchIndex: 'DELETE_BATCH_INDEX'
    }),
    ...mapMutations('ConfirmSegment', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    })
  },
  locales
})

export default class ConfirmSegmentModal extends Vue {
  pageRefTags = pageRefTags
  segments = []
  filter = {
    mpValues: '',
    startDate: '',
    endDate: '',
    reverse: true,
    sortBy: 'last_modify'
  }
  pagination = {
    page_offset: 0,
    pageSize: +localStorage.getItem(this.pageRefTags.confirmSegmentPager) || pageCount
  }
  selectedSegments = []
  selectedSegmentIds = []
  totalSegmentCount = 0
  btnLoading = false
  parallel_build_by_segment = false
  get emptyText () {
    return this.$t('noSegmentList')
  }
  async loadSegments () {
    try {
      const { sortBy, reverse } = this.filter
      const projectName = this.currentSelectedProject
      const modelName = this.isHybridBatch ? this.model.batch_id : this.model.uuid
      // const startTime = startDate && transToUTCMs(startDate)
      // const endTime = endDate && transToUTCMs(endDate)
      const data = { projectName, modelName, sortBy, reverse, ...this.pagination }
      // 有值说明是刷新segment list
      if (this.refrashWarningSegment) {
        data.status = 'WARNING'
      } else {
        if (this.indexes.length && !this.isRemoveIndex) {
          data.without_indexes = this.indexes.join(',')
        } else if (this.indexes.length && this.isRemoveIndex) {
          data.with_indexes = this.indexes.join(',')
        } else {
          data.all_to_complement = true
        }
      }
      const res = await this.fetchSegments(data)
      const { total_size, value } = await handleSuccessAsync(res)
      // const formatedSegments = formatSegments(this, value)
      this.segments = value
      this.totalSegmentCount = total_size
    } catch (e) {
      handleError(e)
    }
  }
  segmentTime (row, data) {
    const isFullLoad = row.segRange.date_range_start === 0 && row.segRange.date_range_end === 9223372036854776000
    return isFullLoad ? this.$t('kylinLang.common.fullLoad') : data
  }
  get isFullLoadModel () {
    return !(this.model.partition_desc && this.model.partition_desc.partition_date_column)
  }

  // renderSubPartitionAmountHeader (h, { column, $index }) {
  //   return (<span class="ky-hover-icon" onClick={e => (e.stopPropagation())}>
  //     <span>{this.$t('subPratitionAmount')}</span>&nbsp;
  //     <common-tip placement="top" content={this.$t('subPratitionAmountTip')}>
  //      <span class='el-ksd-icon-more_info_16'></span>
  //     </common-tip>
  //   </span>)
  // }
  // 更改不同状态对应不同type
  getTagType (row) {
    if (row.status_to_display === 'ONLINE') {
      return 'success'
    } else if (row.status === 'WARNING') {
      return 'warning'
    } else if (['LOCKED'].includes(row.status_to_display)) {
      return 'info'
    } else {
      return ''
    }
  }
  handleSortChange ({ column, prop, order }) {
    if (order === 'ascending') {
      this.filter.reverse = false
    } else {
      this.filter.reverse = true
    }
    this.filter.sortBy = prop === 'storage' ? 'bytes_size' : prop
    this.handleCurrentChange(0, this.pagination.pageSize)
  }
  handleCurrentChange (pager, count) {
    this.pagination.page_offset = pager
    this.pagination.pageSize = count
    this.loadSegments()
  }
  handleSelectSegments (selectedSegments) {
    this.selectedSegments = selectedSegments
    this.selectedSegmentIds = selectedSegments.map(segment => segment.id)
    if (selectedSegments.length < 2) {
      this.parallel_build_by_segment = false
    }
  }
  selectable (row) {
    return (['ONLINE', 'WARNING']).includes(row.status_to_display) && (this.$store.state.project.multi_partition_enabled && this.model.multi_partition_desc ? row.multi_partition_count : true)
  }
  closeModal (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.callback && this.callback(isSubmit)
      this.selectedSegments = []
      this.selectedSegmentIds = []
      this.btnLoading = false
      this.parallel_build_by_segment = false
    }, 200)
  }
  // renderIndexAmountHeader (h, { column, $index }) {
  //   return (<span class="ky-hover-icon" onClick={e => (e.stopPropagation())}>
  //     <span>{this.$t('kylinLang.common.indexAmount')}</span>&nbsp;
  //     <common-tip placement="top" content={this.$t('kylinLang.common.indexAmountTip')}>
  //      <span class='el-ksd-icon-more_info_16'></span>
  //     </common-tip>
  //   </span>)
  // }
  @Watch('isShow')
  changeShowType (val) {
    if (val) {
      this.loadSegments()
    }
  }
  async submit () {
    try {
      this.btnLoading = true
      // 有值说明是刷新segment list
      if (this.refrashWarningSegment) {
        const segmentIds = this.selectedSegmentIds
        const modelId = this.isHybridBatch ? this.model.batch_id : this.model.uuid
        const projectName = this.currentSelectedProject
        const isSubmit = await this.refreshSegments({ projectName, modelId, segmentIds })
        if (isSubmit) {
          this.$emit('reloadModelAndSegment', this.model.alias)
          this.showSuccessMsg()
        }
      } else {
        if (this.indexes.length > 0 && !this.isRemoveIndex) {
          const res = await this.complementBatchIndex({
            modelId: this.isHybridBatch ? this.model.batch_id : this.model.uuid,
            data: {
              project: this.currentSelectedProject,
              segment_ids: this.selectedSegmentIds,
              index_ids: this.indexes,
              parallel_build_by_segment: this.parallel_build_by_segment
            }
          })
          const data = await handleSuccessAsync(res)
          if (data.failed_segments.length) {
            this.showFailedSegmentList(data.failed_segments)
          } else {
            this.showSuccessMsg()
          }
        } else if (this.indexes.length > 0 && this.isRemoveIndex) {
          await this.deleteBatchIndex({
            modelId: this.isHybridBatch ? this.model.batch_id : this.model.uuid,
            data: {
              project: this.currentSelectedProject,
              segment_ids: this.selectedSegmentIds,
              index_ids: this.indexes
            }
          })
          this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
        } else {
          const res = await this.complementAllIndex({
            modelId: this.isHybridBatch ? this.model.batch_id : this.model.uuid,
            data: {
              project: this.currentSelectedProject,
              segment_ids: this.selectedSegmentIds,
              parallel_build_by_segment: this.parallel_build_by_segment
            }
          })
          const data = await handleSuccessAsync(res)
          if (data.failed_segments.length) {
            this.showFailedSegmentList(data.failed_segments)
          } else {
            this.showSuccessMsg()
          }
        }
      }
      this.closeModal()
    } catch (e) {
      handleError(e)
      this.btnLoading = false
    }
  }
  showSuccessMsg () {
    this.$message({
      type: 'success',
      customClass: 'build-full-load-success',
      duration: 10000,
      showClose: true,
      message: (
        <div>
          <span>{this.$t('kylinLang.common.buildSuccess')}</span>
          <a href="javascript:void(0)" onClick={() => this.jumpToJobs()}>{this.$t('kylinLang.common.toJoblist')}</a>
        </div>
      )
    })
  }
  showFailedSegmentList (failedSegments) {
    const tableData = []
    failedSegments.forEach((seg) => {
      const obj = {}
      if (seg.segRange.date_range_start === 0 && seg.segRange.date_range_end === 9223372036854776000) {
        obj['start'] = this.$t('kylinLang.common.fullLoad')
        obj['end'] = this.$t('kylinLang.common.fullLoad')
      } else {
        obj['start'] = transToServerGmtTime(seg.segRange.date_range_start)
        obj['end'] = transToServerGmtTime(seg.segRange.date_range_end)
      }
      tableData.push(obj)
    })
    this.callGlobalDetailDialog({
      msg: this.$t('failedSegmentsTips', {sucNum: this.selectedSegmentIds.length - failedSegments.length, failNum: failedSegments.length}),
      tableTitle: this.$t('details'),
      title: this.$t('failedTitle'),
      detailTableData: tableData,
      detailColumns: [
        {column: 'start', label: this.$t('kylinLang.common.startTime')},
        {column: 'end', label: this.$t('kylinLang.common.endTime')}
      ],
      isShowSelection: false,
      dialogType: 'warning',
      showDetailBtn: false,
      isHideSubmit: true,
      cancelText: this.$t('gotIt')
    })
  }

  // 跳转至job页面
  jumpToJobs () {
    this.$router.push('/monitor/job')
  }
}
</script>
