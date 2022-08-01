<template>
  <div class="streaming-list">
    <div class="segment-actions clearfix" v-if="isShowPageTitle">
      <el-popover
        ref="segmentPopover"
        placement="right"
        width="500"
        trigger="hover">
        <div style="padding:10px">
          <div class="ksd-mb-10">{{$t('segmentSubTitle')}}</div>
          <div class="ksd-center">
            <img src="../../../../../../assets/img/image-seg.png" width="400px" alt="">
          </div>
        </div>
      </el-popover>
      <div class="ksd-title-label-small ksd-mb-10">
        <div class="segment-header-title"><span class="ksd-title-page">{{$t('segmentList')}}</span><i v-popover:segmentPopover class="el-icon-ksd-info ksd-ml-10"></i></div>
      </div>
    </div>
    <div class="segment-actions ksd-mb-10 clearfix">
      <div class="ksd-fleft ky-no-br-space btn-groups" v-if="isShowSegmentActions">
        <!-- <el-button-group v-if="$store.state.project.emptySegmentEnable">
          <el-button size="small" icon="el-icon-ksd-add_2" class="ksd-mr-10" type="default" :disabled="!model.partition_desc && segments.length>0" @click="addSegment">Segment</el-button>
        </el-button-group> -->
        <!-- <el-button-group class="ksd-mr-10">
          <el-button size="small" icon="el-icon-ksd-table_refresh" type="primary" :disabled="!selectedSegments.length || hasEventAuthority('refresh')" @click="handleRefreshSegment">{{$t('kylinLang.common.refresh')}}</el-button>
          <el-button size="small" icon="el-icon-ksd-merge" type="default" :disabled="selectedSegments.length < 2 || hasEventAuthority('merge')" @click="handleMergeSegment">{{$t('merge')}}</el-button>
        </el-button-group>
        <el-button-group>
          <el-button size="small" icon="el-icon-ksd-repair" type="default" v-if="model.segment_holes.length" @click="handleFixSegment">{{$t('fix')}}<el-tooltip class="item tip-item" :content="$t('fixTips')" placement="bottom"><i class="el-icon-ksd-what"></i></el-tooltip></el-button>
        </el-button-group> -->
        <el-button icon="el-ksd-icon-table_delete_22" type="primary" text :disabled="!selectedSegments.length || hasEventAuthority('delete')" @click="handleDeleteSegment">{{$t('kylinLang.common.delete')}}</el-button>
      </div>
      <div class="ksd-fright">
        <div class="segment-action ky-no-br-space" v-if="!filterSegment">
          <span class="ksd-mr-5 ksd-fs-14">{{$t('segmentPeriod')}}</span>
          <el-date-picker
            class="date-picker ksd-mr-5"
            type="datetime"
            size="small"
            v-model="filter.startDate"
            :is-auto-complete="true"
            :picker-options="{ disabledDate: getStartDateLimit }"
            :placeholder="$t('chooseStartDate')">
          </el-date-picker>
          <el-date-picker
            class="date-picker"
            type="datetime"
            size="small"
            v-model="filter.endDate"
            :is-auto-complete="true"
            :picker-options="{ disabledDate: getEndDateLimit }"
            :placeholder="$t('chooseEndDate')">
          </el-date-picker>
        </div>
      </div>
    </div>
    <div :class="[model.model_type === 'STREAMING' ? 'segment-streaming-table' : 'segment-views', 'ksd-mb-15']">
      <el-table nested size="medium" :empty-text="emptyText" :data="segments" @selection-change="handleSelectSegments" @sort-change="handleSortChange">
        <el-table-column type="selection" width="44">
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.startTime')" show-overflow-tooltip prop="start_time" sortable="custom">
          <template slot-scope="scope">{{segmentTime(scope.row, scope.row.startTime) | toServerGMTDate}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.endTime')" show-overflow-tooltip prop="end_time" sortable="custom">
          <template slot-scope="scope">{{segmentTime(scope.row,scope.row.endTime) | toServerGMTDate}}</template>
        </el-table-column>
        <!-- <el-table-column
          header-align="right"
          align="right"
          sortable="custom"
          prop="indexAmount"
          width="185"
          show-overflow-tooltip
          v-if="$store.state.project.multi_partition_enabled && model.multi_partition_desc"
          :render-header="renderSubPartitionAmountHeader">
          <template slot-scope="scope">
            <el-tooltip :content="$t('disabledSubPartitionEnter', {status: scope.row.status_to_display})" :disabled="scope.row.status_to_display !== 'LOCKED'" effect="dark" placement="top">
              <span :class="['ky-a-like', {'is-disabled': scope.row.status_to_display === 'LOCKED' || !model.multi_partition_desc}]" @click="showSubParSegments(scope.row)">{{scope.row.multi_partition_count}} / {{scope.row.multi_partition_count_total}}</span>
            </el-tooltip>
          </template>
        </el-table-column> -->
        <el-table-column
          header-align="right"
          align="right"
          sortable="custom"
          prop="indexAmount"
          width="145"
          show-overflow-tooltip
          :label="$t('kylinLang.common.indexAmount')"
          info-icon="el-ksd-icon-more_info_22"
          :info-tooltip="$t('kylinLang.common.indexAmountTip')">
          <template slot-scope="scope">
              <span v-if="['LOADING', 'REFRESHING', 'MERGING'].indexOf(scope.row.status_to_display) !== -1">--/{{scope.row.index_count_total}}</span>
              <span v-else>{{scope.row.index_count}}/{{scope.row.index_count_total}}</span>
          </template>
        </el-table-column>
        <el-table-column prop="status_to_display" :label="$t('kylinLang.common.status')" width="114">
          <template slot-scope="scope">
            <el-tooltip :content="$t(scope.row.status_to_display)" effect="dark" placement="top">
              <el-tag size="mini" :type="getTagType(scope.row, 'segment')">{{scope.row.status_to_display}}</el-tag>
            </el-tooltip>
          </template>
        </el-table-column>
        <el-table-column prop="last_modified_time" show-overflow-tooltip :label="$t('modifyTime')">
          <template slot-scope="scope">
            <span>{{scope.row.last_modified_time | toServerGMTDate}}</span>
          </template>
        </el-table-column>
        <el-table-column :label="$t('sourceRecords')" width="140" align="right" prop="source_count" sortable="custom">
        </el-table-column>
        <el-table-column :label="$t('storageSize')" width="140" align="right" prop="storage" sortable="custom">
          <template slot-scope="scope">{{scope.row.bytes_size | dataSize}}</template>
        </el-table-column>
        <el-table-column align="left" class-name="ky-hover-icon" fixed="right" :label="$t('kylinLang.common.action')" width="83">
          <template slot-scope="scope">
            <common-tip :content="$t('showDetail')">
              <i class="el-ksd-icon-view_16 ksd-icon-center-text-18" @click="handleShowDetail(scope.row)"></i>
            </common-tip>
          </template>
        </el-table-column>
      </el-table>
      <kylin-pager
        class="ksd-center ksd-mtb-10"
        :background="false"
        :refTag="pageRefTags.streamSegmentPager"
        :curPage="pagination.page_offset+1"
        :totalSize="totalSegmentCount"
        :perPageSize="pagination.pageSize"
        @handleCurrentChange="handleCurrentChange">
      </kylin-pager>
    </div>
    <el-dialog :title="$t('segmentDetail')" append-to-body limited-area :close-on-press-escape="false" :close-on-click-modal="false" :visible.sync="isShowDetail" width="720px">
      <div class="ksd-list segment-detail" v-if="detailSegment">
        <p class="list">
          <span class="label">{{$t('segmentID')}}</span>
          <span class="text">{{detailSegment.id}}</span>
        </p>
        <p class="list">
          <span class="label">{{$t('segmentName')}}</span>
          <span class="text">{{detailSegment.name}}</span>
        </p>
        <p class="list">
          <span class="label">{{$t('segmentPath')}}</span>
          <span class="text segment-path">{{detailSegment.segmentPath}}</span>
        </p>
        <p class="list">
          <span class="label">{{$t('fileNumber')}}</span>
          <span class="text">{{detailSegment.fileNumber}}</span>
        </p>
        <p class="list">
          <span class="label">{{$t('storageSize1')}}</span>
          <span class="text">{{detailSegment.bytes_size | dataSize}}</span>
        </p>
        <p class="list">
          <span class="label">{{$t('startTime')}}</span>
          <span class="text">{{segmentTime(detailSegment, detailSegment.startTime) | toServerGMTDate}}</span>
        </p>
        <p class="list">
          <span class="label">{{$t('endTime')}}</span>
          <span class="text">{{segmentTime(detailSegment, detailSegment.endTime) | toServerGMTDate}}</span>
        </p>
      </div>
      <div slot="footer" class="dialog-footer">
        <el-button type="primary" @click="isShowDetail = false">{{$t('kylinLang.common.close')}}</el-button>
      </div>
    </el-dialog>
    <el-dialog
      :title="$t('refreshSegmentsTitle')"
      append-to-body
      limited-area
      class="refresh-comfirm"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      :visible.sync="isShowRefreshConfirm"
      width="480px">
      <el-alert type="tip" show-icon class="ksd-ptb-0" :show-background="false" :closable="false">
        <span v-if="detailTableData.length">{{$t('confirmRefreshSegments2')}}</span>
        <span v-else>{{$t('confirmRefreshSegments')}}</span>
      </el-alert>
      <div class="ksd-mt-10" v-if="detailTableData.length">
        <el-radio v-model="refreshType" label="refreshOrigin">{{$t('buildCurrentIndexes')}}</el-radio>
        <el-radio v-model="refreshType" label="refreshAll">{{$t('buildAllIndexes')}}</el-radio>
      </div>
      <el-alert v-if="detailTableData.length && refreshType === 'refreshAll'" class="ksd-mt-10 ksd-ptb-0 build-all-tips" type="info" show-icon :show-background="false" :closable="false">
        <span>{{$t('buildAllIndexesTips')}}</span>
      </el-alert>
      <el-table class="ksd-mt-10"
        border
        nested
        size="small"
        max-height="420"
        v-if="detailTableData.length"
        :data="detailTableData">
        <el-table-column
          prop="start"
          :label="$t('kylinLang.common.startTime')"
          show-overflow-tooltip>
          <template slot-scope="scope">{{segmentTime(scope.row, scope.row.startTime) | toServerGMTDate}}</template>
        </el-table-column>
        <el-table-column
          prop="end"
          :label="$t('kylinLang.common.endTime')"
          show-overflow-tooltip>
          <template slot-scope="scope">{{segmentTime(scope.row, scope.row.endTime) | toServerGMTDate}}</template>
        </el-table-column>
        <el-table-column
          prop="currentIndexes"
          align="right"
          width="130"
          :label="$t('currentIndexes')">
          <template slot-scope="scope">
            <span>{{scope.row.index_count}}/{{scope.row.index_count_total}}</span>
          </template>
        </el-table-column>
      </el-table>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button plain @click="handleClose">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" :loading="refreshLoading" @click="handleSubmit()">{{$t('kylinLang.common.refresh')}}</el-button>
    </div>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions, mapGetters } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'
import { handleSuccessAsync, handleError, transToUTCMs, transToServerGmtTime } from 'util'
import locales from '../locales'
import { pageCount, pageRefTags } from 'config'
import { formatStreamSegments } from '../handler'
@Component({
  props: {
    model: {
      type: Object
    },
    isShowSegmentActions: {
      type: Boolean,
      default: true
    },
    isShowPageTitle: {
      type: Boolean,
      default: false
    }
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      fetchSegments: 'FETCH_SEGMENTS',
      refreshSegments: 'REFRESH_SEGMENTS',
      deleteSegments: 'DELETE_SEGMENTS',
      mergeSegments: 'MERGE_SEGMENTS',
      mergeSegmentCheck: 'MERGE_SEGMENT_CHECK',
      checkSegments: 'CHECK_SEGMENTS'
    }),
    ...mapActions('ModelBuildModal', {
      callModelBuildDialog: 'CALL_MODAL'
    }),
    ...mapActions('SourceTableModal', {
      callSourceTableModal: 'CALL_MODAL'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    })
  },
  locales
})
export default class StreamingSegment extends Vue {
  pageRefTags = pageRefTags
  segments = []
  isShowDetail = false
  detailSegment = null
  totalSegmentCount = 0
  filter = {
    mpValues: '',
    startDate: '',
    endDate: '',
    reverse: true,
    sortBy: 'last_modify'
  }
  pagination = {
    page_offset: 0,
    pageSize: +localStorage.getItem(this.pageRefTags.streamSegmentPager) || pageCount
  }
  selectedSegmentIds = []
  isSegmentLoading = false
  isLoading = false
  isShowRefreshConfirm = false
  detailTableData = []
  refreshLoading = false
  refreshType = 'refreshOrigin'

  get filterSegment () {
    return this.segments.filter(item => ['Full Load', '全量加载'].includes(item.startTime) && ['Full Load', '全量加载'].includes(item.endTime)).length
  }
  get selectedSegments () {
    return this.selectedSegmentIds.map(
      segmentId => this.segments.find(segment => segment.id === segmentId)
    )
  }
  segmentTime (row, data) {
    const isFullLoad = row.segRange.date_range_start === 0 && row.segRange.date_range_end === 9223372036854776000
    return isFullLoad ? this.$t('fullLoad') : data
  }
  get emptyText () {
    return this.filter.startDate || this.filter.endDate ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  get modelId () {
    return this.model.uuid
  }
  getTagType (row, type) {
    let status = type === 'segment' ? row.status_to_display : row.status
    if (status === 'ONLINE') {
      return 'success'
    } else if (status === 'WARNING') {
      return 'warning'
    } else if (['LOCKED'].includes(status)) {
      return 'info'
    } else {
      return ''
    }
  }
  getStartDateLimit (time) {
    return this.filter.endDate ? time.getTime() > this.filter.endDate.getTime() : false
  }
  getEndDateLimit (time) {
    return this.filter.startDate ? time.getTime() < this.filter.startDate.getTime() : false
  }
  // 状态控制按钮的使用
  hasEventAuthority (type) {
    let typeList = (type) => {
      return this.selectedSegments.length && typeof this.selectedSegments[0] !== 'undefined' ? this.selectedSegments.filter(it => !type.includes(it.status_to_display)).length > 0 : false
    }
    if (type === 'refresh') {
      return typeList(['ONLINE', 'WARNING'])
    } else if (type === 'merge') {
      return typeList(['ONLINE', 'WARNING'])
    } else if (type === 'delete') {
      return typeList(['ONLINE', 'LOADING', 'REFRESHING', 'MERGING', 'WARNING'])
    }
  }
  handleFixSegment () {
    this.$emit('auto-fix')
  }
  handleSelectSegments (selectedSegments) {
    this.selectedSegmentIds = selectedSegments.map(segment => segment.id)
  }
  handleShowDetail (segment) {
    this.detailSegment = segment
    this.isShowDetail = true
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
  @Watch('filter.startDate')
  @Watch('filter.endDate')
  onDateRangeChange (newVal, oldVal) {
    this.loadSegments()
  }
  async mounted () {
    await this.loadSegments()
    this.$on('refresh', () => {
      this.loadSegments()
    })
  }
  async loadSegments () {
    this.isLoading = true
    try {
      const { startDate, endDate, sortBy, reverse } = this.filter
      const projectName = this.currentSelectedProject
      const modelName = this.modelId
      const startTime = startDate && transToUTCMs(startDate)
      const endTime = endDate && transToUTCMs(endDate)
      this.isSegmentLoading = true
      const res = await this.fetchSegments({ projectName, modelName, startTime, endTime, sortBy, reverse, ...this.pagination })
      const { total_size, value } = await handleSuccessAsync(res)
      const formatedSegments = formatStreamSegments(this, value)
      this.segments = formatedSegments
      this.totalSegmentCount = total_size
      this.isSegmentLoading = false
      this.isLoading = false
    } catch (e) {
      handleError(e)
      this.isLoading = false
    }
  }
  handleClose () {
    this.isShowRefreshConfirm = false
    this.refreshType = 'refreshOrigin'
  }
  async handleSubmit () {
    try {
      const projectName = this.currentSelectedProject
      const modelId = this.modelId
      const segmentIds = this.selectedSegmentIds
      const refresh_all_indexes = this.refreshType === 'refreshAll'
      this.refreshLoading = true
      const isSubmit = await this.refreshSegments({ projectName, modelId, segmentIds, refresh_all_indexes })
      if (isSubmit) {
        await this.loadSegments()
        this.$emit('loadModels')
        this.$message({
          dangerouslyUseHTMLString: true,
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
        this.refreshLoading = false
      }
      this.isShowRefreshConfirm = false
      this.refreshLoading = false
      this.refreshType = 'refreshOrigin'
    } catch (e) {
      handleError(e)
      this.isShowRefreshConfirm = false
      this.refreshLoading = false
      this.refreshType = 'refreshOrigin'
      this.loadSegments()
    }
  }
  handleRefreshSegment () {
    if (this.selectedSegmentIds.length) {
      this.detailTableData = this.selectedSegments.filter((seg) => {
        return seg.index_count < seg.index_count_total
      })
      this.isShowRefreshConfirm = true
    } else {
      this.$message(this.$t('pleaseSelectSegments'))
    }
  }
  async handleMergeSegment () {
    try {
      const segmentIds = this.selectedSegmentIds
      if (!segmentIds.length) {
        this.$message(this.$t('pleaseSelectSegments'))
      } else {
        const projectName = this.currentSelectedProject
        const modelId = this.modelId
        // check merge segment
        const res = await this.mergeSegmentCheck({ project: projectName, modelId, ids: segmentIds, type: 'MERGE' })
        const data = await handleSuccessAsync(res)
        this.mergedSegments = [data]
        this.isShowMergeConfirm = true
      }
    } catch (e) {
      handleError(e)
    }
  }
  async handleSubmitMerge () {
    this.mergeLoading = true
    const projectName = this.currentSelectedProject
    const modelId = this.modelId
    const segmentIds = this.selectedSegmentIds
    try {
      // 合并segment
      const isSubmit = await this.mergeSegments({ projectName, modelId, segmentIds })
      if (isSubmit) {
        await this.loadSegments()
        this.mergeLoading = false
        this.$emit('loadModels')
        this.$message({
          dangerouslyUseHTMLString: true,
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
    } catch (e) {
      handleError(e)
      this.mergeLoading = false
      this.loadSegments()
    }
  }
  async handleDeleteSegment () {
    try {
      const segmentIds = this.selectedSegmentIds
      if (!segmentIds.length) {
        this.$message(this.$t('pleaseSelectSegments'))
      } else {
        const projectName = this.currentSelectedProject
        const modelId = this.modelId
        const segmentIdStr = this.selectedSegmentIds.join(',')
        let tableData = []
        let msg = this.$t('confirmDeleteSegments', {modelName: this.model.name})
        this.selectedSegments.forEach((seg) => {
          const obj = {}
          obj['start'] = transToServerGmtTime(this.segmentTime(seg, seg.startTime))
          obj['end'] = transToServerGmtTime(this.segmentTime(seg, seg.endTime))
          tableData.push(obj)
        })
        const res = await this.checkSegments({ projectName, modelId, ids: this.selectedSegmentIds })
        const data = await handleSuccessAsync(res)
        if (data.segment_holes.length) {
          msg = this.$t('segmentWarning', {modelName: this.model.name})
        }
        await this.callGlobalDetailDialog({
          msg: msg,
          title: this.$t('deleteSegmentTip'),
          detailTableData: tableData,
          detailColumns: [
            {column: 'start', label: this.$t('kylinLang.common.startTime')},
            {column: 'end', label: this.$t('kylinLang.common.endTime')}
          ],
          dialogType: 'warning',
          showDetailBtn: false,
          submitText: this.$t('kylinLang.common.delete')
        })
        await this.deleteSegments({ projectName, modelId, segmentIds: segmentIdStr })
        this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
        await this.loadSegments()
        this.$emit('refreshModel')
      }
    } catch (e) {
      e !== 'cancel' && handleError(e)
      this.loadSegments()
    }
  }
}
</script>
<style lang="less">
.streaming-list {
  height: 100%;
  .segment-streaming-table {
    height: 87%;
    overflow: auto;
  }
  .segment-actions {
    .btn-groups {
      >.el-button--primary.is-text {
        margin-left: -14px;
      }
    }
  }
}
</style>
