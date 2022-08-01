<template>
  <div class="model-aggregate ksd-mb-15" v-if="model" v-loading="isLoading">
    <div class="aggregate-view">
      <div class="btn-groups" v-if="showModelTypeSwitch">
        <el-tabs class="btn-group-tabs" v-model="switchModelType" type="button" @tab-click="changeModelTab">
          <el-tab-pane :label="$t('kylinLang.common.BATCH')" name="BATCH" />
          <el-tab-pane :label="$t('kylinLang.common.STREAMING')" name="STREAMING" />
        </el-tabs>
      </div>
      <div class="index-group">
        <el-card class="agg-detail-card agg-detail">
          <div class="detail-content" v-loading="indexLoading">
            <div class="clearfix">
              <el-popover
                ref="indexPopover"
                placement="right"
                width="500"
                trigger="hover">
                <div style="padding:10px">
                  <div class="ksd-mb-10">{{$t('indexSubTitle')}}</div>
                  <div class="ksd-center">
                    <img src="../../../../../assets/img/index.gif" width="400px" alt="">
                  </div>
                </div>
              </el-popover>
              <div class="date-range ksd-mb-16 ksd-fs-12 ksd-fleft">
                {{$t('dataRange')}}: {{getDataRange}}<span class="data-range-tips"><i v-if="!isRealTimeMode" v-popover:indexPopover class="el-icon-ksd-info ksd-fs-12 ksd-ml-8"></i></span>
              </div>
              <div v-if="isShowAggregateAction && isHaveComplementSegs && !isRealTimeMode" @click="complementedIndexes('allIndexes')" class="text-btn-like ksd-fleft ksd-ml-6">
                <el-tooltip :content="$t('viewIncomplete')" effect="dark" placement="top">
                  <i class="el-ksd-icon-view_range_22"></i>
                </el-tooltip>
              </div>
            </div>
            <div class="actions-header clearfix ksd-mb-10" v-if="!isShowAggregateAction">
              <el-checkbox v-model="indexesByQueryHistory" @change="changeAggList">{{$t('indexesByQueryHistoryTip')}}</el-checkbox>
              <div class="right fix">
                <el-input class="search-input" v-model.trim="filterArgs.key" size="small" :placeholder="$t('searchAggregateID')" prefix-icon="el-ksd-icon-search_22" v-global-key-event.enter.debounce="searchAggs" @clear="searchAggs()"></el-input>
              </div>
            </div>
            <div class="clearfix" v-if="isShowAggregateAction">
              <el-alert class="ksd-mb-8" :title="$t('realTimeModelActionTips')" type="tip" show-icon v-if="isRealTimeMode&&isShowRealTimeModelActionTips" @close="isShowRealTimeModelActionTips = false" />
              <el-dropdown style="margin-left:-14px !important;" class="ksd-ml-5 ksd-fleft" v-if="isShowAggregateAction && isShowIndexActions && !indexLoading">
                <el-button icon="el-ksd-icon-add_22" type="primary" text>{{$t('index')}}</el-button>
                <el-dropdown-menu slot="dropdown">
                  <el-dropdown-item :class="{'action-disabled': !indexUpdateEnabled && model.model_type === 'STREAMING'}" @click.native="handleAggregateGroup" v-if="isShowEditAgg">
                    <el-tooltip :content="$t('refuseAddIndexTip')" effect="dark" placement="top" :disabled="indexUpdateEnabled || model.model_type !== 'STREAMING'">
                      <span>{{$t('aggregateGroup')}}</span>
                    </el-tooltip>
                  </el-dropdown-item>
                  <el-dropdown-item :class="{'action-disabled': !indexUpdateEnabled && model.model_type === 'STREAMING'}" v-if="isShowTableIndexActions&&!isHideEdit" @click.native="confrimEditTableIndex()">
                     <el-tooltip :content="$t('refuseAddIndexTip')" effect="dark" placement="top" :disabled="indexUpdateEnabled || model.model_type !== 'STREAMING'">
                      <span>{{$t('tableIndex')}}</span>
                    </el-tooltip>
                  </el-dropdown-item>
                  <el-dropdown-item :class="{'action-disabled': Object.keys(indexStat).length && !indexStat.need_create_base_agg_index && !indexStat.need_create_base_table_index}" v-if="model.model_type === 'HYBRID' ? switchModelType !== 'STREAMING' : model.model_type !== 'STREAMING'">
                    <span :title="Object.keys(indexStat).length && !indexStat.need_create_base_agg_index && !indexStat.need_create_base_table_index ? $t('unCreateBaseIndexTip') : ''" @click="createBaseIndex">{{$t('baseIndex')}}</span>
                  </el-dropdown-item>
                </el-dropdown-menu>
              </el-dropdown>
              <!-- <el-tooltip :content="$t('disabledBuildIndexTips')" :disabled="checkedList.length==0 || (checkedList.length>0&&!isHaveLockedIndex)">
                <div class="ksd-fleft"> -->
                <el-button icon="el-ksd-icon-build_index_22" :disabled="!checkedList.length || isHaveLockedIndex" text type="primary" class="ksd-ml-2 ksd-fleft" v-if="datasourceActions.includes('buildIndex') && !isRealTimeMode" @click="complementedIndexes('batchIndexes')">{{$t('buildIndex')}}</el-button>
                <!-- </div>
              </el-tooltip> -->
              <template v-if="isRealTimeMode">
                <el-tooltip placement="top" :content="!indexUpdateEnabled ? $t('refuseRemoveIndexTip') : $t('disabledDelBaseIndexTips')" v-if="datasourceActions.includes('delAggIdx') && (isDisableDelBaseIndex || !indexUpdateEnabled)">
                  <div class="ksd-fleft">
                    <el-button v-if="datasourceActions.includes('delAggIdx') && (isDisableDelBaseIndex || !indexUpdateEnabled)" :disabled="isDisableDelBaseIndex || !indexUpdateEnabled" type="primary" icon="el-ksd-icon-table_delete_22" @click="removeIndexes" text>{{$t('kylinLang.common.delete')}}</el-button>
                  </div>
                </el-tooltip>
                <!-- <common-tip :content="$t('refuseRemoveIndexTip')" v-if="datasourceActions.includes('delAggIdx') && !indexUpdateEnabled&&checkedList.length>0">
                  <el-button v-if="datasourceActions.includes('delAggIdx') && !indexUpdateEnabled" :disabled="!indexUpdateEnabled" type="primary" icon="el-ksd-icon-table_delete_22" @click="removeIndexes" class="ksd-fleft" text>{{$t('kylinLang.common.delete')}}</el-button>
                </common-tip> -->
                <el-button v-if="datasourceActions.includes('delAggIdx') && !isDisableDelBaseIndex &&  indexUpdateEnabled" :disabled="!checkedList.length" type="primary" icon="el-ksd-icon-table_delete_22" class="ksd-fleft" @click="removeIndexes" text>{{$t('kylinLang.common.delete')}}</el-button>
              </template>
              <template v-else>
                <el-tooltip placement="top" :content="$t('disabledDelBaseIndexTips')" v-if="datasourceActions.includes('delAggIdx')&&isDisableDelBaseIndex">
                  <div class="ksd-fleft">
                    <el-dropdown
                      split-button
                      type="primary"
                      text
                      btn-icon="el-ksd-icon-table_delete_22"
                      class="split-button ksd-mb-10 ksd-ml-2 ksd-fleft"
                      :class="{'is-disabled': isDisableDelBaseIndex}"
                      placement="bottom-start"
                      :loading="removeLoading"
                      v-if="datasourceActions.includes('delAggIdx')&&isDisableDelBaseIndex">{{$t('kylinLang.common.delete')}}
                      <el-dropdown-menu slot="dropdown" class="model-actions-dropdown">
                        <el-dropdown-item
                          :disabled="isDisableDelBaseIndex">
                          {{$t('deletePart')}}
                        </el-dropdown-item>
                      </el-dropdown-menu>
                    </el-dropdown>
                  </div>
                </el-tooltip>
                <el-dropdown
                  split-button
                  type="primary"
                  text
                  btn-icon="el-ksd-icon-table_delete_22"
                  class="split-button ksd-mb-10 ksd-ml-2 ksd-fleft"
                  :class="{'is-disabled': !checkedList.length}"
                  placement="bottom-start"
                  :loading="removeLoading"
                  @click="removeIndexes"
                  v-if="datasourceActions.includes('delAggIdx')&&!isDisableDelBaseIndex">{{$t('kylinLang.common.delete')}}
                  <el-dropdown-menu slot="dropdown" class="model-actions-dropdown">
                    <el-dropdown-item
                      :disabled="!checkedList.length"
                      @click="complementedIndexes('deleteIndexes')">
                      {{$t('deletePart')}}
                    </el-dropdown-item>
                  </el-dropdown-menu>
                </el-dropdown>
              </template>
              <div class="right fix">
                <el-input class="search-input" v-model.trim="filterArgs.key" size="medium" :placeholder="$t('searchAggregateID')" prefix-icon="el-ksd-icon-search_22" v-global-key-event.enter.debounce="searchAggs" @clear="searchAggs()"></el-input>
              </div>
            </div>
            <div class="filter-tags-agg" v-show="filterTags.length">
              <div class="filter-tags-layout"><el-tag size="mini" closable v-for="(item, index) in filterTags" :key="index" @close="handleClose(item)">{{`${$t(item.source)}：${$t(item.label)}`}}</el-tag></div>
              <span class="clear-all-filters" @click="clearAllTags">{{$t('clearAll')}}</span>
            </div>
            <div class="index-table-list" :class="{'is-show-tips' :isRealTimeMode&&isShowRealTimeModelActionTips, 'is-show-tab-button': showModelTypeSwitch, 'is-show-tips--tab-button': isRealTimeMode&&isShowRealTimeModelActionTips&&showModelTypeSwitch}">
              <el-table
                ref="indexesTable"
                :data="indexDatas"
                class="indexes-table"
                :show-empty-img="false"
                :empty-text="emptyText"
                @sort-change="onSortChange"
                @selection-change="handleSelectionChange"
                :row-class-name="tableRowClassName"
              >
                <el-table-column type="selection" width="44" v-if="isShowAggregateAction"></el-table-column>
                <el-table-column prop="id" show-overflow-tooltip :label="$t('id')" width="100"></el-table-column>
                <el-table-column prop="data_size" sortable="custom" width="200px" :label="$t('storage')">
                  <template slot-scope="scope">
                    <span class="data-size-text">{{formatDataSize(scope.row.data_size)}}</span>
                    <el-progress v-if="'max_data_size' in indexStat" :percentage="indexStat.max_data_size && (scope.row.data_size / indexStat.max_data_size > 0.05) ? scope.row.data_size / indexStat.max_data_size * 100 : scope.row.data_size ? 0.5 : 0" class="data-size-progress"></el-progress>
                  </template>
                </el-table-column>
                <el-table-column
                  prop="usage"
                  sortable="custom"
                  width="200px"
                  :label="$t('queryCount')"
                >
                  <template slot-scope="scope">
                    <span class="usage-text">{{scope.row.usage}}</span>
                    <el-progress v-if="'max_usage' in indexStat" :percentage="indexStat.max_usage && (scope.row.usage / indexStat.max_usage > 0.05) ? scope.row.usage / indexStat.max_usage * 100 : scope.row.usage ? 0.5 : 0" class="usage-progress"></el-progress>
                  </template>
                </el-table-column>
                <el-table-column prop="source" show-overflow-tooltip :filters="realFilteArr.map(item => ({text: $t(item), value: item}))" :filtered-value="filterArgs.sources" :label="$t('source')" filter-icon="el-ksd-icon-filter_22" :show-multiple-footer="false" :filter-change="(v) => filterContent(v, 'sources')">
                  <template slot-scope="scope">
                    <span>{{$t(scope.row.source)}}</span>
                  </template>
                </el-table-column>
                <el-table-column align="left" :label="$t('indexContent')">
                  <template slot-scope="scope">
                    <el-popover
                      ref="index-content-popover"
                      placement="top"
                      trigger="hover"
                      popper-class="col-index-content-popover">
                      <div class="index-content" slot="reference">{{scope.row.col_order.map(it => it.key).slice(0, 20).join(', ')}}</div>
                      <template>
                        <p class="popover-header"><b>{{$t('indexesContent')}}</b><el-button v-if="scope.row.col_order.length > 20" type="primary" text class="view-more-btn ksd-fs-12" @click="showDetail(scope.row)">{{$t('viewIndexDetails')}}<i class="el-icon-ksd-more_02 ksd-fs-12"></i></el-button></p>
                        <p style="white-space: pre-wrap;">{{scope.row.col_order.map(it => it.key).slice(0, 20).join('\n')}}</p>
                        <el-button v-if="scope.row.col_order.length > 20" class="ksd-fs-12" type="primary" text @click="showDetail(scope.row)">{{$t('viewAll')}}</el-button>
                      </template>
                    </el-popover>
                    <span class="detail-icon el-ksd-icon-view_16" @click="showDetail(scope.row)"></span>
                  </template>
                </el-table-column>
                <el-table-column prop="status" show-overflow-tooltip :filters="statusArr.map(item => ({text: $t(item), value: item}))" :filtered-value="filterArgs.status" :label="$t('kylinLang.common.status')" filter-icon="el-ksd-icon-filter_22" :show-multiple-footer="false" :filter-change="(v) => filterContent(v, 'status')" width="100">
                  <template slot-scope="scope">
                    <el-tag size="small" :type="getStatusTagColor(scope.row.status)">{{$t(scope.row.status)}}</el-tag>
                  </template>
                </el-table-column>
                <el-table-column :label="$t('kylinLang.common.action')" fixed="right" width="83" v-if="isShowAggregateAction">
                  <template slot-scope="scope">
                    <common-tip :content="$t('buildIndex')" :disabled="scope.row.status === 'LOCKED'" v-if="isShowAggregateAction&&datasourceActions.includes('buildIndex')&&!isRealTimeMode">
                      <i class="el-ksd-icon-build_index_22 ksd-ml-5" :class="{'is-disabled': scope.row.status === 'LOCKED'}" @click="complementedIndexes('', scope.row.id)"></i>
                    </common-tip>
                    <common-tip :content="$t('editIndex')" v-if="isShowAggregateAction&&datasourceActions.includes('editAggGroup')">
                      <i class="el-icon-ksd-table_edit ksd-ml-5" v-if="scope.row.source === 'MANUAL_TABLE'" @click="confrimEditTableIndex(scope.row)"></i>
                    </common-tip>
                    <common-tip :content="$t('update')" v-if="scope.row.need_update">
                      <i class="action-icons el-ksd-icon-refresh_22 ksd-ml-5" @click="updateBaseIndexEvent(scope.row)"></i>
                    </common-tip>
                  </template>
                </el-table-column>
              </el-table>
            </div>
            <kylin-pager class="ksd-center ksd-mtb-10" ref="indexPager" :perPageSize="filterArgs.page_size" :refTag="pageRefTags.indexPager" :totalSize="totalSize" :curPage="filterArgs.page_offset+1" v-on:handleCurrentChange='pageCurrentChange'></kylin-pager>
          </div>
        </el-card>
      </div>
    </div>
    <index-details :indexDetailTitle="indexDetailTitle" :detailType="detailType" :cuboidData="cuboidData" @close="closeDetailDialog" v-if="indexDetailShow" />
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'
import locales from './locales'
import { handleSuccessAsync } from '../../../../../util'
import { handleError, kylinConfirm, transToServerGmtTime } from '../../../../../util/business'
import { pageRefTags, pageCount } from 'config'
import { BuildIndexStatus } from '../../../../../config/model'
import { formatGraphData } from './handler'
import NModel from '../../ModelEdit/model.js'
import IndexDetails from './indexDetails'

@Component({
  props: {
    model: {
      type: Object
    },
    projectName: {
      type: String
    },
    isShowAggregateAction: {
      type: Boolean,
      default: true
    },
    isShowEditAgg: {
      type: Boolean,
      default: true
    },
    isShowBulidIndex: {
      type: Boolean,
      default: true
    },
    isShowTableIndexActions: {
      type: Boolean,
      default: true
    },
    isHideEdit: {
      type: Boolean,
      default: false
    },
    layoutId: {
      type: [String, Number],
      default: ''
    }
  },
  computed: {
    ...mapGetters([
      'currentProjectData',
      'datasourceActions',
      'isOnlyQueryNode'
    ]),
    modelInstance () {
      this.model.project = this.currentProjectData.name
      return new NModel(this.model)
    }
  },
  methods: {
    ...mapActions('AggregateModal', {
      callAggregateModal: 'CALL_MODAL'
    }),
    ...mapActions('ConfirmSegment', {
      callConfirmSegmentModal: 'CALL_MODAL'
    }),
    ...mapActions('TableIndexEditModal', {
      showTableIndexEditModal: 'CALL_MODAL'
    }),
    ...mapActions({
      fetchIndexGraph: 'FETCH_INDEX_GRAPH',
      buildIndex: 'BUILD_INDEX',
      loadAllIndex: 'LOAD_ALL_INDEX',
      loadBaseIndex: 'LOAD_BASE_INDEX',
      updateBaseIndex: 'UPDATE_BASE_INDEX',
      deleteIndex: 'DELETE_INDEX',
      deleteIndexes: 'DELETE_INDEXES',
      autoFixSegmentHoles: 'AUTO_FIX_SEGMENT_HOLES',
      fetchIndexStat: 'FETCH_INDEX_STAT'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    })
  },
  components: {
    IndexDetails
  },
  locales
})
export default class ModelAggregate extends Vue {
  pageRefTags = pageRefTags
  cuboidCount = 0
  emptyCuboidCount = 0
  brokenCuboidCount = 0
  cuboids = []
  cuboidData = {}
  searchCuboidId = ''
  buildIndexLoading = false
  indexLoading = false
  isLoading = false
  indexDatas = []
  dataRange = null
  totalSize = 0
  indexUpdateEnabled = true
  filterArgs = {
    page_offset: 0,
    page_size: +localStorage.getItem(this.pageRefTags.indexPager) || pageCount,
    key: '',
    sort_by: '',
    reverse: '',
    sources: [],
    status: []
  }
  indexDetailShow = false
  tableIndexBaseList = []
  realFilteArr = ['CUSTOM_AGG_INDEX', 'CUSTOM_TABLE_INDEX', 'BASE_TABLE_INDEX', 'BASE_AGG_INDEX']
  statusArr = ['NO_BUILD', 'ONLINE', 'LOCKED', 'BUILDING']
  detailType = ''
  currentPage = 0
  currentCount = +localStorage.getItem(this.pageRefTags.IndexDetailPager) || pageCount
  totalTableIndexColumnSize = 0
  isFullLoaded = false
  indexDetailTitle = ''
  filterTags = []
  checkedList = []
  removeLoading = false
  indexRangeMap = {
    BATCH: ['HYBRID', 'BATCH'],
    STREAMING: ['HYBRID', 'STREAMING']
  }
  switchIndexValue = 'index'
  switchModelType = 'BATCH' // 默认批数据 - BATCH, 流数据 - STREAMING
  isHaveComplementSegs = false
  indexesByQueryHistory = true // 是否获取查询相关的索引
  isShowRealTimeModelActionTips = true
  indexStat = {}

  // 控制显示流数据，批数据选项
  get showModelTypeSwitch () {
    return this.model && this.model.model_type === 'HYBRID'
  }

  // 判断是否时流数据模式
  get isRealTimeMode () {
    return (this.showModelTypeSwitch && this.switchModelType === 'STREAMING') || (this.model.model_type === 'STREAMING')
  }

  get modelId () {
    if (this.model.model_type !== 'HYBRID') {
      return this.model.uuid
    } else {
      return this.switchModelType === 'BATCH' ? this.model.batch_id : this.model.uuid
    }
  }

  // 标识是融合数据模型下的批数据模式
  get isHybridBatch () {
    return this.model.model_type === 'HYBRID' && this.switchModelType === 'BATCH'
  }

  async changeModelTab (name) {
    // 切换tab 时需要重刷列表
    this.filterArgs.page_offset = 0
    this.filterArgs.page_size = +localStorage.getItem(this.pageRefTags.indexPager) || pageCount
    this.filterArgs.key = ''
    this.indexLoading = true
    await this.freshIndexGraph()
    await this.loadAggIndices()
    this.getIndexInfo()
    this.indexLoading = false
  }

  formatDataSize (dataSize) {
    const [size = +size, ext] = this.$root.$options.filters.dataSize(dataSize).split(' ')
    const intType = ['B', 'KB']
    if (intType.includes(ext)) {
      return `${Math.round(size)} ${ext}`
    } else {
      const num = +size
      return `${num.toFixed(1)} ${ext}`
    }
  }

  get isHaveLockedIndex () {
    if (this.checkedList.length) {
      const indexStatus = this.checkedList.map((c) => {
        return c.status
      })
      return indexStatus.indexOf('LOCKED') !== -1
    }
  }

  async complementedIndexes (indexType, id) {
    let title = this.$t('buildIndex')
    let subTitle = this.$t('subTitle')
    let submitText = this.$t('buildIndex')
    let isRemoveIndex = false
    let indexes = []
    if (indexType === 'allIndexes') {
      title = this.$t('viewIncompleteTitle')
      subTitle = this.$t('incompleteSubTitle')
    } else if (indexType === 'batchIndexes') {
      title = this.$t('buildIndex')
      subTitle = this.$t('batchBuildSubTitle', {number: this.checkedList.length})
      indexes = this.checkedList.map((i) => {
        return i.id
      })
    } else if (indexType === 'deleteIndexes') {
      title = this.$t('deleteIndex')
      subTitle = this.$t('deleteTips', {number: this.checkedList.length})
      submitText = this.$t('kylinLang.common.delete')
      isRemoveIndex = true
      indexes = this.checkedList.map((i) => {
        return i.id
      })
    } else if (indexType === 'baseIndex') {
      title = this.$t('buildIndex')
      subTitle = this.$t('batchBuildSubTitle', {number: id.split(',').length})
      indexes = id.split(',')
    } else {
      indexes.push(id)
    }
    // 这里的 model id 需要根据条件判断传递
    await this.callConfirmSegmentModal({
      title: title,
      subTitle: subTitle,
      indexes: indexes,
      submitText: submitText,
      isRemoveIndex: isRemoveIndex,
      isHybridBatch: this.isHybridBatch,
      model: this.model
    })
    this.refreshIndexGraphAfterSubmitSetting()
  }

  // 索引状态 tag 类型
  getStatusTagColor (type) {
    if (type === 'ONLINE') {
      return 'success'
    } else if (type === 'NO_BUILD') {
      return 'warning'
    } else if (type === 'BUILDING') {
      return ''
    } else {
      return 'info'
    }
  }

  changeAggList () {
    if (this.indexesByQueryHistory) {
      this.loadAggIndices(this.layoutId)
    } else {
      this.loadAggIndices()
    }
  }
  doLayoutIndexTable () {
    this.$nextTick(() => {
      this.$refs.indexesTable && this.$refs.indexesTable.doLayout()
    }, 100)
  }

  handleSelectionChange (val) {
    this.checkedList = val
  }

  get isDisableDelBaseIndex () {
    let isHaveBaseTableIndex = false
    for (let i = 0; i < this.checkedList.length; i++) {
      if (this.checkedList[i].source === 'BASE_TABLE_INDEX' && this.checkedList[i].status !== 'LOCKED') {
        isHaveBaseTableIndex = true
        break
      }
    }
    return isHaveBaseTableIndex && this.model.second_storage_enabled
  }

  async removeIndexes () {
    if (!this.checkedList.length) return
    const layout_ids = this.checkedList.map((index) => {
      return index.id
    }).join(',')
    try {
      await kylinConfirm(this.$t('delIndexesTips', {indexNum: this.checkedList.length}), {confirmButtonText: this.$t('kylinLang.common.delete')}, this.$t('delIndex'))
      this.removeLoading = true
      await this.deleteIndexes({project: this.projectName, model: this.modelId, layout_ids: layout_ids})
      this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
      this.removeLoading = false
      this.refreshIndexGraphAfterSubmitSetting()
      this.getIndexInfo()
      this.$emit('refreshModel')
    } catch (e) {
      handleError(e)
      this.removeLoading = false
    }
  }

  tableRowClassName ({row, rowIndex}) {
    if (row.status === 'EMPTY' || row.status === 'BUILDING') {
      return 'empty-index'
    }
    return ''
  }

  get emptyText () {
    return this.filterArgs.key || this.filterArgs.sources.length || this.filterArgs.status.length ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }

  get isShowIndexActions () {
    const { isShowEditAgg, isShowTableIndexActions, isHideEdit } = this

    return isShowEditAgg || (isShowTableIndexActions && !isHideEdit)
  }

  handleBuildIndexTip (data) {
    let tipMsg = ''
    if (data.type === BuildIndexStatus.NORM_BUILD) {
      tipMsg = this.$t('kylinLang.model.buildIndexSuccess')
      this.$message({message: tipMsg, type: 'success'})
      return
    }
    if (data.type === BuildIndexStatus.NO_LAYOUT) {
      tipMsg = this.$t('kylinLang.model.buildIndexFail2', {indexType: this.$t('kylinLang.model.index')})
    } else if (data.type === BuildIndexStatus.NO_SEGMENT) {
      tipMsg += this.$t('kylinLang.model.buildIndexFail1', {modelName: this.model.name})
    }
    this.$confirm(tipMsg, this.$t('kylinLang.common.notice'), {showCancelButton: false, type: 'warning', dangerouslyUseHTMLString: true})
  }
  confrimEditTableIndex (indexDesc) {
    if (indexDesc && !this.indexUpdateEnabled && ['HYBRID', 'STREAMING'].includes(indexDesc.index_range)) return
    if (!this.indexUpdateEnabled && this.model.model_type === 'STREAMING') return
    this.editTableIndex(indexDesc)
  }
  editTableIndex (indexDesc) {
    const { projectName, model } = this
    this.showTableIndexEditModal({
      isHybridBatch: this.isHybridBatch,
      modelInstance: this.modelInstance,
      tableIndexDesc: indexDesc || {name: 'TableIndex_1'},
      indexUpdateEnabled: this.indexUpdateEnabled,
      indexType: this.showModelTypeSwitch ? this.switchModelType : '',
      projectName,
      model
    }).then((res) => {
      if (res.isSubmit) {
        this.refreshIndexGraphAfterSubmitSetting()
        this.$emit('refreshModel')
      }
    })
  }
  renderColumn (h) {
    let items = []
    for (let i = 0; i < this.realFilteArr.length; i++) {
      items.push(<el-checkbox label={this.realFilteArr[i]} key={this.realFilteArr[i]}>{this.$t(this.realFilteArr[i])}</el-checkbox>)
    }
    return (<span>
      <span>{this.$t('source')}</span>
      <el-popover
        ref="sourceFilterPopover"
        placement="bottom-start"
        popperClass="source-filter">
        <el-checkbox-group class="filter-groups" value={this.filterArgs.sources} onInput={val => (this.filterArgs.sources = val)} onChange={this.filterSouces}>
          {items}
        </el-checkbox-group>
        <i class={this.filterArgs.sources.length ? 'el-icon-ksd-filter isFilter' : 'el-icon-ksd-filter'} slot="reference"></i>
      </el-popover>
    </span>)
  }
  renderColumn2 (h) {
    let items = []
    for (let i = 0; i < this.statusArr.length; i++) {
      items.push(<el-checkbox label={this.statusArr[i]} key={this.statusArr[i]}>{this.$t(this.statusArr[i])}</el-checkbox>)
    }
    return (<span>
      <span>{this.$t('kylinLang.common.status')}</span>
      <el-popover
        ref="sourceFilterPopover"
        placement="bottom-start"
        popperClass="source-filter">
        <el-checkbox-group class="filter-groups" value={this.filterArgs.status} onInput={val => (this.filterArgs.status = val)} onChange={this.filterSouces}>
          {items}
        </el-checkbox-group>
        <i class={this.filterArgs.status.length ? 'el-icon-ksd-filter isFilter' : 'el-icon-ksd-filter'} slot="reference"></i>
      </el-popover>
    </span>)
  }
  async buildAggIndex () {
    if (this.model.segment_holes.length) {
      const segmentHoles = this.model.segment_holes
      try {
        const tableData = []
        let selectSegmentHoles = []
        segmentHoles.forEach((seg) => {
          const obj = {}
          obj['start'] = transToServerGmtTime(seg.date_range_start)
          obj['end'] = transToServerGmtTime(seg.date_range_end)
          obj['date_range_start'] = seg.date_range_start
          obj['date_range_end'] = seg.date_range_end
          tableData.push(obj)
        })
        await this.callGlobalDetailDialog({
          msg: this.$t('segmentHoletips', {modelName: this.model.name}),
          title: this.$t('fixSegmentTitle'),
          detailTableData: tableData,
          detailColumns: [
            {column: 'start', label: this.$t('kylinLang.common.startTime')},
            {column: 'end', label: this.$t('kylinLang.common.endTime')}
          ],
          isShowSelection: true,
          dialogType: 'warning',
          showDetailBtn: false,
          needResolveCancel: true,
          cancelText: this.$t('ignore'),
          submitText: this.$t('fixAndBuild'),
          customCallback: async (segments) => {
            selectSegmentHoles = segments.map((seg) => {
              return {start: seg.date_range_start, end: seg.date_range_end}
            })
            await this.autoFixSegmentHoles({project: this.projectName, model_id: this.modelId, segment_holes: selectSegmentHoles})
            this.confirmBuild()
          }
        })
        this.confirmBuild()
      } catch (e) {
        e !== 'cancel' && handleError(e)
      }
    } else {
      await kylinConfirm(this.$t('bulidTips', {modelName: this.model.name}), {cancelButtonText: this.$t('kylinLang.common.cancel'), confirmButtonText: this.$t('buildIndex'), type: 'warning'})
      this.confirmBuild()
    }
  }
  async confirmBuild () {
    try {
      this.buildIndexLoading = true
      let res = await this.buildIndex({
        project: this.projectName,
        model_id: this.modelId
      })
      let data = await handleSuccessAsync(res)
      this.handleBuildIndexTip(data)
    } catch (e) {
      handleError(e)
    } finally {
      this.buildIndexLoading = false
    }
  }
  currentChange (size, count) {
    this.currentPage = size
    this.currentCount = count
  }
  showDetail (row) {
    this.cuboidData = row
    let idStr = (row.id !== undefined) && (row.id !== null) && (row.id !== '') ? ' [' + row.id + ']' : ''
    this.detailType = row.source.indexOf('AGG') >= 0 ? 'aggDetail' : 'tabelIndexDetail'
    this.indexDetailTitle = row.source.indexOf('AGG') >= 0 ? this.$t('aggDetailTitle') + idStr : this.$t('tabelDetailTitle') + idStr
    this.indexDetailShow = true
  }
  async removeIndex (row) {
    try {
      await kylinConfirm(this.$t('delIndexTip'), null, this.$t('delIndex'))
      await this.deleteIndex({project: this.projectName, model: this.modelId, id: row.id})
      this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
      this.refreshIndexGraphAfterSubmitSetting()
      this.getIndexInfo()
      // this.$emit('loadModels')
    } catch (e) {
      handleError(e)
    }
  }
  onSortChange ({ column, prop, order }) {
    this.filterArgs.sort_by = prop
    this.filterArgs.reverse = !(order === 'ascending')
    this.pageCurrentChange(0, this.filterArgs.page_size)
  }
  async pageCurrentChange (size, count) {
    this.filterArgs.page_offset = size
    this.filterArgs.page_size = count
    this.indexLoading = true
    await this.loadAggIndices()
    this.indexLoading = false
  }
  async filterSouces () {
    this.filterArgs.page_offset = 0
    this.indexLoading = true
    await this.loadAggIndices()
    this.indexLoading = false
  }
  async searchAggs () {
    this.filterArgs.page_offset = 0
    this.indexLoading = true
    await this.loadAggIndices()
    this.indexLoading = false
  }
  async handleClickNode (id) {
    this.switchIndexValue = 'index'
    this.filterArgs.key = id
    this.indexLoading = true
    await this.loadAggIndices()
    this.indexLoading = false
  }
  async freshIndexGraph () {
    try {
      const res = await this.fetchIndexGraph({
        project: this.projectName,
        model: this.modelId
      })
      const data = await handleSuccessAsync(res)
      this.dataRange = [data.start_time, data.end_time]
      this.isHaveComplementSegs = data.segment_to_complement_count > 0
      this.isFullLoaded = data.is_full_loaded
      this.cuboids = formatGraphData(data)
      this.cuboidCount = data.total_indexes
      this.emptyCuboidCount = data.empty_indexes
    } catch (e) {
      handleError(e)
    }
  }
  get getDataRange () {
    if (this.dataRange) {
      if (this.isFullLoaded && this.dataRange[0] === 0 && this.dataRange[1] === 9223372036854776000) {
        return this.$t('kylinLang.dataSource.full')
      } else if (!this.dataRange[0] && !this.dataRange[1]) {
        return this.$t('noDataRange')
      } else {
        return transToServerGmtTime(this.dataRange[0]) + this.$t('to') + transToServerGmtTime(this.dataRange[1])
      }
    } else {
      return ''
    }
    // return this.dataRange ? transToServerGmtTime(this.dataRange[0]) + this.$t('to') + transToServerGmtTime(this.dataRange[1]) : ''
  }
  get noDataNum () {
    let nodeNum = 0
    this.cuboids.forEach((n) => {
      nodeNum = nodeNum + n.children.length
    })
    return nodeNum
  }
  async loadAggIndices (ids) {
    try {
      // this.indexLoading = true
      const params = {}
      if (this.showModelTypeSwitch) {
        params.range = this.indexRangeMap[this.switchModelType]
      }
      if (this.indexesByQueryHistory && !this.layoutId && !this.isShowAggregateAction) {
        this.indexDatas = []
        this.totalSize = 0
        return
      }
      const res = await this.loadAllIndex(Object.assign({
        project: this.projectName,
        model: this.modelId,
        ids: ids || this.indexesByQueryHistory ? this.layoutId : '',
        ...params
      }, this.filterArgs))
      const data = await handleSuccessAsync(res)
      this.indexDatas = data.value
      this.totalSize = data.total_size
      this.indexUpdateEnabled = data.index_update_enabled
      // this.indexLoading = false
    } catch (e) {
      handleError(e)
      // this.indexLoading = false
    }
  }
  created () {
    !this.layoutId && (this.indexesByQueryHistory = false)
  }
  async mounted () {
    this.isLoading = true
    await this.freshIndexGraph()
    await this.loadAggIndices()
    this.getIndexInfo()
    this.isLoading = false
  }
  async refreshIndexGraphAfterSubmitSetting () {
    this.isLoading = true
    await this.freshIndexGraph()
    await this.loadAggIndices()
    this.isLoading = false
  }
  async handleAggregateGroup () {
    if (!this.indexUpdateEnabled && this.model.model_type === 'STREAMING') return
    const { projectName, model } = this
    const { isSubmit } = await this.callAggregateModal({ editType: 'new', model, projectName, indexUpdateEnabled: this.indexUpdateEnabled, indexType: this.showModelTypeSwitch ? this.switchModelType : '' })
    isSubmit && await this.refreshIndexGraphAfterSubmitSetting()
    isSubmit && await this.$emit('refreshModel')
  }
  // 查询状态过滤回调函数
  filterContent (val, type) {
    const maps = {
      sources: 'source',
      status: 'kylinLang.common.status'
    }

    this.filterTags = this.filterTags.filter((item, index) => item.key !== type || item.key === type && val.includes(item.label))
    const list = this.filterTags.filter(it => it.key === type).map(it => it.label)
    val.length && val.forEach(item => {
      if (!list.includes(item)) {
        this.filterTags.push({label: item, source: maps[type], key: type})
      }
    })
    this.filterArgs[type] = val
    this.filterSouces()
  }
  // 删除单个筛选条件
  handleClose (tag) {
    const index = this.filterArgs[tag.key].indexOf(tag.label)
    index > -1 && this.filterArgs[tag.key].splice(index, 1)
    this.filterTags = this.filterTags.filter(item => item.key !== tag.key || item.key === tag.key && tag.label !== item.label)
    this.filterSouces()
  }
  // 清除所有筛选条件
  clearAllTags () {
    this.filterArgs.sources.splice(0, this.filterArgs.sources.length)
    this.filterArgs.status.splice(0, this.filterArgs.status.length)
    this.filterArgs.page_offset = 0
    this.filterTags = []
    this.filterSouces()
  }

  // 关闭索引详情弹窗
  closeDetailDialog () {
    this.indexDetailShow = false
  }

  // 手动更新基础索引
  updateBaseIndexEvent (row) {
    this.$msgbox({
      title: this.$t('updateBaseIndexTitle'),
      message: this.$t('updateBaseIndexTips'),
      showCancelButton: true,
      centerButton: true,
      confirmButtonText: this.$t('update')
    }).then(async () => {
      await this.updateBaseIndex({
        model_id: this.modelId,
        project: this.projectName,
        load_data: false,
        source_types: [row.source]
      })
      this.loadAggIndices()
    }).catch(e => {
      handleError(e)
    })
  }

  // 创建 base index
  createBaseIndex () {
    if (Object.keys(this.indexStat).length && !this.indexStat.need_create_base_agg_index && !this.indexStat.need_create_base_table_index) return
    this.loadBaseIndex({
      model_id: this.modelId,
      project: this.projectName,
      load_data: false,
      source_type: ['BASE_TABLE_INDEX', 'BASE_AGG_INDEX']
    }).then(async (res) => {
      const result = await handleSuccessAsync(res)
      const layoutIds = []
      // const baseAggIndexNum = result.agg_index ? result.agg_index.dimension_count + result.agg_index.measure_count : 0
      // const baseTableIndexNum = result.table_index ? result.table_index.dimension_count + result.table_index.measure_count : 0
      result.base_agg_index && layoutIds.push(result.base_agg_index.layout_id)
      result.base_table_index && layoutIds.push(result.base_table_index.layout_id)
      this.$message({
        type: 'success',
        duration: 10000,
        showClose: true,
        message: <span>{
          this.$t('buildBaseIndexTip', {baseIndexNum: result.base_agg_index && result.base_table_index ? 2 : !result.base_agg_index && !result.base_table_index ? 0 : 1})
        }{ this.model.model_type !== 'STREAMING'
          ? <a href="javascript:void;" onClick={() => this.complementedIndexes('baseIndex', layoutIds.join(','))}>{this.$t('buildIndex')}</a>
          : ''
        }</span>
      })
      this.loadAggIndices()
      this.getIndexInfo()
      this.$emit('refreshModel')
    }).catch((e) => {
      handleError(e)
    })
  }

  // 获取索引特征信息(是否有聚合或明细基础索引。。。)
  getIndexInfo () {
    this.fetchIndexStat({model_id: this.modelId, project: this.projectName}).then(async res => {
      const result = await handleSuccessAsync(res)
      this.indexStat = result
    }).catch((e) => {
      handleError(e)
    })
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';

.model-aggregate {
  height: 100%;
  .aggregate-view {
    background-color: @fff;
    height: 100%;
    .aggregate-tree-map {
      position: relative;
      display: inline-block;
      vertical-align: top;
    }
    .index-group {
      width: 100%;
      height: 100%;
      // padding: 15px 20px;
      box-sizing: border-box;
      position: relative;
      display: inline-block;
      vertical-align: top;
    }
    .btn-groups {
      display: inline-block;
      right: 20px;
      .el-tabs {
        display: inline-block;
        .el-tabs__header {
          margin: 0 0 8px;
        }
      }
      .btn-group-tabs {
        .el-tabs__nav-scroll {
          background-color: @ke-background-color-hover;
          overflow: initial;
        }
      }
    }
  }
  .el-button-group .el-button--primary:last-child {
    border-left-color: @base-color;
  }
  .index-table-list {
    max-height: 90%;
    overflow: auto;
    &.is-show-tips {
      max-height: calc(~'90% - 45px');
    }
    &.is-show-tab-button {
      max-height: calc(~'90% - 36px');
    }
    &.is-show-tips--tab-button {
      max-height: calc(~'90% - 45px - 36px');
    }
  }
  .indexes-table {
    .el-table__empty-img-text {
      padding: 16px;
    }
    .empty-index {
      background: @warning-color-2;
    }
    .el-popover.source-filter {
      min-width: 130px;
      box-sizing: border-box;
    }
    .el-icon-ksd-filter {
      position: relative;
      top: 2px;
      left: 5px;
      &.isFilter,
      &:hover {
        color: @base-color;
      }
    }
    .detail-icon {
      position: absolute;
      top: 50%;
      transform: translate(0, -44%);
      right: 15px;
      font-size: 18px;
      cursor: pointer;
    }
    .action-icons {
      font-size: 18px;
      vertical-align: text-top;
      cursor: pointer;
    }
    .data-size-text, .usage-text {
      width: 40%;
      display: inline-block;
      text-align: right;
    }
    .usage-progress, .data-size-progress {
      width: 70px;
      display: inline-block;
      margin-right: 8px;
      .el-progress-bar {
        padding-right: 0;
        margin-right: 0;
        border-radius: 0;
      }
      .el-progress__text {
        display: none;
      }
      .el-progress-bar__outer {
        border-radius: none;
        background-color: transparent;
        border-radius: 0;
        .el-progress-bar__inner {
          background-color: #2492F7;
          border-radius: 0;
        }
      }
    }
    .is-disabled {
      pointer-events: none;
      color: @text-disabled-color;
    }
  }
  .tabel-scroll {
    overflow: hidden;
    height: 400px;
  }
  .aggregate-actions {
    margin-bottom: 10px;
  }
  .agg-amount-block {
    position: absolute;
    right: 0;
    .el-input {
      width: 120px;
    }
  }
  .el-icon-ksd-desc {
    &:hover {
      color: @base-color;
    }
  }
  .agg-counter {
    * {
      vertical-align: middle;
    }
    .divide {
      border-left: 1px solid @line-border-color;
      margin: 0 5px;
    }
    img {
      width: 18px;
      height: 18px;
    }
    div:not(:last-child) {
      margin-bottom: 5px;
    }
  }
  .cuboid-info {
    margin-bottom: 10px;
    .is-right {
      border-right: none;
    }
    .slot {
      opacity: 0;
    }
  }
  .align-left {
    text-align: left;
  }
  .drag-bar {
    right: 0;
    top: 50%;
    transform: translate(50%, -50%);
    cursor: ew-resize;
  }
  .agg-detail-card {
    // height: 496px;
    height: 100%;
    border: none;
    background: none;
    .el-card__header {
      background: none;
      border-bottom: none;
      height: 24px;
      font-size: 14px;
      padding: 0px;
      margin-bottom: 5px;
    }
    &.agg_index {
      border-right: 1px solid @line-border-color;
      padding-right: 20px;
      padding: 15px 20px;
      .el-card__body {
        overflow: hidden;
      }
    }
    .el-card__body {
      overflow: auto;
      overflow: visible\0;
      height: calc(~'100% - 100px');
      width: 100%;
      position: relative;
      box-sizing: border-box;
      padding: 0px !important;
      .detail-content {
        background-color: transparent;
        padding: 0;
        height: 100%;
        .date-range {
          color: @text-normal-color;
        }
        .data-range-tips {
          .el-icon-ksd-info {
            color: @text-disabled-color;
          }
        }
        .el-icon-question {
          color: @base-color;
        }
        .text-btn-like {
          cursor: pointer;
          font-size: 14px;
          line-height: 16px;
        }
        .el-row {
          margin-bottom: 10px;
          .dim-item {
            margin-bottom: 5px;
          }
        }
        .split-button {
          &.is-disabled {
            .el-button-group > .el-button {
              background-color: @fff;
              opacity: 0.3;
              color: @text-disabled-color;
              cursor: not-allowed;
              background-image: none;
            }
          }
        }
        .actions-header {
          .el-checkbox {
            margin-top: 4px;
            .el-checkbox__inner {
              vertical-align: middle;
            }
          }
        }
      }
    }
    .left {
      display: block;
      float: left;
      position: relative;
      &.fix {
        width: 130px;
      }
    }
    .right {
      display: block;
      float: right;
      white-space: nowrap;
      font-size: 14px;
      &.fix {
        width: calc(~'100% - 130px');
        max-width: 250px;
        .el-input.search-input {
          width: 100%;
        }
      }
      .el-input {
        width: 100px;
      }
    }
    .label {
      text-align: right;
    }
  }
  .filter-tags-agg {
    margin-bottom: 10px;
    padding: 0 5px 4px 3px;
    box-sizing: border-box;
    position: relative;
    font-size: 12px;
    background: @background-disabled-color;
    .filter-tags-layout {
      max-width: calc(~'100% - 80px');
      display: inline-block;
    }
    .el-tag {
      margin-left: 5px;
      margin-top: 4px;
    }
    .clear-all-filters {
      position: absolute;
      top: 5px;
      right: 8px;
      font-size: 12px;
      color: @base-color;
      cursor: pointer;
    }
  }
  .cell.highlight {
    .el-icon-ksd-filter {
      color: @base-color;
    }
  }
  .el-icon-ksd-filter {
    position: relative;
    font-size: 17px;
    top: 2px;
    left: 5px;
    &:hover,
    &.filter-open {
      color: @base-color;
    }
  }
  .index-content {
    cursor: pointer;
    width: calc(~'100% - 30px');
    height: 23px;
    overflow: hidden;
    text-overflow: ellipsis;
    display: -webkit-box;
    -webkit-line-clamp: 1;
    -webkit-box-orient: vertical;
    white-space: nowrap\0;
  }
}
.col-index-content-popover {
  .popover-header {
    position: relative;
    margin-bottom: 8px;
  }
  .view-more-btn {
    position: absolute;
    right: 0;
    top: 0;
    padding: 0;
    i {
      font-size: 12px;
    }
  }
}
.indexes-result-box {
  .no-data_placeholder {
    color: @text-placeholder-color;
    font-size: 12px;
  }
  .indexes-content-details {
    display: flex;
    justify-content: space-between;
  }
}
.el-dropdown-menu__item {
  &.action-disabled {
    color: @text-disabled-color;
    cursor: not-allowed;
  }
}

</style>
