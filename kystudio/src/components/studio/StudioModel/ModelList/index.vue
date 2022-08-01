<template>
  <div class="mode-list" :class="{'full-cell': showFull}" id="modelListPage">
    <div class="ksd-title-page ksd-mt-32">{{$t('kylinLang.model.modelList')}}</div>
    <div class="model-list-contain ksd-mt-16">
      <div class="clearfix">
        <div class="ksd-fright">
          <el-input :placeholder="$t('filterModelOrOwner')" style="width:250px" size="medium" :prefix-icon="searchLoading? 'el-ksd-icon-loading_22':'el-ksd-icon-search_22'" :value="filterArgs.model_alias_or_owner" @input="handleFilterInput" v-global-key-event.enter.debounce="searchModels" @clear="searchModels()" class="show-search-btn" >
          </el-input>
          <el-button
            text
            class="filter-button"
            type="primary"
            @click="handleToggleFilters">
            <span>{{$t('filterButton')}}</span>
            <i :class="['el-ksd-icon-arrow_up_22', isShowFilters && 'reverse']" />
          </el-button>
        </div>
        <div class="ky-no-br-space model-list-header clearfix">
          <el-dropdown
            split-button
            class="ksd-fleft"
            type="primary"
            size="medium"
            id="addModel"
            placement="bottom-start"
            btn-icon="el-ksd-icon-add_22"
            v-if="datasourceActions.includes('modelActions')"
            @click="showAddModelDialog">{{$t('kylinLang.common.model')}}
            <el-dropdown-menu slot="dropdown" class="model-actions-dropdown">
              <el-dropdown-item
                v-if="metadataActions.includes('executeModelMetadata')"
                @click="handleImportModels">
                {{$t('importModels')}}
              </el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
          <common-tip :content="$t('noModelsExport')" v-if="metadataActions.includes('executeModelMetadata')" placement="top" :disabled="!!modelArray.length">
            <el-button icon="el-ksd-icon-export_22" size="medium" class="ksd-ml-8" :disabled="!modelArray.length" @click="handleExportMetadatas">
              <span>{{$t('exportMetadatas')}}</span>
            </el-button>
          </common-tip>
        </div>
      </div>
      <div class="table-filters clearfix ksd-mt-24" v-show="isShowFilters">
        <DropdownFilter
          type="checkbox"
          trigger="click"
          :value="filterArgs.status"
          hideArrow
          @input="v => filterContent(v, 'status')"
          :options="[
            { renderLabel: renderStatusLabel, value: 'ONLINE' },
            { renderLabel: renderStatusLabel, value: 'OFFLINE' },
            { renderLabel: renderStatusLabel, value: 'BROKEN' },
            { renderLabel: renderStatusLabel, value: 'WARNING' },
          ]">
          <el-button text type="primary" iconr="el-ksd-icon-arrow_down_22">{{$t('status_c')}}{{selectedStatus}}</el-button>
        </DropdownFilter>
        <DropdownFilter
          class="ksd-ml-8"
          type="datetimerange"
          trigger="click"
          :value="filterArgs.last_modify"
          hideArrow
          :shortcuts="['lastDay', 'lastWeek', 'lastMonth']"
          @input="v => filterContent(v, 'last_modify')">
          <el-button text type="primary" iconr="el-ksd-icon-arrow_down_22">{{$t('lastModifyTime_c')}}{{selectedRange}}</el-button>
        </DropdownFilter>
        <DropdownFilter
          class="ksd-ml-8"
          type="checkbox"
          trigger="click"
          hideArrow
          :value="filterArgs.model_attributes"
          :options="modelAttributesOptions"
          @input="v => filterContent(v, 'model_attributes')">
          <el-button text type="primary" iconr="el-ksd-icon-arrow_down_22">{{$t('modelType_c')}}{{selectedModelAttributes}}</el-button>
        </DropdownFilter>
        <div class="actions">
          <el-button
            text
            type="primary"
            icon="el-ksd-icon-resure_22"
            class="reset-filters-btn"
            :disabled="isResetFilterDisabled"
            @click="handleResetFilters">{{$t('reset')}}</el-button>
        </div>
      </div>
      <el-table class="model_list_table"
        v-scroll-shadow
        :data="modelArray"
        :empty-text="emptyText"
        tooltip-effect="dark"
        v-loading="loadingModels"
        @row-click="modelRowClickEvent"
        :row-class-name="setRowClass"
        @sort-change="onSortChange"
        :cell-class-name="renderColumnClass"
        :expand-row-keys="expandedRows"
        :row-key="renderRowKey"
        @expand-change="expandRow"
        ref="modelListTable"
        style="width: 100%">
        <el-table-column
          min-width="270px"
          prop="alias"
          :label="modelTableTitle">
          <template slot-scope="scope">
            <div class="alias">
              <el-popover
                popper-class="status-tooltip"
                placement="top-start"
                trigger="hover">
                <template slot="reference">
                  <span :class="['filter-status', scope.row.status]"></span>
                </template>
                <span v-html="$t('modelStatus_c')" />
                <span>{{scope.row.status}}</span>
                <div v-if="scope.row.status === 'WARNING' && scope.row.empty_indexes_count">{{$t('emptyIndexTips')}}</div>
                <div v-if="scope.row.status === 'WARNING' && (scope.row.segment_holes && scope.row.segment_holes.length && scope.row.model_type === 'BATCH') || (scope.row.batch_segment_holes && scope.row.batch_segment_holes.length && scope.row.model_type === 'HYBRID')">
                  <span>{{scope.row.model_type === 'HYBRID' ? $t('modelSegmentHoleTips1') : $t('modelSegmentHoleTips')}}</span><span
                    style="color:#0988DE;cursor: pointer;"
                    @click="autoFix(scope.row.alias, scope.row.model_type === 'HYBRID' ? scope.row.batch_id : scope.row.uuid, scope.row.model_type === 'HYBRID' ? scope.row.batch_segment_holes : scope.row.segment_holes)">{{$t('seeDetail')}}</span>
                </div>
                <div v-if="scope.row.status === 'WARNING' && (scope.row.segment_holes && scope.row.segment_holes.length && scope.row.model_type !== 'BATCH')">
                  <span>{{$t('modelSegmentHoleTips2')}}</span>
                </div>
                <div v-if="scope.row.status === 'WARNING' && scope.row.inconsistent_segment_count">
                  <span>{{$t('modelMetadataChangedTips')}}</span><span
                    style="color:#0988DE;cursor: pointer;"
                    @click="openComplementSegment(scope.row, true)">{{$t('seeDetail')}}</span>
                </div>
                <div v-if="scope.row.status === 'OFFLINE' && scope.row.forbidden_online">
                  <span>{{$t('SCD2ModalOfflineTip')}}</span>
                </div>
                <div v-if="scope.row.status === 'OFFLINE' && !scope.row.has_segments">
                  <span>{{$t('noSegmentOnlineTip')}}</span>
                </div>
                <div v-if="scope.row.status === 'OFFLINE' && !$store.state.project.multi_partition_enabled && scope.row.multi_partition_desc">
                  <span>{{$t('multilParTip')}}</span>
                </div>
              </el-popover>
              <el-popover
                ref="titlePopover"
                placement="top-start"
                width="250"
                trigger="hover"
                popper-class="title-popover-layout"
              >
                <div class="title-popover">
                  <p class="title ksd-mb-20">{{scope.row.alias}}</p>
                  <div :class="['label', {'en': $lang === 'en'}]">
                    <div class="group ksd-mb-8"><span class="title">{{$t('kylinLang.model.ownerGrid')}}</span><span class="item">{{scope.row.owner}}</span></div>
                    <div class="group"><span class="title">{{$t('description')}}</span><span class="item">{{scope.row.description || '-'}}</span></div>
                  </div>
                </div>
              </el-popover>
              <span class="model-alias-title" @mouseenter.prevent v-popover:titlePopover>{{scope.row.alias}}</span>
            </div>
            <el-tooltip class="last-modified-tooltip" effect="dark" :content="`${$t('dataLoadTime')}${scope.row.gmtTime}`" placement="bottom">
              <span>{{scope.row.gmtTime}}</span>
            </el-tooltip>

            <!-- 工具栏 -->
            <model-actions :currentModel="scope.row" @loadModelsList="loadModelsList"/>

          </template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.fact')" width="180">
          <template slot-scope="scope">
            <template v-if="scope.row.status === 'BROKEN'">-</template>
            <template v-else>
              <el-popover
                :ref="`${scope.row.alias}-ERPopover`"
                placement="top-start"
                width="400"
                trigger="hover"
                @after-enter="(e) => afterPoppoverEnter(e, scope.row)"
                popper-class="er-popover-layout"
              >
                <div class="model-ER-layout"><ModelERDiagram v-if="scope.row.showER" :model="dataGenerator.generateModel(scope.row)" /></div>
              </el-popover>
              <span class="model-ER" v-popover="`${scope.row.alias}-ERPopover`">
                <el-icon name="el-ksd-icon-table_er_diagram_22" class="ksd-fs-22" type="mult"></el-icon>
              </span>
              <div class="fact-table" v-if="scope.row.fact_table.split('.').length === 2"><span v-custom-tooltip="{text: scope.row.fact_table.split('.')[1], w: 0, tableClassName: 'model_list_table'}">{{scope.row.fact_table.split('.')[1]}}</span></div>
            </template>
          </template>
        </el-table-column>
        <el-table-column
          prop="model_type"
          show-overflow-tooltip
          width="150px"
          :label="$t('modelType')">
          <template slot-scope="scope">
            <span>{{$t(scope.row.model_type)}}</span>
          </template>
        </el-table-column>
        <el-table-column
          width="100"
          prop="usage"
          align="right"
          sortable="custom"
          show-overflow-tooltip
          :info-tooltip="$t('usageTip', {mode: 'model'})"
          :label="$t('usage')">
        </el-table-column>
        <el-table-column
          width="120"
          prop="input_records_count"
          :label="$t('rowCount')">
          <template slot-scope="scope">
            <div>{{sliceNumber(scope.row.input_records_count)}}</div>
            <div class="update-time ksd-fs-12">
              <span v-if="!!scope.row.last_build_time" v-custom-tooltip="{text: `${$t('lastBuildTime')}${transToServerGmtTime(scope.row.last_build_time)}`, content: transToServerGmtTime(scope.row.last_build_time), w: 0, tableClassName: 'model_list_table'}">{{transToServerGmtTime(scope.row.last_build_time)}}</span>
              <span v-else>-</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column
          align="right"
          prop="expansionrate"
          show-overflow-tooltip
          width="150"
          :info-tooltip="$t('expansionRateTip')"
          :label="$t('expansionRate')"
        >
          <template slot-scope="scope">
            <span v-if="scope.row.storage < 1073741824">
              <el-tooltip placement="top" :content="$t('expansionTip')"><span>-</span></el-tooltip>
            </span>
            <template v-else>
              <span v-if="scope.row.expansion_rate !== '-1'">{{scope.row.expansion_rate}}%</span>
              <el-tooltip v-else class="item" effect="dark" :content="$t('tentativeTips')" placement="top">
                <span class="is-disabled">{{$t('tentative')}}</span>
              </el-tooltip>
            </template>
          </template>
        </el-table-column>
        <el-table-column
          align="right"
          width="120"
          prop="total_indexes"
          show-overflow-tooltip
          :label="$t('aggIndexCount')">
          <template slot-scope="scope">
            <span>{{sliceNumber('streaming_indexes' in scope.row ? scope.row.total_indexes + scope.row.streaming_indexes : scope.row.total_indexes) || 0}}</span>
          </template>
        </el-table-column>
      </el-table>
      <!-- 分页 -->
      <kylin-pager class="ksd-center ksd-mtb-10" ref="pager" :perPageSize="filterArgs.page_size" :refTag="pageRefTags.modelListPager" :curPage="filterArgs.page_offset+1" :totalSize="modelsPagerRenderData.totalSize"  v-on:handleCurrentChange='pageCurrentChange'></kylin-pager>
    </div>

    <!-- 模型构建 -->
    <ModelBuildModal @isWillAddIndex="willAddIndex" ref="modelBuildComp"/>
    <!-- 模型检查 -->
    <ModelCheckDataModal/>
    <!-- 数据分区设置 -->
    <ModelPartition/>
    <!-- 模型重命名 -->
    <ModelRenameModal/>
    <!-- 模型克隆 -->
    <ModelCloneModal/>
    <!-- 模型添加 -->
    <ModelAddModal/>
    <!-- 聚合索引编辑 -->
    <AggregateModal v-on:needShowBuildTips="needShowBuildTips" v-on:openBuildDialog="setModelBuldRange" v-on:openComplementAllIndexesDialog="openComplementSegment"/>
    <!-- 表索引编辑 -->
    <TableIndexEdit v-on:needShowBuildTips="needShowBuildTips" v-on:openBuildDialog="setModelBuldRange" v-on:openComplementAllIndexesDialog="openComplementSegment"/>
    <!-- 选择去构建的segment -->
    <ConfirmSegment v-on:reloadModelAndSegment="reloadModelAndSegment"/>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapState, mapGetters, mapMutations } from 'vuex'
import dayjs from 'dayjs'
import { NamedRegex, pageRefTags, pageCount } from '../../../../config'
import { ModelStatusTagType } from '../../../../config/model.js'
import locales from './locales'
import { handleError, kylinMessage, jumpToJobs } from 'util/business'
import { handleSuccessAsync, dataGenerator, sliceNumber, transToServerGmtTime } from 'util'
import TableIndex from '../TableIndex/index.vue'
import ModelSegment from './ModelSegment/index.vue'
import SegmentTabs from './ModelSegment/SegmentTabs.vue'
import ModelAggregate from './ModelAggregate/index.vue'
import ModelAggregateView from './ModelAggregateView/index.vue'
import TableIndexView from './TableIndexView/index.vue'
import ModelRenameModal from './ModelRenameModal/rename.vue'
import ModelCloneModal from './ModelCloneModal/clone.vue'
import ModelAddModal from './ModelAddModal/addmodel.vue'
import ModelBuildModal from './ModelBuildModal/build.vue'
import ModelCheckDataModal from './ModelCheckData/checkdata.vue'
import ConfirmSegment from './ConfirmSegment/ConfirmSegment.vue'
import ModelPartition from './ModelPartition/index.vue'
import ModelJson from './ModelJson/modelJson.vue'
import ModelSql from './ModelSql/ModelSql.vue'
import ModelStreamingJob from './ModelStreamingJob/ModelStreamingJob.vue'
import { mockSQL } from './mock'
import DropdownFilter from '../../../common/DropdownFilter/DropdownFilter.vue'
import ModelOverview from './ModelOverview/ModelOverview.vue'
import AggregateModal from './AggregateModal/index.vue'
import TableIndexEdit from '../TableIndexEdit/tableindex_edit'
import ModelActions from './ModelActions/modelActions'
import ModelERDiagram from '../../../common/ModelERDiagram/ModelERDiagram'

function getDefaultFilters (that) {
  return {
    page_offset: 0,
    page_size: +localStorage.getItem(that.pageRefTags.modelListPager) || pageCount,
    exact: false,
    model_name: '',
    sort_by: 'last_modify',
    reverse: true,
    status: [],
    model_types: [],
    model_alias_or_owner: '',
    last_modify: [],
    owner: '',
    model_attributes: []
  }
}

@Component({
  beforeRouteEnter (to, from, next) {
    next(vm => {
      // 从编辑页面过来，要默认选中在某个tab上，从这里取
      if (to.params.expandTab) {
        vm.currentEditModel = from.params.modelName
        vm.expandTab = to.params.expandTab
        // vm.showFull = true
      }
      if (to.params.modelAlias) {
        // vm.currentEditModel = to.params.modelAlias
        vm.filterArgs.model_alias_or_owner = to.params.modelAlias
        vm.filterArgs.exact = true
      }
      if (to.query.model_alias) {
        vm.currentEditModel = to.query.model_alias
        vm.filterArgs.model_alias_or_owner = to.query.model_alias
        vm.filterArgs.exact = true
      }
      // onSortChange 中project有值时会 loadmodellist, 达到初始化数据的目的
      vm.filterArgs.project = vm.currentSelectedProject
      const prop = 'gmtTime'
      const order = 'descending'
      vm.onSortChange({ prop, order })
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'modelsPagerRenderData',
      'briefMenuGet',
      'datasourceActions',
      'modelActions',
      'metadataActions',
      'isOnlyQueryNode'
    ]),
    ...mapState({
      currentUser: state => state.user.currentUser,
      platform: state => state.config.platform,
      filterModelNameByKC: state => state.model.filterModelNameByKC,
      streamingEnabled: state => state.system.streamingEnabled
    })
  },
  methods: {
    ...mapActions({
      loadModels: 'LOAD_MODEL_LIST',
      checkModelName: 'CHECK_MODELNAME',
      updataModel: 'UPDATE_MODEL',
      getModelJson: 'GET_MODEL_JSON',
      fetchSegments: 'FETCH_SEGMENTS',
      downloadModelsMetadata: 'DOWNLOAD_MODELS_METADATA',
      downloadModelsMetadataBlob: 'DOWNLOAD_MODELS_METADATA_BLOB',
      autoFixSegmentHoles: 'AUTO_FIX_SEGMENT_HOLES'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelAddModal', {
      callAddModelDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelBuildModal', {
      callModelBuildDialog: 'CALL_MODAL'
    }),
    ...mapActions('ConfirmSegment', {
      callConfirmSegmentModal: 'CALL_MODAL'
    }),
    ...mapActions('ModelsImportModal', {
      callModelsImportModal: 'CALL_MODAL'
    }),
    ...mapActions('ModelsExportModal', {
      callModelsExportModal: 'CALL_MODAL'
    }),
    ...mapMutations({
      updateFilterModelByCloud: 'UPDATE_FILTER_MODEL_NAME_CLOUD'
    })
  },
  components: {
    ModelBuildModal,
    TableIndex,
    ModelSegment,
    SegmentTabs,
    ModelAggregate,
    ModelAggregateView,
    TableIndexView,
    ModelRenameModal,
    ModelCloneModal,
    ModelAddModal,
    ModelCheckDataModal,
    ConfirmSegment,
    ModelPartition,
    ModelJson,
    ModelSql,
    ModelStreamingJob,
    DropdownFilter,
    AggregateModal,
    TableIndexEdit,
    ModelOverview,
    ModelActions,
    ModelERDiagram
  },
  locales
})
export default class ModelList extends Vue {
  pageRefTags = pageRefTags
  mockSQL = mockSQL
  dataGenerator = dataGenerator
  sliceNumber = sliceNumber
  transToServerGmtTime = transToServerGmtTime
  filterArgs = getDefaultFilters(this)
  statusList = ['ONLINE', 'OFFLINE', 'BROKEN', 'WARNING']
  modelTypeList = ['HYBRID', 'STREAMING', 'BATCH']
  currentEditModel = null
  showFull = false
  showSearchResult = false
  searchLoading = false
  isShowFilters = true
  modelArray = []
  expandedRows = []
  filterTags = []
  prevExpendContent = []
  buildVisible = {}
  changeOwnerVisible = false
  expandTab = ''
  isModelListOpen = false
  isShow = false
  loadingModels = false
  debouce = null

  @Watch('modelsPagerRenderData')
  onModelChange (modelsPagerRenderData) {
    this.modelArray = []
    modelsPagerRenderData.list.forEach(item => {
      this.$set(item, 'showModelDetail', false)
      this.modelArray.push({
        ...item,
        tabTypes: this.currentEditModel === item.alias ? this.expandTab : 'overview',
        showER: false
      })
    })
  }

  // 获取模型 E-R 图数据
  getER (row) {
    return this.dataGenerator.generateModel(row)
  }
  needShowBuildTips (uuid) {
    this.buildVisible[uuid] = !localStorage.getItem('hideBuildTips')
  }

  reloadModelAndSegment (alias) {
    this.loadModelsList()
    this.refreshSegment(alias)
  }
  async autoFix (...args) {
    try {
      const [modelName, modleId, segmentHoles] = args
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
        msg: this.$t('segmentHoletips', {modelName: modelName}),
        title: this.$t('fixSegmentTitle'),
        detailTableData: tableData,
        detailColumns: [
          {column: 'start', label: this.$t('kylinLang.common.startTime')},
          {column: 'end', label: this.$t('kylinLang.common.endTime')}
        ],
        isShowSelection: true,
        dialogType: 'warning',
        showDetailBtn: false,
        customCallback: async (segments) => {
          selectSegmentHoles = segments.map((seg) => {
            return {start: seg.date_range_start, end: seg.date_range_end}
          })
          try {
            await this.autoFixSegmentHoles({project: this.currentSelectedProject, model_id: modleId, segment_holes: selectSegmentHoles})
            this.$message({
              dangerouslyUseHTMLString: true,
              type: 'success',
              customClass: 'build-full-load-success',
              duration: 10000,
              showClose: true,
              message: (
                <div>
                  <span>{this.$t('kylinLang.common.submitSuccess')}</span>
                  <a href="javascript:void(0)" onClick={() => jumpToJobs()}>{this.$t('kylinLang.common.toJoblist')}</a>
                </div>
              )
            })
            this.loadModelsList()
          } catch (e) {
            handleError(e)
          }
        }
      })
    } catch (e) {
      e !== 'cancel' && handleError(e)
    }
  }
  openComplementSegment (model, isModelMetadataChanged) {
    let title
    let subTitle
    let submitText
    let refrashWarningSegment
    if (isModelMetadataChanged) {
      title = this.$t('kylinLang.common.seeDetail')
      subTitle = ''
      refrashWarningSegment = true
      submitText = this.$t('kylinLang.common.refresh')
    } else {
      title = this.$t('buildIndex')
      subTitle = this.model.model_type === 'HYBRID' ? this.$t('hybridModelBuildTitle') : this.$t('batchBuildSubTitle')
      submitText = this.$t('buildIndex')
    }
    this.callConfirmSegmentModal({
      title: title,
      subTitle: subTitle,
      refrashWarningSegment: refrashWarningSegment,
      indexes: [],
      submitText: submitText,
      model: model
    })
  }
  setRowClass (res) {
    const {row, rowIndex} = res
    return 'visible' in row && !row.visible ? `model_list_row_${rowIndex} no-authority-model` : `model_list_row_${rowIndex}`
  }
  get emptyText () {
    return this.filterArgs.model_alias_or_owner ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  get modelTableTitle () {
    return this.$t('kylinLang.model.modelNameGrid')
  }
  get selectedStatus () {
    const { filterArgs } = this
    return filterArgs.status.length && this.statusList.length !== filterArgs.status.length
      ? filterArgs.status.map(status => this.$t(status)).join(', ')
      : this.$t('ALL')
  }
  get selectedRange () {
    const { filterArgs } = this
    if (filterArgs.last_modify && filterArgs.last_modify.length !== 0) {
      const [startTime, endTime] = filterArgs.last_modify
      const startDate = dayjs(startTime).format('YYYY-MM-DD HH:mm:ss')
      const endDate = dayjs(endTime).format('YYYY-MM-DD HH:mm:ss')
      return `${startDate} - ${endDate}`
    }
    return this.$t('allTimeRange')
  }
  get selectedModelAttributes () {
    const { filterArgs } = this
    return filterArgs.model_attributes.length && this.modelTypeList.length !== filterArgs.model_attributes.length
      ? filterArgs.model_attributes.map(attributes => this.$t(attributes)).join(', ')
      : this.$t('ALL')
  }
  get isResetFilterDisabled () {
    return !this.filterArgs.last_modify.length && !this.filterArgs.status.length && !this.filterArgs.model_attributes.length
  }
  handleFilterInput (value) {
    this.filterArgs.model_alias_or_owner = value
  }
  handleResetFilters () {
    const defaultFilters = getDefaultFilters(this)

    Object.entries(defaultFilters).map(([key, value]) => {
      if (key === 'model_alias_or_owner') return
      this.filterArgs[key] = value
    })

    this.pageCurrentChange(0, this.filterArgs.page_size)
  }
  handleToggleFilters () {
    this.isShowFilters = !this.isShowFilters
  }
  getModelStatusTagType = ModelStatusTagType
  renderFullExpandClass (row) {
    return (row.showModelDetail || this.currentEditModel === row.alias) ? 'full-cell-content' : ''
  }
  expandRow (row, expandedRows) {
    this.expandedRows = expandedRows && expandedRows.map((m) => {
      return Object.prototype.toString.call(m) === '[object Object]' ? m.alias : m
    }) || []
    this.currentEditModel = null
  }
  renderRowKey (row) {
    return row.alias
  }
  renderColumnClass ({row, column, rowIndex, columnIndex}) {
    if (columnIndex === 0) {
      return 'model-alias-item'
    }
    if (column.label && column.label === this.$t('kylinLang.common.fact')) {
      return 'fact-table-title'
    }
  }
  checkName (rule, value, callback) {
    if (!NamedRegex.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else {
      callback()
    }
  }
  _showFullDataLoadConfirm (storage, modelName) {
    const storageSize = Vue.filter('dataSize')(storage)
    const contentVal = { modelName, storageSize }
    const confirmTitle = this.$t('fullLoadDataTitle')
    const confirmMessage1 = this.$t('fullLoadDataContent1', contentVal)
    const confirmMessage2 = this.$t('fullLoadDataContent2', contentVal)
    const confirmMessage3 = this.$t('fullLoadDataContent3', contentVal)
    const confirmMessage = _render(this.$createElement)
    const confirmButtonText = this.$t('kylinLang.common.ok')
    const cancelButtonText = this.$t('kylinLang.common.cancel')
    const type = 'warning'
    return this.$confirm(confirmMessage, confirmTitle, { confirmButtonText, cancelButtonText, type })

    function _render (h) {
      return (
        <div>
          <p class="break-all">{confirmMessage1}</p>
          <p>{confirmMessage2}</p>
          <p>{confirmMessage3}</p>
        </div>
      )
    }
  }
  async setModelBuldRange (modelDesc, isNeedBuildGuild) {
    if (!modelDesc.total_indexes && !isNeedBuildGuild || (!this.$store.state.project.multi_partition_enabled && modelDesc.multi_partition_desc)) return
    const projectName = this.currentSelectedProject
    const modelName = modelDesc.uuid
    const res = await this.fetchSegments({ projectName, modelName })
    const { total_size, value } = await handleSuccessAsync(res)
    let type = 'incremental'
    if (!(modelDesc.partition_desc && modelDesc.partition_desc.partition_date_column) && modelDesc.model_type !== 'STREAMING') {
      type = 'fullLoad'
    }
    this.isModelListOpen = true
    this.$nextTick(async () => {
      await this.callModelBuildDialog({
        modelDesc: modelDesc,
        type: type,
        title: this.$t('build'),
        isHaveSegment: !!total_size,
        disableFullLoad: type === 'fullLoad' && value.length > 0 && value[0].status_to_display !== 'ONLINE' // 已存在全量加载任务时，屏蔽
      })
      await this.refreshSegment(modelDesc.alias)
      this.isModelListOpen = false
    })
  }
  async refreshSegment (alias) {
    this.$refs['segmentComp' + alias] && await this.$refs['segmentComp' + alias].$emit('refresh')
    this.prevExpendContent = this.modelArray.filter(item => this.expandedRows.includes(item.alias))
    this.$nextTick(() => {
      this.setModelExpand()
    })
  }
  // 还原模型列表展开状态
  async setModelExpand () {
    if (!this.$refs.modelListTable) return
    let obj = {}
    this.prevExpendContent.forEach(item => {
      obj[item.alias] = item
    })
    this.modelArray.forEach(it => {
      (it.alias in obj) && (it.tabTypes = obj[it.alias].tabTypes)
    })
    this.$refs.modelListTable.store.states.expandRows = []
    this.expandedRows.length && this.expandedRows.forEach(item => {
      this.$refs.modelListTable.toggleRowExpansion(item)
    })
  }

  downloadResouceData (project, form) {
    const params = {}
    for (const [key, value] of Object.entries(form)) {
      if (value instanceof Array) {
        value.forEach((item, index) => {
          params[`${key}[${index}]`] = item
        })
      } else if (typeof value === 'object') {
        params[key] = JSON.stringify(value)
      } else {
        params[key] = value
      }
    }
    try {
      this.downloadModelsMetadataBlob({project, params}).then(res => {
        let str = res && res.headers.map['content-disposition'][0]
        let fileName1 = str.split('filename=')[1]
        let fileName = fileName1.includes('"') ? JSON.parse(fileName1) : fileName1
        if (res && res.body) {
          let data = res.body
          const blob = new Blob([data], {type: 'application/json;charset=utf-8'})
          if (window.navigator.msSaveOrOpenBlob) {
            navigator.msSaveBlob(data, fileName)
          } else {
            let link = document.createElement('a')
            link.href = window.URL.createObjectURL(blob)
            link.download = fileName
            link.click()
            window.URL.revokeObjectURL(link.href)
          }
        }
        this.$message.success(this.$t('exportMetadataSuccess'))
      })
    } catch (e) {
      this.$message.error(this.$t('exportMetadataFailed'))
    }
  }
  handleSaveModel (modelDesc) {
    // 如果未选择partition 把partition desc 设置为null
    if (!(modelDesc && modelDesc.partition_desc && modelDesc.partition_desc.partition_date_column)) {
      modelDesc.partition_desc = null
    }
    this.updataModel(modelDesc).then(() => {
      kylinMessage(this.$t('kylinLang.common.saveSuccess'))
      this.loadModelsList()
    }, (res) => {
      handleError(res)
    })
  }

  async handleImportModels () {
    const project = this.currentSelectedProject
    const isSubmit = await this.callModelsImportModal({ project })
    if (isSubmit) {
      this.pageCurrentChange(0, this.filterArgs.page_size)
    }
  }
  async handleExportMetadatas () {
    const project = this.currentSelectedProject
    const type = 'all'
    await this.callModelsExportModal({ project, type })
  }
  onSortChange ({ prop, order }) {
    this.filterArgs.sort_by = prop
    if (prop === 'gmtTime') {
      this.filterArgs.sort_by = 'last_modify'
    }
    this.filterArgs.reverse = !(order === 'ascending')
    if (this.filterArgs.project) {
      this.pageCurrentChange(0, this.filterArgs.page_size)
    }
  }
  // 全屏查看模型附属信息
  toggleShowFull (index, row) {
    var scrollBoxDom = document.getElementById('scrollContent')
    if (!this.showFull && scrollBoxDom) {
      // 展开时记录下展开时候的scrollbar 的top距离，搜索的时候复原该位置
      row.hisScrollTop = scrollBoxDom.scrollTop
    }
    this.$nextTick(() => {
      this.$set(row, 'showModelDetail', !this.showFull)
      this.showFull = !this.showFull
      this.$nextTick(() => {
        if (scrollBoxDom) {
          if (this.showFull) {
            scrollBoxDom.scrollTop = 0
          } else {
            scrollBoxDom.scrollTop = row.hisScrollTop
          }
          this.currentEditModel = null
        }
      })
    })
  }
  // 加载模型列表
  loadModelsList () {
    this.prevExpendContent = this.modelArray.filter(item => this.expandedRows.includes(item.alias))
    this.loadingModels = true
    this.$el.click()
    return this.loadModels(this.filterArgs).then(() => {
      if (this.filterArgs.model_alias_or_owner || this.modelsPagerRenderData.list.length) {
        this.showSearchResult = true
      } else {
        this.showSearchResult = false
      }
      this.loadingModels = false
      this.$nextTick(() => {
        this.expandedRows = this.currentEditModel ? [this.currentEditModel] : this.expandedRows
        this.setModelExpand()
      })
    }).catch((res) => {
      this.loadingModels = false
      handleError(res)
    })
  }
  // 分页
  pageCurrentChange (size, count) {
    this.filterArgs.page_offset = size
    this.filterArgs.page_size = count
    this.loadModelsList()
  }
  // 搜索模型
  searchModels () {
    if (this.filterArgs.exact) {
      this.filterArgs.exact = false
    }
    this.filterArgs.page_offset = 0
    this.searchLoading = true
    this.updateFilterModelByCloud('')
    this.loadModelsList().then(() => {
      this.searchLoading = false
    }).finally((res) => {
      this.searchLoading = false
    })
  }
  showAddModelDialog () {
    if (!this.modelArray.length && !localStorage.getItem('isFirstAddModel')) {
      localStorage.setItem('isFirstAddModel', 'true')
    }
    this.callAddModelDialog()
  }
  // 查询状态过滤回调函数
  filterContent (val, type) {
    const maps = {
      status: 'status',
      model_types: 'model_types'
    }

    this.filterTags = this.filterTags.filter((item, index) => item.key !== type || item.key === type && val.includes(item.label))
    const list = this.filterTags.filter(it => it.key === type).map(it => it.label)
    val.length && val.forEach(item => {
      if (!list.includes(item)) {
        this.filterTags.push({label: item, source: maps[type], key: type})
      }
    })
    this.filterArgs[type] = val
    clearTimeout(this.debouce)
    this.debouce = setTimeout(() => {
      this.pageCurrentChange(0, this.filterArgs.page_size)
    }, 300)
  }
  // 删除单个筛选条件
  handleClose (tag) {
    const index = this.filterArgs[tag.key].indexOf(tag.label)

    index > -1 && this.filterArgs[tag.key].splice(index, 1)
    this.filterTags = this.filterTags.filter(item => item.key !== tag.key || item.key === tag.key && tag.label !== item.label)
    this.pageCurrentChange(0, this.filterArgs.page_size)
  }
  // 清除所有筛选条件
  clearAllTags () {
    this.filterArgs.status.splice(0, this.filterArgs.status.length)
    this.filterTags = []
    this.pageCurrentChange(0, this.filterArgs.page_size)
  }

  renderStatusLabel (h, option) {
    const { value } = option
    return [
      <i class={['filter-bar filter-status', value]} />,
      <span>{value}</span>
    ]
  }

  renderModelTypeLabel (h, option) {
    const { value } = option
    return [
      <span>{this.$t(value)}</span>
    ]
  }

  get modelAttributesOptions () {
    let options = []
    if (this.streamingEnabled === 'false') { // 暂不支持流数据融合数据模型 或者没有开启流模型配置
      options = [
        { renderLabel: this.renderModelTypeLabel, value: 'BATCH' }
      ]
    } else {
      options = [
        { renderLabel: this.renderModelTypeLabel, value: 'HYBRID' },
        { renderLabel: this.renderModelTypeLabel, value: 'STREAMING' },
        { renderLabel: this.renderModelTypeLabel, value: 'BATCH' }
      ]
    }
    return options
  }

  // 模型展开自动滚动到可视区域
  scrollViewArea (index) {
    const scrollDom = this.$el.querySelector(`.model_list_row_${index}`)
    scrollDom.nextSibling.querySelector('.aggregate-view').scrollIntoView({block: 'center', inline: 'nearest', behavior: 'smooth'})
  }

  async willAddIndex (alias) {
    this.$refs['segmentComp' + alias] && await this.$refs['segmentComp' + alias].$emit('willAddIndex')
  }

  modelRowClickEvent (row, e) {
    if (row.status === 'BROKEN' || ('visible' in row && !row.visible)) return
    this.$router.push({name: 'ModelDetails', params: {modelName: row.alias}, query: {modelPageOffest: this.filterArgs.page_offset}})
  }

  // 展示 E-R 图
  afterPoppoverEnter (e, row) {
    this.$nextTick(() => {
      this.$set(row, 'showER', true)
    })
  }
}
</script>
<style lang="less">
@import '../../../../assets/styles/variables.less';
.mode-list{
  position:relative;
  margin-left: 24px;
  margin-right: 24px;
  .model-list-contain {
    position: relative;
  }
  .ts-storage {
    font-size: 12px;
    color: @text-disabled-color;
  }
  .specialDropdown{
    min-width:96px;
    .el-dropdown-menu__item {
      white-space: nowrap;
    }
  }
  .dropdown-filter + .dropdown-filter {
    margin-left: 5px;
  }
  .broken-column {
    .cell {
      display: none;
    }
  }
  .full-model-slide-fade-enter-active {
    transition: all .3s ease;
  }
  .full-model-slide-fade-leave-active {
    transition: all .3s cubic-bezier(1.0, 0.5, 0.8, 1.0);
  }
  .full-model-slide-fade-enter, .full-model-slide-fade-leave-to {
    transform: translateY(10px);
    opacity: 0;
  }
  .row-action {
    position: absolute;
    right:0;
    text-align: right;
    z-index: 2;
    cursor: pointer;
    color: @text-normal-color;
    &:hover {
      color: @base-color;
      .tip-text {
        color: @base-color;
      }
    }
    .tip-text {
      top:10px;
      color: @text-normal-color;
    }
  }
  .notice-box {
    position:relative;
    .el-alert{
      background-color:@base-color-9;
      a {
        text-decoration: underline;
        color:@base-color-1;
      }
    }
    .tip-toggle-btnbox {
      position:absolute;
      top:4px;
      right:10px;
    }
  }
  .model_list_table {
    user-select: none;
    .el-table__header th {
      vertical-align: top;
    }
    .el-table__body tr {
      cursor: pointer;
    }
    .el-table__body td {
      vertical-align: top;
    }
    .custom-tooltip-layout {
      vertical-align: middle;
      width: calc(~'100% - 5px');
    }
    .model-ER {
      cursor: pointer;
    }
    .rec-btn {
      color: @ke-color-primary;
    }
    .build-disabled > .el-ksd-icon-build_index_22 {
      color: @color-text-disabled;
      &:hover {
        color: @color-text-disabled;
      }
    }
    .clickable-btn {
      color: @base-color;
      cursor: pointer;
    }
    span.is-disabled {
      color: @text-disabled-color;
    }
    .el-table__expanded-cell {
      background-color: @background-color-base-1;
      padding:0;
      &:hover {
        background-color: @background-color-base-1 !important;
      }
      .full-cell-content {
        position: relative;
      }
      .full-model-box {
        vertical-align:middle;
        font-size: 16px;
        margin-left:10px;
        z-index: 10;
      }
      .model-detail-tabs {
        &.el-tabs--card>.el-tabs__header .el-tabs__item.is-active{
          border-bottom-color: #fbfbfb;
        }
        > .el-tabs__header {
          margin-bottom: 0px;
          z-index: 1;
          .el-tabs__item {
            height: 40px;
            line-height: 40px;
          }
          .el-tabs__item.is-active{
            border-bottom-color: #fbfbfb;
          }
          &> .el-tabs__nav-wrap {
            box-shadow:0px 2px 4px 0px rgba(229,229,229,1);
            background-color: @fff;
          }
        }
      }
    }
    .el-table__row.no-authority-model {
      background-color: #f5f5f5;
      color: @text-disabled-color;
      // pointer-events: none;
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
    .cell.highlight {
      .el-icon-ksd-filter {
        color: @base-color;
      }
    }
    .ky-hover-icon {
      .cell {
        .tip_box {
          margin-left: 10px;
          &:first-child {
            margin-left: 0;
          }
        }
      }
    }
    .el-icon-ksd-lock {
      color: @text-title-color;
    }
    .model-alias-title {
      max-width: calc(~'100% - 30px');
      display: inline-block;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      white-space: nowrap\0 !important;
    }
    .model-alias-item {
      .action-items {
        opacity: 0;
        position: absolute;
        right: 10px;
        top: 6px;
      }
      &:hover {
        .model-alias-title {
          max-width: calc(~'100% - 120px');
        }
        .action-items {
          opacity: 1;
        }
      }
    }
    .fact-table-title {
      .fact-table {
        width: calc(~'100% - 35px');
        margin-left: 8px;
        display: inline-block;
      }
      .cell > span {
        width: 100%;
        display: inline-block;
      }
    }
    .update-time {
      width: 100%;
      color: @text-disabled-color;
      overflow: hidden;
      text-overflow: ellipsis;
      span {
        font-size: 12px;
      }
    }
  }
  .row-action {
    right:20px;
    top: 4px;
  }
  &.full-cell {
    margin: 0 20px;
    position: relative;
    .segment-settings {
      display: block;
    }
    .segment-actions .left {
      display: block;
    }
    .model_list_table {
      position: static !important;
      border: none;
      td {
        position: static !important;
      }
      .el-table__body-wrapper {
        position: static !important;
        .el-table__expanded-cell {
          padding: 0;
          .full-cell-content {
            z-index: 9;
            position: absolute;
            padding-top: 10px;
            background: @fff;
            top: 0px;
            height: 100vh;
            width: calc(~'100% + 40px');
            padding-right: 20px;
            padding-left: 20px;
            margin-left: -20px;
            border-top: 1px solid #CFD8DC;
            &.hidden-cell {
              display: none;
            }
            .full-model-box {
              top: 20px;
              right: 20px;
            }
          }
        }
      }
    }
  }
  .el-tabs__nav {
    margin-left: 0;
  }
  .el-tabs__content {
    overflow: initial;
  }
  .table-filters {
    margin-bottom: 8px;
    >.dropdown-filter {
      margin-left: -8px;
    }
    .actions {
      float: right;
      .reset-filters-btn.is-disabled {
        i {
          cursor: not-allowed;
        }
      }
    }
  }
  .alias {
    font-weight: bold;
    line-height: 20px;
    width: 100%;
    height: 20px;
    // margin-bottom: 5px;
    float: left;
  }
  .last-modified {
    font-size: 12px;
    line-height: 18px;
    float: left;
    margin-right: 15px;
    i {
      color: #989898;
      cursor: default;
    }
  }
  .streaming {
    float: left;
    font-size: 12px;
    line-height: 18px;
  }
  .alias .filter-status {
    float: left;
    position: relative;
    top: 6px;
  }
  .text-container {
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .text-sample {
    // white-space: nowrap; 在safari浏览器上有问题
    display: -webkit-box;
    -webkit-line-clamp: 1;
    /*! autoprefixer: off */
    -webkit-box-orient: vertical;
    /* autoprefixer: on */
    white-space: nowrap\0;
  }
}
.no-acl-model {
  .dialog-detail {
    .dialog-detail-scroll {
      max-height: 200px;
    }
  }
}
.filter-button {
  margin-left: 5px;
  vertical-align: bottom;
  .el-ksd-icon-arrow_up_22 {
    transform: rotate(180deg);
  }
  .el-ksd-icon-arrow_up_22.reverse {
    transform: rotate(0);
  }
}
.filter-status {
  border-radius: 100%;
  width: 10px;
  height: 10px;
  display: inline-block;
  margin-right: 10px;
  &.ONLINE {
    background-color: @color-success;
  }
  &.OFFLINE {
    background-color: @ke-color-info-secondary;
  }
  &.BROKEN {
    background-color: @color-danger;
  }
  &.WARNING {
    background-color: @color-warning;
  }
}
.filter-bar {
  &.filter-status {
    border-radius: 100%;
    width: 10px;
    height: 10px;
    display: inline-block;
    margin-right: 10px;
    &.ONLINE {
      background-color: @color-success;
    }
    &.OFFLINE {
      background-color: @ke-color-info-secondary;
    }
    &.BROKEN {
      background-color: @color-danger;
    }
    &.WARNING {
      background-color: @color-warning;
    }
  }
}
.last-modified-tooltip {
  min-width: unset;
  transform: translate(-5px, 5px);
  margin-left: 15px;
  color: #8B99AE;
  font-size: 12px;
  .popper__arrow {
    left: 5px !important;
  }
}
.status-tooltip {
  min-width: unset;
  transform: translate(-3px, 0);
  .popper__arrow {
    left: 8px !important;
  }
}
.model-actions-dropdown {
  text-align: left;
  min-width: 95px;
}
.title-popover-layout {
  font-size: 14px;
  word-break: break-all;
  .title {
    color: @text-title-color;
    font-weight: bold;
  }
  .label {
    display: inline-block;
    .group {
      color: @text-disabled-color;
      // text-align: right;
      margin-right: 15px;
      .title {
        color: @text-disabled-color;
        display: inline-block;
        min-width: 40px;
        word-break: break-all;
        text-align: right;
        margin-right: 10px;
      }
      .item {
        display: inline-block;
        max-width: 180px;
        word-break: break-all;
        vertical-align: top;
      }
    }
    &.en {
      .title {
        min-width: 80px;
      }
      .item {
        max-width: 140px;
      }
    }
  }
}
.er-popover-layout {
  width: 400px;
  height: 300px;
  position: relative;
  .model-ER-layout {
    width: 100%;
    height: 100%;
    position: relative;
    overflow: hidden;
  }
}
.el-tooltip__popper {
  .popper__arrow {
    &::after {
      content: none\0;
    }
  }
}
</style>
