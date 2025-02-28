<template>
  <div class="model-layout" :key="randomKey">
    <div class="header-layout">
      <div class="title"><el-button type="primary" text icon-button-mini icon="el-ksd-icon-arrow_left_16" size="small" @click="jumpBack"></el-button>
        <model-title-description :modelData="currentModelRow" @autoFix="autoFix" @openSegment="openComplementSegment" source="modelLayout" v-if="currentModelRow" hideTimeTooltip />
        <el-button class="ksd-ml-8" type="primary" text @click.stop="showModelList = !showModelList" icon-button-mini icon="el-ksd-icon-arrow_down_16" size="small"></el-button>
        <div class="model-filter-list" v-if="showModelList">
          <div class="search-bar"><el-input class="search-model-input" v-model="searchModelName" size="small" :placeholder="$t('kylinLang.common.pleaseInput')" prefix-icon="el-ksd-icon-search_22" v-global-key-event.enter.debounce="searchModel" @clear="searchModel()"></el-input></div>
          <div class="model-list" v-loading="showSearchResult">
            <template v-if="!modelList.length">
              <div class="no-data">{{$t('noResult')}}</div>
            </template>
            <template v-else>
              <div class="items" v-for="item in modelList" :key="item.uuid" @click="chooseOtherModel({model: item})">
                <i class="el-icon-ksd-accept" v-if="item.alias === modelName"></i>
                <span v-custom-tooltip="{text: item.alias, w: 60}" :class="['model-name', item.alias === modelName ? 'ksd-ml-5' : 'ksd-ml-25', {'is-disabled': item.status === 'BROKEN'}]">{{item.alias}}</span>
              </div>
            </template>
          </div>
        </div>
      </div>
      <model-actions
        v-if="currentModelRow"
        @rename="changeModelName"
        @loadModelsList="reloadModel"
        @loadModels="reloadModel"
        :modelListFilters="modelListFilters"
        :currentModel="currentModelRow"
        :appendToBody="true"
        :editText="$t('modelEditAction')"
        :buildText="$t('modelBuildAction')"
        :moreText="$t('moreAction')"
        other-icon="el-ksd-n-icon-more-outlined"
      />
    </div>
    <el-tabs class="el-tabs--default model-detail-tabs" tab-position="left" v-if="currentModelRow" v-model="currentModelRow.tabTypes" :key="$lang">
      <el-tab-pane :class="['tab-pane-item']" :label="$t('overview')" name="overview">
        <ModelOverview
          v-if="currentModelRow.tabTypes === 'overview'"
          :ref="`$model-overview-${currentModelRow.uuid}`"
          :data="currentModelRow"
        />
      </el-tab-pane>
      <el-tab-pane class="tab-pane-item data-features" :label="$t('dataFeatures')" name="dataFeatures">
        <DataFeatures
          v-if="currentModelRow.tabTypes === 'dataFeatures'"
          :data="currentModelRow"
        />
      </el-tab-pane>
      <el-tab-pane class="tab-pane-item" :label="$t('segment')" name="first">
        <SegmentTabs
          v-if="currentModelRow.tabTypes === 'first' && currentModelRow.model_type === 'HYBRID'"
          :ref="'segmentComp' + currentModelRow.alias"
          :model="currentModelRow"
          :isShowSegmentActions="datasourceActions.includes('segmentActions')"
          @purge-model="model => handleCommand('purge', model)"
          @loadModels="reloadModel"
          @refreshModel="refreshModelData"
          @willAddIndex="() => {currentModelRow.tabTypes = 'third'}"
          @auto-fix="autoFix"/>
        <StreamingSegment
          :isShowPageTitle="true"
          v-if="currentModelRow.tabTypes === 'first' && currentModelRow.model_type === 'STREAMING'"
          :isShowSegmentActions="datasourceActions.includes('segmentActions')"
          :model="currentModelRow"
          @refreshModel="refreshModelData"
        />
        <ModelSegment
          :ref="'segmentComp' + currentModelRow.alias"
          :model="currentModelRow"
          :isShowSegmentActions="datasourceActions.includes('segmentActions')"
          v-if="currentModelRow.tabTypes === 'first' && (currentModelRow.model_type !== 'HYBRID' && currentModelRow.model_type !== 'STREAMING')"
          @loadModels="reloadModel"
          @refreshModel="refreshModelData"
          @purge-model="model => handleCommand('purge', model)"
          @willAddIndex="() => {currentModelRow.tabTypes = 'third'}"
          @auto-fix="autoFix(currentModelRow.alias, currentModelRow.uuid, currentModelRow.segment_holes)" />
      </el-tab-pane>
      <el-tab-pane class="tab-pane-item" :label="$t('indexes')" name="second">
        <el-tabs class="model-indexes-tabs" v-if="currentModelRow.tabTypes === 'second'" v-model="currentIndexTab">
          <el-tab-pane class="tab-pane-item" :label="$t('indexOverview')" name="indexOverview">
            <ModelAggregate
              :model="currentModelRow"
              :project-name="currentSelectedProject"
              :isShowEditAgg="datasourceActions.includes('editAggGroup') || isAdvancedOperatorUser()"
              :isShowBulidIndex="datasourceActions.includes('buildIndex') || isAdvancedOperatorUser()"
              :isShowTableIndexActions="datasourceActions.includes('tableIndexActions') || isAdvancedOperatorUser()"
              ref="modelAggregateItem"
              @refreshModel="refreshModelData"
              v-if="currentIndexTab === 'indexOverview'" />
          </el-tab-pane>
          <el-tab-pane class="tab-pane-item" :label="$t('recommendationsBtn')" name="recommendations" v-if="$store.state.project.isSemiAutomatic && (datasourceActions.includes('accelerationActions') || isAdvancedOperatorUser()) && currentModelRow.model_type !== 'HYBRID'">
            <recommendations v-if="currentIndexTab==='recommendations'" @refreshModel="refreshModelData" :modelDesc="currentModelRow" />
          </el-tab-pane>
          <el-tab-pane class="tab-pane-item" v-if="datasourceActions.includes('editAggGroup') || isAdvancedOperatorUser()" :label="$t('aggregateGroup')" name="aggGroup">
            <ModelAggregateView
              :model="currentModelRow"
              :project-name="currentSelectedProject"
              :isShowEditAgg="datasourceActions.includes('editAggGroup') || isAdvancedOperatorUser()"
              @refreshModel="refreshModelData"
              v-if="currentIndexTab === 'aggGroup'" />
          </el-tab-pane>
          <el-tab-pane class="tab-pane-item" v-if="datasourceActions.includes('editAggGroup') || isAdvancedOperatorUser()" :label="$t('tableIndex')" name="tableIndex">
            <TableIndexView
              :model="currentModelRow"
              :project-name="currentSelectedProject"
              :isShowTableIndexActions="datasourceActions.includes('tableIndexActions') || isAdvancedOperatorUser()"
              @refreshModel="refreshModelData"
              v-if="currentIndexTab === 'tableIndex'" />
          </el-tab-pane>
        </el-tabs>
      </el-tab-pane>
      <el-tab-pane class="tab-pane-item" :label="$t('developers')" name="fifth">
        <Developers v-if="currentModelRow.tabTypes === 'fifth'" :currentModelRow="currentModelRow"/>
      </el-tab-pane>
    </el-tabs>

    <!-- 模型构建 -->
    <ModelBuildModal @isWillAddIndex="willAddIndex" ref="modelBuildComp"/>
    <!-- 聚合索引编辑 -->
    <AggregateModal v-on:needShowBuildTips="needShowBuildTips" v-on:openBuildDialog="setModelBuldRange" v-on:openComplementAllIndexesDialog="openComplementSegment"/>
    <!-- 表索引编辑 -->
    <TableIndexEdit v-on:needShowBuildTips="needShowBuildTips" v-on:openBuildDialog="setModelBuldRange" v-on:openComplementAllIndexesDialog="openComplementSegment"/>
    <!-- 选择去构建的segment -->
    <ConfirmSegment v-on:reloadModelAndSegment="reloadModelAndSegment"/>
    <!-- 数据分区设置 -->
    <ModelPartition/>
    <!-- 模型重命名 -->
    <ModelRenameModal/>
    <!-- 模型克隆 -->
    <ModelCloneModal/>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapGetters, mapActions } from 'vuex'
import { handleError, jumpToJobs } from '../../../../../util/business'
import {getQueryString, transToServerGmtTime, handleSuccessAsync } from '../../../../../util'
import { pageCount } from '../../../../../config'
import locales from './locales'
import ModelOverview from '../ModelOverview/ModelOverview.vue'
import ModelSegment from '../ModelSegment/index.vue'
import SegmentTabs from '../ModelSegment/SegmentTabs.vue'
import StreamingSegment from '../ModelSegment/StreamingSegment/StreamingSegment.vue'
import ModelAggregate from '../ModelAggregate/index.vue'
import ModelAggregateView from '../ModelAggregateView/index.vue'
import TableIndexView from '../TableIndexView/index.vue'
import Developers from '../Developers/developers.vue'
import ModelBuildModal from '../ModelBuildModal/build.vue'
import AggregateModal from '../AggregateModal/index.vue'
import TableIndexEdit from '../../TableIndexEdit/tableindex_edit'
import ConfirmSegment from '../ConfirmSegment/ConfirmSegment.vue'
import DataFeatures from '../DataFeatures/dataFeatures.vue'
import ModelActions from '../ModelActions/modelActions'
import ModelRenameModal from '../ModelRenameModal/rename.vue'
import ModelCloneModal from '../ModelCloneModal/clone.vue'
import ModelPartition from '../ModelPartition/index.vue'
import Recommendations from '../ModelAggregate/sub/recommendations.vue'
import ModelTitleDescription from '../Components/ModelTitleDescription'

@Component({
  beforeRouteEnter (to, from, next) {
    next(vm => {
      // 从模型页面过来，用模型列表的过滤条件获取modelList
      if (to.query.modelListFilters || getQueryString('modelListFilters')) {
        vm.modelListFilters = JSON.parse(to.query.modelListFilters) || JSON.parse(getQueryString('modelListFilters'))
      }

      if (from.name === 'ModelList') {
        vm.filterData = vm.modelListFilters
      } else if (to.query.filterData) {
        vm.filterData = JSON.parse(to.query.filterData)
      }
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'datasourceActions',
      'modelActions'
    ]),
    ...mapState({
      modelList: state => state.model.modelsList
    })
  },
  inject: [
    'forceUpdateRoute',
    'isAdvancedOperatorUser'
  ],
  methods: {
    ...mapActions({
      autoFixSegmentHoles: 'AUTO_FIX_SEGMENT_HOLES',
      loadModels: 'LOAD_MODEL_LIST',
      fetchSegments: 'FETCH_SEGMENTS',
      getModelByModelName: 'LOAD_MODEL_INFO'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    }),
    ...mapActions('GuideModal', {
      callGuideModal: 'CALL_MODAL'
    }),
    ...mapActions('ConfirmSegment', {
      callConfirmSegmentModal: 'CALL_MODAL'
    }),
    ...mapActions('ModelBuildModal', {
      callModelBuildDialog: 'CALL_MODAL'
    })
  },
  components: {
    ModelOverview,
    ModelSegment,
    SegmentTabs,
    StreamingSegment,
    ModelAggregate,
    ModelAggregateView,
    TableIndexView,
    Developers,
    ModelBuildModal,
    AggregateModal,
    TableIndexEdit,
    ConfirmSegment,
    DataFeatures,
    ModelActions,
    ModelRenameModal,
    ModelCloneModal,
    ModelPartition,
    Recommendations,
    ModelTitleDescription
  },
  locales
})
export default class ModelLayout extends Vue {
  randomKey = Date.now().toString(32)
  initData = false
  currentModelRow = null
  currentIndexTab = 'indexOverview'
  modelName = ''
  searchModelName = ''
  buildVisible = {}
  showModelList = false
  showSearchResult = false
  modelListFilters = {}
  filterData = {
    page_offset: 0,
    page_size: pageCount,
    exact: false,
    model_name: this.searchModelName,
    sort_by: 'last_modify',
    reverse: true,
    status: [],
    model_alias_or_owner: '',
    last_modify: [],
    owner: '',
    project: this.currentSelectedProject
  }

  created () {
    this.__init()
    document.addEventListener('click', this.handleClick)
  }

  async __init () {
    try {
      const { modelName } = this.$route.params
      this.modelName = modelName
      this.$nextTick(async () => {
        await this.loadModelList()
        this.initModelData()
      })
    } catch (e) {
      handleError(e)
    }
  }

  async initModelData () {
    try {
      await this.refreshModelData('init')
    } catch (e) {
      handleError(e)
    }
  }

  needShowBuildTips (uuid) {
    this.buildVisible[uuid] = !localStorage.getItem('hideBuildTips')
  }

  jumpBack () {
    this.$router.push({name: 'ModelList', query: { modelListFilters: JSON.stringify(this.modelListFilters) }})
  }

  // 模型搜索
  searchModel () {
    this.resetFilters()
    this.loadModelList()
  }

  resetFilters () {
    this.filterData = {
      page_offset: 0,
      page_size: pageCount,
      exact: false,
      model_name: this.searchModelName,
      sort_by: 'last_modify',
      reverse: true,
      status: [],
      model_alias_or_owner: '',
      last_modify: [],
      owner: '',
      project: this.currentSelectedProject
    }
  }

  chooseOtherModel ({model, ...args}) {
    if (model.status && model.status === 'BROKEN') return
    this.$router.push({name: 'refresh'})
    this.$nextTick(() => {
      this.$router.replace({name: 'ModelDetails', params: {modelName: model.alias, ...args}, query: {modelListFilters: JSON.stringify(this.modelListFilters), filterData: JSON.stringify(this.filterData)}})
    })
  }

  async loadModelList () {
    try {
      this.showSearchResult = true
      this.filterData.project = this.currentSelectedProject
      await this.loadModels(this.filterData)
      this.showSearchResult = false
    } catch (e) {
      handleError(e)
    }
  }

  // 仅刷新当前 model 数据
  async refreshModelData (type) {
    try {
      const response = await this.getModelByModelName({model_name: this.modelName, project: this.currentSelectedProject})
      const { value } = await handleSuccessAsync(response)
      // 能通过模型名称获取到模型数据，说明该模型存在
      if (value.length) {
        if (type === 'init') {
          const { jump, tabTypes, indexTab } = this.$route.params
          this.currentIndexTab = indexTab ?? 'indexOverview'
          const isNeedJumpToRecommendation = (jump && jump === 'recommendation') || (this.platform === 'iframe' && getQueryString('jump') === 'recommendation')
          this.currentModelRow = { tabTypes: isNeedJumpToRecommendation ? 'second' : (typeof tabTypes !== 'undefined' ? tabTypes : 'overview'), ...value[0] }
          if (isNeedJumpToRecommendation) {
            this.jumpToRecommendation()
          }
          if (this.currentModelRow.tabTypes === 'second' && localStorage.getItem('isFirstSaveModel') === 'true') {
            this.showGuide()
          }
        } else {
          this.currentModelRow = {...this.currentModelRow, ...value[0]}
        }
      } else {
        this.jumpBack()
      }
    } catch (e) {
      handleError(e)
    }
  }

  handleClick (e) {
    if (e.target.closest && !e.target.closest('.model-filter-list') && !e.target.closest('.icon--right')) {
      this.showModelList = false
    }
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
                <div class="el-message__content">
                  <span>{this.$t('kylinLang.common.submitSuccess')}</span>
                  <a href="javascript:void(0)" onClick={() => jumpToJobs()}>{this.$t('kylinLang.common.toJoblist')}</a>
                </div>
              )
            })
            this.reloadModel()
            this.refreshSegment(modelName)
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
      subTitle = this.$t('modelMetadataChangedDesc')
      refrashWarningSegment = true
      submitText = this.$t('kylinLang.common.refresh')
    } else {
      title = this.$t('buildIndex')
      subTitle = this.currentModelRow.model_type === 'HYBRID' ? this.$t('hybridModelBuildTitle') : this.$t('batchBuildSubTitle')
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

  async setModelBuldRange (modelDesc, isNeedBuildGuild) {
    if (!modelDesc.total_indexes && !isNeedBuildGuild || (!this.$store.state.project.multi_partition_enabled && modelDesc.multi_partition_desc)) return
    const projectName = this.currentSelectedProject
    const modelName = modelDesc.uuid
    const res = await this.fetchSegments({ projectName, modelName })
    const { total_size, value } = await handleSuccessAsync(res)
    let type = 'incremental'
    if (!(modelDesc.partition_desc && modelDesc.partition_desc.partition_date_column)) {
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

  // 更新 segment 列表
  reloadModelAndSegment (alias) {
    // this.loadModelsList()
    this.refreshSegment(alias)
  }

  async refreshSegment (alias) {
    this.$refs['segmentComp' + alias] && await this.$refs['segmentComp' + alias].$emit('refresh')
    // this.prevExpendContent = this.modelArray.filter(item => this.expandedRows.includes(item.alias))
    // this.$nextTick(() => {
    //   this.setModelExpand()
    // })
  }

  async willAddIndex (alias) {
    this.$refs['segmentComp' + alias] && await this.$refs['segmentComp' + alias].$emit('willAddIndex')
  }

  // 跳转并展示优化建议界面
  jumpToRecommendation () {
    this.$nextTick(() => {
      this.currentIndexTab = 'recommendations'
      // this.$refs.modelAggregateItem && (this.$refs.modelAggregateItem.switchIndexValue = 'rec')
    })
  }

  // 更改模型名称
  changeModelName (name) {
    this.currentModelRow.alias = name
    this.reloadModel()
  }

  // 重新加载模型数据
  async reloadModel () {
    await this.refreshModelData()
    this.randomKey = Date.now().toString(32)
  }

  // 首次创建模型引导
  async showGuide () {
    await this.callGuideModal({ isShowBuildGuide: true, isStreamingModel: this.currentModelRow.model_type === 'STREAMING' })
    localStorage.setItem('isFirstSaveModel', 'false')
  }

  beforeDestroy () {
    document.removeEventListener('click', this.handleClick)
  }
}
</script>
<style lang="less">
  @import '../../../../../assets/styles/variables.less';
  .model-layout {
    height: 100%;
    .header-layout {
      height: 56px;
      width: 100%;
      padding: 0 14px;
      box-sizing: border-box;
      line-height: 56px;
      border-bottom: 1px solid #ECF0F8;
      background-color: @fff;
      position: relative;
      .model-alias-label {
        display: inline-block;
        height: 100%;
        margin-top: 10px;
        margin-left: 8px;
        max-width: 270px;
        .alias {
          margin-top: 1px;
          max-width: 300px;
          .model-alias-title {
            max-width: 90%;
          }
          .filter-status {
            top: -4px;
            margin-right: 2px;
          }
        }
        .last-modified-tooltip {
          margin-top: -4px;
          font-weight: initial;
        }
      }
      .title {
        font-weight: 600;
        line-height: 23px\0;
        display: flex;
        align-items: center;
        height: 100%;
        .el-button {
          vertical-align: middle;
        }
        .model-name {
          margin-left: -3px;
          .name {
            color: @text-title-color;
          }
        }
        i {
          cursor: pointer;
        }
      }
      .action-items {
        position: absolute;
        top: 50%;
        right: 10px;
        transform: translate(0, -50%);
        line-height: 23px;
        display: flex;
        align-items: center;
        .el-dropdown {
          position: inherit;
        }
        .el-dropdown-menu {
          max-width: 140px;
        }
      }
    }
    .model-detail-tabs.el-tabs--left {
      .el-tabs__item.is-left {
        padding: 0 20px;
      }
      .el-tabs__nav-wrap:not(.is-top) {
        border-right: 1px solid @ke-border-divider-color;
      }
      .segment-actions {
        margin-bottom: 10px;
        .segment-header-title {
          i {
            color: @text-disabled-color;
            vertical-align: text-top;
          }
        }
      }
    }
    .el-tabs__header.is-left {
      margin-right: 0;
    }
    .el-tabs__item.is-left {
      text-align: left;
    }
    .el-tabs--default {
      height: calc(~'100% - 56px');
      .el-tabs__header {
        margin: 0 1px 16px;
        .el-tabs__nav-wrap {
          width: 144px;
        }
      }
      .el-tabs__content {
        height: 100%;
        .tab-pane-item:not(.data-features) {
          padding: 24px;
          box-sizing: border-box;
        }
        #pane-overview {
          padding: 24px 0 0 0;
        }
        .el-tab-pane {
          height: 100%;
        }
        .el-tabs__header {
          .el-tabs__nav-wrap {
            width: inherit;
          }
        }
      }
    }
    .el-tabs--default .model-indexes-tabs {
      height: 100%;
      .el-tabs__content {
        overflow: visible;
        .el-card {
          overflow: initial;
          .el-card__body {
            overflow: initial;
          }
        }
      }
      .el-tabs__nav-scroll {
        background-color: @ke-background-color-white;
      }
      .el-tabs__content .tab-pane-item {
        padding: 0;
      }
    }
    .model-filter-list {
      position: absolute;
      padding: 0 0 14px 0;
      box-sizing: border-box;
      z-index: 9999;
      line-height: 56px;
      background: @ke-background-color-white;
      box-shadow: 0px 2px 8px rgba(50, 73, 107, 24%);
      border-radius: 6px;
      border: 1px solid @ke-border-divider-color;
      max-width: 240px;
      top: 56px;
      left: 100px;
      .search-bar {
        padding: 0 16px;
        box-sizing: border-box;
      }
      .model-list {
        max-height: 300px;
        overflow: auto;
        font-weight: initial;
        font-weight: normal\0;
        .no-data {
          text-align: center;
          color: @text-disabled-color;
        }
        .items {
          font-size: 14px;
          line-height: initial;
          line-height: normal\0;
          padding: 5px 16px;
          box-sizing: border-box;
          cursor: pointer;
          overflow: hidden;
          // text-overflow: ellipsis;
          &:hover {
            background-color: @ke-background-color-secondary;
          }

          .model-name {
            &.is-disabled {
              color: @text-disabled-color;
            }
          }

          .custom-tooltip-layout {
            line-height: 1;
            overflow: initial;
          }
        }
      }
    }
  }
</style>
