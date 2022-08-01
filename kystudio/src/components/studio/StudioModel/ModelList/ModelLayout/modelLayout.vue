<template>
  <div class="model-layout" :key="randomKey">
    <div class="header-layout">
      <div class="title"><el-button type="primary" text icon-button-mini icon="el-ksd-icon-arrow_left_16" size="small" @click="jumpBack"></el-button>
        <span class="model-name"><span class="name ksd-fs-16">{{modelName}}</span><el-button class="ksd-ml-2" type="primary" text @click.stop="showModelList = !showModelList" icon-button-mini icon="el-ksd-icon-arrow_down_16" size="small"></el-button></span>
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
        :currentModel="currentModelRow"
        :appendToBody="true"
        :editText="$t('modelEditAction')"
        :buildText="$t('modelBuildAction')"
        :moreText="$t('moreAction')"
        other-icon="el-ksd-icon-more_with_border_22"
      />
    </div>
    <el-tabs class="el-tabs--default model-detail-tabs" tab-position="left" v-if="currentModelRow" v-model="currentModelRow.tabTypes" :key="$lang">
      <el-tab-pane class="tab-pane-item" :label="$t('overview')" name="overview">
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
              :isShowEditAgg="datasourceActions.includes('editAggGroup')"
              :isShowBulidIndex="datasourceActions.includes('buildIndex')"
              :isShowTableIndexActions="datasourceActions.includes('tableIndexActions')"
              ref="modelAggregateItem"
              @refreshModel="refreshModelData"
              v-if="currentIndexTab === 'indexOverview'" />
          </el-tab-pane>
          <el-tab-pane class="tab-pane-item" v-if="datasourceActions.includes('editAggGroup')" :label="$t('aggregateGroup')" name="aggGroup">
            <ModelAggregateView
              :model="currentModelRow"
              :project-name="currentSelectedProject"
              :isShowEditAgg="datasourceActions.includes('editAggGroup')"
              @refreshModel="refreshModelData"
              v-if="currentIndexTab === 'aggGroup'" />
          </el-tab-pane>
          <el-tab-pane class="tab-pane-item" v-if="datasourceActions.includes('editAggGroup')" :label="$t('tableIndex')" name="tableIndex">
            <TableIndexView
              :model="currentModelRow"
              :project-name="currentSelectedProject"
              :isShowTableIndexActions="datasourceActions.includes('tableIndexActions')"
              @refreshModel="refreshModelData"
              v-if="currentIndexTab === 'tableIndex'" />
          </el-tab-pane>
        </el-tabs>
      </el-tab-pane>
      <el-tab-pane class="tab-pane-item" :label="$t('developers')" name="fifth">
        <Developers v-if="currentModelRow.tabTypes === 'fifth'" :currentModelRow="currentModelRow"/>
      </el-tab-pane>
      <!-- <el-tab-pane class="tab-pane-item" :label="$t('streaming')" name="streaming" v-if="currentModelRow.model_type !== 'BATCH'">
        <ModelStreamingJob v-if="currentModelRow.tabTypes === 'streaming'" class="ksd-mrl-15 ksd-mt-15" :model="currentModelRow.uuid"/>
      </el-tab-pane> -->
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
import { handleError, jumpToJobs } from 'util/business'
import { transToServerGmtTime, handleSuccessAsync } from 'util'
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
import ModelStreamingJob from '../ModelStreamingJob/ModelStreamingJob.vue'

@Component({
  beforeRouteEnter (to, from, next) {
    if (!from.name || from.name !== 'ModelList') {
      next((vm) => {
        vm.initData = true
        vm.modelPageOffest = to.query.modelPageOffest
        vm.__init()
      })
    } else {
      next((vm) => {
        vm.initData = true
        vm.modelPageOffest = to.query.modelPageOffest
        vm.initModelData()
      })
    }
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
    'forceUpdateRoute'
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
    ModelStreamingJob
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
  modelPageOffest = 0

  created () {
    // if (!this.initData) {
    //   this.initModelData()
    // }
    document.addEventListener('click', this.handleClick)
  }

  async __init () {
    await this.loadModelList()
    this.initModelData()
  }

  initModelData () {
    const { modelName, searchModelName, tabTypes } = this.$route.params
    this.modelName = modelName
    this.searchModelName = searchModelName || ''
    if (!this.modelList.filter(it => it.alias === this.modelName).length) {
      // 没有匹配到相应的 model
      this.$router.replace({name: 'ModelList'})
      return
    }
    this.currentModelRow = {...this.modelList.filter(it => it.alias === this.modelName)[0], tabTypes: typeof tabTypes !== 'undefined' ? tabTypes : 'overview'}
    if (this.currentModelRow.tabTypes === 'second' && localStorage.getItem('isFirstSaveModel') === 'true') {
      this.showGuide()
    }
  }

  needShowBuildTips (uuid) {
    this.buildVisible[uuid] = !localStorage.getItem('hideBuildTips')
  }

  jumpBack () {
    this.$router.push({name: 'ModelList'})
  }

  // 模型搜索
  searchModel (val) {
    this.loadModelList()
  }

  chooseOtherModel ({model, ...args}) {
    if (model.status && model.status === 'BROKEN') return
    this.$router.push({name: 'refresh'})
    this.$nextTick(() => {
      this.$router.replace({name: 'ModelDetails', params: {modelName: model.alias, searchModelName: this.searchModelName, ...args}, query: {modelPageOffest: this.modelPageOffest}})
    })
  }

  selectModel ({model, ...args}) {
    // this.$router.replace({name: 'ModelDetails', params: {modelName: model.alias, searchModelName: this.searchModelName, ...args}})
    // this.$nextTick(() => {
    //   this.forceUpdateRoute()
    // })
    // if (model.status && model.status === 'BROKEN') return
    // this.$router.push({name: 'refresh'})
    // this.$nextTick(() => {
    //   this.$router.replace({name: 'ModelDetails', params: {modelName: model.alias, searchModelName: this.searchModelName, ...args}, query: {modelPageOffest: this.modelPageOffest}})
    // })
    let data = {searchModelName: this.searchModelName, ...args}
    let modelData = this.modelList.filter(it => it.alias === this.modelName)
    this.modelName = model.alias
    if (!modelData.length) {
      this.$router.replace({name: 'ModelList'})
      return
    }
    this.$set(this, 'currentModelRow', {...modelData[0], tabTypes: typeof data.tabTypes !== 'undefined' ? data.tabTypes : 'overview'})
    this.randomKey = Date.now().toString(32)
  }

  loadModelList (name = '') {
    const { modelPageOffest } = this
    return new Promise((resolve, reject) => {
      const modelName = this.searchModelName || name
      this.showSearchResult = true
      this.loadModels({
        page_offset: modelPageOffest || 0,
        page_size: 10,
        exact: false,
        model_name: modelName || '',
        sort_by: 'last_modify',
        reverse: true,
        status: [],
        model_alias_or_owner: '',
        last_modify: [],
        owner: '',
        project: this.currentSelectedProject
      }).then(() => {
        this.showSearchResult = false
        resolve()
      }).catch((res) => {
        handleError(res)
        reject()
      })
    })
  }

  // 仅刷新当前 model 数据
  async refreshModelData () {
    const response = await this.getModelByModelName({model_name: this.modelName, project: this.currentSelectedProject})
    const { value } = await handleSuccessAsync(response)
    if (value.length) {
      this.currentModelRow = {...this.currentModelRow, ...value[0]}
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
                <div>
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
      isHybridBatch: this.currentModelRow.model_type === 'HYBRID',
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

  // 更改模型名称
  changeModelName (name) {
    this.currentModelRow.alias = name
    this.reloadModel()
  }

  // 重新加载模型数据
  async reloadModel () {
    await this.loadModelList()
    this.selectModel({model: this.currentModelRow, tabTypes: this.currentModelRow.tabTypes})
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
      // box-shadow: 1px 1px 4px #ccc;
      border-bottom: 1px solid #ECF0F8;
      background-color: @ke-background-color-secondary;
      position: relative;
      .title {
        display: inline-block;
        // height: 100%;
        font-weight: 600;
        line-height: 23px\0;
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
      .el-tabs__nav-wrap {
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
        margin: 0 0 16px;
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
      z-index: 10;
      line-height: 56px;
      background: @ke-background-color-white;
      box-shadow: 0px 2px 8px rgba(50, 73, 107, 24%);
      border-radius: 6px;
      border: 1px solid @ke-border-divider-color;
      max-width: 240px;
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
