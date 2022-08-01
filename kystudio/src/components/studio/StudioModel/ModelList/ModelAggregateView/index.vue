<template>
  <div class="model-aggregate-view" v-loading="isLoading">
    <div class="title-list">
      <div class="title-header">
        <el-select v-model="aggType" size="small" disabled class="agg-types" :class="{'no-advanced': model.model_type !== 'BATCH'}">
          <el-option
            v-for="item in options"
            :key="item"
            :label="$t(item)"
            :value="item">
          </el-option>
        </el-select>
        <div class="icon-group ksd-fright" v-if="isShowEditAgg">
          <common-tip :content="!editOrAddIndex ? $t('refuseAddIndexTip') : $t('addAggGroup')">
            <el-button text type="primary" :disabled="!editOrAddIndex" icon-button icon="el-ksd-icon-add_with_border_22" size="small" @click="(e) => handleAggregateGroup(e)"></el-button>
          </common-tip><common-tip :content="$t('aggAdvanced')" v-if="model.model_type === 'BATCH'">
            <el-button text type="primary" icon-button icon="el-ksd-icon-setting_old" size="small" @click="openAggAdvancedModal()"></el-button>
          </common-tip>
        </div>
      </div>
      <div class="agg-total-info-block" v-if="aggregationGroups.length">
        <div>{{$t('numTitle', {num: cuboidsInfo.total_count && cuboidsInfo.total_count.result})}}</div>
        <div>{{$t('maxDimCom')}}<common-tip :content="$t('maxDimComTips')"><i class="el-ksd-icon-more_info_22 ksd-fs-22 ksd-mrl-2"></i></common-tip>{{$t('colon')}}
        <span v-if="aggregationObj&&!aggregationObj.global_dim_cap" class="nolimit-dim">{{$t('noLimitation')}}</span>
        <span v-if="aggregationObj&&aggregationObj.global_dim_cap" class="global-dim">{{aggregationObj.global_dim_cap}}</span>
        </div>
      </div>
      <ul v-if="aggregationGroups.length">
        <li class="agg-title" @click="scrollToMatched(aggregateIdx)" v-for="(aggregate, aggregateIdx) in aggregationGroups" :key="aggregateIdx">
          {{$t('aggregateGroupTitle', { id: aggregateIdx + 1 })}}
        </li>
      </ul>
      <kylin-nodata v-else>
      </kylin-nodata>
    </div>
    <div class="agg-detail-block" v-if="aggregationGroups.length">
      <div class="agg-detail ksd-mb-15" v-for="(aggregate, aggregateIdx) in aggregationGroups" :key="aggregate.id">
        <div class="agg-content-title">
          <span class="ksd-fs-14 font-medium">
            {{$t('aggregateGroupTitle', { id: aggregateIdx + 1 })}}
          </span><span class="agg-type ksd-ml-10">
            {{$t('custom')}}</span><span class="ksd-ml-15">
            {{$t('numTitle', {num: cuboidsInfo.agg_index_counts[aggregateIdx] && cuboidsInfo.agg_index_counts[aggregateIdx].result})}}
          </span><span class="divide">
          </span><span class="dimCap-block">
            <span>{{$t('maxDimCom')}}<common-tip :content="$t('dimComTips')"><i class="el-ksd-icon-more_info_22 ksd-fs-22 ksd-mrl-2"></i></common-tip>{{$t('colon')}}
            </span>
            <span v-if="!aggregate.select_rule.dim_cap&&aggregationObj&&!aggregationObj.global_dim_cap" class="nolimit-dim">{{$t('noLimitation')}}</span>
            <span v-if="aggregate.select_rule.dim_cap&&aggregationObj">{{aggregate.select_rule.dim_cap}}</span>
            <span v-if="!aggregate.select_rule.dim_cap&&aggregationObj&&aggregationObj.global_dim_cap" class="global-dim">{{aggregationObj.global_dim_cap}}</span>
          </span>
          <span class="ksd-fright icon-group">
            <common-tip :content="!handleEditOrDelIndex(aggregate) ? $t('refuseEditIndexTip') : $t('kylinLang.common.edit')">
              <el-button text type="primary" icon-button icon="el-ksd-icon-edit_16" size="small" :disabled="!handleEditOrDelIndex(aggregate)" @click="editAggGroup(aggregateIdx)"></el-button>
            </common-tip><common-tip :content="!handleEditOrDelIndex(aggregate) ? $t('refuseRemoveIndexTip') : $t('kylinLang.common.delete')">
              <el-button text type="primary" icon-button icon="el-ksd-icon-table_delete_16" size="small" :disabled="!handleEditOrDelIndex(aggregate)" @click="deleteAggGroup(aggregateIdx)"></el-button>
            </common-tip>
          </span>
        </div>
        <h4 v-if="model.model_type === 'HYBRID'">
          <span>{{$t('indexTimeRange')}}</span> <span v-if="aggregate.index_range">{{$t('kylinLang.common.' + aggregate.index_range)}}</span>
        </h4>
        <el-tabs v-model="aggregate.activeTab">
          <el-tab-pane :label="$t(item.key, {size: aggregate[item.target].length, total: totalSize(item.name)})" :name="item.key" v-for="item in tabList" :key="item.key"></el-tab-pane>
        </el-tabs>
        <template v-if="aggregate.activeTab === 'dimension'">
          <!-- Include聚合组 -->
          <div class="row ksd-mb-16">
            <div class="title font-medium ksd-mb-8">{{$t('include')}}({{aggregate.includes.length}})</div>
            <div class="content">
              <el-tag :class="['ksd-mr-8', 'ksd-mt-8', {'is-used': showSelectedIncludes(aggregate, item)}]" size="mini" v-for="(item, index) in getIncludesDimension(aggregate)" :key="index">{{item}}</el-tag>
              <!-- {{aggregate.includes.join(', ')}} -->
              <span class="show-more" v-if="aggregate.showIncludeMore" @click="toggleMore(aggregate, 'Include')">{{$t('showAll')}}<i class="icon el-icon-ksd-more_01"></i></span>
            </div>
          </div>
          <!-- Mandatory聚合组 -->
          <div class="row ksd-mb-16">
            <div class="title font-medium ksd-mb-8">{{$t('mandatory')}}({{aggregate.mandatory.length}})</div>
            <div class="content">
              <el-tag class="ksd-mr-8 ksd-mt-8 is-used" size="mini" v-for="(item, index) in aggregate.mandatory" :key="index">{{item}}</el-tag>
              <!-- {{aggregate.mandatory.join(', ')}} -->
            </div>
          </div>
          <!-- Hierarchy聚合组 -->
          <div class="row ksd-mb-16">
            <div class="title font-medium ksd-mb-10">{{$t('hierarchy')}}({{aggregate.hierarchyArray.length}})</div>
            <div class="list content"
              v-for="(hierarchy, hierarchyRowIdx) in aggregate.hierarchyArray"
              :key="`hierarchy-${hierarchyRowIdx}`">
              {{$t('group') + `${hierarchyRowIdx + 1}`}}: <span>
                <el-tag class="ksd-mr-8 ksd-mt-8 is-used" size="mini" v-for="(item, index) in hierarchy.items" :key="index">{{item}}</el-tag>
                <!-- {{hierarchy.items.join(', ')}} -->
              </span>
            </div>
          </div>
          <!-- Joint聚合组 -->
          <div class="row ksd-mb-16">
            <div class="title font-medium ksd-mb-10">{{$t('joint')}}({{aggregate.jointArray.length}})</div>
            <div class="list content"
              v-for="(joint, jointRowIdx) in aggregate.jointArray"
              :key="`joint-${jointRowIdx}`">
              {{$t('group') + `-${jointRowIdx + 1}`}}: <span>
                <el-tag :class="['ksd-mr-8', 'ksd-mt-8', 'is-used']" size="mini" v-for="(item, index) in joint.items" :key="index">{{item}}</el-tag>
                <!-- {{joint.items.join(', ')}} -->
              </span>
              <p class="cardinality-multiple"><span>{{$t('cardinalityMultiple')}}</span><span>{{getMultipleCardinality(aggregateIdx, jointRowIdx)}}</span></p>
            </div>
          </div>
        </template>
        <template v-else>
          <div class="row ksd-mb-16">
            <div class="title font-medium ksd-mb-8">{{$t('includeMeasure')}}</div>
            <div class="content">
              <el-tag :class="['ksd-mr-8', 'ksd-mt-8', 'is-used']" size="mini" v-for="(item, index) in getAggregateMeasures(aggregate)" :key="index">{{item}}</el-tag>
              <!-- {{aggregate.measures.join(', ')}} -->
              <span class="show-more" v-if="aggregate.showMeasureMore" @click="toggleMore(aggregate, 'Measure')">{{$t('showAll')}}<i class="icon el-icon-ksd-more_01"></i></span>
            </div>
          </div>
        </template>
      </div>
    </div>
    <div class="agg-detail-block" v-else>
      <div class="empty-block">
        <div>{{$t('aggGroupTips')}}</div>
        <common-tip :content="$t('refuseAddIndexTip')" :disabled="editOrAddIndex">
          <el-button type="primary" text :disabled="!editOrAddIndex" icon="el-ksd-icon-table_add_old" @click="(e) => handleAggregateGroup(e)" v-if="isShowEditAgg">{{$t('aggGroup')}}</el-button>
        </common-tip>
      </div>
    </div>
    <AggAdvancedModal/>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccessAsync, objectClone, handleError, getFullMapping, kylinConfirm, ArrayFlat } from 'util'
import locales from './locales'
import AggAdvancedModal from './AggAdvancedModal/index.vue'
@Component({
  props: {
    model: {
      type: Object
    },
    projectName: {
      type: String
    },
    isShowEditAgg: {
      type: Boolean,
      default: true
    }
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'isOnlyQueryNode'
    ])
  },
  methods: {
    ...mapActions({
      fetchAggregateGroups: 'FETCH_AGGREGATE_GROUPS',
      getCalcCuboids: 'GET_AGG_CUBOIDS',
      updateAggregateGroups: 'UPDATE_AGGREGATE_GROUPS'
    }),
    ...mapActions('AggAdvancedModal', {
      callAggAdvancedModal: 'CALL_MODAL'
    }),
    ...mapActions('AggregateModal', {
      callAggregateModal: 'CALL_MODAL'
    })
  },
  components: {
    AggAdvancedModal
  },
  locales
})
export default class AggregateView extends Vue {
  aggType = 'custom'
  options = ['custom']
  aggregationObj = null
  aggregationGroups = []
  cuboidsInfo = {
    total_count: {},
    agg_index_counts: []
  }
  tabList = [
    {name: 'dimensions', key: 'dimension', size: 0, total: 0, target: 'includes'},
    {name: 'measures', key: 'measure', size: 0, total: 0, target: 'measures'}
  ]
  isLoading = false
  tagMaxLength = {
    includes: 20,
    measure: 20
  }
  indexUpdateEnabled = true
  get dimensions () {
    if (this.model) {
      return this.model.simplified_dimensions
        .filter(column => column.status === 'DIMENSION')
        .map(dimension => ({
          label: dimension.column,
          value: dimension.column,
          id: dimension.id
        }))
    } else {
      return []
    }
  }
  get measures () {
    return this.model ? this.model.simplified_measures.map(measure => ({label: measure.name, value: measure.name, id: measure.id})) : []
  }

  get editOrAddIndex () {
    return !(!this.indexUpdateEnabled && this.model.model_type === 'STREAMING')
  }

  handleEditOrDelIndex (aggregate) {
    return !(!this.indexUpdateEnabled && (this.model.model_type === 'STREAMING' || ['HYBRID', 'STREAMING'].includes(aggregate.index_range)))
  }

  getIncludesDimension (aggregate) {
    if (aggregate.showIncludeMore) {
      return aggregate.includes.slice(0, this.tagMaxLength.includes)
    } else {
      return aggregate.includes
    }
  }
  getAggregateMeasures (aggregate) {
    if (aggregate.showMeasureMore) {
      return aggregate.measures.slice(0, this.tagMaxLength.measure)
    } else {
      return aggregate.measures
    }
  }
  getMultipleCardinality (aggregateIdx, jointRowIdx) {
    const dimensionList = this.model ? this.model.simplified_dimensions.filter(column => column.status === 'DIMENSION') : []
    const joint = this.aggregationGroups[aggregateIdx].jointArray[jointRowIdx].items
    const valueList = dimensionList.filter(it => joint.includes(it.column)).map(d => d.cardinality).filter(it => !!it)
    return valueList.length ? (valueList.reduce((prev, next) => prev * next) + '').replace(/\d{1,3}(?=(\d{3})+$)/g, (v) => `${v},`) : '--'
  }
  showSelectedIncludes (aggregate, currentItem) {
    return [...aggregate.mandatory, ...ArrayFlat(aggregate.hierarchyArray.map(it => it.items)), ...ArrayFlat(aggregate.jointArray.map(it => it.items))].includes(currentItem)
  }
  totalSize (name) {
    return this[name].length
  }
  async handleAggregateGroup (event) {
    event && event.target.parentElement.className.split(' ').includes('icon') && event.target.parentElement.blur()
    const { projectName, model } = this
    const { isSubmit } = await this.callAggregateModal({ editType: 'new', model, projectName, indexUpdateEnabled: this.indexUpdateEnabled })
    isSubmit && this.loadAggregateGroups()
    isSubmit && this.$emit('refreshModel')
  }
  async editAggGroup (aggregateIdx) {
    const { projectName, model } = this
    const { isSubmit } = await this.callAggregateModal({ editType: 'edit', model, projectName, aggregateIdx: aggregateIdx + '', indexUpdateEnabled: this.indexUpdateEnabled })
    isSubmit && this.loadAggregateGroups()
    isSubmit && this.$emit('refreshModel')
  }
  async deleteAggGroup (aggregateIdx) {
    try {
      await kylinConfirm(this.$t('delAggregateTip', {aggId: aggregateIdx + 1}), {type: 'warning', confirmButtonText: this.$t('kylinLang.common.delete')}, this.$t('delAggregateTitle'))
      this.aggregationObj.aggregation_groups.splice(aggregateIdx, 1)
      const { projectName, model } = this
      await this.updateAggregateGroups({
        projectName,
        modelId: model.uuid,
        isCatchUp: false,
        globalDimCap: this.aggregationObj.global_dim_cap,
        aggregationGroups: this.aggregationObj.aggregation_groups
      })
      this.loadAggregateGroups()
      this.$emit('refreshModel')
    } catch (e) {
      handleError(e)
    }
  }
  // 打开高级设置
  openAggAdvancedModal () {
    this.callAggAdvancedModal({
      model: objectClone(this.model),
      aggIndexAdvancedDesc: null
    })
  }
  async mounted () {
    await this.loadAggregateGroups()
    this.$on('refresh', () => {
      this.loadAggregateGroups()
    })
  }
  async loadAggregateGroups () {
    this.isLoading = true
    try {
      const projectName = this.currentSelectedProject
      const modelId = this.model.uuid
      const nameMapping = this.getMapping(this.dimensions)
      const measuresMapping = this.getMapping(this.measures)
      const res = await this.fetchAggregateGroups({ projectName, modelId })
      const data = await handleSuccessAsync(res)
      this.indexUpdateEnabled = data.index_update_enabled
      if (data) {
        const calcRes = await this.getCalcCuboids({ projectName, modelId, aggregationGroups: data.aggregation_groups, globalDimCap: data.global_dim_cap })
        this.cuboidsInfo = await handleSuccessAsync(calcRes)

        this.aggregationObj = objectClone(data)
        this.aggregationGroups = data.aggregation_groups.map((aggregationGroup, aggregateIdx) => {
          aggregationGroup.id = data.aggregation_groups.length - aggregateIdx
          aggregationGroup.activeTab = 'dimension'
          aggregationGroup.includes = aggregationGroup.includes.map(include => nameMapping[include])
          aggregationGroup.measures = aggregationGroup.measures.map(measures => measuresMapping[measures])
          const selectRules = aggregationGroup.select_rule
          aggregationGroup.mandatory = selectRules.mandatory_dims.map(mandatory => nameMapping[mandatory])
          aggregationGroup.jointArray = selectRules.joint_dims.map((jointGroup, groupIdx) => {
            const items = jointGroup.map(joint => nameMapping[joint])
            return { id: groupIdx, items }
          })
          aggregationGroup.hierarchyArray = selectRules.hierarchy_dims.map((hierarchyGroup, groupIdx) => {
            const items = hierarchyGroup.map(hierarchy => nameMapping[hierarchy])
            return { id: groupIdx, items }
          })
          aggregationGroup.showIncludeMore = aggregationGroup.includes.length > this.tagMaxLength.includes
          aggregationGroup.showMeasureMore = aggregationGroup.measures.length > this.tagMaxLength.measure
          return aggregationGroup
        })
      }
      this.isLoading = false
    } catch (e) {
      handleError(e)
      this.isLoading = false
    }
  }
  getMapping (data) {
    const mapping = data.reduce((mapping, item) => {
      mapping[item.value] = item.id
      return mapping
    }, {})
    return getFullMapping(mapping)
  }
  scrollToMatched (index) {
    this.$nextTick(() => {
      const detailContents = this.$el.querySelectorAll('.agg-detail-block .agg-detail')
      this.$el.querySelector('.agg-detail-block').scrollTop = detailContents[index].offsetTop - 15
    })
  }
  // 是否展示更多维度或度量
  toggleMore (aggregate, type) {
    aggregate[`show${type}More`] = !aggregate[`show${type}More`]
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
.model-aggregate-view {
  height: calc(~'100% - 40px');
  overflow: auto;
  width: 100%;
  display: flex;
  .el-button {
    line-height: normal\0;
  }
  .title-list {
    width: 220px;
    height: 100%;
    box-shadow:2px 2px 4px 0px rgba(229,229,229,1);
    background-color: @fff;
    float:left;
    overflow-y: auto;
    padding-top: 15px;
    position: relative;
    flex-shrink: 0;
    .title-header {
      padding: 0 10px 10px 10px;
      line-height: 24px;
      .agg-types {
        width: 140px;
        &.no-advanced {
          width: 168px;
        }
      }
    }
    .agg-title {
      height: 40px;
      line-height: 40px;
      cursor: pointer;
      padding-left: 10px;
      font-size: 14px;
      &:hover {
        background-color: @base-color-9;
      }
    }
  }
  .agg-total-info-block {
    background-color: @background-disabled-color;
    padding: 10px;
    color: @text-normal-color;
    font-size: 12px;
  }
  .agg-detail-block {
    height: 100%;
    margin: 15px;
    overflow-y: auto;
    padding-bottom: 15px;
    position: relative;
    min-width: 750px;
    flex: 1;
    .content {
      color: @text-normal-color;
      .is-used {
        background-color: #EAFFEA;
        color: #4CB050;
        border: 1px solid #4CB050;
      }
    }
    .icon-group i:hover {
      color: @base-color;
    }
    .agg-detail {
      padding: 15px;
      background-color: @fff;
      border: 1px solid @line-border-color4;
      .agg-content-title {
        height: 30px;
        line-height: 30px;
        margin-bottom: 10px;
        .agg-type {
          font-size: 12px;
          border: 1px solid @text-disabled-color;
          color: @text-disabled-color;
          border-radius: 2px;
          padding: 0 2px;
        }
        .divide {
          border-left: 1px solid @line-border-color;
          margin: 0 10px;
        }
        .dimCap-block {
          .nolimit-dim {
            color: @text-disabled-color;
          }
          .global-dim {
            font-style: oblique;
            color: @text-disabled-color;
          }
        }
      }
      .show-more {
        color: #0988DE;
        font-size: 12px;
        cursor: pointer;
        .icon {
          margin-left: 5px;
          font-size: 10px;
        }
      }
      .cardinality-multiple {
        font-size: 12px;
        color: @text-normal-color;
        margin-top: 5px;
      }
    }
    .empty-block {
      text-align: center;
      color:@text-disabled-color;
      position: absolute;
      top: 50%;
      text-align: center;
      width: 100%;
    }
  }
}
</style>
