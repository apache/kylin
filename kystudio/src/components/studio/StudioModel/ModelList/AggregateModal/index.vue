<template>
  <div class="aggregate-modal" :class="{'brief-Menu': briefMenuGet}" v-if="isShow">
    <div class="aggregate-dialog">
      <div class="header">
        <div class="el-dialog__title ksd-mb-10">{{$t(modalTitle)}}
        </div>
        <el-alert
          class="ksd-pt-0"
          :title="$t('aggGroupTip')"
          type="info"
          :show-background="false"
          :closable="false"
          show-icon>
        </el-alert>
      </div>
      <!-- <div class="loading" v-if="isLoading" v-loading="isLoading"></div> -->
      <div class="agg-group-layout">
        <div v-if="model" class="agg-list" ref="aggListLayout">
          <!-- 聚合组按钮 -->
          <div v-loading="calcLoading || isSubmit"
                element-loading-text=" "
                element-loading-spinner=" "
                element-loading-background="rgba(0, 0, 0, 0)">
            <div class="content">
              <!-- 聚合组表单 -->
              <div class="aggregate-group" v-for="(aggregate, aggregateIdx) in form.aggregateArray" :key="aggregate.id" :class="{'js_exceedLimit': !isWaitingCheckCuboids[aggregate.id] && renderCoboidTextCheck(cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregate.id]) === 'overLimit'}">
                <div class="header">
                  <h1 class="title font-medium">
                    <i @click="openAggregateItem(aggregateIdx)" :class="['el-icon-caret-right', {'is-open': aggregate.open}]"></i>
                    {{$t('aggregateGroupTitle', { id: aggregateIdx + 1 })}}
                  </h1>
                  <span class="ksd-ml-15 ksd-fleft">
                    <!-- 超出上限的情况 -->
                    <span class="cuboid-error" v-if="!isWaitingCheckCuboids[aggregate.id] && renderCoboidTextCheck(cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregate.id]) === 'overLimit'"><span class="cuboid-result errorClass">( {{$t('exceedLimitTitle')}}<el-tooltip :content="$t('maxCombinationNum', {num: maxCombinationNum})"><i class="el-ksd-icon-more_info_16 ksd-ml-5"></i></el-tooltip> )</span></span>
                    <!-- 数字的情况 -->
                    <span v-if="!isWaitingCheckCuboids[aggregate.id] && renderCoboidTextCheck(cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregate.id]) === 'number'"><span class="cuboid-result">{{$t('numTitle', {num: isNeedCheck ? $t('needCheck') : cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregate.id] && cuboidsInfo.agg_index_counts[aggregate.id].result})}}</span></span>
                    <!-- 待检测的情况 -->
                    <span v-if="isWaitingCheckCuboids[aggregate.id]">{{$t('numTitle', {num: $t('needCheck')})}}</span>
                    <!-- 正在检测的情况 -->
                    <span v-if="!isWaitingCheckCuboids[aggregate.id] && renderCoboidTextCheck(cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregate.id]) === 'loading'">{{$t('numTitle1')}}<i class="el-icon-loading"></i></span>
                  </span>
                  <div class="dimCap-block ksd-ml-10">
                    <span class="divide"></span>
                    <span>{{$t('maxDimCom')}}<common-tip :content="$t('dimComTips')"><i class="el-ksd-icon-more_info_16 ksd-mrl-2"></i></common-tip>{{$t('colon')}}
                    </span>
                    <span v-if="!aggregate.isEditDim&&!aggregate.dimCap&&!form.globalDimCap" class="nolimit-dim">{{$t('noLimitation')}}</span>
                    <span v-if="!aggregate.isEditDim&&aggregate.dimCap">{{aggregate.dimCap}}</span>
                    <span v-if="!aggregate.isEditDim&&!aggregate.dimCap&&form.globalDimCap" class="global-dim">{{form.globalDimCap}}</span>
                    <el-input
                      class="dim-input"
                      v-if="aggregate.isEditDim"
                      size="mini"
                      :clearable="false"
                      v-model="groupsDim[aggregateIdx]"
                      v-number2="groupsDim[aggregateIdx]"
                      :placeholder="!groupsDim[aggregateIdx]?(form.globalDimCap?form.globalDimCap+'':$t('noLimitation')) : ''"
                    ></el-input>
                    <span v-if="!aggregate.isEditDim">
                      <common-tip :content="$t('kylinLang.common.edit')"><i class="dim-btn el-ksd-icon-edit_22 ksd-ml-5" @click.stop="editDimCan(aggregateIdx, true)"></i></common-tip>
                    </span>
                    <span v-else>
                      <i class="el-icon-ksd-right ksd-ml-6" @click.stop="saveDimCan(aggregateIdx)"></i><i class="el-icon-ksd-error_02 ksd-ml-10" @click.stop="editDimCan(aggregateIdx, false)"></i>
                    </span>
                  </div>
                  <div class="actions" v-show="!(!indexUpdateEnabled && ['HYBRID', 'STREAMING'].includes(aggregate.index_range))">
                    <!-- <el-button type="mini" @click="() => openAggregateItem(aggregateIdx)">{{ aggregate.open ? $t('retract') : $t('open') }}</el-button> -->
                    <common-tip :content="$t('kylinLang.common.copy')"><i class="el-ksd-icon-iconcopy-1_old ksd-fs-16" @click.stop="() => handleCopyAggregate(aggregateIdx)"></i></common-tip>
                    <common-tip :content="$t('kylinLang.common.delete')"><i class="el-ksd-icon-table_delete_16 ksd-fs-16" @click.stop="() => handleDeleteAggregate(aggregateIdx, aggregateIdx + 1)"></i></common-tip>
                  </div>
                </div>
                <div class="body" :class="{'overLimit': !isWaitingCheckCuboids[aggregate.id] && renderCoboidTextCheck(cuboidsInfo.agg_index_counts && cuboidsInfo.agg_index_counts[aggregate.id]) === 'overLimit', 'open': aggregate.open}" :style="aggregateStyle[aggregateIdx] ? aggregateStyle[aggregateIdx] : (!aggregate.open && {'display': 'none'})">
                  <div class="contain">
                    <div class="row" v-if="model.model_type === 'HYBRID'">
                      <h2 class="title font-medium">
                        <span class="is-required">*</span>
                        <span>{{$t('indexTimeRange')}}</span>
                        <common-tip :content="$t('indexTimeRangeTips')"><i class="el-ksd-icon-more_info_16 ksd-fs-16"></i></common-tip>
                      </h2>
                      <el-radio-group v-model="form.aggregateArray[aggregateIdx].index_range" :disabled="form.aggregateArray[aggregateIdx].curAggIsEdit">
                        <el-tooltip placement="top" :disabled="indexUpdateEnabled || form.aggregateArray[aggregateIdx].curAggIsEdit" :content="$t('refuseAddIndexTip')">
                          <el-radio :label="'HYBRID'" :disabled="!indexUpdateEnabled">{{$t('kylinLang.common.HYBRID')}}</el-radio>
                        </el-tooltip>
                        <el-radio :label="'BATCH'">{{$t('kylinLang.common.BATCH')}}</el-radio>
                        <el-tooltip placement="top" :disabled="indexUpdateEnabled || form.aggregateArray[aggregateIdx].curAggIsEdit" :content="$t('refuseAddIndexTip')">
                          <el-radio :label="'STREAMING'" :disabled="!indexUpdateEnabled">{{$t('kylinLang.common.STREAMING')}}</el-radio>
                        </el-tooltip>
                      </el-radio-group>
                    </div>
                    <el-tabs v-model="aggregate.activeTab" @tab-click="handleClickTab">
                      <el-tab-pane :label="$t(item.key, {size: aggregate[item.target].length, total: totalSize(item.name)})" :name="item.key" v-for="item in tabList" :key="item.key"></el-tab-pane>
                    </el-tabs>
                    <template v-if="aggregate.activeTab === 'dimension'">
                      <!-- Include聚合组 -->
                      <div class="row">
                        <div class="ksd-mb-10">
                          <span class="title font-medium include-title"><span class="is-required">*</span> {{$t('include')}}</span>
                          <div class="row ksd-fright ky-no-br-space">
                            <common-tip placement="top" :content="$t('refuseAddIndexTip')"
                              :disabled="!(!indexUpdateEnabled && ['HYBRID', 'STREAMING'].includes(aggregate.index_range))">
                              <el-button
                                plain
                                class="ksd-ml-10"
                                size="mini"
                                :disabled="!indexUpdateEnabled && ['HYBRID', 'STREAMING'].includes(aggregate.index_range)"
                                @click="handleRemoveAllIncludes(aggregateIdx, aggregateIdx + 1, aggregate.id)"
                              >{{$t('clearAll')}}</el-button>
                            </common-tip>
                            <common-tip placement="top" :content="(model.model_type === 'HYBRID' && !form.aggregateArray[aggregateIdx].index_range) ? $t('disableAddDim') : $t('refuseAddIndexTip')"
                              :disabled="!(model.model_type === 'HYBRID' && !form.aggregateArray[aggregateIdx].index_range) || (!indexUpdateEnabled && ['HYBRID', 'STREAMING'].includes(aggregate.index_range))">
                              <el-button
                                plain
                                size="mini"
                                icon="el-ksd-icon-edit_22"
                                class="add-all-item"
                                type="primary"
                                :disabled="(model.model_type === 'HYBRID' && !form.aggregateArray[aggregateIdx].index_range) || (!indexUpdateEnabled && ['HYBRID', 'STREAMING'].includes(aggregate.index_range))"
                                @click="handleEditIncludes(aggregateIdx, aggregate.id)">{{$t('edit')}}</el-button>
                            </common-tip>
                          </div>
                        </div>
                        <div class="include-agg">
                          <template v-if="aggregate.includes.length">
                            <el-tag :class="{'is-active': currentSelectedTag.ctx === item && currentSelectedTag.aggregateIdx === aggregateIdx, 'is-used': showSelectedIncludes(aggregate, item)}" size="small" v-for="(item, index) in aggregate.includes" :key="index" :closable="!(!indexUpdateEnabled && ['HYBRID', 'STREAMING'].includes(aggregate.index_range))" @click.native="handleClickTag(item, aggregate.activeTab, aggregateIdx)" @close.stop="removeIncludesTag(item, aggregateIdx)">{{item}}</el-tag>
                          </template>
                          <div class="no-includes" v-else>
                            <span>{{$t('noIncludesTip')}}</span>
                            <common-tip placement="top" :content="$t('disableAddDim')" :disabled="!(model.model_type === 'HYBRID' && !form.aggregateArray[aggregateIdx].index_range)">
                              <span
                                class="add-includes-btn"
                                :class="{'disabled': model.model_type === 'HYBRID' && !form.aggregateArray[aggregateIdx].index_range}"
                                @click="handleEditIncludes(aggregateIdx, aggregate.id)"><i class="el-ksd-icon-table_add_old ksd-mr-2"></i>{{$t('kylinLang.common.add')}}</span>
                            </common-tip>
                          </div>
                        </div>
                      </div>
                      <div class="dimension-group">
                        <div class="layout-mask" v-show="!indexUpdateEnabled && ['HYBRID', 'STREAMING'].includes(aggregate.index_range)"></div>
                        <!-- Mandatory聚合组 -->
                        <div class="row mandatory">
                          <h2 class="title font-medium">{{$t('mandatory')}}
                            <common-tip placement="right" :content="$t('mandatoryDesc')">
                              <i class="el-ksd-icon-more_info_16"></i>
                            </common-tip>
                          </h2>
                          <el-select
                            multiple
                            filterable
                            class="mul-filter-select mandatory-select"
                            popper-class="js_mandatory-select"
                            :class="{'reset-padding': aggregate.mandatory.length}"
                            :value="aggregate.mandatory"
                            :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
                            @change="value => handleInput(`aggregateArray.${aggregateIdx}.mandatory`, value, aggregate.id)">
                            <i slot="prefix" v-show="!aggregate.mandatory.length" class="el-input__icon el-ksd-icon-search_22"></i>
                            <el-option
                              v-for="dimension in getUnusedDimensions(aggregateIdx)"
                              :key="dimension.value"
                              :label="dimension.label"
                              :value="dimension.value">
                            </el-option>
                          </el-select>
                        </div>
                        <!-- Hierarchy聚合组 -->
                        <div class="row hierarchy">
                          <h2 class="title font-medium">{{$t('hierarchy')}}
                            <common-tip placement="right" :content="$t('hierarchyDesc')">
                              <i class="el-ksd-icon-more_info_16"></i>
                            </common-tip>
                          </h2>
                          <div class="list"
                            v-for="(hierarchy, hierarchyRowIdx) in aggregate.hierarchyArray"
                            :key="`hierarchy-${hierarchyRowIdx}`">
                            <el-select
                              multiple
                              filterable
                              class="mul-filter-select hierarchy-select"
                              popper-class="js_hierarchy-select"
                              :class="{'reset-padding': hierarchy.items.length}"
                              :value="hierarchy.items"
                              :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
                              @change="value => handleInput(`aggregateArray.${aggregateIdx}.hierarchyArray.${hierarchyRowIdx}.items`, value, aggregate.id)">
                              <i slot="prefix" v-show="!hierarchy.items.length" class="el-input__icon el-ksd-icon-search_22"></i>
                              <el-option
                                v-for="dimension in getUnusedDimensions(aggregateIdx)"
                                :key="dimension.value"
                                :label="dimension.label"
                                :value="dimension.value">
                              </el-option>
                            </el-select>
                            <div class="list-actions clearfix ky-no-br-space">
                              <el-button circle plain type="primary" size="mini" icon="el-ksd-icon-add_22"
                                @click="handleAddDimensionRow(`aggregateArray.${aggregateIdx}.hierarchyArray`, aggregate.id)">
                              </el-button>
                              <el-button circle class="delete" size="mini" icon="el-ksd-icon-minus_22"
                                :disabled="aggregate.hierarchyArray.length === 1"
                                @click="handleRemoveDimensionRow(`aggregateArray.${aggregateIdx}.hierarchyArray`, aggregateIdx, hierarchyRowIdx, aggregate.id)">
                              </el-button>
                            </div>
                          </div>
                        </div>
                        <!-- Joint聚合组 -->
                        <div class="row joint">
                          <h2 class="title font-medium">{{$t('joint')}}
                            <common-tip placement="right" :content="$t('jointDesc')">
                              <i class="el-ksd-icon-more_info_16"></i>
                            </common-tip>
                          </h2>
                          <div class="list"
                            v-for="(joint, jointRowIdx) in aggregate.jointArray"
                            :key="`joint-${jointRowIdx}`">
                            <el-select
                              multiple
                              filterable
                              class="mul-filter-select joint-select"
                              popper-class="js_joint-select"
                              :class="{'reset-padding': joint.items.length}"
                              :value="joint.items"
                              :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
                              @change="value => handleInput(`aggregateArray.${aggregateIdx}.jointArray.${jointRowIdx}.items`, value, aggregate.id)">
                              <i slot="prefix" v-show="!joint.items.length" class="el-input__icon el-ksd-icon-search_22"></i>
                              <el-option
                                v-for="dimension in getUnusedDimensions(aggregateIdx)"
                                :key="dimension.value"
                                :label="dimension.label"
                                :value="dimension.value">
                              </el-option>
                            </el-select>
                            <div class="list-actions clearfix ky-no-br-space">
                              <el-button circle plain type="primary" size="mini" icon="el-ksd-icon-add_22"
                                @click="handleAddDimensionRow(`aggregateArray.${aggregateIdx}.jointArray`, aggregate.id)">
                              </el-button>
                              <el-button circle class="delete" size="mini" icon="el-ksd-icon-minus_22"
                                :disabled="aggregate.jointArray.length === 1"
                                @click="handleRemoveDimensionRow(`aggregateArray.${aggregateIdx}.jointArray`, aggregateIdx, jointRowIdx, aggregate.id)">
                              </el-button>
                            </div>
                            <p class="cardinality-multiple"><span>{{$t('cardinalityMultiple')}}</span><span>{{getMultipleCardinality(aggregateIdx, jointRowIdx)}}</span></p>
                          </div>
                        </div>
                      </div>
                    </template>
                    <template v-else>
                      <div class="row">
                        <div class="ksd-mb-10">
                          <span class="title font-medium measure-title">{{$t('includeMeasure')}}</span>
                          <div class="row ksd-mb-10 ksd-fright ky-no-br-space">
                            <!-- <el-button plain size="mini" class="add-all-item" type="primary" @click="handleAddAllMeasure(aggregateIdx, aggregate.id)">{{$t('selectAllMeasure')}}<el-tooltip class="item tip-item" popper-class='aggregate-tip' effect="dark" :content="$t('measureTabTip')" placement="bottom"><i class="el-icon-ksd-what"></i></el-tooltip></el-button> -->
                            <common-tip placement="top" :content="$t('refuseAddIndexTip')"
                              :disabled="!(!indexUpdateEnabled && ['HYBRID', 'STREAMING'].includes(aggregate.index_range))">
                              <el-button plain size="mini" class="ksd-ml-10"
                                :disabled="!indexUpdateEnabled && ['HYBRID', 'STREAMING'].includes(aggregate.index_range)"
                                @click="handleRemoveAllMeasure(aggregateIdx, aggregateIdx+1, aggregate.id)">{{$t('clearAll')}}</el-button>
                            </common-tip>
                            <common-tip placement="top" :content="(model.model_type === 'HYBRID' && !form.aggregateArray[aggregateIdx].index_range) ? $t('disableAddDim') : $t('refuseAddIndexTip')"
                              :disabled="!(model.model_type === 'HYBRID' && !form.aggregateArray[aggregateIdx].index_range) || (!indexUpdateEnabled && ['HYBRID', 'STREAMING'].includes(aggregate.index_range))">
                              <el-button plain size="mini" class="add-all-item" type="primary"
                                :disabled="(model.model_type === 'HYBRID' && !form.aggregateArray[aggregateIdx].index_range) || (!indexUpdateEnabled && ['HYBRID', 'STREAMING'].includes(aggregate.index_range))"
                                @click="handleEditMeasures(aggregateIdx, aggregate.id)"><i class="el-ksd-icon-edit_16 ksd-mr-5"></i>{{$t('edit')}}</el-button>
                            </common-tip>
                          </div>
                        </div>
                        <!-- <el-select
                          multiple
                          filterable
                          class="mul-filter-select"
                          :class="{'reset-padding': aggregate.measures.length}"
                          :ref="`aggregate.measures.${aggregateIdx}`"
                          :value="aggregate.measures"
                          :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')"
                          @input="value => handleInput(`aggregateArray.${aggregateIdx}.measures`, value, aggregate.id)"
                          @remove-tag="value => handleRemoveMeasureRules(value, aggregateIdx, aggregate.id)">
                          <i slot="prefix" v-show="!aggregate.measures.length" class="el-input__icon el-ksd-icon-search_22"></i>
                          <el-option
                            v-for="measure in measures"
                            :key="measure.value"
                            :label="measure.label"
                            :disabled="measure.value === 'COUNT_ALL'"
                            :value="measure.value">
                          </el-option>
                        </el-select> -->
                        <div class="include-measure">
                          <el-tag :class="{'is-active': currentSelectedTag.ctx === item && currentSelectedTag.aggregateIdx === aggregateIdx}" size="small" v-for="(item, index) in aggregate.measures" :key="index" @click.native="handleClickTag(item, aggregate.activeTab, aggregateIdx)">{{item}}</el-tag>
                        </div>
                      </div>
                    </template>
                  </div>
                </div>
              </div>
              <div class="aggregate-buttons ksd-mt-20">
                <el-button type="primary" icon="el-ksd-icon-add_22" @click="handleAddAggregate">{{$t('addAggregateGroup')}}</el-button>
              </div>
            </div>
          </div>
        </div>
        <div class="metadata-detail">
          <template v-if="currentSelectedTag.type === 'dimension'">
            <!-- && currentSelectedTag.data.sample -->
            <template v-if="currentSelectedTag.ctx && currentSelectedTag.data.simple">
              <p class="title">{{$t('statistics')}}</p>
              <el-table class="statistics_list_table"
                v-scroll-shadow
                :data="characteristicsData"
                border
                tooltip-effect="dark"
                style="width: 100%">
                <el-table-column
                  show-overflow-tooltip
                  :label="$t('column')">
                  <template slot-scope="scope">
                    {{$t(scope.row.column)}}
                  </template>
                </el-table-column>
                <el-table-column
                  prop="value"
                  show-overflow-tooltip
                  :label="currentSelectedTag.ctx">
                  <span slot-scope="scope">
                    <template v-if="scope.row.value === null"><i class="no-data_placeholder">NULL</i></template>
                    <template v-else>{{ scope.row.value }}</template>
                  </span>
                </el-table-column>
              </el-table>
              <p class="title">{{$t('sample')}}</p>
              <el-table class="statistics_list_table"
                v-scroll-shadow
                :data="sampleData"
                border
                tooltip-effect="dark"
                style="width: 100%">
                <el-table-column
                  width="60"
                  prop="key"
                  show-overflow-tooltip
                >
                </el-table-column>
                <el-table-column
                  prop="value"
                  show-overflow-tooltip
                  :label="currentSelectedTag.ctx">
                </el-table-column>
              </el-table>
            </template>
            <template v-else>
              <div class="noData">
                <template v-if="currentSelectedTag.isCC">
                  <i class="icon el-ksd-icon-select_old"></i><p class="tip">{{$t('ccDimensionTip')}}</p>
                </template>
                <template v-else-if="!currentSelectedTag.ctx">
                  <i class="icon el-ksd-icon-select_old"></i><p class="tip">{{$t('noSelectDimensionTip')}}</p>
                </template>
                <template v-else-if="!currentSelectedTag.data.simple">
                  <i class="icon el-icon-ksd-sampling"></i><p class="tip">{{$t('noSamplingTip')}}</p>
                </template>
              </div>
            </template>
          </template>
          <template v-else>
            <template v-if="currentSelectedTag.ctx">
              <p class="title">{{$t('property')}}</p>
              <label for="measure">
                <p class="title2">{{$t('measureName')}}</p>
                <p class="content">{{currentSelectedTag.data.name}}</p>
              </label>
              <label for="expression">
                <p class="title2">{{$t('expression')}}</p>
                <p class="content">{{currentSelectedTag.data.expression}}</p>
              </label>
              <label for="parameters">
                <p class="title2">{{$t('parameters')}}</p>
                <div v-for="(item, index) in currentSelectedTag.data.parameter_value" :key="index">
                  <p class="content"><span class="label">Type：</span>{{item.type}}</p>
                  <p class="content"><span class="label">Value：</span>{{item.value}}</p>
                </div>
              </label>
              <label for="returnType">
                <p class="title2">{{$t('returnType')}}</p>
                <p class="content">{{currentSelectedTag.data.return_type}}</p>
              </label>
            </template>
            <template v-else>
              <div class="noData">
                <i class="icon el-ksd-icon-select_old"></i><p class="tip">{{$t('noSelectMeasureTip')}}</p>
              </div>
            </template>
          </template>
        </div>
      </div>

      <div slot="footer" class="dialog-footer clearfix">
        <div class="left">
          <span class="ksd-fleft">
            <!-- 超出上限的情况 -->
            <span class="cuboid-error" v-if="!isWaitingCheckAllCuboids && renderCoboidTextCheck(cuboidsInfo.total_count) === 'overLimit'"><span class="cuboid-result errorClass">( {{$t('exceedLimitTitle')}}<el-tooltip :content="$t('maxCombinationTotalNum', {num: maxCombinationNum, numTotal: maxCombinationNum * 10 + 1})"><i class="el-ksd-icon-more_info_16 ksd-ml-5"></i></el-tooltip> )</span></span>
            <!-- 数字的情况 -->
            <span v-if="!isWaitingCheckAllCuboids && renderCoboidTextCheck(cuboidsInfo.total_count) === 'number'"><span class="cuboid-result">{{$t('numTitle', {num: isNeedCheck ? $t('needCheck') : cuboidsInfo.total_count.result})}}</span></span>
            <!-- 待检测的情况 -->
            <span v-if="isWaitingCheckAllCuboids">{{$t('numTitle', {num: $t('needCheck')})}}</span>
            <!-- 正在检测的情况 -->
            <span v-if="!isWaitingCheckAllCuboids && renderCoboidTextCheck(cuboidsInfo.total_count) === 'loading'">{{$t('numTitle1')}}<i class="el-icon-loading"></i></span>
            <common-tip :content="$t('includesEmpty')" v-if="isDisabledSaveBtn" >
              <i class="el-ksd-icon-refresh_22 ksd-fs-22 ksd-ml-10 is-disabled" @click="checkCuboids(true)"></i>
            </common-tip>
            <i class="el-ksd-icon-refresh_22 ksd-fs-22 ksd-ml-10" v-else @click="checkCuboids(true)"></i>
          </span>
          <div class="dimCap-block ksd-ml-10">
            <span class="divide"></span>
            <span>{{$t('maxDimCom')}}<common-tip :content="$t('maxDimComTips')"><i class="el-ksd-icon-more_info_16 ksd-mrl-2"></i></common-tip>{{$t('colon')}}
            </span>
            <span v-if="!isEditGlobalDim&&!form.globalDimCap">{{$t('noLimitation')}}</span>
            <span v-if="!isEditGlobalDim&&form.globalDimCap">{{form.globalDimCap}}</span>
            <el-input class="dim-input" v-if="isEditGlobalDim" :placeholder="form.globalDimCap?'':$t('noLimitation')" size="mini" :clearable="false" v-number2="globalDim" v-model="globalDim"></el-input>
            <span v-if="!isEditGlobalDim">
              <common-tip :content="$t('kylinLang.common.edit')" v-show="isShowTooltips"><i
                class="dim-btn el-ksd-icon-edit_16 ksd-fs-16 ksd-ml-5" @click="editGlobalDim"></i>
              </common-tip><common-tip
                v-show="isShowTooltips"
                :content="clearTips">
                <i class="dim-btn el-ksd-icon-clear_old ksd-fs-16 ksd-ml-10" :class="{'disable-clear': !form.isDimClearable}" @click = clearDims()></i>
              </common-tip>
            </span>
            <span v-else>
              <el-popover
                ref="popover"
                placement="top"
                width="160"
                trigger="click"
                v-model="popoverVisible">
                <p>{{$t('dimConfirm')}}</p>
                <div style="text-align: right; margin: 0">
                  <el-button size="mini" type="info" text @click="popoverVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
                  <el-button type="primary" size="mini" @click="saveGlobalDim">{{$t('kylinLang.common.ok')}}</el-button>
                </div>
              </el-popover>
              <i class="el-icon-ksd-right ksd-ml-6" v-popover:popover></i><i class="el-icon-ksd-error_02 ksd-ml-10" @click="isEditGlobalDim = false"></i>
            </span>
          </div>
        </div>
        <div class="right ksd-fs-0">
          <el-button :type="!onlyRealTimeType ? 'primary' : ''" :text="!onlyRealTimeType" size="medium" @click="handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button :type="onlyRealTimeType ? 'primary' : ''" size="medium" class="ksd-ml-10" :disabled="isDisabledSaveBtn" v-if="isShow" :loading="isSubmit" @click="handleSubmit(false)">{{$t('kylinLang.common.save')}}</el-button>
          <el-button v-if="isShow && !onlyRealTimeType" type="primary" size="medium" class="ksd-ml-10" :disabled="isDisabledSaveBtn" :loading="isSubmit" @click="handleSubmit(true)">{{$t('saveAndBuild')}}</el-button>
        </div>
      </div>
    </div>
    <!-- 编辑 includes 度量 -->
    <el-dialog
      class="edit-includes-dimensions"
      :title="$t('editIncludeDimensions')"
      width="1000px"
      :append-to-body="true"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      @close="editIncludeDimension = false"
      :visible="true"
      v-if="editIncludeDimension"
      ref="includesDimensionDialog"
    >
      <div class="action-layout">
        <p class="alert">{{$t('editIncludeDimensionTip')}}</p>
        <el-alert
          v-if="hasManyToManyAndAntiTable"
          class="ksd-pt-0"
          :title="$t('manyToManyAntiTableTip')"
          type="info"
          :show-background="false"
          :closable="false"
          show-icon>
        </el-alert>
        <div class="filter-dimension">
          <el-tooltip :content="$t('excludeTableCheckboxTip')" effect="dark" placement="top"><el-checkbox class="ksd-mr-5" v-model="displayExcludedTables" @change="changeExcludedTables" v-if="showExcludedTableCheckBox">{{$t('excludeTableCheckbox')}}</el-checkbox></el-tooltip>
          <el-input v-model="searchName" v-global-key-event.enter.debounce="filterChange" @clear="clearFilter" size="medium" prefix-icon="el-ksd-icon-search_22" style="width:240px" :placeholder="$t('kylinLang.common.pleaseFilter')"></el-input>
        </div>
      </div>
      <div class="ky-simple-table" @scroll="scrollEvent">
        <el-row class="table-header table-row dim-table-header">
          <el-col :span="1"><el-checkbox v-model="isSelectAllDimensions" :indeterminate="getSelectedIncludeDimensions.length > 0 && getSelectedIncludeDimensions.length < dimensions().length" @change="selectAllIncludes" size="small"/></el-col>
          <el-col :span="6" :key="dataDragData.width" :style="{width: dataDragData.width + 'px'}">{{$t('th_name')}}
            <div class="ky-table-drag-layout-line" unselectable="on" v-drag:change.width="dataDragData"></div>
          </el-col>
          <el-col :span="7" :key="dataDragData2.width" :style="{width: dataDragData2.width + 'px'}">{{$t('th_column')}}
            <div class="ky-table-drag-layout-line" unselectable="on" v-drag:change.width="dataDragData2"></div>
          </el-col>
          <el-col :span="3">{{$t('th_dataType')}}</el-col>
          <el-col :span="2">{{$t('cardinality')}}</el-col>
          <el-col :span="3">{{$t('th_info')}}</el-col>
          <el-col :span="2">{{$t('th_order')}}</el-col>
        </el-row>
        <transition-group name="flip-list" tag="div">
          <el-row class="table-row" v-for="(item, index) in includeDimensions" :key="item.id">
            <el-col :span="1"><el-checkbox size="small" :disabled="getDisabledTableType(item)" v-model="item.isCheck" @change="(val) => selectIncludDimensions(item, val)"/></el-col>
            <el-col :span="6" :key="dataDragData.width" :style="{width: dataDragData.width + 'px'}"><span class="text" v-custom-tooltip="{text: item.name, w: 20}">{{item.name}}</span></el-col>
            <el-col :span="7" :key="dataDragData2.width" :style="{width: dataDragData2.width + 'px'}"><span v-custom-tooltip="{text: item.column, w: 40}">{{item.column}}</span><el-tooltip :content="$t('excludedTableIconTip')" effect="dark" placement="top"><i class="excluded_table-icon el-icon-ksd-exclude" v-if="isExistExcludeTable(item) && displayExcludedTables"></i></el-tooltip></el-col>
            <el-col :span="3">{{item.type}}</el-col>
            <el-col :span="2">
              <template v-if="item.cardinality === null"><i class="no-data_placeholder">NULL</i></template>
              <template v-else>{{ item.cardinality }}</template>
            </el-col>
            <el-col :span="3"><span v-custom-tooltip="{text: item.comment, w: 20}">{{item.comment}}</span></el-col>
            <el-col :span="2" class="order-actions">
              <template v-if="item.isCheck">
                <span :class="['icon', 'el-icon-ksd-move_to_top', {'is-disabled': index === 0 && !searchName}]" @click="moveTo('top', item)"></span>
                <span :class="['icon', 'el-icon-ksd-move_up', {'is-disabled': index === 0}]" @click="moveTo('up', item)"></span>
                <span :class="['icon', 'el-icon-ksd-move_down', {'is-disabled': !includeDimensions[index + 1] || !includeDimensions[index + 1].isCheck}]" @click="moveTo('down', item)"></span>
                <!-- <el-tooltip :content="$t('moveTop')" effect="dark" placement="top">
                  <span :class="['icon', 'el-icon-ksd-move_to_top', {'is-disabled': index === 0}]" @click="moveTo('top', item)"></span>
                </el-tooltip>
                <el-tooltip :content="$t('moveUp')" effect="dark" placement="top">
                  <span :class="['icon', 'el-icon-ksd-move_up', {'is-disabled': index === 0}]" @click="moveTo('up', item)"></span>
                </el-tooltip>
                <el-tooltip :content="$t('moveDown')" effect="dark" placement="top">
                  <span :class="['icon', 'el-icon-ksd-move_down', {'is-disabled': !includeDimensions[index + 1] || !includeDimensions[index + 1].isCheck}]" @click="moveTo('down', item)"></span>
                </el-tooltip> -->
              </template>
            </el-col>
          </el-row>
        </transition-group>
      </div>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button size="medium" @click="editIncludeDimension = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" @click="saveIncludes" :disabled="!getSelectedIncludeDimensions.length">{{$t('ok')}}</el-button>
      </div>
    </el-dialog>
    <!-- 编辑度量 -->
    <el-dialog
      class="edit-measures"
      :title="$t('editMeasure')"
      width="1000px"
      :append-to-body="true"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      @close="editMeasure = false"
      :visible="true"
      v-if="editMeasure"
    >
      <div class="action-measure-layout">
        <!-- <p class="alert">{{$t('editMeasuresTip')}}</p> -->
        <div class="filter-measure">
          <el-input v-model="searchMeasure" v-global-key-event.enter.debounce="filterMeasure" @clear="clearMeasureFilter" size="medium" prefix-icon="el-ksd-icon-search_22" style="width:200px" :placeholder="$t('kylinLang.common.pleaseFilter')"></el-input>
        </div>
      </div>
      <div class="ky-simple-table measure-table">
        <el-row class="table-header measure-table-header table-row ksd-mt-10">
          <el-col :span="1"><el-checkbox v-model="isSelectAllMeasure" :indeterminate="getSelectedMeasures.length > 0 && getSelectedMeasures.length < measures.length" @change="selectAllMeasures" size="small"/></el-col>
          <el-col :span="5" :key="dataDragData3.width" :style="{width: dataDragData3.width + 'px'}">{{$t('th_name')}}
            <div class="ky-table-drag-layout-line" unselectable="on" v-drag:change.width="dataDragData3"></div>
          </el-col>
          <el-col :span="3">{{$t('expression')}}</el-col>
          <el-col :span="12" :key="dataDragData4.width" :style="{width: dataDragData4.width + 'px'}">{{$t('parameters')}}
            <div class="ky-table-drag-layout-line" unselectable="on" v-drag:change.width="dataDragData4"></div>
          </el-col>
          <el-col :span="3">{{$t('returnType')}}</el-col>
        </el-row>
        <el-row class="table-row" v-for="item in measureList" :key="item.id">
          <el-col :span="1"><el-tooltip :content="$t('disabledConstantMeasureTip')" :disabled="item.name !== 'COUNT_ALL'" placement="top-start" offset="10"><el-checkbox size="small" v-model="item.isCheck" :disabled="item.name === 'COUNT_ALL'" @change="(type) => changeMeasureBox(item, type)"/></el-tooltip></el-col>
          <el-col :span="5" :key="dataDragData3.width" :style="{width: dataDragData3.width + 'px'}"><span class="text" v-custom-tooltip="{text: item.name, w: 20}">{{item.name}}</span></el-col>
          <el-col :span="3">{{item.expression}}</el-col>
          <el-col :span="12" :key="dataDragData4.width" :style="{width: dataDragData4.width + 'px'}"><span v-custom-tooltip="{text: JSON.stringify(item.parameter_value), w: 20}">{{JSON.stringify(item.parameter_value)}}</span></el-col>
          <el-col :span="3">{{item.return_type}}</el-col>
        </el-row>
      </div>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button size="medium" @click="editMeasure = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" @click="saveMeasures">{{$t('ok')}}</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from 'store'
import locales from './locales'
import { BuildIndexStatus } from 'config/model'
import store, { types, initialAggregateData } from './store'
import { titleMaps, getPlaintDimensions, findIncludeDimension } from './handler'
import { handleError, get, set, push, kylinConfirm, handleSuccessAsync, sampleGuid, objectClone, ArrayFlat } from 'util'
import { handleSuccess } from 'util/business'
import common_tip from '../../../../common/common_tip.vue'

vuex.registerModule(['modals', 'AggregateModal'], store)

@Component({
  components: { common_tip },
  computed: {
    ...mapState('AggregateModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      indexUpdateEnabled: state => state.indexUpdateEnabled,
      editType: state => state.editType,
      callback: state => state.callback,
      model: state => state.model,
      projectName: state => state.projectName,
      aggregateIdx: state => state.aggregateIdx,
      formDataLoaded: state => state.formDataLoaded
    }),
    ...mapGetters('AggregateModal', [
      'dimensions',
      'dimensionIdMapping',
      'measures',
      'measureIdMapping'
    ]),
    ...mapGetters([
      'briefMenuGet'
    ])
  },
  methods: {
    ...mapMutations('AggregateModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM,
      showLoading: types.SHOW_LOADING,
      hideLoading: types.HIDE_LOADING
    }),
    ...mapActions({
      updateAggregateGroups: 'UPDATE_AGGREGATE_GROUPS',
      getCalcCuboids: 'GET_AGG_CUBOIDS',
      getIndexDiff: 'GET_INDEX_DIFF'
    }),
    ...mapMutations({
      setChangedForm: 'SET_CHANGED_FORM',
      setProject: 'SET_PROJECT'
    })
  },
  locales
})
export default class AggregateModal extends Vue {
  isFormShow = false
  isDimensionShow = false
  isSubmit = false
  isWaitingCheckCuboids = {}
  isWaitingCheckAllCuboids = true
  maxCombinationNum = 0
  tabList = [
    {name: 'dimensions', key: 'dimension', size: 0, total: 0, target: 'includes'},
    {name: 'measures', key: 'measure', size: 0, total: 0, target: 'measures'}
  ]
  aggregateStyle = []
  isShowConfirm = false
  isShowBuildAndConfirm = false
  confirmMsg = ''
  isEditGlobalDim = false
  popoverVisible = false
  globalDim = null
  groupsDim = []
  isShowTooltips = false
  isNeedCheck = false
  cloneForm = ''
  characteristicsData = []
  sampleData = []
  currentSelectedTag = {
    ctx: '',
    type: 'dimension',
    data: {}
  }
  editIncludeDimension = false
  includeDimensions = []
  selectedIncludeDimension = []
  currentAggregateInfo = {}
  isSelectAllDimensions = false
  searchName = ''

  measureList = []
  editMeasure = false
  isSelectAllMeasure = false
  selectedMeasures = []
  searchMeasure = ''
  backUpDimensions = []
  pageOffset = 0
  pageSize = 50
  generateDeletedIndexes = true
  displayExcludedTables = false
  dataDragData = {
    width: 238,
    limit: {
      width: [50, 1000]
    }
  }
  dataDragData2 = {
    width: 300,
    limit: {
      width: [50, 1000]
    }
  }
  dataDragData3 = {
    width: 200,
    limit: {
      width: [50, 1000]
    }
  }
  dataDragData4 = {
    width: 476,
    limit: {
      width: [50, 1000]
    }
  }

  @Watch('$lang')
  changeCurrentLang (newVal, oldVal) {
    this.$nextTick(() => {
      this.$refs.aggListLayout && this.$refs.aggListLayout.click()
    })
  }

  get clearTips () {
    return this.form.isDimClearable ? this.$t('clearDimTips') : this.$t('disableClear')
  }
  get modalTitle () {
    return titleMaps[this.editType]
  }
  get isFormVaild () {
    const { aggregateArray } = this.form
    for (const { includes, measures } of aggregateArray) {
      if (!includes.length || !measures.length) return false
    }

    return true
  }
  get usedDimensions () {
    const dimensionMaps = {}
    this.form.aggregateArray.forEach(aggregate => {
      aggregate.includes.forEach(include => {
        dimensionMaps[include] = 1
      })
    })
    return Object.keys(dimensionMaps)
  }
  get getSelectedIncludeDimensions () {
    return this.selectedIncludeDimension
  }
  get getSelectedMeasures () {
    return this.selectedMeasures
  }

  // 是否展示屏蔽表 checkbox
  get showExcludedTableCheckBox () {
    return this.backUpDimensions.length ? this.backUpDimensions.filter(it => typeof it.depend_lookup_table !== 'undefined' && it.depend_lookup_table).length > 0 : false
  }

  // 是否存在多对多且被屏蔽的表
  get hasManyToManyAndAntiTable () {
    let flag = false
    for (let item of this.backUpDimensions) {
      if (this.getDisabledTableType(item)) {
        flag = true
        break
      }
    }
    return flag
  }

  get onlyRealTimeType () {
    return (this.model.model_type === 'HYBRID' && this.form.aggregateArray.filter(item => item.index_range === 'STREAMING').length === this.form.aggregateArray.length) || this.model.model_type === 'STREAMING'
  }

  // 当表为屏蔽表且表关联关系为多对多时，不能作为维度添加到索引中
  getDisabledTableType (col) {
    const [currentTable] = col.column.split('.')
    const { join_tables } = this.model
    const manyToManyTables = join_tables.filter(it => it.join_relation_type === 'MANY_TO_MANY').map(item => item.alias)
    return manyToManyTables.includes(currentTable) && this.isExistExcludeTable(col)
  }

  // 是否为屏蔽表的 column
  isExistExcludeTable (col) {
    return typeof col.depend_lookup_table !== 'undefined' ? col.depend_lookup_table : false
  }

  getMultipleCardinality (aggregateIdx, jointRowIdx) {
    const jointData = this.form.aggregateArray[aggregateIdx].jointArray[jointRowIdx].items
    let valueList = this.dimensions().filter(it => jointData.includes(it.column)).map(item => item.cardinality).filter(it => !!it)
    return valueList.length ? (valueList.reduce((prev, next) => prev * next) + '').replace(/\d{1,3}(?=(\d{3})+$)/g, (v) => `${v},`) : '--'
  }

  showSelectedIncludes (aggregate, currentItem) {
    return [...aggregate.mandatory, ...ArrayFlat(aggregate.hierarchyArray.map(it => it.items)), ...ArrayFlat(aggregate.jointArray.map(it => it.items))].includes(currentItem)
  }
  totalSize (name) {
    return name === 'dimensions' ? this[name]().length : this[name].length
  }
  getUnusedDimensions (aggregateIdx) {
    const aggregate = this.form.aggregateArray[aggregateIdx]
    const includes = aggregate.includes
    const mandatory = aggregate.mandatory
    const hierarchy = getPlaintDimensions(aggregate.hierarchyArray)
    const joint = getPlaintDimensions(aggregate.jointArray)

    return this.dimensions().filter(dimension => {
      return includes.includes(dimension.value) && !(
        hierarchy.includes(dimension.value) ||
        mandatory.includes(dimension.value) ||
        joint.includes(dimension.value)
      )
    })
  }
  cuboidsInfo = {
    total_count: {},
    agg_index_counts: {}
  }
  resetCuboidInfo () {
    this.cuboidsInfo = {
      total_count: {},
      agg_index_counts: {}
    }
  }
  calcLoading = false
  calcCuboids (isShowCheckSuccess) {
    // 防重复提交
    if (this.calcLoading) {
      return false
    }
    // 只要点击了计算，全局的那个待检标志就要置回 false
    this.isWaitingCheckAllCuboids = false
    // 所有 group 中的待检都变成 false
    for (let prop in this.isWaitingCheckCuboids) {
      this.isWaitingCheckCuboids[prop] = false
    }
    this.calcLoading = true
    // this.showLoading()
    let paramsData = this.getSubmitData()
    if (paramsData.dimensions.length <= 0) {
      this.calcLoading = false
      this.resetCuboidInfo()
      return
    }
    delete paramsData.dimensions // 后台处理规整顺序
    this.getCalcCuboids(paramsData).then((res) => {
      handleSuccess(res, (data) => {
        if (data) {
          this.cuboidsInfo = objectClone(data)
          // this.cuboidsInfo.agg_index_counts = data.agg_index_counts.reverse()
          let resultData = objectClone(data.agg_index_counts)
          // 用聚合组的唯一id 做标识
          this.cuboidsInfo.agg_index_counts = {}
          for (let i = 0; i < this.form.aggregateArray.length; i++) {
            let id = this.form.aggregateArray[i].id
            this.cuboidsInfo.agg_index_counts[id] = objectClone(resultData[i])
          }
          this.maxCombinationNum = data.max_combination_num
          let singleIsLimit = data.agg_index_counts.filter((item) => {
            return !/^\d+$/.test(item.result)
          })
          // 单个索引组的个数超了限制，显示报错，并不往下执行了
          if (singleIsLimit.length > 0) {
            this.$message.error(this.$t('maxCombinationTip'))
            // 操作滚动
            this.dealScrollToFirstError()
          } else if (!/^\d+$/.test(data.total_count.result)) {
            // 单个没超过，总量超了，显示总量的报错，也不往下执行了
            this.$message.error(this.$t('maxTotalCombinationTip'))
          } else {
            isShowCheckSuccess && this.$message.success(this.$t('calcSuccess'))
          }
        }
        this.calcLoading = false
        this.isNeedCheck = false
        // this.hideLoading()
      })
    }, (res) => {
      this.maxCombinationNum = 0
      // 获取个数失败，全局所有的文案应该处于待检状态
      for (let i = 0; i < this.form.aggregateArray.length; i++) {
        let id = this.form.aggregateArray[i].id
        this.isWaitingCheckCuboids[id] = true
      }
      this.resetCuboidInfo()
      this.calcLoading = false
      // this.hideLoading()
      // handleError(res)
      this.$message.error(this.$t('calcError'))
    })
  }
  get isDisabledSaveBtn () {
    // 正在计算的时候按钮disable，选的维度有空的时候，disable，聚合组数为0 时
    const cloneForm = objectClone(this.form)
    if (cloneForm && cloneForm.aggregateArray.length) {
      cloneForm.aggregateArray.forEach((a) => {
        delete a.activeTab // 切换tab不属于编辑内容变化
      })
    }
    return this.calcLoading || this.isSubmit || !this.isFormVaild || !this.form.aggregateArray || this.form.aggregateArray.length === 0 || this.cloneForm === JSON.stringify(cloneForm)
  }
  renderCoboidTextCheck (cuboidsInfo, id) {
    let cuboidText = ''
    if (this.isWaitingCheckCuboids[id]) { // 如果是待检，直接显示空
      cuboidText = ''
    } else {
      if (this.calcLoading) {
        cuboidText = 'loading'
      } else {
        if (cuboidsInfo && cuboidsInfo.result !== undefined) {
          if (cuboidsInfo.status !== 'SUCCESS') {
            cuboidText = 'cuboid-error'
          }
          if (!/^\d+$/.test(cuboidsInfo.result)) {
            cuboidText = 'overLimit'
          } else {
            cuboidText = 'number'
          }
        } else { // 如果 cuboidsInfo 没数据，就判断是否是在检测中
          cuboidText = ''
        }
      }
    }
    return cuboidText
  }
  @Watch('formDataLoaded')
  onFormDataLoaded (newVal, oldVal) {
    // 打开弹窗时，会有一个请求rule的接口，要等接口回来，再发calcCuboids的数目检测接口
    if (newVal) {
      for (let i = 0; i < this.form.aggregateArray.length; i++) {
        let id = this.form.aggregateArray[i].id
        this.isWaitingCheckCuboids[id] = true
      }
      const cloneForm = objectClone(this.form)
      if (cloneForm && cloneForm.aggregateArray.length) {
        cloneForm.aggregateArray.forEach((a) => {
          delete a.activeTab // 切换tab不属于编辑内容变化
        })
      }
      this.cloneForm = JSON.stringify(cloneForm)
      this.calcCuboids()
      this.$nextTick(() => {
        const detailContents = this.$el.querySelectorAll('.aggregate-modal .aggregate-dialog .aggregate-group')
        const index = this.aggregateIdx === -1 ? detailContents.length - 1 : this.aggregateIdx
        if (!detailContents[index]) return
        this.$el.querySelector('.aggregate-modal .aggregate-dialog').parentElement.scrollTop = detailContents[index].offsetTop - 100
      })
    }
  }
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      this.groupsDim = this.form.aggregateArray.map((agg) => {
        return agg.dimCap
      })
      this.isFormShow = true
      this.resetCuboidInfo()
      this.$nextTick(() => {
        if (this.$el.querySelector('.dialog-footer .left')) {
          this.$el.querySelector('.dialog-footer .left').onmouseover = () => {
            this.isShowTooltips = true
          }
          this.$el.querySelector('.dialog-footer .left').onmouseout = () => {
            this.isShowTooltips = false
          }
          this.$el.querySelector('.aggregate-modal .aggregate-dialog').onclick = (e) => {
            if (e.target.classList.value.indexOf('dim-btn') === -1 && e.target.parentElement.classList.value.indexOf('dim-input') === -1 && e.target.classList.value.indexOf('el-icon-ksd-right') === -1) {
              this.isEditGlobalDim = false
              const aggregateArray = get(this.form, 'aggregateArray')
              this.form.aggregateArray.forEach((agg) => {
                this.$set(agg, 'isEditDim', false)
              })
              this.groupsDim = aggregateArray.map((agg) => { return agg.dimCap })
              this.popoverVisible = false
            }
          }
        }
      })
    } else {
      setTimeout(() => {
        this.isFormShow = false
        this.groupsDim = []
      }, 300)
    }
  }
  toggleDimensionShow () {
    this.isDimensionShow = !this.isDimensionShow
  }
  toggleIncludeDimension (aggregateIdx, dimensionValue, isUsed) {
    const includeSelectRefId = `aggregate.include.${aggregateIdx}`
    if (!this.$refs[includeSelectRefId] || !this.$refs[includeSelectRefId][0]) return
    const includeSelectEl = this.$refs[includeSelectRefId][0].$el
    const includeOptEls = includeSelectEl.querySelectorAll('.el-select__tags span .el-tag')

    let targetDimensionEl = findIncludeDimension(includeOptEls, dimensionValue)
    targetDimensionEl && targetDimensionEl.setAttribute('data-tag', isUsed ? 'used' : '')
  }
  handleAddAggregate () {
    const aggregateArray = get(this.form, 'aggregateArray')
    const aggregateData = {
      ...JSON.parse(initialAggregateData),
      ...{ measures: this.reorganizedMeasures() },
      // id: aggregateArray.length
      id: sampleGuid()
    }
    this.aggregateStyle = []
    this.setModalForm({ aggregateArray: [ ...aggregateArray, aggregateData ] })
    this.isWaitingCheckCuboids[aggregateData.id] = true
    this.isWaitingCheckAllCuboids = true
    this.aggregateStyle = []
    // this.calcCuboids()
    this.$nextTick(() => {
      const scrollTop = this.$el.querySelector('.aggregate-modal .aggregate-dialog').parentElement.scrollTop
      this.$el.querySelector('.aggregate-modal .aggregate-dialog').parentElement.scrollTop = scrollTop + 370
    })
  }
  editDimCan (aggregateIdx, isEdit) {
    const aggregateArray = get(this.form, 'aggregateArray')
    // 如果没有选具体类型，就不往下执行
    if (this.model.model_type === 'HYBRID' && !aggregateArray[aggregateIdx].index_range) {
      return false
    }
    aggregateArray[aggregateIdx].isEditDim = isEdit
    this.setModalForm({ aggregateArray })
    this.groupsDim[aggregateIdx] = aggregateArray[aggregateIdx].dimCap
  }
  saveDimCan (aggregateIdx) {
    const aggregateArray = get(this.form, 'aggregateArray')
    aggregateArray[aggregateIdx].dimCap = this.groupsDim[aggregateIdx]
    aggregateArray[aggregateIdx].isEditDim = false
    this.setModalForm({ aggregateArray })
    this.setModalForm({isDimClearable: set(this.form, 'isDimClearable', !!this.groupsDim[aggregateIdx])['isDimClearable']})
    this.isNeedCheck = true
  }
  handleCopyAggregate (aggregateIdx) {
    const aggregateArray = get(this.form, 'aggregateArray')
    const copyedAggregate = {
      ...aggregateArray[aggregateIdx],
      // id: aggregateArray.length
      id: sampleGuid()
    }
    this.setModalForm({ aggregateArray: [...aggregateArray, copyedAggregate] })
    this.groupsDim = [...aggregateArray, copyedAggregate].map((agg) => {
      return agg.dimCap
    })
    this.isWaitingCheckCuboids[copyedAggregate.id] = true
    this.isWaitingCheckAllCuboids = true
    this.aggregateStyle = []
    // this.calcCuboids()
    this.$nextTick(() => {
      const detailContents = this.$el.querySelectorAll('.aggregate-modal .aggregate-dialog .aggregate-group')
      this.$el.querySelector('.aggregate-modal .aggregate-dialog').parentElement.scrollTop = detailContents[this.form.aggregateArray.length - 1].offsetTop - 100
    })
  }
  handleDeleteAggregate (aggregateIdx, titleId) {
    kylinConfirm(this.$t('delAggregateTip', {aggId: titleId}), {type: 'warning'}, this.$t('delAggregateTitle')).then(() => {
      const aggregateArray = get(this.form, 'aggregateArray')
      aggregateArray.splice(aggregateIdx, 1)
      this.setModalForm({ aggregateArray })
      // this.isWaitingCheckCuboids = true
      this.isWaitingCheckAllCuboids = true
      this.aggregateStyle = []
      // this.calcCuboids()
    })
  }
  handleAddDimensionRow (path, id) {
    const rootKey = path.split('.')[0]
    const dimensionRows = get(this.form, path)
    const newId = dimensionRows.length
    const newDimensionRow = { id: newId, items: [] }
    this.setModalForm({[rootKey]: push(this.form, path, newDimensionRow)[rootKey]})
    this.isWaitingCheckCuboids[id] = true
    this.isWaitingCheckAllCuboids = true
    // this.calcCuboids()
  }
  handleRemoveDimensionRow (path, aggregateIdx, dimensionRowIndex, id) {
    const rootKey = path.split('.')[0]
    const dimensionRows = get(this.form, path)
    if (dimensionRows.length > 1) {
      dimensionRows.splice(dimensionRowIndex, 1)[0]
      this.setModalForm({[rootKey]: set(this.form, path, dimensionRows)[rootKey]})
    }
    this.isWaitingCheckCuboids[id] = true
    this.isWaitingCheckAllCuboids = true
    // this.calcCuboids()
  }
  beforeDestroy () {
    // 销毁组件前需要重置组件里的相关信息，以防切换模型，展开聚合组时，信息混乱
    this.hideModal()
    this.isEditGlobalDim = false
    this.setChangedForm(false)
    this.resetModalForm()
    this.callback && this.callback(false)
  }
  get isEditForm () {
    const cloneForm = objectClone(this.form)
    if (cloneForm && cloneForm.aggregateArray.length) {
      cloneForm.aggregateArray.forEach((a) => {
        delete a.activeTab // 切换tab不属于编辑内容变化
      })
    }
    return this.cloneForm !== JSON.stringify(cloneForm) && this.isShow
  }
  @Watch('isEditForm')
  onChangeForm (val) {
    this.setChangedForm(val)
  }
  async handleClose (isSubmit) {
    // 有修改时取消确认
    if (!isSubmit && this.isEditForm) {
      await kylinConfirm(this.$t('kylinLang.common.unSavedTips'), {type: 'warning', confirmButtonText: this.$t('kylinLang.common.discardChanges')}, this.$t('kylinLang.common.tip'))
    }
    this.hideModal()
    this.isEditGlobalDim = false
    this.generateDeletedIndexes = true
    setTimeout(() => {
      this.resetModalForm()
      this.setChangedForm(false)
      this.callback && this.callback({isSubmit})
    }, 200)
  }
  handleInput (key, value, id) {
    if (key !== 'isCatchUp') {
      this.isWaitingCheckCuboids[id] = true
      this.isWaitingCheckAllCuboids = true
      // this.calcCuboids()
    }
    const rootKey = key.split('.')[0]
    this.setModalForm({[rootKey]: set(this.form, key, value)[rootKey]})
    this.isNeedCheck = true
  }
  // 移除 includes tags
  removeIncludesTag (value, aggregateIdx) {
    let includes = this.form.aggregateArray[aggregateIdx].includes
    const index = includes.findIndex(it => it === value)
    index >= 0 && includes.splice(index, 1)
    this.handleRemoveIncludeRules(value, aggregateIdx)
  }
  handleRemoveIncludeRules (removedValue, aggregateIdx, id) {
    const { aggregateArray = [] } = this.form
    const { mandatory, hierarchyArray, jointArray } = aggregateArray[aggregateIdx]

    if (mandatory.includes(removedValue)) {
      const mandatoryKey = `aggregateArray.${aggregateIdx}.mandatory`
      this.handleInput(mandatoryKey, mandatory.filter(item => item !== removedValue), id)
    }
    hierarchyArray.forEach((hierarchyGroup, hierarchyRowIdx) => {
      const hierarchy = hierarchyGroup.items
      if (hierarchy.includes(removedValue)) {
        const hierarchyKey = `aggregateArray.${aggregateIdx}.hierarchyArray.${hierarchyRowIdx}.items`
        this.handleInput(hierarchyKey, hierarchy.filter(item => item !== removedValue), id)
      }
    })
    jointArray.forEach((jointGroup, jointRowIdx) => {
      const joint = jointGroup.items
      if (joint.includes(removedValue)) {
        const jointKey = `aggregateArray.${aggregateIdx}.jointArray.${jointRowIdx}.items`
        this.handleInput(jointKey, joint.filter(item => item !== removedValue), id)
      }
    })
  }
  // handleAddAllIncludes (aggregateIdx, id) {
  //   const allDimensions = this.dimensions.map(dimension => dimension.label)
  //   this.handleInput(`aggregateArray.${aggregateIdx}.includes`, allDimensions, id)
  // }
  handleRemoveAllIncludes (aggregateIdx, titleId, id) {
    kylinConfirm(this.$t('clearAllAggregateTip', {aggId: titleId}), {type: 'warning'}, this.$t('clearAggregateTitle')).then(() => {
      const { aggregateArray = [] } = this.form
      const currentAggregate = aggregateArray[aggregateIdx] || {}
      const currentIncludes = currentAggregate.includes || []

      for (const include of currentIncludes) {
        this.handleRemoveIncludeRules(include, aggregateIdx)
      }
      this.handleInput(`aggregateArray.${aggregateIdx}.includes`, [], id)
    })
  }
  handleBuildIndexTip (data) {
    let tipMsg = this.$t('kylinLang.model.saveIndexSuccess', {indexType: this.$t('kylinLang.model.aggregateGroupIndex')})
    if (this.form.isCatchUp) {
      if (data.type === BuildIndexStatus.NORM_BUILD) {
        tipMsg += ' ' + this.$t('kylinLang.model.buildIndexSuccess1', {indexType: this.$t('kylinLang.model.aggregateGroupIndex')})
        this.$message({
          type: 'success',
          dangerouslyUseHTMLString: true,
          duration: 10000,
          showClose: true,
          message: (
            <div>
              <span>{tipMsg}</span>
              <a href="javascript:void(0)" onClick={() => this.jumpToJobs()}>{this.$t('kylinLang.common.toJoblist')}</a>
            </div>
          )
        })
        return
      }
      if (data.type === BuildIndexStatus.NO_LAYOUT) {
        tipMsg = this.$t('kylinLang.model.buildIndexFail2', {indexType: this.$t('kylinLang.model.aggregateGroupIndex')})
        this.$confirm(tipMsg, this.$t('kylinLang.common.notice'), {showCancelButton: false, type: 'warning', dangerouslyUseHTMLString: true})
      } else if (data.type === BuildIndexStatus.NO_SEGMENT) {
        tipMsg += '<br/>' + this.$t('kylinLang.model.buildIndexFail1', {modelName: this.model.name})
        this.$confirm(tipMsg, this.$t('kylinLang.common.notice'), {showCancelButton: false, type: 'success', dangerouslyUseHTMLString: true})
      }
    } else {
      this.$message({message: tipMsg, type: 'success'})
    }
  }
  checkCuboids (isShowCheckSuccess) {
    if (this.isDisabledSaveBtn) { return }
    if (this.checkFormVaild()) {
      // 每次检测前，reset cuboid的信息
      this.resetCuboidInfo()
      this.calcCuboids(isShowCheckSuccess)
    }
  }
  dealScrollToFirstError () {
    this.$nextTick(() => {
      // 第一个数量超过的元素
      if (this.$el.querySelector('.js_exceedLimit')) {
        let firstErrorDomTop = document.querySelector('.js_exceedLimit').offsetTop - 100
        this.$el.querySelector('.aggregate-modal .aggregate-dialog').scrollTop = firstErrorDomTop
      }
    })
  }
  async handleSubmit (isCatchUp) {
    // 该字段只有在保存并构建时才会用到，纯流模型是屏蔽保存并构建的
    const isHaveBatchSegment = this.model.model_type === 'HYBRID' ? this.model.batch_segments.length > 0 : this.model.segments.length > 0
    // 保存并全量构建时，可以直接提交构建任务，保存并增量构建时，需弹出segment list选择构建区域
    if (isCatchUp && isHaveBatchSegment && !this.model.partition_desc || !isCatchUp) {
      this.setModalForm({isCatchUp: set(this.form, 'isCatchUp', isCatchUp)['isCatchUp']})
    }
    this.isSubmit = true
    // this.showLoading()
    try {
      if (this.checkFormVaild()) {
        const data = this.getSubmitData()
        delete data.dimensions // 后台处理规整顺序
        // 重置总的计算标志
        this.isWaitingCheckAllCuboids = false
        for (let prop in this.isWaitingCheckCuboids) {
          this.isWaitingCheckCuboids[prop] = true
        }
        // 发一个获取数据的接口
        let cuboidsRes = await this.getCalcCuboids(data)
        let cuboidsResult = await handleSuccessAsync(cuboidsRes)
        this.isNeedCheck = false
        for (let prop in this.isWaitingCheckCuboids) {
          this.isWaitingCheckCuboids[prop] = false
        }
        if (cuboidsResult) {
          this.maxCombinationNum = cuboidsResult.max_combination_num
          this.cuboidsInfo = objectClone(cuboidsResult)
          // this.cuboidsInfo.agg_index_counts = cuboidsResult.agg_index_counts.reverse()
          let resultData = objectClone(cuboidsResult.agg_index_counts)
          // 用聚合组的唯一id 做标识
          this.cuboidsInfo.agg_index_counts = {}
          for (let i = 0; i < this.form.aggregateArray.length; i++) {
            let id = this.form.aggregateArray[i].id
            this.cuboidsInfo.agg_index_counts[id] = objectClone(resultData[i])
          }
          let singleIsLimit = cuboidsResult.agg_index_counts.filter((item) => {
            return !/^\d+$/.test(item.result)
          })
          // 单个索引组的个数超了限制，显示报错，并不往下执行了
          if (singleIsLimit.length > 0) {
            this.$message.error(this.$t('maxCombinationTip'))
            this.calcLoading = false
            this.isSubmit = false
            // this.hideLoading()
            // 操作滚动
            this.dealScrollToFirstError()
            return false
          } else if (!/^\d+$/.test(cuboidsResult.total_count.result)) {
            // 单个没超过，总量超了，显示总量的报错，也不往下执行了
            this.$message.error(this.$t('maxTotalCombinationTip'))
            this.calcLoading = false
            this.isSubmit = false
            // this.hideLoading()
            return false
          }
        }
        let diffRes = await this.getIndexDiff(data)
        let diffResult = await handleSuccessAsync(diffRes)
        await this.confirmSubmitTips(diffResult, isCatchUp)
        // 获取数字正常的情况下，才进行 submit
        let res = await this.submit({...data, restoreDeletedIndex: !this.generateDeletedIndexes})
        let result = await handleSuccessAsync(res)
        // if (!isCatchUp && !this.model.segments.length) {
        //   this.$emit('needShowBuildTips', this.model.uuid)
        // }
        if (isCatchUp && !isHaveBatchSegment) {
          this.$emit('openBuildDialog', this.model, true)
        }
        // 保存并增量构建时，需弹出segment list选择构建区域
        if (isCatchUp && isHaveBatchSegment && this.model.partition_desc && this.model.partition_desc.partition_date_column) {
          this.$emit('openComplementAllIndexesDialog', this.model)
        }
        if (result.warn_code && result.type === 'NORM_BUILD') {
          this.confirmBuildIndexTip()
        } else {
          this.handleBuildIndexTip(result)
        }
        this.isSubmit = false
        this.handleClose(true)
        // this.hideLoading()
      } else {
        this.isSubmit = false
        // this.hideLoading()
      }
    } catch (e) {
      this.calcLoading = false
      for (let prop in this.isWaitingCheckCuboids) {
        this.isWaitingCheckCuboids[prop] = false
      }
      e && handleError(e)
      this.isSubmit = false
      // this.hideLoading()
    }
  }
  confirmBuildIndexTip () {
    kylinConfirm(this.$t('buildIndexTip'), {showCancelButton: false, confirmButtonText: this.$t('kylinLang.common.ok'), type: 'error', closeOnClickModal: false, showClose: false, closeOnPressEscape: false}, this.$t('kylinLang.common.tip'))
  }
  editGlobalDim () {
    this.globalDim = this.form.globalDimCap
    this.isEditGlobalDim = true
  }
  saveGlobalDim () {
    this.setModalForm({globalDimCap: set(this.form, 'globalDimCap', this.globalDim)['globalDimCap']})
    this.setModalForm({isDimClearable: set(this.form, 'isDimClearable', !!this.globalDim)['isDimClearable']})
    this.popoverVisible = false
    this.isEditGlobalDim = false
    this.isNeedCheck = true
  }
  handleCloseConfirm () {
    this.isShowConfirm = false
    this.isShowBuildAndConfirm = false
    this.confirmMsg = ''
    this.isSubmit = false
  }
  async clearDims () {
    if (!this.form.isDimClearable) {
      return
    }
    await kylinConfirm(this.$t('clearConfirm'), {type: 'warning', confirmButtonText: this.$t('clearbtn')})
    this.clearAllDim()
  }
  clearAllDim () {
    this.setModalForm({globalDimCap: set(this.form, 'globalDimCap', null)['globalDimCap']})
    this.setModalForm({isDimClearable: set(this.form, 'isDimClearable', false)['isDimClearable']})
    const aggregateArray = get(this.form, 'aggregateArray')
    aggregateArray.forEach((agg) => {
      agg.dimCap = null
    })
    this.globalDim = null
    this.groupsDim.forEach((dim) => {
      dim = null
    })
    this.setModalForm({ aggregateArray })
    this.$message.success(this.$t('clearSuccess'))
    this.isNeedCheck = true
  }
  confirmSubmitTips (diffResult, isCatchUp) {
    const saveText = isCatchUp ? `${this.$t('confirmTextBySaveAndBuild')}${this.model.model_type === 'HYBRID' ? this.$t('habirdModelBuildTips') : ''}` : this.$t('confirmTextBySave')
    const saveBtnText = isCatchUp ? this.$t('bulidAndSubmit') : this.$t('kylinLang.common.save')
    const ctx = this.$createElement
    // this.generateDeletedIndexes = true // 默认 checkbox 勾选状态
    this.$nextTick(() => {
      this.chengeGenerateDeletedIndexesStatus('init', this.generateDeletedIndexes, diffResult)
    })
    return this.$msgbox({
      message: ctx('div', null, [
        ctx('p', { class: diffResult.increase_layouts > 0 && diffResult.decrease_layouts > 0 ? 'agg-build-message-all_title' : 'agg-build-message-single_title' },
          !this.generateDeletedIndexes
            ? this.$t(diffResult.increase_layouts > 0 && diffResult.decrease_layouts > 0 ? 'mixTips' : 'onlyIncreaseOrDecreaseTip', {increaseNum: diffResult.increase_layouts + diffResult.rollback_layouts, decreaseNum: diffResult.decrease_layouts})
            : this.$t(diffResult.increase_layouts > 0 && diffResult.decrease_layouts > 0 ? 'mixTips' : 'onlyIncreaseOrDecreaseTip', {increaseNum: diffResult.increase_layouts, decreaseNum: diffResult.decrease_layouts})
        ),
        ctx('p', null, saveText),
        ctx('div', null,
          diffResult.rollback_layouts > 0
            ? [
              ctx('el-checkbox', { class: 'ksd-mt-10', props: { checked: this.generateDeletedIndexes }, nativeOn: { change: (v) => this.chengeGenerateDeletedIndexesStatus('change', v, diffResult) } }, this.$t('generateDeletedIndexes', {rollbackNum: diffResult.rollback_layouts}))
            ] : null
        )]
      ),
      cancelButtonText: this.$t('kylinLang.common.cancel'),
      confirmButtonText: saveBtnText,
      type: 'warning',
      closeOnClickModal: false,
      showClose: false,
      closeOnPressEscape: false,
      showCancelButton: true,
      // customClass: diffResult.increase_layouts > 0 && diffResult.decrease_layouts > 0 ? 'aggBuildChangeMessageAll' : 'aggBuildChangeMessageSinger',
      title: this.$t('kylinLang.common.tip')
    })
  }
  chengeGenerateDeletedIndexesStatus (type, v, diffResult) {
    this.generateDeletedIndexes = type === 'init' ? v : v.target.checked
    // this.$set(this, 'generateDeletedIndexes', v.target.checked)
    this.$nextTick(() => {
      if (document.body.getElementsByClassName('agg-build-message-all_title').length || document.body.getElementsByClassName('agg-build-message-single_title').length) {
        if (diffResult.increase_layouts > 0 && diffResult.decrease_layouts > 0) {
          document.body.getElementsByClassName('agg-build-message-all_title')[0].innerHTML = !this.generateDeletedIndexes ? this.$t('mixTips', {increaseNum: diffResult.increase_layouts + diffResult.rollback_layouts, decreaseNum: diffResult.decrease_layouts}) : this.$t('mixTips', {increaseNum: diffResult.increase_layouts, decreaseNum: diffResult.decrease_layouts})
        } else {
          document.body.getElementsByClassName('agg-build-message-single_title')[0].innerHTML = !this.generateDeletedIndexes ? this.$t('onlyIncreaseOrDecreaseTip', {increaseNum: diffResult.increase_layouts + diffResult.rollback_layouts, decreaseNum: diffResult.decrease_layouts}) : this.$t('onlyIncreaseOrDecreaseTip', {increaseNum: diffResult.increase_layouts, decreaseNum: diffResult.decrease_layouts})
        }
      }
    })
  }
  checkFormVaild () {
    if (!this.isFormVaild) {
      this.$message(this.$t('includesEmpty'))
      return false
    }
    return this.isFormVaild
  }
  submit (data) {
    return this.updateAggregateGroups(data)
  }
  getSubmitData () {
    const { form, usedDimensions, dimensionIdMapping, measureIdMapping, projectName, model } = this
    const { aggregateArray, isCatchUp, globalDimCap } = form
    return {
      projectName,
      modelId: model.uuid,
      isCatchUp,
      globalDimCap,
      dimensions: usedDimensions.map(dimensionItem => dimensionIdMapping[dimensionItem]),
      aggregationGroups: aggregateArray.map(aggregate => ({
        index_range: aggregate.index_range || 'EMPTY', // 纯批或纯流，这个参数为空
        includes: aggregate.includes.map(includeItem => dimensionIdMapping[includeItem]),
        measures: aggregate.measures.map(measure => measureIdMapping[measure]),
        select_rule: {
          mandatory_dims: aggregate.mandatory.map(includeItem => dimensionIdMapping[includeItem]),
          hierarchy_dims: aggregate.hierarchyArray.map(hierarchys => {
            return hierarchys.items.map(hierarchyItem => dimensionIdMapping[hierarchyItem])
          }).filter(hierarchys => hierarchys.length),
          joint_dims: aggregate.jointArray.map(joints => {
            return joints.items.map(jointItem => dimensionIdMapping[jointItem])
          }).filter(joints => joints.length),
          dim_cap: aggregate.dimCap
        }
      }))
    }
  }
  repaintDimensionSelector () {
    const { form = {} } = this
    const { aggregateArray = [] } = form

    aggregateArray.forEach((aggregateGroup, aggregateIdx) => {
      const { includes, mandatory, jointArray, hierarchyArray } = aggregateGroup
      const jointBundle = jointArray.reduce((result, jointGroup) => [...result, ...jointGroup.items], [])
      const hierarchyBundle = hierarchyArray.reduce((result, hierarchyGroup) => [...result, ...hierarchyGroup.items], [])
      const selectedItems = [ ...mandatory, ...jointBundle, ...hierarchyBundle ]

      includes.forEach((include) => {
        const isSelected = selectedItems.includes(include)
        this.toggleIncludeDimension(aggregateIdx, include, isSelected)
      })
    })
  }

  // 删除选中的度量tag
  handleRemoveMeasureRules (value, aggregateIdx, id) {
    value === 'COUNT_ALL' && this.handleInput(`aggregateArray.${aggregateIdx}.measures`, ['COUNT_ALL', ...this.form.aggregateArray[aggregateIdx].measures], id)
  }

  // 选择所有的度量
  handleAddAllMeasure (aggregateIdx, id) {
    this.handleInput(`aggregateArray.${aggregateIdx}.measures`, this.reorganizedMeasures(), id)
  }

  // 清除所有的度量
  handleRemoveAllMeasure (aggregateIdx, titleId, id) {
    kylinConfirm(this.$t('clearAllMeasuresTip', {aggId: titleId}), {type: 'warning'}, this.$t('clearMeasureTitle')).then(() => {
      this.handleInput(`aggregateArray.${aggregateIdx}.measures`, this.form.aggregateArray[aggregateIdx].measures.filter(m => ['COUNT_ALL'].includes(m)), id)
    })
  }

  reorganizedMeasures () {
    const measureList = this.measures.map(m => m.label)
    return measureList.includes('COUNT_ALL') ? ['COUNT_ALL', ...measureList.filter(item => item !== 'COUNT_ALL')] : measureList
  }

  // 是否展开聚合组
  openAggregateItem (index) {
    this.form.aggregateArray[index].open = !this.form.aggregateArray[index].open
    this.computeCurrentAggHeigth(index)
    this.setModalForm(this.form)
  }

  aggregateHeight (index) {
    return this.$el.querySelectorAll('.aggregate-group .body').length ? (document.querySelectorAll('.aggregate-group .body')[index].offsetHeight || null) : null
  }

  computeCurrentAggHeigth (index) {
    let height = this.aggregateHeight(index)
    const showAggregate = () => {
      setTimeout(() => {
        this.aggregateStyle = []
      }, 1000)
    }
    if (this.form.aggregateArray[index].open) {
      this.$set(this.aggregateStyle, index, {'display': 'block'})
      this.$nextTick(() => {
        height = this.aggregateHeight(index)
        this.$set(this.aggregateStyle, index, {'height': 0})
        this.$nextTick(() => {
          this.$set(this.aggregateStyle, index, {...this.aggregateStyle[index], ...{'height': height ? `${height}px` : 'auto'}})
          showAggregate()
        })
      })
    } else {
      this.$set(this.aggregateStyle, index, {'height': height ? `${height}px` : 'auto'})
      this.$nextTick(() => {
        this.$set(this.aggregateStyle, index, {'height': 0})
        showAggregate()
      })
    }
  }

  // 切换维度、度量tab
  handleClickTab (tab) {
    this.currentSelectedTag = {
      ctx: '',
      type: tab.name,
      data: {}
    }
  }

  handleClickTag (name, type, aggregateIdx) {
    let data = null
    let isCC = false
    if (type === 'measure') {
      data = this.measures.filter(it => it.name === name)[0]
    } else {
      data = this.dimensions().filter(it => it.column === name)[0]
      const columns = ['cardinality', 'max_value', 'min_value', 'null_count']
      this.characteristicsData = columns.map(it => ({column: it, value: data[it]}))
      const ccList = this.model.computed_columns.map(it => `${it.tableAlias}.${it.columnName}`)
      isCC = ccList.includes(name)
      this.characteristicsData.unshift({column: 'column', value: name.split('.')[1]})
      this.sampleData = data.simple ? data.simple.map((it, index) => ({key: index + 1, value: it})) : []
      this.sampleData.unshift({key: 'ID', value: name.split('.')[1]})
    }
    this.currentSelectedTag = {
      ctx: name,
      type: type,
      aggregateIdx,
      data,
      isCC
    }
  }

  // 编辑 Includes 包含的维度
  handleEditIncludes (aggregateIdx, id) {
    if (this.model.model_type === 'HYBRID' && !this.form.aggregateArray[aggregateIdx].index_range) {
      return false
    }
    this.searchName = ''
    this.editIncludeDimension = true
    // 此处用方法调用避免 getters 数据缓存
    const dimensions = this.dimensions()
    let includes = []
    this.form.aggregateArray[aggregateIdx].includes.forEach(item => {
      const idx = dimensions.findIndex(it => it.label === item)
      includes.push({...dimensions[idx], isCheck: true})
    })
    if (includes.length !== dimensions.length) {
      this.isSelectAllDimensions = false
    }
    this.selectedIncludeDimension = includes
    this.backUpDimensions = [...includes, ...dimensions.filter(it => !this.form.aggregateArray[aggregateIdx].includes.includes(it.label))]
    this.currentAggregateInfo = {
      aggregateIdx,
      id
    }
    this.getDimensionList()
  }

  scrollEvent (e) {
    const { clientHeight, scrollHeight, scrollTop } = e.target
    if (clientHeight + scrollTop + 40 >= scrollHeight) {
      if (this.pageOffset * this.pageSize >= this.backUpDimensions.length) return
      this.pageOffset += 1
      this.getDimensionList()
    }
  }

  getDimensionList () {
    const selectedItems = this.selectedIncludeDimension.map(it => it.label)
    const filterData = this.backUpDimensions.filter(it => {
      if (this.displayExcludedTables) {
        return it.name.toLocaleLowerCase().indexOf(this.searchName.toLocaleLowerCase()) > -1 || it.column.toLocaleLowerCase().indexOf(this.searchName.toLocaleLowerCase()) > -1
      } else {
        return (it.name.toLocaleLowerCase().indexOf(this.searchName.toLocaleLowerCase()) > -1 || it.column.toLocaleLowerCase().indexOf(this.searchName.toLocaleLowerCase()) > -1) && !this.isExistExcludeTable(it)
      }
    }).map(it => ({...it, isCheck: selectedItems.includes(it.label)}))
    const labels = filterData.map(it => it.label)
    const dataList = [...this.selectedIncludeDimension.filter(it => labels.includes(it.label)), ...filterData.filter(it => !it.isCheck)]
    this.includeDimensions = dataList.slice(0, (this.pageOffset + 1) * this.pageSize)
    this.orderIncludeDimensions()
  }

  // 全选或取消全选 includes 维度
  selectAllIncludes (v) {
    if (v) {
      this.includeDimensions.forEach(it => {
        if (this.getDisabledTableType(it)) return
        it.isCheck = true
        this.collectSelectedDimensions(it, true)
      })
    } else {
      this.includeDimensions.forEach(it => {
        it.isCheck = false
        this.collectSelectedDimensions(it, false)
      })
    }
  }

  // 收集已勾选的维度
  collectSelectedDimensions (item, type) {
    const list = this.selectedIncludeDimension.filter(it => it.label === item.label)
    if (type) {
      if (!list.length) {
        this.selectedIncludeDimension.push(item)
      }
    } else {
      if (list.length) {
        const index = this.selectedIncludeDimension.findIndex(dimension => dimension.label === item.label)
        this.selectedIncludeDimension.splice(index, 1)
      }
    }
  }

  // 选择包含维度
  selectIncludDimensions (item, val) {
    this.collectSelectedDimensions(item, val)
    this.orderIncludeDimensions()
  }

  // 将勾选的include度量排在前面
  orderIncludeDimensions () {
    const unSelected = this.includeDimensions.filter(it => !it.isCheck)
    const selected = this.includeDimensions.filter(it => it.isCheck)
    this.includeDimensions = [...selected, ...unSelected]
    selected.length === this.backUpDimensions.length && (this.isSelectAllDimensions = true)
    unSelected.length === this.backUpDimensions.length && (this.isSelectAllDimensions = false)
  }

  // 移动（置顶、上移、下移）
  moveTo (type, scope) {
    const index = this.includeDimensions.findIndex(it => it.id === scope.id)
    const idx = this.selectedIncludeDimension.findIndex(it => it.id === scope.id)
    if (index < 0) return
    if (type === 'up') {
      this.includeDimensions.splice(index - 1, 0, scope)
      this.includeDimensions.splice(index + 1, 1)
      this.selectedIncludeDimension.splice(idx - 1, 0, scope)
      this.selectedIncludeDimension.splice(idx + 1, 1)
    } else if (type === 'down') {
      this.includeDimensions.splice(index + 2, 0, scope)
      this.includeDimensions.splice(index, 1)
      this.selectedIncludeDimension.splice(idx + 2, 0, scope)
      this.selectedIncludeDimension.splice(idx, 1)
    } else if (type === 'top') {
      // this.includeDimensions.splice(0, 0, scope)
      // this.includeDimensions.splice(index, 1)
      this.selectedIncludeDimension.splice(0, 0, scope)
      this.selectedIncludeDimension.splice(idx + 1, 1)
      this.filterChange()
    }
  }

  // 保存includes 包含维度
  saveIncludes () {
    const allDimensions = this.getSelectedIncludeDimensions.map(dimension => dimension.label)
    const { aggregateIdx, id } = this.currentAggregateInfo
    const unExistDimensions = this.form.aggregateArray[aggregateIdx].includes.filter(item => !allDimensions.includes(item))
    if (unExistDimensions.length) {
      unExistDimensions.forEach(v => {
        this.handleRemoveIncludeRules(v, aggregateIdx)
      })
    }
    this.handleInput(`aggregateArray.${aggregateIdx}.includes`, allDimensions, id)
    this.editIncludeDimension = false
    this.currentSelectedTag.ctx = ''
  }

  // 筛选包含维度的列名
  filterChange () {
    this.pageOffset = 0
    if (this.$refs.includesDimensionDialog) {
      const scrollDom = this.$refs.includesDimensionDialog.$el.querySelector('.ky-simple-table')
      scrollDom.scrollTop = 0
    }
    this.getDimensionList()
  }

  clearFilter () {
    this.filterChange()
  }

  // 编辑度量
  handleEditMeasures (aggregateIdx, id) {
    this.searchMeasure = ''
    this.editMeasure = true
    const selectedMeasure = this.form.aggregateArray[aggregateIdx].measures
    this.selectedMeasures = selectedMeasure
    this.measureList = this.measures.map(it => ({...it, isCheck: it.name === 'COUNT_ALL' || selectedMeasure.includes(it.name)}))
    selectedMeasure.length === this.measureList.length && (this.isSelectAllMeasure = true)
    this.currentAggregateInfo = {
      aggregateIdx,
      id
    }
  }

  selectAllMeasures (type) {
    this.isSelectAllMeasure = type
    if (type) {
      this.measureList.forEach(it => {
        it.isCheck = true
        this.collectSelectedMeasures(it, type)
      })
    } else {
      this.measureList.forEach(it => {
        if (it.name === 'COUNT_ALL') return
        it.isCheck = false
        this.collectSelectedMeasures(it, type)
      })
    }
  }

  // 收集已勾选的度量
  collectSelectedMeasures (item, type) {
    const list = this.selectedMeasures.filter(it => it === item.name)
    if (type) {
      if (!list.length) {
        this.selectedMeasures.push(item.name)
      }
    } else {
      if (list.length) {
        const index = this.selectedMeasures.findIndex(measure => measure === item.label)
        this.selectedMeasures.splice(index, 1)
      }
    }
  }

  changeMeasureBox (item, type) {
    const unSelectMeasure = this.measureList.filter(it => !it.isCheck)
    this.collectSelectedMeasures(item, type)
    this.getSelectedMeasures.length === this.measureList.length && (this.isSelectAllMeasure = true)
    unSelectMeasure.length === this.measureList.length && (this.isSelectAllMeasure = false)
  }

  // 筛选度量
  filterMeasure () {
    this.measureList = this.measures.filter(item => item.name.toLocaleLowerCase().indexOf(this.searchMeasure.toLocaleLowerCase()) > -1).map(it => ({...it, isCheck: it.name === 'COUNT_ALL' || this.selectedMeasures.includes(it.name)}))
  }

  // 清除筛选内容
  clearMeasureFilter () {
    this.measureList = this.measures.map(it => ({...it, isCheck: it.name === 'COUNT_ALL' || this.selectedMeasures.includes(it.name)}))
  }

  // 保存度量
  saveMeasures () {
    this.editMeasure = false
    const { aggregateIdx } = this.currentAggregateInfo
    let measures = this.getSelectedMeasures
    const constantMeasure = this.measureList.filter(it => it.expression === 'COUNT' && it.parameter_value && it.parameter_value[0].type === 'constant')
    !constantMeasure.length && !measures.includes('COUNT_ALL') && measures.unshift('COUNT_ALL')
    this.$set(this.form.aggregateArray[aggregateIdx], 'measures', measures)
  }

  // 是否显示被屏蔽的列
  changeExcludedTables () {
    this.getDimensionList()
  }

  // 跳转至job页面
  jumpToJobs () {
    this.$router.push('/monitor/job')
  }

  updated () {
    this.repaintDimensionSelector()
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';

.aggregate-modal {
  z-index: 10;
  position: fixed;
  // padding-top: 20px;
  background: @fff;
  top: 52px;
  height: 100vh;
  width: calc(~'100% - 184px');
  // margin: 0 -20px 0 -20px;
  // padding: 20px;
  overflow-y: auto;
  &.brief-Menu {
    width: calc(~'100% - 42px');
  }
  .aggregate-dialog {
    // margin: 20px;
    height: calc(~'100% - 50px');
    width: 100%;
    .header {
      padding: 20px;
      box-sizing: border-box;
    }
    .agg-group-layout {
      background: #f5f5f5;
      padding: 20px;
      box-sizing: border-box;
      display: flex;
      height: calc(~'100% - 150px');
    }
    .cardinality-multiple {
      font-size: 12px;
      color: @text-normal-color;
      margin: 5px;
    }
  }
  .el-icon-ksd-right {
    color: @base-color;
  }
  .dialog-footer{
    line-height: 50px;
    height: 50px;
    border-top: 1px solid #ccc;
    // margin: 0px -20px 0 -20px;
    padding: 0 20px;
    position: fixed;
    bottom: 0px;
    width: calc(~'100% - 184px');
    background-color: #fff;
    z-index: 11;
    box-sizing: border-box;
    .dim-btn {
      cursor: pointer;
      display: none;
      &.el-icon-ksd-clear:hover {
        color: @base-color;
      }
      &.disable-clear,
      &.disable-clear:hover {
        color: @color-text-disabled;
      }
    }
    .left {
      min-width: 230px;
      text-align: left;
      .el-ksd-icon-refresh_22.is-disabled {
        color: @color-text-disabled;
      }
      .dimCap-block {
        margin-left: 10px;
        float: left;
        .divide {
          border-left: 1px solid @line-border-color;
          margin-right: 10px;
        }
      }
      .dim-input{
        display: inline-block;
        width: 58px;
        .el-input__inner {
          padding-right: 5px;
        }
      }
      &:hover {
        .dim-btn {
          display: inline-block;
        }
      }
    }
    .el-button{
      .plainWhat{
        color:@base-color;
      }
      &.is-disabled{
        .plainWhat{
          color: #6bb8eb;
        }
        &:hover{
          .plainWhat{
            color: #6bb8eb;
          }
        }
      }
    }
  }
  &.brief-Menu .dialog-footer {
    width: calc(~'100% - 42px');
  }
  .el-select {
    width: 100%;
  }
  .mul-filter-select {
    .el-select__tags {
      padding-left: 15px;
      .el-tag {
        max-width: 841px;
        padding-right: 20px;
        position: relative;
        display: inline-block;
        text-overflow: ellipsis;
        overflow: hidden;
      }
      .el-tag__close {
        position: absolute;
        top: 50%;
        right: 2px;
        transform: scale(0.8) translateY(-50%);
      }
    }
    &.reset-padding {
      .el-select__tags {
        padding-left: 0px;
      }
    }
  }
  .cuboid-error {
    .cuboid-result {
      color:@error-color-1;
      font-weight: @font-regular;
      .el-ksd-icon-more_info_16{
        color:@error-color-1;
        font-weight: @font-regular;
      }
    }
  }
  .el-button + .el-button { margin-left: 5px;}
  .dimension {
    float: left;
    padding: 6px 13px;
    margin: 0 10px 10px 0;
  }
  .dimension.disable {
    background: @grey-4;
    border: 1px solid @text-secondary-color;
    border-radius: 2px;
  }
  .dimension-buttons {
    text-align: center;
    margin: 0 -20px;
    padding: 0px 0 20px 0;
    border-bottom: 1px solid @line-split-color;
  }
  .less {
    transform: rotate(180deg);
  }
  .agg-list {
    background: @fff;
    padding: 20px;
    box-sizing: border-box;
    flex: 1 0;
    overflow: auto;
    margin-right: 10px;
  }
  .metadata-detail {
    width: 310px;
    background: @fff;
    padding: 20px;
    box-sizing: border-box;
    margin-right: 10px;
    position: relative;
    overflow: auto;
    label {
      display: block;
      margin-bottom: 20px;
    }
    .title {
      color: @text-title-color;
      font-weight: bold;
      margin-bottom: 10px;
    }
    .title2 {
      color: @text-normal-color;
      font-size: 12px;
      font-weight: bold;
      margin-bottom: 5px;
    }
    .content {
      color: @text-title-color;
      font-size: 14px;
      font-weight: 400;
      word-break: break-all;
      span {
        font-size: 12px;
      }
      .label {
        color: @text-normal-color;
      }
    }
    .noData {
      width: 200px;
      position: absolute;
      top: 50%;
      left: 0;
      right: 0;
      margin: auto;
      text-align: center;
      transform: translate(0, -50%);
      .icon {
        font-size: 36px;
        color: @text-placeholder-color;
      }
      .tip {
        margin-top: 10px;
        font-size: 12px;
        color: @text-normal-color;
      }
    }
    .statistics_list_table {
      margin-bottom: 30px;
      .el-table__header-wrapper {
        display: none;
      }
      .el-table__body-wrapper {
        .el-table__row {
          &:first-child {
            background-color: #f5f5f5;
            font-weight: bold;
            &:hover {
              background-color: transparent;
            }
          }
        }
      }
    }
  }
  .aggregate-group {
    position: relative;
    &:not(:last-child) {
      margin-bottom: 30px;
    }
    .title {
      color: @text-title-color;
      margin-bottom: 10px;
      &.include-title,
      &.measure-title {
        position: relative;
        top: 6px;
      }
      .is-required{
        color:@color-danger;
      }
    }
    .include-agg, .include-measure {
      width: calc(~'100% - 3px');
      height: 85px;
      min-height: 85px;
      max-height: max-content;
      resize: vertical;
      overflow: auto;
      border: 1px solid #cccccc;
      padding: 5px 10px;
      box-sizing: border-box;
      position: relative;
      .el-tag {
        margin-right: 5px;
        margin-top: 5px;
        cursor: pointer;
        &:hover {
        background-color: #0988DE;
        color: @fff;
        }
      }
      .is-used {
        background-color: #EAFFEA;
        color: #4CB050;
        border: 1px solid #4CB050;
      }
      .is-active {
        background-color: #0988DE;
        color: @fff;
      }
    }
    .no-includes {
      color: @text-placeholder-color;
      display: inline-block;
      position: absolute;
      top: 30%;
      left: 50%;
      transform: translate(-50%, -30%);
      .add-includes-btn {
        color: #0988DE;
        cursor: pointer;
        &.disabled{
          color: @color-text-disabled;
          cursor:not-allowed;
        }
      }
    }
    .header {
      width: 100%;
      height: 36px;
      background: @background-disabled-color;
      line-height: 36px;
      padding: 0 10px;
      box-sizing: border-box;
      border-radius: 2px 2px 0 0;
      // border: 1px solid @line-border-color;
      overflow: hidden;
      .title {
        float: left;
        margin: 0;
      }
      .dimCap-block {
        .divide {
          border-left: 1px solid @line-border-color;
          margin-right: 10px;
        }
        min-width: 222px;
        float: left;
        i {
          cursor: pointer;
        }
        .nolimit-dim {
          color: @text-disabled-color;
        }
        .global-dim {
          font-style: oblique;
          color: @text-disabled-color;
        }
        .dim-btn {
          display: none;
        }
        .dim-input{
          display: inline-block;
          width: 58px;
          .el-input__inner {
            padding-right: 5px;
          }
        }
        &:hover {
          .dim-btn {
            display: inline-block;
          }
        }
      }
      .el-icon-caret-right.is-open {
        transform: rotate(90deg);
      }
    }
    .body {
      // border: 1px solid @line-border-color;
      // border-top: 0;
      overflow: hidden;
      transition: height 1s;
      box-sizing: border-box;
      position: relative;
      &.overLimit{
        border: 1px solid @error-color-1;
      }
      .contain {
        padding: 15px 0 0px 0;
        box-sizing: border-box;
      }
      .dimension-group {
        position: relative;
      }
      .layout-mask {
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        z-index: 10;
      }
    }
    .open-aggregate-group {
      width: 100%;
      height: 20px;
      background: @aceditor-bg-color;
      text-align: center;
      border-radius: 0 0 2px 2px;
      border: 1px solid @line-border-color;
      border-top: 0;
      box-sizing: border-box;
      cursor: pointer;
      .el-icon-d-arrow-left {
        transform: rotate(-90deg);
        &.open {
          transform: rotate(90deg)
        }
      }
      &:hover {
        color: @color-primary;
        background-color: @base-color-9;
      }
    }
    .row {
      margin-bottom: 15px;
      &.joint {
        margin-bottom: 0;
      }
      .add-all-item {
        .el-ksd-icon-more_info_16 {
          // margin-left: 5px;
          color: @color-primary;
        }
      }
    }
    .actions {
      font-size: 14px;
      float: right;
      padding: 0 2px 0 12px;
      i {
        color: @text-title-color;
        margin-left: 6px;
      }
    }
  }
  h1 {
    font-size: 14px;
  }
  h2 {
    font-size: 14px;
  }
  .el-select {
    width: 100%;
  }
  .el-button {
    i[class^=el-icon-] {
      cursor: inherit;
    }
  }
  .list {
    padding-right: 68px;
    position: relative;
    &:not(:last-child) {
      margin-bottom: 10px;
    }
  }
  .list-actions {
    position: absolute;
    right: 6px;
    top: 0;
    transform: translateY(2px);
    .is-text {
      font-size: 24px;
      border: 0;
      padding: 4px 0px;
    }
    .el-button--medium {
      float: left;
    }
  }
  .mandatory,
  .hierarchy,
  .joint {
    .el-tag {
      background: rgba(76, 176, 80, 0.1);
      border: 1px solid @color-success;
      border-radius: 2px;
      color: @color-success;
      .el-icon-close {
        color: @color-success;
      }
      .el-icon-close:hover {
        background: @color-success;
        color: @fff;
      }
    }
  }
  .el-tag {
    background: @base-color-10;
    color: @base-color;
    border: 1px solid @base-color;
    &[data-tag='used'] {
      background: rgba(76, 176, 80, 0.1);
      border: 1px solid @color-success;
      border-radius: 2px;
      color: @color-success;
      .el-icon-close {
        color: @color-success;
      }
      .el-icon-close:hover {
        background: @color-success;
        color: @fff;
      }
    }
  }
  .left {
    float: left;
  }
  .right {
    float: right;
    margin-right: 20px;
    .el-button {
      line-height: normal\0;
    }
  }
  .loading {
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
  }
}
.confirm-dialog {
  .el-message-box__content {
    padding: 0;
  }
  .el-dialog__footer {
    padding: 5px 20px 15px 20px;
    border-top: none;
  }
}
.edit-includes-dimensions {
  .el-dialog__body {
    height: 500px;
    overflow: auto;
  }
  .flip-list-move {
    transition: transform .5s;
  }
  .action-layout {
    .filter-dimension {
      text-align: right;
      .el-checkbox {
        font-size: 0;
        display: inline-block;
        top: -4px;
        .el-checkbox__inner {
          vertical-align: middle;
        }
      }
    }
  }
  .alert {
    margin-bottom: 10px;
  }
  .ky-simple-table {
    height: calc(~'100% - 70px');
    margin-top: 10px;
    overflow: auto;
    .ky-table-drag-layout-line {
      height: 100%;
      border-left: 1px solid #ddd;
      cursor: col-resize;
      float: right;;
      z-index: 9;
      position: relative;
      right: -10px;
      .ky-drag-layout-bar {
        right:-5px;
        top:200px;
      }
    }
    .order-actions {
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .icon {
      font-size: 12px;
      margin-left: 10px;
      color: #232323;
      cursor: pointer;
      &:first-child {
        margin-left: 0;
      }
      &.is-disabled {
        pointer-events: none;
        color: @text-disabled-color;
      }
    }
    .table-row {
      height: 32px;
      display: flex;
      display: -webkit-box;
      display: -moz-box;
      display: -ms-flexbox;
      display: -webkit-flex;
      .el-col {
        height: 100%;
        line-height: 32px;
        text-align: left;
        font-size: 12px;
        font-weight: bold;
        position: relative;
        flex-grow: 1;
        &:nth-child(1) {
          flex-grow: 0;
          flex-basis: 38px;
          width: 38px;
          min-width: 38px;
        }
      }
      .el-checkbox {
        cursor: pointer;
      }
      .text {
        white-space: pre-wrap;
      }
      &.dim-table-header {
        .el-col {
          &:nth-child(2),
          &:nth-child(3) {
            border-right: none;
          }
        }
      }
    }
    .excluded_table-icon {
      position: absolute;
      right: 10px;
      line-height: 32px;
    }
  }
}
.edit-measures {
  .action-measure-layout {
    .filter-measure {
      margin-top: 5px;
      text-align: right;
    }
  }
}
.measure-table {
  .ky-table-drag-layout-line {
    height: 100%;
    border-left: 1px solid #ddd;
    cursor: col-resize;
    float: right;;
    z-index: 9;
    position: relative;
    right: -10px;
    .ky-drag-layout-bar {
      right:-5px;
      top:200px;
    }
  }
  .table-row {
    display: flex;
    display: -webkit-box;
    display: -moz-box;
    display: -ms-flexbox;
    display: -webkit-flex;
    .el-col {
      text-align: left;
      flex-grow: 1;
      &:nth-child(1) {
        flex-grow: 0;
        flex-basis: 38px;
        width: 38px;
        min-width: 38px;
      }
    }
    .text {
      white-space: pre-wrap;
    }
    &.measure-table-header {
      .el-col {
        &:nth-child(2),
        &:nth-child(4) {
          border-right: none;
        }
      }
    }
  }
}
.no-data_placeholder {
  color: @text-placeholder-color;
  font-size: 12px;
}
</style>
