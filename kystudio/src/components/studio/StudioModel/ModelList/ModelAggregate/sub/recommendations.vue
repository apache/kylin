<template>
  <el-card class="recommendations-card">
    <div slot="header">
      <p>{{$t('recommendations')}}</p>
    </div>
    <div class="detail-content">
      <p class="title-tip ksd-mb-8">{{$t('recommendationsTip1')}}<span v-if="$lang !== 'en'&&datasourceActions.includes('acceRuleSettingActions')">{{$t('recommendationsTip2')}}</span><a href="javascript:void(0);" v-if="datasourceActions.includes('acceRuleSettingActions')" @click="jumpToSetting">{{$t('modifyRules')}}</a><span v-if="$lang === 'en' && datasourceActions.includes('acceRuleSettingActions')">{{$t('recommendationsTip2')}}</span></p>
      <el-alert :title="$t('realTimeModelAcceptRecommendTips')" type="tip" show-icon v-if="modelDesc.model_type === 'STREAMING'" />
      <div class="ksd-fleft ksd-mb-10 ksd-mt-10 ksd-fs-12" >
        <el-button text :disabled="!selectedList.length" @click="batchAccept" type="primary" icon="el-ksd-icon-confirm_22">{{$t('accept')}}</el-button><el-button text :disabled="!selectedList.length" type="primary" @click="batchDelete" icon="el-ksd-icon-table_delete_22">{{$t('delete')}}</el-button>
      </div>
      <div class="search-contain ksd-fright ksd-mt-10">
        <el-input class="search-input" v-model.trim="recommendationsList.key" size="medium" :placeholder="$t('searchContentOrIndexId')" prefix-icon="el-ksd-icon-search_22" v-global-key-event.enter.debounce="searchRecommendation" @clear="searchRecommendation"></el-input>
      </div>
      <el-table
        :data="recommendationsList.list"
        class="recommendations-table clearfix"
        size="medium"
        max-height="350"
        v-loading="loadingRecommends"
        :empty-text="emptyText"
        @selection-change="handleSelectionChange"
        @sort-change="changeSort"
      >
        <el-table-column type="selection" width="44"></el-table-column>
        <el-table-column
          width="160"
          :label="$t('th_recommendType')"
          :filters="typeList.map(item => ({text: $t(item), value: item}))"
          :filtered-value="checkedStatus"
          filter-icon="el-ksd-icon-filter_22"
          :show-multiple-footer="false"
          :filter-change="(v) => filterType(v, 'checkedStatus')"
          prop="type"
          show-overflow-tooltip>
          <template slot-scope="scope">
            <el-tag size="mini" :type="scope.row.type.split('_')[0] === 'ADD' ? 'success' : 'danger'" v-if="['ADD', 'REMOVE'].includes(scope.row.type.split('_')[0])">{{scope.row.type.split('_')[0] === 'ADD' ? $t('newAdd') : $t('delete')}}</el-tag>
            <span style="position:relative;top:1px;">{{$t(scope.row.type.split('_')[1])}}</span>
          </template>
        </el-table-column>
        <el-table-column
          width="120"
          prop="id"
          label="Index ID">
          <template slot-scope="scope">
            <span v-if="scope.row.type !== 'ADD_AGG_INDEX' && scope.row.type !== 'ADD_TABLE_INDEX'">{{$t(scope.row.index_id)}}</span>
          </template>
        </el-table-column>
        <!-- <el-table-column
          width="110"
          :label="$t('th_source')"
          :filters="source.map(item => ({text: $t(item), value: item}))"
          :filtered-value="sourceCheckedStatus"
          filter-icon="el-icon-ksd-filter"
          :show-multiple-footer="false"
          :filter-change="(v) => filterType(v, 'sourceCheckedStatus')">
          <template slot-scope="scope">
            {{$t(scope.row.source)}}
          </template>
        </el-table-column> -->
        <el-table-column
          width="110"
          prop="data_size"
          :label="$t('th_dataSize')"
          sortable>
          <template slot-scope="scope">
            {{formatDataSize(scope.row.data_size)}}
          </template>
        </el-table-column>
        <el-table-column
          width="120"
          prop="usage"
          :label="$t('th_useCount')"
          :info-tooltip="$t('usedCountTip')"
          sortable>
        </el-table-column>
        <el-table-column
          width='220'
          prop="last_modified"
          :label="$t('th_updateDate')"
          sortable
          show-overflow-tooltip>
          <template slot-scope="scope">
            {{transToGmtTime(scope.row.last_modified)}}
          </template>
        </el-table-column>
        <el-table-column align="left" :label="$t('indexContent')">
          <template slot-scope="scope">
            <el-popover
              ref="index-content-popover"
              placement="top"
              trigger="hover"
              popper-class="col-index-content-popover">
              <div class="index-content" slot="reference">{{getRowContentList(scope.row).map(it => it.name).slice(0, 20).join(', ')}}</div>
              <template>
                <p class="popover-header"><b>{{$t('indexesContent')}}</b><el-button v-if="getRowContentList(scope.row).length > 20" type="primary" text class="view-more-btn ksd-fs-12" @click="showDetail(scope.row)">{{$t('viewIndexDetails')}}<i class="el-icon-ksd-more_02 ksd-fs-12"></i></el-button></p>
                <p style="white-space: pre-wrap;">{{getRowContentList(scope.row).map(it => it.name).slice(0, 20).join('\n')}}</p>
                <el-button v-if="getRowContentList(scope.row).length > 20" class="ksd-fs-12" type="primary" text @click="showDetail(scope.row)">{{$t('viewAll')}}</el-button>
              </template>
            </el-popover>
            <span class="detail-icon el-ksd-icon-view_16" @click="showDetail(scope.row)"></span>
          </template>
        </el-table-column>
        <el-table-column
          width='120'
          :label="$t('th_note')">
          <div slot-scope="scope" class="col-tab-note">
            <!-- <el-tag class="th-note-tag" size="small" type="warning">查询历史</el-tag> -->
            <template v-if="'recommendation_source' in scope.row.memo_info">
              <el-tooltip class="item" effect="dark" :content="removeReasonTip(scope)" placement="top">
                <el-tag class="th-note-tag" size="small" type="warning">{{$t(scope.row.memo_info.recommendation_source)}}</el-tag>
              </el-tooltip>
            </template>
          </div>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.action')" width="83" fixed="right">
          <template slot-scope="scope">
            <common-tip :content="$t('accept')">
              <i class="el-icon-ksd-accept ksd-ml-5" @click="doConfirm([scope.row])"></i>
            </common-tip>
            <common-tip :content="$t('delete')">
              <i class="el-icon-ksd-table_delete ksd-ml-5" @click="removeIndex(scope.row)"></i>
            </common-tip>
          </template>
        </el-table-column>
      </el-table>
      <kylin-pager class="ksd-center ksd-mtb-10" ref="indexPager" :totalSize="recommendationsList.totalSize" :refTag="pageRefTags.recommendationsPager" :perPageSize="recommendationsList.page_size" :curPage="recommendationsList.page_offset+1" v-on:handleCurrentChange='pageCurrentChange'></kylin-pager>
    </div>
    <!-- 索引详情 -->
    <el-dialog
      class="layout-details"
      :title="indexDetailTitle"
      width="880px"
      :append-to-body="true"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      @close="showIndexDetail = false"
      :visible="true"
      v-if="showIndexDetail"
    >
      <el-table
        border
        v-loading="loadingDetails"
        :data="detailData"
        class="index-details-table"
        size="medium"
        :fit="false"
        :empty-text="emptyText"
        style="width: 100%"
        :cell-class-name="getCellClassName"
      >
        <el-table-column width="34" type="expand">
          <template slot-scope="scope" v-if="scope.row.content">
            <template v-if="scope.row.type === 'cc'">
              <p><span class="label">{{$t('th_expression')}}：</span>{{scope.row.content}}</p>
            </template>
            <template v-if="scope.row.type === 'dimension'">
              <p><span class="label">{{$t('th_column')}}：</span>{{JSON.parse(scope.row.content).column}}</p>
              <p><span class="label">{{$t('th_dataType')}}：</span>{{JSON.parse(scope.row.content).data_type}}</p>
            </template>
            <template v-if="scope.row.type === 'measure'">
              <p><span class="label">{{$t('th_column')}}：</span>{{JSON.parse(scope.row.content).name}}</p>
              <p><span class="label">{{$t('th_function')}}：</span>{{JSON.parse(scope.row.content).function.expression}}</p>
              <p><span class="label">{{$t('th_parameter')}}：</span>{{JSON.parse(scope.row.content).function.parameters}}</p>
            </template>
          </template>
        </el-table-column>
        <el-table-column type="index" :label="$t('order')" width="50"></el-table-column>
        <el-table-column :label="$t('th_name')" width="550">
          <template slot-scope="scope">
            <span class="column-name" :title="scope.row.name" :style="{width: scope.row.add ? 'calc(100% - 50px)' : '100%'}">{{scope.row.name}}</span>
            <el-tag class="add-tag" size="mini" type="success" v-if="scope.row.add">{{$t('newAdd')}}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="type" :label="$t('th_type')" width="70" show-overflow-tooltip>
          <template slot-scope="scope">
            {{$t(scope.row.type)}}
          </template>
        </el-table-column>
        <el-table-column prop="cardinality" :label="$t('cardinality')" info-icon="el-ksd-icon-more_info_22" width="130" :info-tooltip="$t('cardinalityColumnTips')" sortable>
          <template slot-scope="scope">
            <span v-if="scope.row.cardinality">{{scope.row.cardinality}}</span>
            <span v-else><i class="no-data_placeholder">NULL</i></span>
          </template>
        </el-table-column>
      </el-table>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button size="medium" @click="showIndexDetail = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" icon="el-ksd-icon-confirm_22" @click="acceptLayout" :loading="accessLoading">{{$t('accept')}}</el-button>
      </div>
    </el-dialog>
    <!-- cc/度量/维度更名 -->
    <el-dialog
      class="layout-details"
      :title="$t('validateTitle')"
      width="480px"
      :append-to-body="true"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      @close="showValidate = false"
      :visible="true"
      v-if="showValidate"
    >
      <p>{{$t('validateModalTip')}}</p>
      <el-table
        nested
        border
        :data="getValidateList"
        class="validate-table"
        size="medium"
        :empty-text="emptyText"
      >
        <el-table-column width="34" type="expand">
          <template slot-scope="scope">
            <template v-if="scope.row.type === 'cc'">
              <p><span class="label">{{$t('th_expression')}}：</span>{{scope.row.content}}</p>
            </template>
            <template v-if="scope.row.type === 'dimension'">
              <p><span class="label">{{$t('th_column')}}：</span>{{JSON.parse(scope.row.content).column}}</p>
              <p><span class="label">{{$t('th_dataType')}}：</span>{{JSON.parse(scope.row.content).data_type}}</p>
            </template>
            <template v-if="scope.row.type === 'measure'">
              <p><span class="label">{{$t('th_column')}}：</span>{{JSON.parse(scope.row.content).name}}</p>
              <p><span class="label">{{$t('th_function')}}：</span>{{JSON.parse(scope.row.content).function.expression}}</p>
              <p><span class="label">{{$t('th_parameter')}}：</span>{{JSON.parse(scope.row.content).function.parameters}}</p>
            </template>
          </template>
        </el-table-column>
        <el-table-column :label="$t('th_name')" width="300" show-overflow-tooltip>
          <template slot-scope="scope">
            <el-form :model="scope.row" :ref="`validateForm_${scope.row.item_id}`" :rules="scope.row.type !== 'cc' ? rules : rulesCC">
              <el-form-item prop="name">
                <el-tooltip class="item" effect="dark" :content="$t('usedInOtherModel')" placement="top" :disabled="scope.row.type === 'cc' ? !scope.row.cross_model : true">
                  <el-input :class="{'is-error': sameNameErrorStatus(scope.row.item_id)}" v-model="scope.row.name" size="mini" @change="changeInputContent" :disabled="scope.row.type === 'cc' ? scope.row.cross_model : false"></el-input>
                </el-tooltip>
                <p class="error-text" v-if="sameNameErrorStatus(scope.row.item_id)">{{$t('sameNameTips')}}</p>
              </el-form-item>
            </el-form>
          </template>
        </el-table-column>
        <el-table-column prop="type" :label="$t('th_type')" show-overflow-tooltip>
          <template slot-scope="scope">
            {{$t(scope.row.type)}}
          </template>
        </el-table-column>
      </el-table>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button size="medium" @click="showValidate = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" icon="" @click="addLayout" :loading="isLoading">{{$t('add')}}</el-button>
      </div>
    </el-dialog>
  </el-card>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapState, mapGetters } from 'vuex'
import { handleSuccessAsync, handleError, ArrayFlat, transToGmtTime } from '../../../../../../util'
import { pageRefTags, NamedRegex1, NamedRegex, pageCount } from '../../../../../../config'
import filterElements from '../../../../../../filter/index'

@Component({
  props: {
    modelDesc: {
      type: Object,
      default: () => {
        return {}
      }
    }
  },
  computed: {
    ...mapState({
      currentProject: state => state.project.selected_project
    }),
    ...mapGetters([
      'datasourceActions'
    ])
  },
  methods: {
    ...mapActions({
      getAllRecommendations: 'GET_ALL_RECOMMENDATIONS',
      deleteRecommendations: 'DELETE_RECOMMENDATIONS',
      accessRecommendations: 'ACCESS_RECOMMENDATIONS',
      getRecommendDetails: 'GET_RECOMMEND_DETAILS',
      validateRecommend: 'VALIDATE_RECOMMEND',
      refreshRecommendationCount: 'RECOMMENDATION_COUNT_REFRESH'
    }),
    ...mapActions('ConfirmSegment', {
      callConfirmSegmentModal: 'CALL_MODAL'
    })
  },
  locales: {
    en: {
      recommendations: 'Recommendations',
      recommendationsTip1: 'Recommendations are generated by analyzing the query history and model usage.',
      recommendationsTip2: 'in project settings.',
      modifyRules: ' Modify rules ',
      th_recommendType: 'Type',
      th_name: 'Name',
      th_table: 'Table',
      th_column: 'Column',
      th_dataType: 'Data Type',
      th_function: 'Function',
      th_parameter: 'Function Parameter',
      th_expression: 'Expression',
      th_dataSize: 'Data Size',
      th_useCount: 'Usage',
      th_column_count: 'Column Totals',
      th_updateDate: 'Last Updated Time',
      th_note: 'Note',
      th_source: 'Source',
      th_type: 'Type',
      imported: 'Import',
      query_history: 'Query History',
      AGG: 'Aggregate Index',
      TABLE: 'Table Index',
      ADD_AGG_INDEX: 'Add Aggregate Index',
      REMOVE_AGG_INDEX: 'Delete Aggregate Index',
      ADD_TABLE_INDEX: 'Add Table Index',
      REMOVE_TABLE_INDEX: 'Delete Table Index',
      usage_time_tip: 'The usage of this index in the past {date} is lower than {time} times.',
      exist_index_tip: 'There already exists one index or more who could include this index.',
      similar_index_tip: 'There already exists one index or more who has a high similarity with this index.',
      imported_index_tip: 'Generated by the imported SQLs',
      query_history_tip: 'Generated by query history',
      LOW_FREQUENCY: 'Low Frequency',
      INCLUDED: 'Included by other indexes',
      SIMILAR: 'High Similarity',
      IMPORTED: 'Imported',
      QUERY_HISTORY: 'Query History',
      accept: 'Accept',
      delete: 'Delete',
      usedCountTip: 'For adding indexes, it means how many historical queries could be optimized;  for deleting indexes, it means how many times the index has been used.',
      viewDetail: 'Details',
      deleteRecommendTip: 'Selected recommendations would be permanently deleted. Do you want to continue?',
      deleteTitle: 'Delete Recommendations',
      deleteSuccess: 'Deleted successfully',
      aggDetailTitle: 'Aggregate Index Details',
      tableDetailTitle: 'Table Index Details',
      order: 'Order',
      cc: 'Computed Columns',
      dimension: 'Dimension',
      measure: 'Measure',
      newAdd: 'Add',
      cardinality: 'Cardinality',
      cardinalityColumnTips: 'Total amount of unique data in this column. Could be gathered from sampling.',
      validateTitle: 'Add Items to Model',
      validateModalTip: 'To accept the selected recommendations, the following items have to be added to the model:',
      add: 'Add',
      requiredName: 'Please enter alias',
      sameNameTips: 'The name already exists. Please rename and try again.',
      bothAcceptAddAndDelete: 'Successfully added {addLength} index(es), and deleted {delLength} index(es)',
      onlyAcceptAdd: 'Successfully added {addLength} index(es)',
      onlyAcceptDelete: 'Successfully deleted {delLength} index(es)',
      bothAddAggAndTableBaseIndex: '{createBaseIndexNum} base index(es) would be added, {updatebaseIndexNum} base index(es) would be updated',
      onlyCreateBaseIndex: '{createBaseIndexNum} base index(es) would be added',
      onlyUpdateBaseIndex: '{updatebaseIndexNum} base index(es) would be updated',
      buildIndexTip: ' Build Index',
      buildIndex: 'Build Index',
      batchBuildSubTitle: 'Please choose which data ranges you\'d like to build with the added indexes.',
      onlyStartLetters: 'Only supports starting with a letter',
      usedInOtherModel: 'Can\'t rename this computed column, as it\'s been used in other models.',
      searchContentOrIndexId: 'Search index content or ID',
      indexContent: 'Content',
      viewIndexDetails: 'More details',
      viewAll: 'View all',
      indexesContent: 'Index Content',
      realTimeModelAcceptRecommendTips: 'To accept recommendations for streaming indexes, follow the steps: stop the streaming job, delete all the streaming segments, accept recommendations, start the streaming job.'
    },
    'zh-cn': {
      recommendations: '优化建议',
      recommendationsTip1: '以下为系统根据您的查询历史及使用情况对模型生成的优化建议。',
      recommendationsTip2: '可在项目设置中',
      modifyRules: '配置规则',
      th_recommendType: '建议类型',
      th_name: '名称',
      th_table: '表',
      th_column: '列',
      th_dataType: '数据类型',
      th_function: '函数',
      th_parameter: '函数参数',
      th_expression: '表达式',
      th_dataSize: '数据大小',
      th_useCount: '使用次数',
      th_column_count: '列数总计',
      th_updateDate: '最后更新时间',
      th_note: '备注',
      th_source: '来源',
      th_type: '类型',
      imported: '导入',
      query_history: '查询历史',
      AGG: '聚合索引',
      TABLE: '明细索引',
      ADD_AGG_INDEX: '新增聚合索引',
      REMOVE_AGG_INDEX: '删除聚合索引',
      ADD_TABLE_INDEX: '新增明细索引',
      REMOVE_TABLE_INDEX: '删除明细索引',
      usage_time_tip: '该索引在过去{date}内使用频率低于{time}次。',
      exist_index_tip: '已有索引可以包含该索引。',
      similar_index_tip: '存在与该索引相似的索引。',
      imported_index_tip: '根据导入 SQL 生成',
      query_history_tip: '根据查询历史生成',
      LOW_FREQUENCY: '低频使用',
      INCLUDED: '包含关系',
      SIMILAR: '高相似度',
      IMPORTED: '导入',
      QUERY_HISTORY: '查询历史',
      accept: '通过',
      delete: '删除',
      usedCountTip: '若为新增索引，表示该索引可优化多少条历史查询；若为删除索引，表示该索引被使用的次数。',
      viewDetail: '查看详情',
      deleteRecommendTip: '所选优化建议删除后不可恢复。确定要删除吗？',
      deleteTitle: '删除优化建议',
      deleteSuccess: '已删除',
      aggDetailTitle: '聚合索引详情',
      tableDetailTitle: '明细索引详情',
      order: '顺序',
      cc: '可计算列',
      dimension: '维度',
      measure: '度量',
      newAdd: '新增',
      cardinality: '基数',
      cardinalityColumnTips: '该列不重复的数据量。该数据可通过采样获得。',
      validateTitle: '添加以下内容至模型',
      validateModalTip: '通过所选优化建议需要添加以下内容至模型：',
      add: '确认添加',
      requiredName: '请输入别名',
      sameNameTips: '该名称已存在，请重新命名。',
      bothAcceptAddAndDelete: '成功新增 {addLength} 条索引，删除 {delLength} 条索引',
      onlyAcceptAdd: '成功新增 {addLength} 条索引',
      onlyAcceptDelete: '成功删除 {delLength} 条索引',
      bothAddAggAndTableBaseIndex: '新增 {createBaseIndexNum} 个基础索引，更新 {updatebaseIndexNum} 个基础索引',
      onlyCreateBaseIndex: '新增 {createBaseIndexNum} 个基础索引',
      onlyUpdateBaseIndex: '更新 {updatebaseIndexNum} 个基础索引',
      buildIndexTip: '立即构建索引',
      buildIndex: '构建索引',
      batchBuildSubTitle: '请为新增的索引选择需要构建至的数据范围。',
      onlyStartLetters: '仅支持字母开头',
      usedInOtherModel: '该可计算列已在其他模型中使用，不可修改名称。',
      searchContentOrIndexId: '搜索索引内容或 ID',
      indexContent: '内容',
      viewIndexDetails: '更多详情',
      viewAll: '查看全部',
      indexesContent: '索引内容',
      realTimeModelAcceptRecommendTips: '通过实时索引的优化建议需依次进行以下操作：停止实时任务，清空实时 Segment，通过优化建议，启动实时任务。'
    }
  }
})
export default class IndexList extends Vue {
  pageRefTags = pageRefTags
  transToGmtTime = transToGmtTime
  loadingRecommends = false
  loadingDetails = false
  recommendationsList = {
    list: [],
    page_offset: 0,
    totalSize: 0,
    sort_by: '',
    reverse: false,
    key: '',
    page_size: +localStorage.getItem(this.pageRefTags.recommendationsPager) || pageCount
  }

  typeList = ['ADD_AGG_INDEX', 'REMOVE_AGG_INDEX', 'ADD_TABLE_INDEX', 'REMOVE_TABLE_INDEX']
  source = ['imported', 'query_history']
  lowFrequency = {
    frequency_time_window: '',
    low_frequency_threshold: 0
  }

  checkedStatus = []
  sourceCheckedStatus = []
  selectedList = []
  showIndexDetail = false
  detailData = []
  currentIndex = null
  accessLoading = false
  validateData = {}
  showValidate = false
  isLoading = false
  rules = {
    name: [{ required: true, validator: this.validateName, trigger: 'blur' }]
  }

  rulesCC = {
    name: [{ required: true, validator: this.validateNameCC, trigger: 'blur' }]
  }

  hasError = false
  sameNameErrorIds = []

  get emptyText () {
    return this.$t('kylinLang.common.noData')
  }

  get indexDetailTitle () {
    return this.currentIndex ? this.currentIndex.type.split('_')[1] === 'AGG' ? this.$t('aggDetailTitle') : this.$t('tableDetailTitle') : ''
  }

  get getValidateList () {
    return this.validateData.list.filter(item => (item.type === 'dimension' && item.add) || (item.type === 'measure' && item.add) || (item.type === 'cc' && item.add))
  }

  sameNameErrorStatus (id) {
    return this.sameNameErrorIds.includes(id)
  }

  // 更改 input 内容
  changeInputContent () {
    this.sameNameErrorIds = []
  }

  formatDataSize (dataSize) {
    if (dataSize < 0) {
      return ''
    } else {
      return filterElements.dataSize(dataSize)
    }
  }

  getCellClassName (scope) {
    return scope.columnIndex === 0 && !scope.row.content ? 'hide-cell-expand' : ''
  }

  created () {
    this.getRecommendations()
  }

  mounted () {
  }

  validateName (rule, value, callback) {
    if (!value || !value.trim()) {
      callback(new Error(this.$t('requiredName')))
    } else if (!NamedRegex1.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip2')))
    } else {
      callback()
    }
  }

  validateNameCC (rule, value, callback) {
    if (!NamedRegex.test(value.toUpperCase())) {
      return callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else if (/^\d|^_+/.test(value)) {
      return callback(new Error(this.$t('onlyStartLetters')))
    } else {
      callback()
    }
  }

  async acceptLayout () {
    this.showIndexDetail = false
    await this.doConfirm([this.currentIndex])
  }

  handleSelectionChange (val) {
    this.selectedList = val
  }

  // 展示优化建议详情
  showDetail (row) {
    this.currentIndex = row
    this.showIndexDetail = true
    this.detailData = [...row.rec_detail_response.cc_items.map(it => ({ ...it, type: 'cc' })), ...row.rec_detail_response.dimension_items.map(it => ({ ...it, type: 'dimension' })), ...row.rec_detail_response.measure_items.map(it => ({ ...it, type: 'measure' }))]
  }

  // 获取优化建议
  getRecommendations (type) {
    const { page_offset, page_size, reverse, sort_by, key } = this.recommendationsList
    this.loadingRecommends = true
    this.getAllRecommendations({
      project: this.currentProject,
      modelId: this.modelDesc.uuid,
      page_offset,
      page_size,
      type: this.checkedStatus.join(','),
      reverse,
      key,
      sort_by
    }).then(async (res) => {
      const data = await handleSuccessAsync(res)
      this.recommendationsList.list = data.layouts
      this.recommendationsList.totalSize = data.size
      if (type && type === 'refreshCount') {
        this.refreshModelRecCount()
      }
      this.loadingRecommends = false
      if (data.broken_recs && data.broken_recs.length > 0) {
        this.refreshRecommendationCount({ model_id: this.modelDesc.uuid, project: this.currentProject, action: 'refresh' })
      }
    }).catch(e => {
      handleError(e)
      this.loadingRecommends = false
    })
  }

  getRowContentList (row) {
    return [...row.rec_detail_response.dimension_items, ...row.rec_detail_response.measure_items, ...row.rec_detail_response.cc_items]
  }

  // 批量删除
  batchDelete () {
    const recs_to_add_layout = []
    const recs_to_remove_layout = []
    this.selectedList.forEach(item => {
      if (item.type.split('_')[0] === 'ADD') {
        recs_to_add_layout.push(item.item_id)
      } else {
        recs_to_remove_layout.push(item.item_id)
      }
    })
    this.removeApi(recs_to_add_layout, recs_to_remove_layout)
  }

  // 删除优化建议
  removeIndex (row) {
    const recs_to_add_layout = []
    const recs_to_remove_layout = []
    row.type.split('_')[0] === 'ADD' ? recs_to_add_layout.push(row.item_id) : recs_to_remove_layout.push(row.item_id)
    this.removeApi(recs_to_add_layout, recs_to_remove_layout)
  }

  async removeApi (recs_to_add_layout, recs_to_remove_layout) {
    await this.$confirm(this.$t('deleteRecommendTip'), this.$t('deleteTitle'), {
      confirmButtonText: this.$t('delete')
    })
    this.deleteRecommendations({
      project: this.currentProject,
      modelId: this.modelDesc.uuid,
      recs_to_add_layout: recs_to_add_layout.join(','),
      recs_to_remove_layout: recs_to_remove_layout.join(',')
    }).then(async (res) => {
      await handleSuccessAsync(res)
      this.$message({
        type: 'success',
        message: this.$t('deleteSuccess')
      })
      this.recommendationsList.page_offset = 0
      this.getRecommendations('refreshCount')
    }).catch(e => {
      handleError(e)
    })
  }

  // 批量通过
  batchAccept () {
    this.doConfirm(this.selectedList)
  }

  // 通过优化建议
  doConfirm (idList) {
    this.validateRecommend({
      project: this.currentProject,
      modelId: this.modelDesc.uuid,
      recs_to_add_layout: idList.filter(it => it.is_add).map(v => v.item_id),
      recs_to_remove_layout: idList.filter(it => !it.is_add).map(v => v.item_id)
    }).then(async (res) => {
      const data = await handleSuccessAsync(res)
      const { recs_to_add_layout, recs_to_remove_layout, cc_items, dimension_items, measure_items } = data
      const addCCLen = cc_items.filter(it => it.add).length
      const addDimensionLen = dimension_items.filter(it => it.add).length
      const addMeasure = measure_items.filter(it => it.add).length
      if (recs_to_add_layout.length && addCCLen + addDimensionLen + addMeasure > 0) {
        this.showValidate = true
        this.validateData = {
          recs_to_add_layout,
          recs_to_remove_layout,
          list: [
            ...cc_items.map(it => ({ ...it, name: it.name.split('.').splice(-1).join(''), type: 'cc' })),
            ...dimension_items.map(it => {
              const name = it.name.split('.').splice(-1).join('')
              if (this.sameNameValidation(name).length > 1) {
                return { ...it, name: it.name.split('.').reverse().join('_'), type: 'dimension' }
              } else {
                return { ...it, name: it.name.split('.').splice(-1).join(''), type: 'dimension' }
              }
            }),
            ...measure_items.filter(item => item.name !== 'COUNT_ALL').map(it => ({ ...it, type: 'measure' }))
          ]
        }
      } else {
        this.validateData = { recs_to_add_layout, recs_to_remove_layout, list: [] }
        this.accessApi(recs_to_add_layout, recs_to_remove_layout)
      }
    }).catch((e) => {
      handleError(e)
    })
  }

  // 判断是否同名，同名则改成 column_table 形式
  sameNameValidation (name) {
    const { all_named_columns } = this.modelDesc
    return all_named_columns.filter(v => {
      const table = v.column ? v.column.split('.')[0] : ''
      const regx = new RegExp(`_${table}$`)
      return v.name === name || v.name.replace(regx, '') === name
    })
  }

  // 添加更名后的layout
  async addLayout () {
    const rfs = Object.keys(this.$refs).filter(it => /^validateForm_/.test(it) && this.$refs[it] && this.$refs[it].model.add)
    for (const item of rfs) {
      const flag = await this.$refs[item].validate()
      if (!flag) {
        this.hasError = true
        break
      }
    }
    if (this.hasError) return
    const names = {}
    const { recs_to_add_layout, recs_to_remove_layout } = this.validateData
    this.isLoading = true
    this.validateData.list.forEach((it) => {
      names[it.item_id] = it.name
    })
    this.accessApi(recs_to_add_layout, recs_to_remove_layout, names).then(() => {
      this.sameNameErrorIds = []
      this.isLoading = false
      this.showValidate = false
    }).catch(e => {
      this.isLoading = false
    })
  }

  // 建议通过接口调用
  async accessApi (recs_to_add_layout, recs_to_remove_layout, names) {
    names = names || {}
    return new Promise((resolve, reject) => {
      const emptyName = Object.values(names).some(it => !it)
      if (emptyName) {
        reject(new Error('emptyName'))
      } else {
        this.accessRecommendations({
          project: this.currentProject,
          modelId: this.modelDesc.uuid,
          recs_to_add_layout,
          recs_to_remove_layout,
          names
        }).then(async (res) => {
          try {
            const result = await handleSuccessAsync(res)
            const acceptIndexs = () => {
              return {
                add: result.added_layouts.length,
                del: result.removed_layouts.length
              }
            }
            this.$message({
              type: 'success',
              message: <span>{this.getAcceptIndexes(acceptIndexs) + this.acceptBaseIndex(result)}<a href="javascript:void(0);" onClick={() => this.buildIndex({ layoutIds: [...result.added_layouts, ...this.getBaseIndexLayout(result)] })}>{
                this.canBuildIndex(acceptIndexs, result) ? this.$t('buildIndex') : ''
              }</a></span>
            })
            this.recommendationsList.page_offset = 0
            this.getRecommendations('refreshCount')
            // this.$emit('accept')
            this.$emit('refreshModel')
            resolve()
          } catch (e) {
            reject(e)
          }
        }).catch(error => {
          const { body: { exception } } = error
          try {
            const data = JSON.parse(exception.split('\n')[1])
            const errorIds = ArrayFlat(Object.values(data))
            if (errorIds.length > 0) {
              this.sameNameErrorIds = errorIds
            } else {
              handleError(error)
            }
          } catch (e) {
            handleError(error)
          }
          // handleError(e)
          reject(error)
        })
      }
    })
  }

  // 是否需要展示构建索引内容
  canBuildIndex (acceptIndexs, result) {
    const { createBaseIndexNum, updateBaseIndexNum } = this.getBaseIndexCount(result.base_index_info)
    return ((acceptIndexs().add > 0 && acceptIndexs().del > 0) || acceptIndexs().add > 0 || createBaseIndexNum > 0 || updateBaseIndexNum > 0) && this.modelDesc.segments.length && this.modelDesc.model_type !== 'STREAMING'
  }

  // 获取 base index layoutId
  getBaseIndexLayout (result) {
    const { base_agg_index, base_table_index } = result.base_index_info
    const list = []
    base_agg_index && list.push(base_agg_index.layout_id)
    base_table_index && list.push(base_table_index.layout_id)
    return list
  }

  // 获取 base index 创建或更新的数量
  getBaseIndexCount (result) {
    const { base_agg_index, base_table_index } = result
    const createBaseIndexNum = [...[base_agg_index ?? {}].filter(it => it.operate_type === 'CREATE'), ...[base_table_index ?? {}].filter(it => it.operate_type === 'CREATE')].length
    const updateBaseIndexNum = [...[base_agg_index ?? {}].filter(it => it.operate_type === 'UPDATE'), ...[base_table_index ?? {}].filter(it => it.operate_type === 'UPDATE')].length
    return { createBaseIndexNum, updateBaseIndexNum }
  }

  // 是否有接受或删除的 layout
  getAcceptIndexes (acceptIndexsFunc) {
    const acceptIndexs = (() => acceptIndexsFunc())()
    return acceptIndexs.add > 0 && acceptIndexs.del > 0
      ? this.$t('bothAcceptAddAndDelete', { addLength: acceptIndexs.add, delLength: acceptIndexs.del })
      : acceptIndexs.add > 0
        ? this.$t('onlyAcceptAdd', { addLength: acceptIndexs.add })
        : this.$t('onlyAcceptDelete', { delLength: acceptIndexs.del })
  }

  // 是否有创建或刷新的基础索引
  acceptBaseIndex (res) {
    const { createBaseIndexNum, updateBaseIndexNum } = this.getBaseIndexCount(res.base_index_info)
    return createBaseIndexNum > 0 && updateBaseIndexNum > 0
      ? `${this.$t('kylinLang.common.comma')}${this.$t('bothAddAggAndTableBaseIndex', { createBaseIndexNum, updatebaseIndexNum: updateBaseIndexNum })}${this.$t('kylinLang.common.dot')}`
      : createBaseIndexNum > 0
        ? `${this.$t('kylinLang.common.comma')}${this.$t('onlyCreateBaseIndex', { createBaseIndexNum })}${this.$t('kylinLang.common.dot')}`
        : updateBaseIndexNum > 0
          ? `${this.$t('kylinLang.common.comma')}${this.$t('onlyUpdateBaseIndex', { updatebaseIndexNum: updateBaseIndexNum })}${this.$t('kylinLang.common.dot')}`
          : `${this.$t('kylinLang.common.dot')}`
  }

  // 新增建议构建索引
  buildIndex ({ layoutIds }) {
    this.callConfirmSegmentModal({
      title: this.$t('buildIndex'),
      subTitle: this.$t('batchBuildSubTitle'),
      indexes: layoutIds || [],
      submitText: this.$t('buildIndex'),
      model: this.modelDesc
    })
  }

  // 删除索引备注hover提示
  removeReasonTip (data) {
    const timeMap = {
      MONTH: { 'zh-cn': '一个月', en: 'month' },
      DAY: { 'zh-cn': '一天', en: 'day' },
      WEEK: { 'zh-cn': '一周', en: 'week' }
    }
    const reason = {
      LOW_FREQUENCY: this.$t('usage_time_tip', { date: this.lowFrequency.frequency_time_window && timeMap[this.lowFrequency.frequency_time_window][this.$store.state.system.lang], time: this.lowFrequency.low_frequency_threshold }),
      INCLUDED: this.$t('exist_index_tip'),
      SIMILAR: this.$t('similar_index_tip'),
      IMPORTED: this.$t('imported_index_tip'),
      QUERY_HISTORY: this.$t('query_history_tip')
    }
    return reason[data.row.memo_info.recommendation_source]
  }

  // 筛选类型来源
  filterType (v, type) {
    this.recommendationsList.page_offset = 0
    type === 'checkedStatus' ? (this.checkedStatus = v) : (this.sourceCheckedStatus = v)
    this.getRecommendations()
  }

  // 分页操作
  pageCurrentChange (offset, size) {
    if (size !== this.recommendationsList.page_size) {
      this.recommendationsList.page_offset = 0
      this.recommendationsList.page_size = size
    } else {
      this.recommendationsList.page_offset = offset
    }
    this.getRecommendations()
  }

  // 更改排序
  changeSort ({ prop, order }) {
    this.recommendationsList = {
      ...this.recommendationsList,
      reverse: order === 'descending',
      sort_by: prop,
      page_offset: 0
    }
    this.getRecommendations()
  }

  // 跳转至 setting 设置界面
  jumpToSetting () {
    this.$router.push({ path: '/setting', query: { moveTo: 'index-suggest-setting' } })
  }

  // 刷新模型列表上的优化建议个数
  refreshModelRecCount () {
    const { page_offset, page_size, reverse, sort_by } = this.recommendationsList
    this.getAllRecommendations({
      project: this.currentProject,
      modelId: this.modelDesc.uuid,
      page_offset,
      page_size,
      type: '',
      reverse,
      sort_by
    }).then(async (res) => {
      const data = await handleSuccessAsync(res)
      this.modelDesc.recommendations_count = data.size
    })
  }

  // 搜索优化建议
  searchRecommendation () {
    this.recommendationsList.page_offset = 0
    this.getRecommendations()
  }
}
</script>

<style lang="less">
@import '../../../../../../assets/styles/variables.less';
.el-card.recommendations-card {
  border: none;
  margin-top: 16px;
  .el-card__header {
    background: none;
    border-bottom: none;
    height: 24px;
    font-size: 14px;
    padding: 0px;
    margin-bottom: 5px;
  }
  .el-card__body {
    padding: 0 !important;

    .detail-content {
      background-color: transparent;
    }
  }
  .title-tip {
    color: @text-normal-color;
    font-size: 12px;
    font-weight: 400;
    line-height: 18px;
  }
}
.el-table.index-details-table {
  .cell {
    height: 28px;
    line-height: 28px;
    span:first-child {
      line-height: 28px;
      vertical-align: revert;
    }
    .el-table__expand-icon {
      >.el-icon {
        margin-top: -2px;
      }
    }
    .column-name {
      display: inline-block;
      width: calc(~'100% - 50px');
      text-overflow: ellipsis;
      overflow: hidden;
      white-space: nowrap;
    }
  }
  .expanded {
    .el-table__expand-icon {
      >.el-icon {
        margin-top: -6px;
        margin-left: -2px;
      }
    }
  }
  .el-table__expanded-cell {
    padding: 24px;
    font-size: 12px;
    color: @text-title-color;
    p {
      margin-bottom: 5px;
      &:last-child {
        margin-bottom: 0;
      }
    }
    .label {
      color: @text-normal-color;
    }
  }
  .add-tag {
    position: absolute;
    right: 10px;
    top: 8px;
  }
  .el-table__expand-column.hide-cell-expand {
    .cell {
      pointer-events: none;
    }
    .el-table__expand-icon {
      color: @text-disabled-color;
      cursor: not-allowed;
    }
  }
}
.el-table.validate-table {
  margin-top: 10px;
  .el-form-item__content {
    line-height: 23px;
    > .is-error {
      .el-input__inner {
        border: 1px solid @error-color-1;
      }
    }
    .error-text {
      color: @error-color-1;
      font-size: 12px;
    }
  }
  .cell {
    height: initial;
  }
  .el-table__expanded-cell {
    padding: 10px;
    font-size: 12px;
    color: @text-title-color;
    p {
      margin-bottom: 5px;
      &:last-child {
        margin-bottom: 0;
      }
    }
    .label {
      color: @text-normal-color;
    }
  }
}
.layout-details {
  .el-dialog__body {
    max-height: 400px;
    overflow: auto;
    .el-form-item__error {
      white-space: normal;
    }
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
.recommendations-table{
  .detail-icon {
    position: absolute;
    top: 50%;
    transform: translate(0, -44%);
    right: 15px;
    font-size: 18px;
    cursor: pointer;
  }
  .index-content {
    cursor: pointer;
    width: calc(~'100% - 30px');
    height: 23px;
    overflow: hidden;
    text-overflow: ellipsis;
    display: -webkit-box;
    -webkit-line-clamp: 1;
     /*! autoprefixer: off */
    -webkit-box-orient: vertical;
    /* autoprefixer: on */
    white-space: nowrap\0;
  }
}

</style>
