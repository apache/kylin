<template>
  <div id="subPartitionValuesList">
    <div class="ksd-title-label">
      <el-tooltip :content="$t('kylinLang.common.back')" effect="dark" placement="top">
        <i class="back-btn el-icon-ksd-iconback_1414" @click="backToModelList"></i>
      </el-tooltip>
      <span>{{$t('subParValuesTitle')}}</span>
    </div>
    <div class="clearfix">
      <div class="ksd-fleft">
        <el-button icon="el-ksd-icon-add_22" type="primary" text class="ksd-mt-10" @click="openAddDialog">{{$t('subParValuesTitle')}}</el-button>
        <el-button icon="el-ksd-icon-table_delete_22" type="primary" text class="ksd-mt-10" :disabled="!selectedParValues.length" @click="dropSubPartitionValues">{{$t('kylinLang.common.delete')}}</el-button>
      </div>
      <div class="ksd-fright">
        <el-input class="ksd-mt-10" :placeholder="$t('searchPlaceholder')" prefix-icon="el-ksd-icon-search_22" v-global-key-event.enter.debounce="onFilterChange" @clear="onFilterChange()" v-model="subParValuesFilter"></el-input>
      </div>
    </div>
    <el-table
      ref="subPartitionValuesTable"
      :data="pagerTableData"
      style="width: 100%"
      class="ksd-mt-10"
      @sort-change="sortChangePartition"
      @selection-change="handleSelectionChange">
      <el-table-column type="selection" width="44"> </el-table-column>
      <el-table-column :label="$t('subParValuesTitle')" sortable="custom" prop="partition_value">
        <template slot-scope="scope">
          {{scope.row.partition_value[0]}}
        </template>
      </el-table-column>
      <el-table-column
        header-align="right"
        align="right"
        prop="built_segment_count"
        sortable="custom"
        :label="$t('segmentAmount')"
        :info-tooltip="$t('segmentAmountTips')"
        info-icon="el-ksd-icon-more_info_22"
        width="300">
        <template slot-scope="scope">
          {{scope.row.built_segment_count}} / {{scope.row.total_segment_count}}
        </template>
      </el-table-column>
    </el-table>
    <kylin-pager :totalSize="subPartitionValuesTotal" :curPage="filter.page_offset+1"  v-on:handleCurrentChange='pageSizeChange' ref="subPartitionValuesPager" :refTag="pageRefTags.subPartitionValuesPager" class="ksd-mtb-10 ksd-center" ></kylin-pager>
    <el-dialog
      :visible.sync="addSubParValueVisible"
      width="480px"
      class="add-sub-par-value-dialog"
      :close-on-click-modal="false"
      :before-close="handleClose">
      <span slot="title" class="ksd-title-label">{{$t('addSubParValueTitle')}}</span>
      <div class="ksd-mb-10"><i class="icon el-icon-ksd-alert ksd-mr-5"></i>{{$t('addSubParValueDesc')}}</div>
      <div :class="['arealabel-block', {'error-border': duplicateValueError}]">
        <arealabel
          ref="selectSubPartition"
          class="select-sub-partition"
          :duplicateremove="false"
          splitChar=","
          :isNeedNotUpperCase="true"
          :allowcreate="true"
          :isSignSameValue="true"
          :selectedlabels="addedPartitionValues"
          :placeholder="$t('multiPartitionPlaceholder')"
          @duplicateTags="checkDuplicateValue"
          @refreshData="refreshPartitionValues"
          @removeTag="removeSelectedMultiPartition"
          :datamap="{label: 'label', value: 'value'}">
        </arealabel>
      </div>
      <p class="duplicate-tips" v-if="duplicateValueError"><span class="error-msg">{{$t('duplicatePartitionValueTip')}}</span><span class="clear-value-btn" @click="removeDuplicateValue"><i class="el-icon-ksd-clear ksd-mr-5"></i>{{$t('removeDuplicateValue')}}</span></p>
      <span slot="footer" class="dialog-footer">
        <el-button plain @click="handleClose">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button :loading="submitLoading" :disabled="duplicateValueError || !partitionValues.length" @click="submitAddSubParValues">{{$t('kylinLang.common.add')}}</el-button>
      </span>
    </el-dialog>
    <!-- 删除子分区值二次确认弹窗 -->
    <el-dialog
      v-if="showDeleteSubPartitionDialog"
      :visible="true"
      width="480px"
      class="delete-sub-par-value-dialog"
      :close-on-click-modal="false"
      :before-close="closeDeleteSubPartitionDialog">
      <span slot="title" class="ksd-title-label">{{$t('deleteSubPartitionValuesTitle')}}</span>
      <!-- <div class="ksd-title-label-small ksd-mb-10">{{$t('deleteSubPartitionValuesTip')}}</div> -->
      <div v-if="!hasBuildInSegement"><i class="el-icon-ksd-info alert-info ksd-mr-5"></i><p class="alert-msg" v-html="$t('deleteSubPartitionValuesTip', {subSegsLength: selectedParValues.length})"></p></div>
      <div v-else><i class="el-icon-ksd-info alert-info ksd-mr-5"></i><p class="alert-msg" v-html="$t('deleteSubPartitionValuesByBuild', {subSegsLength: selectedParValues.length, subSegsByBuildLength: selectedParValues.filter(it => it.built_segment_count > 0).length})"></p></div>
      <el-table
        border
        :data="deletePartitionValueData"
        style="width: 100%"
        class="ksd-mt-10"
        v-if="hasBuildInSegement"
      >
        <el-table-column :label="$t('subParValuesTitle')" prop="partition_value" width="160">
          <template slot-scope="scope">
            {{scope.row.partition_value[0]}}
          </template>
        </el-table-column>
        <el-table-column
          header-align="right"
          align="right"
          prop="built_segment_count"
          :label="$t('segmentAmount')"
          :info-tooltip="$t('segmentAmountTips')"
          info-icon="el-ksd-icon-more_info_22"
        >
          <template slot-scope="scope">
            {{scope.row.built_segment_count}} / {{scope.row.total_segment_count}}
          </template>
        </el-table-column>
      </el-table>
      <span slot="footer" class="dialog-footer">
        <el-button plain @click="closeDeleteSubPartitionDialog">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button @click="confirmDropSubPartitionValues">{{$t('kylinLang.common.delete')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { pageRefTags, bigPageCount } from '../../../../config'
import locales from './locales'
import arealabel from '../../../common/area_label.vue'
import { handleSuccessAsync, handleError, split_array } from '../../../../util'
@Component({
  beforeRouteEnter (to, from, next) {
    next(vm => {
      vm.fromRoute = from
    })
  },
  components: {
    arealabel
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      fetchSubPartitionValues: 'FETCH_SUB_PARTITION_VALUES',
      addPartitionValues: 'ADD_PARTITION_VALUES',
      deletePartitionValues: 'DELETE_PARTITION_VALUES'
    })
  },
  locales
})
export default class subPartitionValues extends Vue {
  fromRoute = null
  pageRefTags = pageRefTags
  subParValuesFilter = ''
  subPartitionValuesList = []
  pagerTableData = []
  subPartitionValuesTotal = 0
  filter = {
    page_offset: 0,
    page_size: +localStorage.getItem(this.pageRefTags.subPartitionValuesPager) || bigPageCount,
    sort_by: '',
    reverse: false
  }
  addSubParValueVisible = false
  addedPartitionValues = []
  partitionValues = []
  submitLoading = false
  selectedParValues = []
  duplicateValueError = false
  showDeleteSubPartitionDialog = false

  get hasBuildInSegement () {
    return this.selectedParValues.filter(it => it.built_segment_count > 0).length > 0
  }

  handleSelectionChange (rows) {
    this.selectedParValues = rows
  }

  get deletePartitionValueData () {
    return this.selectedParValues.filter(it => it.built_segment_count > 0)
  }

  dropSubPartitionValues () {
    this.showDeleteSubPartitionDialog = true
  }

  async confirmDropSubPartitionValues () {
    // await kylinConfirm(this.$t('deleteSubPartitionValuesTip', {subSegsLength: this.selectedParValues.length}), {confirmButtonText: this.$t('kylinLang.common.delete')}, this.$t('deleteSubPartitionValuesTitle'))
    try {
      const ids = this.selectedParValues.map((sub) => {
        return sub.id
      })
      await this.deletePartitionValues({project: this.currentSelectedProject, model_id: this.$route.params.modelId, ids: ids.join(',')})
      this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
      this.showDeleteSubPartitionDialog = false
      this.loadSubPartitionValues()
    } catch (e) {
      handleError(e)
      this.showDeleteSubPartitionDialog = false
      this.loadSubPartitionValues()
    }
  }

  closeDeleteSubPartitionDialog () {
    this.showDeleteSubPartitionDialog = false
  }

  refreshPartitionValues (val) {
    this.partitionValues = val
  }

  openAddDialog () {
    this.addSubParValueVisible = true
  }
  async submitAddSubParValues () {
    this.submitLoading = true
    try {
      const partitionValuesArr = split_array(this.partitionValues, 1)
      await this.addPartitionValues({ project: this.currentSelectedProject, model_id: this.$route.params.modelId, sub_partition_values: partitionValuesArr })
      this.submitLoading = false
      this.addSubParValueVisible = false
      this.addedPartitionValues = []
      this.partitionValues = []
      this.loadSubPartitionValues()
    } catch (e) {
      handleError(e)
      this.submitLoading = false
      this.addedPartitionValues = []
      this.partitionValues = []
      this.addSubParValueVisible = false
    }
  }
  backToModelList () {
    if (this.fromRoute && this.fromRoute.name && this.fromRoute.name === 'ModelDetails') {
      this.$router.replace({name: this.fromRoute.name, params: {modelName: this.$route.params.modelName}})
      return
    }
    if (this.$route.params && this.$route.params.expandTab) {
      this.$router.replace({name: 'ModelList', params: { expandTab: 'first' }})
    } else {
      this.$router.replace({name: 'ModelList'})
    }
  }
  handleClose () {
    this.addSubParValueVisible = false
    this.addedPartitionValues = []
    this.partitionValues = []
    this.duplicateValueError = false
  }
  removeSelectedMultiPartition () {}

  // renderSegmentAmountHeader (h, { column, $index }) {
  //   return (<span class="ky-hover-icon" onClick={e => (e.stopPropagation())}>
  //     <span>{this.$t('segmentAmount')}</span>&nbsp;
  //     <common-tip placement="top" content={this.$t('segmentAmountTips')}>
  //      <span class='el-ksd-icon-more_info_22'></span>
  //     </common-tip>
  //   </span>)
  // }
  async loadSubPartitionValues () {
    try {
      const res = await this.fetchSubPartitionValues({ project: this.currentSelectedProject, model_id: this.$route.params.modelId })
      const data = await handleSuccessAsync(res)
      this.subPartitionValuesList = data
      this.pageSizeChange(0)
    } catch (e) {
      handleError(e)
    }
  }
  pageSizeChange (currentPage, pageSize) {
    const {sort_by, reverse, page_size} = this.filter
    const size = pageSize || page_size
    this.filter.page_offset = currentPage
    this.filter.page_size = size
    const filteredData = this.subPartitionValuesList.filter((s) => {
      return s.partition_value[0].toLowerCase().indexOf(this.subParValuesFilter) !== -1
    }).sort((prev, next) => {
      if (sort_by === 'partition_value') {
        return reverse ? next['partition_value'][0].charCodeAt() - prev['partition_value'][0].charCodeAt() : prev['partition_value'][0].charCodeAt() - next['partition_value'][0].charCodeAt()
      } else {
        return reverse ? next['built_segment_count'] - prev['built_segment_count'] : prev['built_segment_count'] - next['built_segment_count']
      }
    })
    this.subPartitionValuesTotal = filteredData.length
    this.pagerTableData = filteredData.slice(currentPage * size, (currentPage + 1) * size)
  }

  onFilterChange () {
    this.pageSizeChange(0)
  }

  sortChangePartition ({column, prop, order}) {
    this.filter = {
      ...this.filter,
      sort_by: prop,
      reverse: order === 'descending'
    }
    this.pageSizeChange(0)
  }

  // 检测是否输入了重复的子分区值
  checkDuplicateValue (type) {
    this.duplicateValueError = type
  }

  removeDuplicateValue () {
    this.$refs.selectSubPartition && this.$refs.selectSubPartition.clearDuplicateValue()
  }

  created () {
    this.loadSubPartitionValues()
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
#subPartitionValuesList {
  margin: 20px;
  .back-btn {
    cursor: pointer;
    &:hover {
      color: @base-color-2;
    }
  }
  .error-msg {
    color: @error-color-1;
    font-size: 12px;
    margin-top: 5px;
  }
  .duplicate-tips {
    font-size: 12px;
    margin-top: 5px;
    .clear-value-btn {
      cursor: pointer;
      color: @text-normal-color;
      &:hover {
        color: @base-color;
      }
    }
  }
  .select-sub-partition.error-border {
    border: 1px solid @error-color-1;
  }
}
.add-sub-par-value-dialog {
  .arealabel-block {
    border: 1px solid @line-border-color3;
    max-height: 150px;
    min-height: 60px;
    overflow: auto;
    .el-input__inner {
      border: none;
    }
  }
  .icon {
    color: @text-disabled-color;
  }
}
.delete-sub-par-value-dialog {
  .alert-info {
    color: @warning-color-1;
  }
  .alert-msg {
    display: inline-block;
    width: calc(~'100% - 20px');
    vertical-align: top;
  }
}
</style>
