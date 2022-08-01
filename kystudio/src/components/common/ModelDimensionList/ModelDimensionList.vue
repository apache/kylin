<template>
  <div class="model-dimension-list">
    <div class="list-empty" v-if="model.dimensions.length === 0">
      <div>{{$t('kylinLang.common.noDimensionInModel')}}</div>
      <div>{{$t('kylinLang.common.pleaseClickEditModel')}}</div>
    </div>
    <template v-else>
      <div class="dimension-list-header clearfix">
        <div class="ksd-fright">
          <el-input
            class="dimension-filter"
            v-model.trim="filters[0].name"
            prefix-icon="el-icon-search"
            :placeholder="$t('kylinLang.common.searchDimensionName')"
          />
        </div>
      </div>
      <el-table class="model-dimension-table" :data="dimensions.data" style="width: 100%">
        <el-table-column :label="$t('kylinLang.common.dimension')">
          <span slot-scope="scope" class="text">{{scope.row.name}}</span>
        </el-table-column>
        <el-table-column prop="table" :label="$t('kylinLang.common.tableName')" />
        <el-table-column prop="column" :label="$t('kylinLang.dataSource.columnName')" />
        <el-table-column prop="dataType" :label="$t('kylinLang.dataSource.dataType')" width="160px" />
      </el-table>
      <kylin-pager
        class="ksd-center ksd-mtb-10"
        :refTag="pageRefTags.modelDimensionPager"
        layout="total, prev, pager, next"
        :totalSize="dimensions.totalCount"
        :perPageSize="dimensions.pageSize"
        :curPage="dimensions.pageOffset + 1"
        @handleCurrentChange="value => pageOffset = value">
      </kylin-pager>
    </template>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { pageRefTags, pageCount } from 'config'
import { dataHelper } from '../../../util'

@Component({
  props: {
    model: {
      type: Object
    }
  }
})
export default class ModelDimensionList extends Vue {
  pageRefTags = pageRefTags
  pageOffset = 0
  pageSize = +localStorage.getItem(this.pageRefTags.modelDimensionPager) || pageCount
  filters = [
    { name: '' }
  ]

  get dimensions () {
    const { filters, model: { dimensions: datas }, pageOffset, pageSize } = this
    return dataHelper.getPaginationTable({ filters, datas, pageOffset, pageSize })
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.model-dimension-list {
  .list-empty {
    position: absolute;
    top: 30%;
    left: 50%;
    transform: translate(-50%, -30%);
    color: @text-secondary-color;
    text-align: center;
  }
  .dimension-list-header {
    margin-bottom: 10px;
  }
  .dimension-filter {
    width: 250px;
  }
  // .el-table__body-wrapper {
    // font-size: 12px;
  // }
  // .el-table .cell {
  //   line-height: 18px;
  // }
  .model-dimension-table {
    .text {
      white-space: pre-wrap;
    }
  }
}
</style>
