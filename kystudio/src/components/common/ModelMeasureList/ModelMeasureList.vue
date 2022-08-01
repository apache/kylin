<template>
  <div class="model-measure-list">
    <div class="list-empty" v-if="model.measures.length === 0">
      <div>{{$t('kylinLang.common.noMeasureInModel')}}</div>
      <div>{{$t('kylinLang.common.pleaseClickEditModel')}}</div>
    </div>
    <template v-else>
      <div class="measure-list-header clearfix">
        <div class="ksd-fright">
          <el-input
            class="measure-filter"
            v-model.trim="filters[0].name"
            prefix-icon="el-icon-search"
            :placeholder="$t('kylinLang.common.searchMeasureName')"
          />
        </div>
      </div>
      <el-table class="model-measure-table" :data="measures.data" style="width: 100%">
        <el-table-column prop="name" :label="$t('kylinLang.cube.measure')" width="150px">
          <span slot-scope="scope" class="text">{{scope.row.name}}</span>
        </el-table-column>
        <el-table-column prop="comment" :label="$t('kylinLang.dataSource.comment')" show-overflow-tooltip width="150px"></el-table-column>
        <el-table-column prop="expression" :label="$t('kylinLang.dataSource.expression')" width="170px" />
        <el-table-column prop="parameters" :label="$t('kylinLang.model.parameters')">
          <template slot-scope="scope">
            <template v-if="scope.row.expression === 'TOP_N'">
              <div class="parameter" v-for="(parameter, idx) in scope.row.parameters" :key="`${parameter.type}.${parameter.value}`">
                <span class="parameter-label" v-if="idx === 0">{{$t('kylinLang.model.orderBy_c')}}</span>
                <span class="parameter-label" v-else>{{$t('kylinLang.model.groupBy_c')}}</span>
                <span class="parameter-value">{{parameter.value}}</span>
              </div>
            </template>
            <template v-else>
              <div class="parameter" v-for="parameter in scope.row.parameters" :key="`${parameter.type}.${parameter.value}`">
                <span class="parameter-label">{{$t('kylinLang.model.type_c')}}</span>
                <span class="parameter-type">{{parameter.type}}</span>
                <span class="parameter-label">{{$t('kylinLang.model.value_c')}}</span>
                <span class="parameter-value">{{parameter.value}}</span>
              </div>
            </template>
          </template>
        </el-table-column>
        <el-table-column prop="returnType" :label="$t('kylinLang.model.returnType')" width="160px" />
      </el-table>
      <kylin-pager
        class="ksd-center ksd-mtb-10"
        :refTag="pageRefTags.modelMeasurePager"
        layout="total, prev, pager, next"
        :totalSize="measures.totalCount"
        :perPageSize="measures.pageSize"
        :curPage="measures.pageOffset + 1"
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
export default class ModelMeasureList extends Vue {
  pageRefTags = pageRefTags
  pageOffset = 0
  pageSize = +localStorage.getItem(this.pageRefTags.modelMeasurePager) || pageCount
  filters = [
    { name: '' }
  ]

  get measures () {
    const { filters, model: { measures: datas }, pageOffset, pageSize } = this
    return dataHelper.getPaginationTable({ filters, datas, pageOffset, pageSize })
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.model-measure-list {
  .list-empty {
    position: absolute;
    top: 30%;
    left: 50%;
    transform: translate(-50%, -30%);
    color: @text-secondary-color;
    text-align: center;
  }
  .measure-list-header {
    margin-bottom: 10px;
  }
  .measure-filter {
    width: 250px;
  }
  .parameter-label {
    color: @color-info;
    white-space:nowrap;
  }
  .parameter-type {
    white-space:nowrap;
  }
  .parameter-type:nth-child(2) + .parameter-label {
    margin-left: 5px;
  }
  // .el-table__body-wrapper {
  //   font-size: 12px;
  // }
  // .el-table .cell {
  //   line-height: 18px;
  // }
  .model-measure-table {
    .parameter {
      display: flex;
    }
    .text {
      white-space: pre-wrap;
    }
  }
}
</style>
