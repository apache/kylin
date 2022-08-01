<template>
  <el-dialog
    :title="$t('aggIndexAdvancedTitle')"
    limited-area top="5vh"
    class="aggAdvancedModal"
    width="880px"
    :visible="isShow"
    :append-to-body="true"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @close="isShow && closeModal()">
    <div>
      <el-alert
        :title="$t('advancedTips')"
        type="info"
        :closable="false"
        :show-background="false"
        show-icon>
      </el-alert>
      <!-- 操作区 begin -->
      <div class="clearfix ksd-mt-10">
        <el-input v-model="searchColumn" prefix-icon="el-ksd-icon-search_22" class="ksd-fright" style="width:200px" :placeholder="$t('filter')"></el-input>
      </div>
      <!-- 操作区 end -->

      <!-- list 区 begin -->
      <div class="ky-simple-table">
        <el-row class="table-header table-row ksd-mt-10">
          <el-col :span="18">{{$t('dimension')}}</el-col>
          <el-col :span="3" class="ksd-left">{{$t('cardinality')}}</el-col>
          <el-col :span="3">Shard by</el-col>
        </el-row>
        <div class="table-content" v-scroll.observe.reactive @scroll-bottom="scrollLoad">
          <transition-group name="flip-list" tag="div">
            <el-row v-for="col in searchAllColumns" :key="col.fullName" class="table-row" :class="tableRowClassName(col)">
              <el-col :span="18" :title="col.fullName" class="ksd-left">{{col.fullName}}</el-col>
              <el-col :span="3" class="ksd-left">{{col.cardinality}}</el-col>
              <el-col :span="3">
                <div class="action-list" @click="toggleColumnShard(col)" v-if="!(sortCount >= 9 && getRowIndex(col, 'fullName') + 1 > 9)">
                  <i class="el-icon-success" :class="{active: col.isShared}"></i>
                  <!-- <span class="ky-dot-tag" v-if="col.isUsed" :class="{'no-sorted': !col.isSorted}">{{col.isSorted ? getRowIndex(col, 'fullName') + 1 : sortCount + 1}}</span> -->
                  <!-- <span class="up-down" :class="{hide: searchColumn}">
                    <i v-visible="col.isUsed && col.isSorted && !checkIsTopSort(col)" @click.stop="upRow(col)" class="el-icon-ksd-arrow_up"></i>
                    <i v-visible="col.isUsed && col.isSorted && !checkIsBottomSort(col)" @click.stop="downRow(col)" class="el-icon-ksd-arrow_down"></i>
                  </span> -->
                </div>
              </el-col>
            </el-row>
          </transition-group>
        </div>
      </div>
      <!-- list 区 end -->
    </div>
    <div slot="footer" class="dialog-footer clearfix">
      <div class="ksd-fleft" v-if="aggIndexAdvancedMeta.show_load_data">
        <el-checkbox v-model="loadData">{{$t('kylinLang.common.catchUp')}}</el-checkbox>
      </div>
      <div class="ksd-fright">
        <el-button @click="closeModal" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" :loading="btnLoading" size="medium" @click="submit">{{$t('kylinLang.common.save')}}</el-button>
      </div>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'
import vuex from '../../../../../../store'
import locales from './locales'
import store, { types } from './store'
import { indexOfObjWithSomeKey, filterObjectArray, handleSuccessAsync, changeObjectArrProperty } from 'util/index'
import { handleError, handleSuccess } from 'util/business'

vuex.registerModule(['modals', 'AggAdvancedModal'], store)

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    ...mapState('AggAdvancedModal', {
      isShow: state => state.isShow,
      model: state => state.model,
      callback: state => state.callback
    })
  },
  methods: {
    ...mapMutations('AggAdvancedModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    ...mapActions({
      addAggIndexAdvanced: 'ADD_AGG_INDEX_ADVANCED',
      getAggIndexAdvanced: 'GET_AGG_INDEX_ADVANCED'
    })
  },
  locales
})
export default class AggregateModal extends Vue {
  btnLoading = false
  openShared = false
  searchColumn = ''
  pager = 0
  lockSortAnimate = false // 切换sort 状态列动画控制的标志字段
  currentPager = 1
  pagerSize = 50
  allColumns = []
  loadData = true
  aggIndexAdvancedMetaStr = JSON.stringify({
    shard_by_columns: []
  })

  aggIndexAdvancedMeta = JSON.parse(this.aggIndexAdvancedMetaStr)

  // computed 相关
  get sortCount () {
    if (!this.isShow) {
      return
    }
    return filterObjectArray(this.allColumns, 'isSorted', true).length
  }

  get filterResult () {
    if (!this.isShow) {
      return []
    }
    return this.allColumns.filter((col) => {
      return !this.searchColumn || col.fullName.toUpperCase().indexOf(this.searchColumn.toUpperCase()) >= 0
    })
  }

  get searchAllColumns () {
    if (!this.isShow) {
      return []
    }
    return this.filterResult.slice(0, this.pagerSize * this.currentPager)
  }

  /* get saveBtnDisable () {
    return filterObjectArray(this.allColumns, 'isUsed', true).length === 0
  } */

  // watch 相关
  @Watch('isShow')
  async initTableIndex (val) {
    if (val) {
      // 拉接口获取之前的高级设置
      let params = {
        model: this.model.uuid,
        project: this.currentSelectedProject
      }
      try {
        const res = await this.getAggIndexAdvanced(params)
        Object.assign(this.aggIndexAdvancedMeta, await handleSuccessAsync(res))
        this.getAllColumns()
      } catch (e) {
        // todo 模拟数据 begin
        // let temp = ['P_LINEORDER.LO_SUPPKEY', 'P_LINEORDER.LO_PARTKEY']
        // Object.assign(this.aggIndexAdvancedMeta, {shard_by_columns: temp})
        // todo 模拟数据 end
        this.getAllColumns()
      }
    } else {
      this.aggIndexAdvancedMeta = JSON.parse(this.aggIndexAdvancedMetaStr)
    }
  }

  getDimensions () {
    if (this.model) {
      return this.model.simplified_dimensions
        .filter(column => column.status === 'DIMENSION')
        .map(dimension => ({
          ...dimension,
          label: dimension.column,
          value: dimension.column,
          id: dimension.id
        }))
    } else {
      return []
    }
  }

  // 行 上移
  upRow (col) {
    let i = this.getRowIndex(col, 'fullName')
    this.allColumns.splice(i - 1, 0, col)
    this.allColumns.splice(i + 1, 1)
  }
  // 行 下移
  downRow (col) {
    let i = this.getRowIndex(col, 'fullName')
    this.allColumns.splice(i + 2, 0, col)
    this.allColumns.splice(i, 1)
  }

  tableRowClassName (row) {
    return row.colorful || row.isSorted ? 'row-colorful' : ''
  }

  getRowIndex (t, key) {
    return indexOfObjWithSomeKey(this.allColumns, key, t[key])
  }

  checkIsTopSort (col) {
    let i = this.getRowIndex(col, 'fullName')
    if (i === 0 && col.isSorted) {
      return true
    }
  }

  checkIsBottomSort (col) {
    let i = this.getRowIndex(col, 'fullName')
    let nextCol = this.allColumns[i + 1]
    if (nextCol && !nextCol.isSorted && col.isSorted) {
      return true
    }
  }

  toggleColumnShard (t) {
    // 如果是未选中，则调用时是选中，先所有列置为false（单选）
    if (!t.isShared) {
      this.toggleShard(t)
    } else {
      t.isShared = !t.isShared
    }
    this.toggleSort(t)
  }

  toggleDisplay (t) {
    let i = this.getRowIndex(t, 'fullName')
    if (t.isUsed) {
      if (t.isSorted) {
        this.toggleSort(t, i)
      }
      t.isSorted = false
      t.isShared = false
    }
    t.isUsed = !t.isUsed
  }

  // 切换sort状态的列，并带模拟缓动效果
  toggleSort (t, index) {
    if (this.lockSortAnimate) {
      return
    }
    this.lockSortAnimate = true
    let i = index === undefined ? this.getRowIndex(t, 'fullName') : index
    if (!t.isSorted) {
      changeObjectArrProperty(this.allColumns, '*', 'isSorted', false)
      let sortedLen = filterObjectArray(this.allColumns, 'isSorted', true).length
      this.allColumns.splice(i, 1)
      this.allColumns.splice(sortedLen, 0, t)
    } else {
      let sortedLen = filterObjectArray(this.allColumns, 'isSorted', true).length
      let s = indexOfObjWithSomeKey(this.allColumns, 'isSorted', false)
      if (s === -1) {
        s = this.allColumns.length
      }
      this.allColumns.splice(i, 1)
      this.allColumns.splice(sortedLen - 1, 0, t)
    }
    t.isSorted = !t.isSorted
    setTimeout(() => {
      this.lockSortAnimate = false
    }, 300)
  }

  // 切换shard
  toggleShard (t) {
    let shardStatus = t.isShared
    changeObjectArrProperty(this.allColumns, '*', 'isShared', false)
    t.isShared = !shardStatus
  }
  // 懒加载
  scrollLoad () {
    if (this.searchAllColumns && this.searchAllColumns.length !== this.filterResult.length) {
      this.currentPager += 1
    }
  }

  getAllColumns () {
    this.allColumns = []
    let result = []
    let modelUsedTables = [...this.getDimensions()] || []
    modelUsedTables.forEach((col) => {
      result.push({...col, fullName: col.value})
    })
    // cc列也要放到这里
    /* let ccColumns = this.model && this.model.computed_columns || []
    ccColumns.forEach((col) => {
      result.push(col.tableAlias + '.' + col.columnName)
    }) */
    if (this.aggIndexAdvancedMeta.shard_by_columns) {
      const selected = this.aggIndexAdvancedMeta.shard_by_columns.map(item => {
        const index = result.findIndex(it => it.value === item)
        return {...result[index]}
      })
      const unSort = result.filter(item => !this.aggIndexAdvancedMeta.shard_by_columns.includes(item.fullName))
      result = [...selected, ...unSort]
    }
    result.forEach((item, index) => {
      let obj = {...item, isSorted: false, isUsed: false, isShared: false, colorful: false}
      if (this.aggIndexAdvancedMeta.shard_by_columns.indexOf(item.fullName) >= 0) {
        obj.isSorted = true
        obj.isUsed = true
        obj.isShared = true
      }
      this.allColumns.push(obj)
    })
  }

  pagerChange (pager) {
    this.pager = pager
  }

  closeModal (isSubmit) {
    this.hideModal()
    this.btnLoading = false
    setTimeout(() => {
      this.callback && this.callback({
        isSubmit: isSubmit
      })
      this.resetModalForm()
    }, 200)
  }

  async submit () {
    this.btnLoading = true
    let successCb = (res) => {
      handleSuccess(res, (data) => {
        // 提交成功的回调，这里重新处理
        this.$message({message: this.$t('kylinLang.common.submitSuccess'), type: 'success'})
        this.$emit('refreshCuboids')
      })
      this.closeModal(true)
      this.btnLoading = false
    }
    let errorCb = (res) => {
      this.btnLoading = false
      handleError(res)
    }
    // 按照sort选中列的顺序对col_order进行重新排序
    this.aggIndexAdvancedMeta.shard_by_columns = []
    this.allColumns.forEach((col) => {
      if (col.isShared) {
        this.aggIndexAdvancedMeta.shard_by_columns.push(col.fullName)
      }
    })
    this.aggIndexAdvancedMeta.project = this.currentSelectedProject
    this.aggIndexAdvancedMeta.model_id = this.model.uuid
    this.aggIndexAdvancedMeta.load_data = this.aggIndexAdvancedMeta.show_load_data ? this.loadData : false
    this.addAggIndexAdvanced(this.aggIndexAdvancedMeta).then(successCb, errorCb)
  }
}
</script>

<style lang="less">
@import '../../../../../../assets/styles/variables.less';
.aggAdvancedModal{
  .el-dialog {
    min-width: 700px;
  }
  .flip-list-move {
    transition: transform .5s;
  }
  .action-list {
    position:relative;
    .up-down {
      position: absolute;
      right:-8px;
      display: none;
      i {
        color:@base-color;
      }
    }
    &:hover {
      cursor: default;
      .up-down {
        display: inline-block;
      }
    }
  }
  .row-colorful {
    background:@lighter-color-tip!important;
  }
  .el-icon-success {
    cursor:pointer;
    &.active{
      color:@color-success;
    }
    color:@text-placeholder-color;
  }
  .ky-dot-tag {
    cursor:pointer;
  }
  .no-sorted {
    background:@text-placeholder-color;
  }
  .sub-title {
    margin-top:60px;
  }
  .show-pagers{
    width: 42px;
    position: absolute;
    top: 100px;
    right: -16px;
    ul {
      li {
        border:solid 1px #ccc;
        margin-bottom:12px;
        text-align:center;
      }
    }
  }
  .show-more-block {
    width:120px;
    height:20px;
    line-height:20px;
    text-align:center
  }
  .sort-icon {
    .ky-square-box(32px, 32px);
    background: @text-secondary-color;
    display: inline-block;
    color:@fff;
    vertical-align: baseline;
  }
  .table-index-columns {
    li {
      margin-top:20px;
      height:32px;
    }
  }
}
</style>
