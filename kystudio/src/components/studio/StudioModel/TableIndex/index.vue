<template>
   <div class="tableIndex-box">
     <kylin-empty-data v-if="isNoData" size="small"></kylin-empty-data>
     <div class="left-part">
      <div class="ksd-mb-20">
        <el-button type="primary" size="small" icon="el-icon-ksd-add_2" v-if="isShowTableIndexActions" v-visible="!isHideEdit" @click="editTableIndex(true)">{{$t('tableIndex')}}
        </el-button><el-button :loading="buildIndexLoading"
          type="primary" size="small"  @click="buildTableIndex" v-visible="isShowBulidIndex && !isHideEdit">
        {{$t('buildIndex')}}
       </el-button>
        <!-- <el-button type="primary" disabled icon="el-icon-ksd-table_refresh">Refresh</el-button> -->
        <!-- <el-button icon="el-icon-ksd-table_delete">Delete</el-button> -->
        <el-input style="width:200px" size="small" v-model="tableIndexFilter" :prefix-icon="searchLoading? 'el-icon-loading':'el-ksd-icon-search_22'" :placeholder="$t('searchTip')" class="ksd-fright ksd-mr-15"></el-input>
      </div>
      <el-steps direction="vertical">
        <template v-for="(tableIndex, key) in tableIndexGroup">
          <el-step :title="$t(key) + '(' + tableIndex.length + ')'" status="finish" v-if="tableIndex.length"  :key="key">
            <div slot="icon"><i class="el-icon-ksd-elapsed_time"></i></div>
            <div slot="description">
              <el-carousel @change="(i) => {changeTableIndexCard(tableIndex[i])}"  indicator-position="none" :arrow="tableIndex.length === 1 ? 'never' : 'hover'"  :interval="4000" type="card" height="187px" :autoplay="false" :initial-index="tableIndex.length - 1">
                <el-carousel-item class="card-box" v-for="item in tableIndex" :key="item.name" @click.native="showTableIndexDetal(item)" :class="{'table-index-active': currentShowTableIndex && currentShowTableIndex.id === item.id}">
                  <img v-if="item.manual" class="icon-tableindex-type" src="../../../../assets/img/icon_model/index_manual.png"/>
                  <img v-else class="icon-tableindex-type" src="../../../../assets/img/icon_model/index_auto.png"/>
                  <div class="card-content" :class="{'empty-table-index': item.status === 'EMPTY', 'is-manual': item.manual}">
                    <div class="slider-content-above">
                      <div class="main-title" :title="item.name">{{item.name|omit(24, '...')}}</div>
                      <div class="status-list">
                        <!-- <div v-if="item.status === 'AVAIABLE'" class="broken-icon">{{$t('available ')}}</div> -->
                        <div v-if="item.status === 'BROKEN'" class="broken-icon">{{$t('broken')}}</div>
                        <div v-if="item.status === 'EMPTY'" class="empty-icon">{{$t('empty')}}</div>
                      </div>
                      <div class="sub-info">
                        <div class="sub-title"><span>{{$t('tableIndexId')}}</span>{{item.id}}</div>
                        <i class="el-icon-ksd-elapsed_time ksd-mr-4"></i>{{transToGmtTime(item.update_time)}}
                        <div class="actions ksd-fright">
                          <span class="del-icon" v-if="item.manual&&isShowTableIndexActions" @click="delTableIndex(item.id, item.name)">{{$t('kylinLang.common.delete')}}</span>
                        </div>
                      </div>
                    </div>
                    <div class="ky-line"></div>
                    <div class="slider-content-below">
                      <span class="tableindex-user">{{item.owner}}</span>
                      <span class="tableindex-count"><span>{{item.col_order.length}}</span> Columns</span>
                    </div>
                  </div>
                </el-carousel-item>
              </el-carousel>
            </div>
          </el-step>
        </template>
      </el-steps>
    </div>
    <div class="right-part">
      <el-card class="box-card">
        <div slot="header" class="clearfix">
          <span><i class="el-icon-ksd-index_handy table-index-type" v-if="currentShowTableIndex && currentShowTableIndex.manual"></i><i v-if="currentShowTableIndex && !currentShowTableIndex.manual" class="el-icon-ksd-index_auto"></i> {{$t('tableIndexDetail')}}</span>
          <div class="ksd-fright ksd-fs-14"><span class="ksd-mr-4" v-if="currentShowTableIndex && currentShowTableIndex.manual">{{$t('manualAdvice')}}</span><span class="ksd-mr-4" v-if="currentShowTableIndex && !currentShowTableIndex.manual">{{$t('autoAdvice')}}</span><el-button size="mini" v-if="currentShowTableIndex && currentShowTableIndex.manual && isShowTableIndexActions" @click="editTableIndex(false)"  icon="el-icon-ksd-table_edit">{{$t('kylinLang.common.edit')}}</el-button></div>
        </div>
        <div>
          <el-table
          nested
          size="medium"
          v-scroll-shadow
          :data="showTableIndexDetail"
          height="529px"
          class="table-index-detail">
          <el-table-column
            :label="$t('ID')"
            prop="id"
            width="64">
          </el-table-column>
          <el-table-column
            show-overflow-tooltip
            :label="$t('column')"
            prop="column">
          </el-table-column> 
          <el-table-column
          :label="$t('sort')"
          prop="sort"
          width="60"
          align="center">
          <template slot-scope="scope">
            <span class="ky-dot-tag" v-show="scope.row.sort">{{scope.row.sort}}</span>
          </template>
            </el-table-column>
          <el-table-column
          label="Shard"
          align="center"
          width="70">
            <template slot-scope="scope">
                <i class="el-icon-ksd-good_health ky-success" v-show="scope.row.shared"></i>
            </template>
            </el-table-column>         
          </el-table>
          <kylin-pager layout="prev, pager, next" :background="false" class="ksd-mt-10 ksd-center" ref="pager" :refTag="pageRefTags.tableIndexDetailPager" :perPageSize="currentCount" :totalSize="totalTableIndexColumnSize"  v-on:handleCurrentChange='currentChange'></kylin-pager>
        </div> 
      </el-card>
    </div>
     <TableIndexEdit/>
   </div>
</template>
<script>

import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import locales from './locales'
import { handleSuccess, handleError, transToGmtTime, kylinConfirm } from 'util/business'
import { isToday, isThisWeek, isLastWeek } from 'util/index'
import { BuildIndexStatus } from 'config/model'
import TableIndexEdit from '../TableIndexEdit/tableindex_edit'
import NModel from '../ModelEdit/model.js'
import { pageRefTags, pageCount } from 'config'
@Component({
  props: ['modelDesc', 'isHideEdit', 'layoutId', 'isShowTableIndexActions', 'isShowBulidIndex'],
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    modelInstance () {
      this.modelDesc.project = this.currentSelectedProject
      return new NModel(this.modelDesc)
    },
    // 当天的数据
    todayTableIndex () {
      return this.tableIndexBaseList.filter((t) => {
        if (isToday(t.update_time) && (t.id === +this.tableIndexFilter || this.tableIndexFilter === '')) {
          return t
        }
      })
    },
    // 本周的数据（排除当天）
    thisWeekTableIndex () {
      return this.tableIndexBaseList && this.tableIndexBaseList.filter((t) => {
        if (!isToday(t.update_time) && isThisWeek(t.update_time) && (t.id === +this.tableIndexFilter || this.tableIndexFilter === '')) {
          return t
        }
      }) || []
    },
    // 上周的数据
    lastWeekTableIndex () {
      return this.tableIndexBaseList && this.tableIndexBaseList.filter((t) => {
        if (isLastWeek(t.update_time) && (t.id === +this.tableIndexFilter || this.tableIndexFilter === '')) {
          return t
        }
      }) || []
    },
    // 更久远的数据...
    longAgoTableIndex () {
      return this.tableIndexBaseList && this.tableIndexBaseList.filter((t) => {
        if (!isLastWeek(t.update_time) && !isThisWeek(t.update_time) && (t.id === +this.tableIndexFilter || this.tableIndexFilter === '')) {
          return t
        }
      }) || []
    },
    tableIndexGroup () {
      return {
        today: this.todayTableIndex,
        thisWeek: this.thisWeekTableIndex,
        lastWeek: this.lastWeekTableIndex,
        longAgo: this.longAgoTableIndex
      }
    },
    isNoData () {
      return !(this.tableIndexGroup.today.length > 0 || this.tableIndexGroup.thisWeek.length > 0 || this.tableIndexGroup.lastWeek.length > 0 || this.tableIndexGroup.longAgo.length > 0)
    },
    totalTableIndexColumnSize () {
      return this.currentShowTableIndex && this.currentShowTableIndex.col_order && this.currentShowTableIndex.col_order.length || 0
    },
    showTableIndexDetail () {
      if (!this.currentShowTableIndex || !this.currentShowTableIndex.col_order) {
        return []
      }
      let tableIndexList = this.currentShowTableIndex.col_order.slice(this.currentCount * this.currentPage, this.currentCount * (this.currentPage + 1))
      let renderData = tableIndexList.map((item, i) => {
        let newitem = {
          id: this.currentCount * this.currentPage + i + 1,
          column: item,
          sort: this.currentShowTableIndex.sort_by_columns.indexOf(item) + 1 || '',
          shared: this.currentShowTableIndex.shard_by_columns.includes(item)
        }
        return newitem
      })
      return renderData
    }
  },
  methods: {
    ...mapActions('TableIndexEditModal', {
      showTableIndexEditModal: 'CALL_MODAL'
    }),
    ...mapActions({
      'loadAllTableIndex': 'GET_TABLE_INDEX',
      'deleteTableIndex': 'DELETE_TABLE_INDEX',
      'buildIndex': 'BUILD_INDEX'
    })
  },
  components: {
    TableIndexEdit
  },
  locales
})
export default class TableIndex extends Vue {
  pageRefTags = pageRefTags
  sortbyColumns = [{table: 'default', column: 'kylin'}]
  tableIndexDesc = [{table: 'default', column: 'kylin'}]
  sortByOptitions = [{table: 'default', column: 'kylin'}]
  convertedRawTable = [{table: 'default', column: 'kylin'}]
  rawTableIndexOptions = []
  shardbyColumn = ''
  totalRawTable = 10
  currentPage = 0
  currentCount = +localStorage.getItem(this.pageRefTags.tableIndexDetailPager) || pageCount
  searchLoading = false
  transToGmtTime = transToGmtTime
  tableIndexFilter = this.layoutId || ''
  currentShowTableIndex = null
  tableIndexBaseList = []
  buildIndexLoading = false
  mounted () {
    this.getAllTableIndex()
  }
  handleBuildIndexTip (data) {
    let tipMsg = ''
    if (data.type === BuildIndexStatus.NORM_BUILD) {
      tipMsg = this.$t('kylinLang.model.buildIndexSuccess')
      this.$message({message: tipMsg, type: 'success'})
      return
    }
    if (data.type === BuildIndexStatus.NO_LAYOUT) {
      tipMsg = this.$t('kylinLang.model.buildIndexFail2', {indexType: this.$t('kylinLang.model.tableIndex')})
    } else if (data.type === BuildIndexStatus.NO_SEGMENT) {
      tipMsg += this.$t('kylinLang.model.buildIndexFail1', {modelName: this.modelInstance.name})
    }
    this.$confirm(tipMsg, this.$t('kylinLang.common.notice'), {showCancelButton: false, type: 'warning', dangerouslyUseHTMLString: true})
  }
  buildTableIndex () {
    this.buildIndexLoading = true
    this.buildIndex({
      project: this.currentSelectedProject,
      model_id: this.modelDesc.uuid
    }).then((res) => {
      this.buildIndexLoading = false
      handleSuccess(res, (data) => {
        this.handleBuildIndexTip(data)
      })
    }, (res) => {
      this.buildIndexLoading = false
      handleError(res)
    })
  }
  showTableIndexDetal (item) {
    this.currentShowTableIndex = item
  }
  currentChange (size, count) {
    this.currentPage = size
    this.currentCount = count
  }
  delTableIndex (id, name) {
    // 删除警告
    kylinConfirm(this.$t('delTableIndexTip', {tableIndexName: name}), {type: 'warning'}, this.$t('delTableIndexTitle')).then(() => {
      this.deleteTableIndex({
        project: this.currentSelectedProject,
        model: this.modelDesc.uuid,
        tableIndexId: id
      }).then((res) => {
        handleSuccess(res, (data) => {
          if (this.currentShowTableIndex && this.currentShowTableIndex.id === id) {
            this.currentShowTableIndex = null
          }
          // 成功提示
          this.getAllTableIndex()
        })
      }, (res) => {
        handleError(res)
      })
    })
  }
  getAllTableIndex () {
    this.loadAllTableIndex({
      project: this.currentSelectedProject,
      model: this.modelDesc.uuid
    }).then((res) => {
      handleSuccess(res, (data) => {
        this.tableIndexBaseList.splice(0, this.tableIndexBaseList.length - 1)
        this.tableIndexBaseList = data.value
        setTimeout(() => {
          this.currentShowTableIndex = data.value[data.value.length - 1]
        }, 0)
      })
    }, (res) => {
      handleError(res)
    })
  }
  changeTableIndexCard (item) {
    this.currentShowTableIndex = item
  }
  editTableIndex (isNew) {
    let defaultName = isNew ? 'TableIndex_' + (this.tableIndexBaseList.length + 1) : ''
    this.showTableIndexEditModal({
      modelInstance: this.modelInstance,
      tableIndexDesc: isNew ? {name: defaultName} : this.currentShowTableIndex
    }).then((res) => {
      if (res.isSubmit) {
        this.getAllTableIndex()
      }
    })
  }
}
</script>
<style lang="less">
@import '../../../../assets/styles/variables.less';
.full-cell {
  .tableIndex-box {
    height:calc(~"100vh");
  }
}
.tableIndex-box {
  div {
    box-sizing: border-box;
  }
  .box-card {
    .el-card__header {
      padding: 0 15px;
      height: 34px;
      line-height:34px;
    }
  }
  .icon-tableindex-type {
    width:24px;
    height:28px;
    position:absolute;
    left:10px;
    top:5px;
    z-index: 1;
  }
  .card-box {
    width:328px;
    &.table-index-active {
      .card-content {
        &.is-manual {
          border: solid 1px @normal-color-1!important;
        }
        border: solid 1px @warning-color-1!important;
      }
    }
  }
  float:left;
  width:100%;
  padding-bottom:20px;
  .el-step__head.is-finish .el-step__icon.is-text {
    background:@fff;
    color:@base-color;
    border-width:1px;
  }
  .el-step__description {
    padding-right:20px!important;
    padding-left:20px!important;
  }
  @right-width:489px;
  position: relative;
  .empty-data {
    left: calc(~"50% - @{right-width}/2");
  }
  .left-part {
    min-height:200px;
    float:left;
    width:calc(~"100% - @{right-width}");
    position: relative;
    // padding-right:489px;
    // width:100%
   }
  .right-part {
    width:@right-width;
    float: right;
    bottom:0;
    right:0;
    top:0;
    .el-card {
      .table-index-type {
        cursor:default;
      }
      height:100%;
      .el-card__body {
        padding:20px;
      }
    }
  }
  .el-carousel__item{
    h3 {
      color: #475669;
      font-size: 14px;
      opacity: 0.75;
      line-height: 200px;
      margin: 0;
    }
  }
  .slider-content-below {
    height:40px;
    line-height:40px;
    color:@text-normal-color;
    .tableindex-user {
      float:left;
      margin-left:20px;
      font-size:12px;
    }
    .tableindex-count {
      float:right;
      margin-right:20px;
      span{
        font-weight:@font-medium;
      }
    }
  }
  // .empty-table-index {
  //   .slider-content-below {
  //     .tableindex-user {
  //       // color:@text-secondary-color;
  //     }
  //     .tableindex-count {
  //       // color:@text-secondary-color;
  //     }
  //   }
  // }
  .slider-content-above{
    .broken-icon {
      color:@error-color-1;
      border: 1px solid @error-color-1;
      border-radius: 2px;
      width:56px;
      height:24px;
      line-height:24px;
      text-align:center;
      background:@error-color-3;
    }
    .empty-icon {
      color:@text-normal-color;
      border: 1px solid @text-secondary-color;
      border-radius: 2px;
      padding:0 10px;
      height:24px;
      line-height:24px;
      text-align:center;
      background:@grey-4;
      // color:@text-disabled-color;
      font-size:12px;
    }
    .status-list {
      position:absolute;
      right:11px;
      top:12px;
      height:34px;
      padding-top:10px;
      cursor: default;
    }
    height:132px;
    padding:20px;
    .del-icon {
      color:@base-color;
      display: none;
    }
    &:hover {
      .del-icon {
        display: inline-block;
      }
    }
    .sub-info {
      font-size:12px;
      margin-top:5px;
      .sub-title {
        span{
          font-weight:@font-medium;
        }
      }
      .actions {
        position: absolute;
        right: 11px;
        bottom: 48px;
      }
    }
    .main-title {
      font-size: 16px;
      font-weight:@font-medium;
      i {
        color:@warning-color-1;
      }
    }
  }
  .el-carousel__item{
    .card-content {
      width:320px;
      position:relative;
      background:@fff;
      border-radius:2px;
      margin-top:10px;
      border:solid 1px @text-placeholder-color;
      box-shadow: 0 0 4px 0 @text-placeholder-color;
      overflow: hidden;
      &:hover {
        box-shadow: 0 0 8px 0 @text-placeholder-color;
      }
    }
  }
}
</style>
