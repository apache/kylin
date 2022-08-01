<template>
  <div id="newQuery">
    <div class="table-layout clearfix">
      <div class="layout-left">
        <DataSourceBar
          :project-name="currentSelectedProject"
          :is-show-load-source="false"
          :is-show-load-table="datasourceActions.includes('loadSource')"
          :is-expand-on-click-node="false"
          :is-show-drag-width-bar="true"
          :default-width="240"
          :expand-node-types="['datasource', 'database']"
          :hide-bar-title="false"
          @autoComplete="handleAutoComplete"
          @click="clickTable">
        </DataSourceBar>
      </div>
      <div class="layout-right">
        <div class="ksd_right_box">
          <div class="query_result_box ksd-border-tab">
            <div class="btn-group">
              <el-button type="primary" text size="small" :disabled="editableTabs.length<2" @click.native="closeAllTabs" style="display:inline-block">{{$t('closeAll')}}</el-button><el-button
              size="small" type="primary" text @click.native="openSaveQueryListDialog" style="display:inline-block">{{$t('savedQueries')}}({{savedSize}})</el-button>
            </div>
            <tab class="insight_tab" :isedit="true" :tabslist="editableTabs" :active="activeSubMenu" v-on:clicktab="activeTab"  v-on:removetab="delTab">
              <template slot-scope="props">
                <queryTab
                  v-on:addTab="addTab"
                  v-on:changeView="changeTab"
                  v-on:resetQuery="resetQuery"
                  v-on:refreshSaveQueryCount="loadSavedQuerySize"
                  :completeData="completeData"
                  :tipsName="tipsName"
                  :tabsItem="props.item"></queryTab>
              </template>
            </tab>
          </div>
          <el-dialog
            :title="$t('savedQueries')"
            width="960px"
            class="saved_query_dialog"
            top="5vh"
            limited-area
            ref="savedQueriesDialog"
            v-if="savedQueryListVisible"
            :close-on-press-escape="false"
            :close-on-click-modal="false"
            @close="savedQueryListVisible = false"
            :visible="true">
            <kylin-empty-data v-if="!savedSize" size="small">
            </kylin-empty-data>
            <div class="list_block" v-scroll v-else>
              <div class="saved_query_content">
                <div class="form_block" v-for="(savequery, index) in savedList" :key="savequery.name" >
                  <el-checkbox v-model="checkedQueryList" :label="index" class="query_check">
                    <el-form class="narrowForm" label-position="left" label-width="105px">
                      <el-form-item :label="$t('kylinLang.query.name')+' :'" class="ksd-mb-2 narrowFormItem" >
                        <span class="save-query_name">{{savequery.name}}</span>
                      </el-form-item>
                      <el-form-item :label="$t('kylinLang.query.desc')+' :'" class="ksd-mb-2 narrowFormItem" >
                        <span class="desc-block">{{savequery.description}}</span>
                      </el-form-item>
                      <el-form-item :label="$t('kylinLang.query.querySql')+' :'" prop="sql" class="ksd-mb-2 narrowFormItem">
                        <el-button plain size="mini" @click="toggleDetail(index)">
                          {{$t('kylinLang.common.seeDetail')}}
                          <i class="el-icon-arrow-down" v-show="!savequery.isShow"></i>
                          <i class="el-icon-arrow-up" v-show="savequery.isShow"></i>
                        </el-button>
                        <kylin-editor width="99%" height="180" lang="sql" theme="chrome" v-model="savequery.sql" dragbar="#393e53" ref="saveQueries" :readOnly="true" v-if="savequery.isShow" class="ksd-mt-6" :isAbridge="true">
                        </kylin-editor>
                      </el-form-item>
                      <div class="btn-group">
                        <el-button size="small" type="info" class="remove_query_btn" text @click="removeQuery(savequery)">{{$t('kylinLang.common.delete')}}</el-button>
                      </div>
                    </el-form>
                  </el-checkbox>
                </div>
                <el-button plain size="small" class="reload-more-btn" @click="pageCurrentChange" v-if="savedList.length < savedSize">{{$t('more')}}</el-button>
              </div>
            </div>
            <span slot="footer" class="dialog-footer">
              <el-button @click="cancelResubmit" size="medium">{{$t('kylinLang.common.cancel')}}</el-button><el-button
               @click="resubmit" type="primary" size="medium" :disabled="!checkedQueryList.length">{{$t('runQuery')}}</el-button>
            </span>
          </el-dialog>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import tab from '../common/tab'
import queryTab from './query_tab'
import DataSourceBar from '../common/DataSourceBar'
import { kylinConfirm, hasRole, handleSuccess, handleError } from '../../util/business'
import { mapActions, mapMutations, mapGetters } from 'vuex'
import { insightKeyword } from '../../config'
@Component({
  beforeRouteLeave (to, from, next) {
    let isQueryLoading = false
    for (let i = this.editableTabs.length - 1; i > 0; i--) {
      if (this.editableTabs[i].icon === 'el-icon-loading') {
        isQueryLoading = true
        break
      }
    }
    if (isQueryLoading) {
      next(false)
      setTimeout(() => {
        kylinConfirm(this.$t('willStopQuery'), {
          confirmButtonText: this.$t('kylinLang.common.ok'),
          cancelButtonText: this.$t('kylinLang.common.cancel'),
          type: 'warning'
        }, this.$t('kylinLang.common.notice')).then(() => {
          this.editableTabs.forEach(item => {
            if (item.icon === 'el-icon-loading') {
              item.queryObj.stopId && this.stop({id: item.queryObj.stopId})
              const extraoption = {
                queryId: null,
                isException: true,
                is_stop_by_user: true,
                exceptionMessage: 'Stopped by user.',
                results: null,
                traces: []
              }
              const errorInfo = 'Stopped by user.'
              item.icon = 'el-icon-ksd-error_01'
              item.spin = false
              item.extraoption = extraoption
              item.queryErrorInfo = errorInfo
              item.isStop = true
              this.editableTabs[0].extraoption = extraoption
              this.editableTabs[0].queryErrorInfo = errorInfo
              this.editableTabs[0].cancelQuery = true
              this.editableTabs[0].isStop = true
            }
          })
          this.saveTabs({tabs: {[this.currentSelectedProject]: this.editableTabs}})
          next()
        }).catch(() => {
          next(false)
        })
      })
    } else {
      next()
    }
  },
  methods: {
    ...mapActions({
      getSavedQueries: 'GET_SAVE_QUERIES',
      delQuery: 'DELETE_QUERY',
      loadDataSourceByProject: 'LOAD_DATASOURCE',
      query: 'QUERY_BUILD_TABLES',
      stop: 'STOP_QUERY_BUILD'
    }),
    ...mapMutations({
      saveTabs: 'SET_QUERY_TABS'
    })
  },
  components: {
    tab,
    queryTab,
    DataSourceBar
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'getQueryTabs',
      'datasourceActions'
    ])
  },
  locales: {
    'en': {
      dialogHiveTreeNoData: 'Please click data source to load source tables',
      trace: 'Trace',
      savedQueries: 'Saved Queries',
      queryBox: 'Query Box',
      more: 'More',
      closeAll: 'Close All',
      delSqlTitle: 'Delete SQL',
      confirmDel: 'Are you sure you want to delete {queryName}?',
      runQuery: 'Run Query',
      willStopQuery: 'The currently running query would be stopped when leaving the page. Are you sure you want to leave?'
    }
  }
})
export default class NewQuery extends Vue {
  savedQueryListVisible = false
  editableTabs = []
  activeSubMenu = 'WorkSpace'
  savedQuriesSize = 0
  queryCurrentPage = 1
  datasource = []
  savedList = []
  savedSize = 0
  checkedQueryList = []
  completeData = []
  tipsName = ''
  handleAutoComplete (data) {
    this.completeData = [...data, ...insightKeyword]
  }
  toggleDetail (index) {
    this.savedList[index].isShow = !this.savedList[index].isShow
  }
  loadSavedQuerySize () {
    if (!this.currentSelectedProject) {
      return
    }
    this.getSavedQueries({
      project: this.currentSelectedProject,
      limit: 10,
      offset: 0
    }).then((res) => {
      handleSuccess(res, (data) => {
        if (data) {
          this.savedSize = data.total_size
        } else {
          this.savedSize = 0
        }
      })
    }, (res) => {
      this.savedSize = 0
    })
  }
  loadSavedQuery (pageIndex) {
    if (!this.currentSelectedProject) {
      return
    }
    this.getSavedQueries({
      project: this.currentSelectedProject || null,
      limit: 10,
      offset: pageIndex
    }).then((res) => {
      handleSuccess(res, (data) => {
        data.value.forEach((item) => {
          item['isShow'] = false
        })
        this.savedList = this.savedList.concat(data.value)
        this.savedSize = data.total_size
      })
    }, (res) => {
      handleError(res)
    })
  }
  resetQuery () {
    this.editableTabs[0].queryObj = null
    this.editableTabs[0].queryErrorInfo = ''
    this.editableTabs[0].extraoption = null
    this.cacheTabs()
  }
  addTab (targetName, queryObj) {
    this.editableTabs[0].queryObj = queryObj
    this.editableTabs[0].cancelQuery = false
    var tabIndex = this.editableTabs.length > 1 ? this.editableTabs[1].index + 1 : this.editableTabs[0].index + 1
    var tabName = targetName + tabIndex
    this.editableTabs.splice(1, 0, {
      title: tabName,
      name: tabName,
      icon: 'el-icon-loading',
      spin: true,
      extraoption: null,
      queryErrorInfo: '',
      queryObj: queryObj,
      index: tabIndex
    })
    // this.activeSubMenu = tabName
    this.cacheTabs()
  }
  activeTab (tabName) {
    this.activeSubMenu = tabName
  }
  delTab (targetName) {
    if (targetName === 'WorkSpace') {
      return
    }
    let tabs = this.editableTabs
    let activeName = this.activeSubMenu
    if (activeName === targetName) {
      tabs.forEach((tab, index) => {
        if (tab.name === targetName) {
          let nextTab = tabs[index - 1] || tabs[index + 1]
          if (nextTab) {
            activeName = nextTab.name
          }
        }
      })
    }
    const targetQuery = tabs.filter(tab => tab.name === targetName)[0]
    targetQuery.icon === 'el-icon-loading' && targetQuery.queryObj.stopId && this.stop({id: targetQuery.queryObj.stopId})
    this.editableTabs = tabs.filter(tab => tab.name !== targetName)
    this.activeSubMenu = activeName
    this.editableTabs[0].cancelQuery = true
    this.cacheTabs()
  }
  clickTable (leaf) {
    this.tipsName = ''
    this.$nextTick(() => {
      if (leaf) {
        this.tipsName = leaf.label
      }
    })
  }
  closeAllTabs () {
    this.editableTabs.forEach(item => {
      if (item.icon === 'el-icon-loading') {
        item.queryObj.stopId && this.stop({id: item.queryObj.stopId})
      }
    })
    this.editableTabs.splice(1, this.editableTabs.length - 1)
    this.editableTabs[0].cancelQuery = true
    this.activeSubMenu = 'WorkSpace'
    this.cacheTabs()
  }
  pageCurrentChange () {
    this.queryCurrentPage++
    this.loadSavedQuery(this.queryCurrentPage - 1)
  }

  removeQuery (query) {
    kylinConfirm(this.$t('confirmDel', {queryName: query.name}), null, this.$t('delSqlTitle')).then(() => {
      this.delQuery({project: this.currentSelectedProject, id: query.id}).then((response) => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.delSuccess')
        })
        this.savedList = []
        this.loadSavedQuery(this.queryCurrentPage - 1)
      })
    })
  }
  cancelResubmit () {
    this.savedQueryListVisible = false
  }
  resubmit () {
    if (this.checkedQueryList.length > 0) {
      this.checkedQueryList.forEach((index) => {
        var queryObj = {
          accept_partial: true,
          limit: this.listRows,
          offset: 0,
          project: this.currentSelectedProject,
          sql: this.savedList[index].sql
        }
        this.addTab('query', queryObj)
      })
    }
    this.savedQueryListVisible = false
  }
  changeTab (index, data, errorInfo, isSystemBlock) {
    if (index === 0 || index === this.editableTabs[1].index) { // 编辑器结果中断查询时置空或者显示最新一条查询的结果
      this.editableTabs[0].extraoption = data
      this.editableTabs[0].queryErrorInfo = errorInfo
      this.editableTabs[0].cancelQuery = true
      this.editableTabs[0].isStop = data && data.is_stop_by_user || isSystemBlock || false
    }
    if (index) {
      let tabs = this.editableTabs
      for (var k = 1; k < tabs.length; k++) {
        if (tabs[k].index === index) {
          tabs[k].icon = errorInfo || isSystemBlock ? 'el-icon-ksd-error_01' : 'el-icon-ksd-good_health'
          tabs[k].spin = errorInfo === 'circle-o-notch'
          tabs[k].extraoption = data
          tabs[k].queryErrorInfo = errorInfo
          tabs[k].isStop = data && data.is_stop_by_user || isSystemBlock || false
          break
        }
      }
    }
    this.cacheTabs(isSystemBlock)
  }
  cacheTabs (isSystemBlock) { // 系统报错情况不缓存SQL语句，不然每次切换insight都会弹框报错block用户操作
    const project = this.currentSelectedProject
    const obj = {[project]: isSystemBlock ? [{
      title: 'sqlEditor',
      i18n: 'sqlEditor',
      name: 'WorkSpace',
      icon: '',
      spin: true,
      extraoption: null,
      queryErrorInfo: '',
      queryObj: null,
      index: 0,
      cancelQuery: false
    }] : this.editableTabs}
    this.saveTabs({tabs: obj})
  }
  openSaveQueryDialog () {
    this.$nextTick(() => {
      this.saveQueryFormVisible = true
    })
  }
  openSaveQueryListDialog () {
    this.savedQueryListVisible = true
    this.queryCurrentPage = 1
    this.checkedQueryList = []
    this.savedList = []
    this.loadSavedQuery()
  }
  get isAdmin () {
    return hasRole(this, 'ROLE_ADMIN')
  }
  created () {
    this.editableTabs = this.getQueryTabs && this.getQueryTabs[this.currentSelectedProject]
      ? this.getQueryTabs[this.currentSelectedProject]
      : [{
        title: 'sqlEditor',
        i18n: 'sqlEditor',
        name: 'WorkSpace',
        icon: '',
        spin: true,
        extraoption: null,
        queryErrorInfo: '',
        queryObj: null,
        index: 0,
        cancelQuery: false
      }]
    this.loadSavedQuerySize()
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  #newQuery {
    position: relative;
    height: 100%;
    .layout-left {
      min-width: 240px;
      .data-source-bar .el-tree {
        border: none;
      }
    }
    .data-source-bar .el-tree__empty-block {
      display: none;
    }
    #tab-WorkSpace .el-icon-close {
      visibility: hidden;
      display: none;
    }
    .el-icon-ksd-keyboard {
      cursor: inherit;
    }
    .saved_query_dialog {
      .el-dialog__body {
        padding: 0;
        height: 503px;
        position: relative;
        .list_block {
          width: 100%;
          height: 503px;
          .saved_query_content {
            padding: 20px;
            .save-query_name {
              word-break: break-all;
              display: inline-block;
              white-space: pre-wrap;
              width: calc(~'100% - 50px');
            }
          }
        }
      }
      .desc-block {
        display: inline-block;
        width: 535px;
        word-break: break-word;
        white-space: pre-wrap;
        overflow: hidden;
        line-height: 21px;
      }
    }
    // .nodata {
    //   text-align: center;
    //   color: @text-disabled-color;
    //   position: absolute;
    //   top: 50%;
    //   left: 50%;
    //   transform: translate(-50%, -50%);
    // }
    .form_block {
      .query_check {
        display: flex;
        .el-checkbox__input {
          flex-grow: 0;
        }
        .el-checkbox__label {
          flex-grow: 1;
        }
        .narrowForm {
          border-radius: 4px;
          padding: 10px;
          margin-bottom: 10px;
          position: relative;
          box-shadow: 0 0 2px 0 @ke-border-secondary-hover, 0 0px 2px 0 @ke-border-secondary-hover;
          /* background-color: @aceditor-bg-color; */
          &:hover {
            box-shadow: 0 0 2px 0 @ke-border-secondary-active, 0 0px 2px 0 @ke-border-secondary-active;
          }
          .narrowFormItem {
            .el-form-item__content, .el-form-item__label {
              line-height: 22px;
            }
            .el-button--mini {
              padding: 5px 8px;
            }
          }
          .btn-group {
            position: absolute;
            top: 5px;
            right: 10px;
            .remove_query_btn {
              color: @text-normal-color;
              &:hover {
                color: @base-color;
              }
            }
          }
        }
        &.is-checked {
          .narrowForm {
            border: 1px solid @base-color;
          }
        }
      }
    }
    .smyles_editor_wrap {
      border: none;
      border-bottom-left-radius: 0px;
      border-bottom-right-radius: 0px;
      .smyles_editor {
        border-bottom-left-radius: 0px;
        border-bottom-right-radius: 0px;
      }
    }
    .operatorBox{
      .tips_box{
        color: @text-normal-color;
        flex:1;
        display: flex;
        align-items: center;
      }
      .operator{
        height: 24px;
        line-height: 24px;
        .el-form-item__label{
          padding:0 12px 0 0;
        }
        .el-form-item{
          margin-bottom:0;
          margin-right: 8px;
          &:last-child {
            margin-right: 0;
            .el-form-item__content {
              position: relative;
              top: -1px;
            }
          }
        }
        .el-form-item__content{
          line-height: 1;
        }
      }
    }
    .submit-tips {
      float: right;
      font-size: 12px;
      line-height: 18px;
      vertical-align: middle;
      margin-top: 4px;
      color: @text-disabled-color;
      i {
        cursor: default;
      }
    }
    .insight_tab{
      .el-tabs__new-tab{
        display: none;
      }
      > .el-tabs > .el-tabs__header {
        margin-bottom: 0;
        width: calc(~'100% - 180px');;
        &::after {
          content: '';
          width: calc(~'100% + 180px');
          border-top: 1px solid #ECF0F8;
          position: absolute;
          left: 0;
          bottom: 0;
        }
      }
    }
    .query_result_box{
      position: relative;
      > .btn-group {
        position: absolute;
        right: 0px;
        top: 4px;
        z-index: 99;
      }
      h3{
        margin: 20px;
      }
      .el-tabs{
        // margin-top: 3px;
        // .el-tabs__nav-wrap {
        //   width: calc(~'100% - 250px');
        //   &::after {
        //     background-color: transparent;
        //   }
        // }
        .el-tabs__content{
          padding: 0px;
          .el-tab-pane{
            padding-top: 16px;
          }
        }
        &.el-tabs--button {
          .el-tab-pane{
            padding-top: 8px;
          }
          .el-tabs__header {
            width: 136px;
          }
        }
        &.en-model {
          .el-tabs__header {
            width: 174px !important;
          }
        }
      }
    }
    .el-icon-ksd-good_health {
      color: @color-success;
      font-size: 12px;
    }
    .el-icon-error {
      color: @color-danger;
      font-size: 12px;
    }
    .el-icon-ksd-error_01 {
      color: red;
      font-size: 12px;
    }
    .reload-more-btn {
      width: 100px;
      margin: 0 auto;
      display: block;
      margin-top: 20px;
    }
  }
</style>
