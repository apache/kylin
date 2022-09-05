<template>
  <div class="capacity-top-bar">
    <el-popover ref="activeNodes" width="290" popper-class="nodes-popover" v-model="showNodes">
      <div class="contain" @mouseover="showNodeDetails = false">
        <div class="lastest-update-time">
          <el-tooltip :content="$t('lastUpdateTime')" effect="dark" placement="top"><i class="icon el-icon-ksd-type_time"></i></el-tooltip>{{latestUpdateTime | timeFormatHasTimeZone}}</div>
        <div class="data-valumns">
          <p :class="['label', 'node-item']" @mouseover.stop @mouseenter.stop="showNodeDetails = true">
            <span>{{$t('usedNodes')}}：<span :class="['font-medium']">{{nodeList.length}}</span></span>
            <template>
              <!-- <span class="font-disabled" v-if="systemNodeInfo.fail">{{$t('failApi')}}</span> -->
              <!-- <el-tooltip :content="$t('failedTagTip')" effect="dark" placement="top">
                <el-tag size="mini" type="danger">{{$t('failApi')}}</el-tag>
              </el-tooltip> -->
              <el-tag size="mini" type="danger" v-if="isOnlyQueryNode">{{$t('noActiveAllNode')}}</el-tag>
              <!-- <el-tag size="mini" type="danger" v-if="systemNodeInfo.node_status === 'OVERCAPACITY'">{{$t('excess')}}</el-tag> -->
            </template>
            <span class="icon el-icon-ksd-more_02 node-list-icon"></span></p>
        </div>
      </div>
      <div class="nodes" v-if="showNodeDetails && isNodeLoadingSuccess && !isNodeLoading" @mouseenter="showNodeDetails = true" @mouseleave="showNodeDetails = false">
        <!-- <p class="error-text" v-if="!nodeList.filter(it => it.mode === 'all').length">{{$t('noNodesTip1')}}</p> -->
        <div class="node-details" v-if="nodeList.length > 0">
          <div class="node-list" v-for="(node, index) in nodeList" :key="index">
            <span v-custom-tooltip="{text: `${node.host}(${node.mode === 'All' ? 'All' : $t(`kylinLang.common.${node.mode.toLocaleLowerCase()}Node`)})`, w: 20}">{{`${node.host}(${node.mode === 'All' ? 'All' : $t(`kylinLang.common.${node.mode.toLocaleLowerCase()}Node`)})`}}</span>
          </div>
        </div>
        <div class="node-details nodata" v-else>{{$t('kylinLang.common.noData')}}</div>
      </div>
    </el-popover>
    <p class="active-nodes" v-popover:activeNodes @click="showNodes = !showNodes">
      <span :class="['flag', getNodesNumColor]"></span>
      <span class="server-status">{{$t('serverStatus')}}</span>
    </p>
  </div>
</template>
<script>
  import Vue from 'vue'
  import { Component } from 'vue-property-decorator'
  import { mapActions, mapState, mapGetters } from 'vuex'
  import locales from './locales'
  import filterElements from '../../../filter/index'
  import { handleError } from '../../../util/business'

  @Component({
    methods: {
      ...mapActions({
        getNodeList: 'GET_NODES_LIST'
      })
    },
    computed: {
      ...mapState({
        latestUpdateTime: state => state.capacity.latestUpdateTime
      }),
      ...mapGetters([
        'isOnlyQueryNode',
        'isOnlyJobNode',
        'isAdminRole'
      ])
    },
    locales
  })
  export default class CapacityTopBar extends Vue {
    showNodes = false
    isNodeLoadingSuccess = false
    nodeList = []
    isNodeLoading = true
    showNodeDetails = false
    filterElements = filterElements
    nodesTimer = null
    modelObj = {
      all: 'All',
      job: 'Job',
      query: 'Query'
    }

    get getNodesNumColor () {
      return 'is-success'
    }

    created () {
      this.getHANodes()
    }

    getHANodes () {
      if (this._isDestroyed) {
        return
      }
      this.isNodeLoading = true
      const data = {ext: true}
      if (this.nodesTimer) {
        data.isAuto = true
      }
      this.getNodeList(data).then((res) => {
        if (this._isDestroyed) {
          return
        }
        this.isNodeLoadingSuccess = true
        res.servers.length && (this.nodeList = res.servers.map(it => ({...it, mode: this.modelObj[it.mode]})))
        this.isNodeLoading = false
        clearTimeout(this.nodesTimer)
        this.nodesTimer = setTimeout(() => {
          this.getHANodes()
        }, 1000 * 60)
      }).catch((e) => {
        if (e.status === 401) {
          handleError(e)
        } else {
          clearTimeout(this.nodesTimer)
          this.timer = setTimeout(() => {
            this.getHANodes()
          }, 1000 * 60)
        }
      })
    }
  }
</script>
<style lang="less" scoped>
  @import '../../../assets/styles/variables.less';
  
  .capacity-top-bar {
    position: relative;
    min-width: 65px;
    // padding-right: 20px;
    .active-nodes {
      position: relative;
      &:hover {
        color: @text-normal-color !important;
      }
      .server-status {
        font-weight: @font-regular;
        &:hover {
          color: @base-color;
        }
      }
      .flag {
        width: 10px;
        height: 10px;
        // position: absolute;
        // left: -8px;
        display: inline-block;
        border-radius: 100%;
        &.is-danger {
          background-color: @error-color-1;
        }
        &.is-warning {
          background-color: @warning-color-1;
        }
        &.is-success {
          background-color: @normal-color-1;
        }
      }
      .el-icon-ksd-restart {
        color: @base-color;
      }
    }
    .font-disabled {
      color: @text-disabled-color;
    }
  }
  .is-danger {
    color: @error-color-1;
  }
  .is-warning {
    color: @warning-color-1;
  }
  .is-success {
    color: @normal-color-1;
  }
  .error-text {
    color: @error-color-1;
    font-size: 12px;
    margin-bottom: 13px;
  }
</style>

<style lang="less">
  @import '../../../assets/styles/variables.less';
  .nodes-popover {
    padding: 0 !important;
    margin-left: -200px;
    position: relative;
    .popper__arrow {
      // margin-left: 50px;
      left: initial !important;
      right: 38px;
    }
    .font-disabled {
      color: @text-disabled-color;
    }
    .contain {
      .lastest-update-time {
        height: 30px;
        padding: 5px 10px;
        line-height: 20px;
        box-sizing: border-box;
        color: @text-normal-color;
        border-bottom: 1px solid @line-border-color3;
        .icon {
          margin-right: 5px;
          color: @text-disabled-color;
        }
      }
      .data-valumns {
        padding: 10px 0;
        box-sizing: border-box;
        .label {
          line-height: 28px;
          padding: 0 10px;
          box-sizing: border-box;
          .over-thirty-days {
            cursor: pointer;
          }
        }
        .node-item {
          position: relative;
          cursor: pointer;
          .node-list-icon {
            position: absolute;
            right: 10px;
            top: 10px;
            font-size: 9px;
          }
          &:hover {
            background: @base-color-9;
          }
          &.is-disabled {
            pointer-events: none;
          }
        }
      }
    }
    .nodes {
      max-height: 170px;
      overflow: auto;
      text-align: left;
      position: absolute;
      background: #ffffff;
      transform: translate(-105%, 0);
      margin-top: -39px;
      width: 208px;
      padding: 10px;
      box-sizing: border-box;
      box-shadow: 0 0px 6px 0px #E5E5E5;
      .node-details {
        text-align: left;
        width: 100%;
        &.nodata {
          color: @text-disabled-color;
          text-align: center;
        }
      }
      .node-list {
        color: @text-normal-color;
        margin-top: 8px;
        width: 100%;
        display: inline-block;
        &:first-child {
          margin-top: 0;
        }
        .custom-tooltip-layout {
          vertical-align: middle;
          line-height: 1;
          width: 100%;
        }
      }
    }
  }
</style>
