<template>
    <div class="shortcuts-group">
      <el-tooltip :content="$t('reset')" placement="top" v-if="showReset">
        <div class="resetER el-ksd-n-icon-resure-outlined" @click="resetER"></div>
      </el-tooltip>
      <el-tooltip :content="isFullScreen ? $t('cancelFullScreen') : $t('fullScreen')" placement="top" >
        <div :class="['full-screen', !isFullScreen ? 'el-ksd-n-icon-arrows-alt-outlined' : 'el-ksd-n-icon-shrink-exit-outlined']" @click="fullScreen"></div>
      </el-tooltip>
      <el-tooltip :content="$t('autoLayout')" placement="top" >
        <div class="auto-layout el-ksd-n-icon-map-outlined" @click="autoLayout"></div>
      </el-tooltip>
      <span class="divide-line">|</span>
      <el-dropdown class="action-dropdown" @command="(command) => handleActionsCommand(command)">
        <el-button icon="el-ksd-n-icon-column-3-outlined" iconr="el-ksd-n-icon-arrow-table-down-filled" nobg-text></el-button>
        <el-dropdown-menu slot="dropdown" class="model-action-tools">
          <el-dropdown-item command="expandAllTables" :class="{'is-active': activeToolItem === 'expandAllTables'}"><i class="el-ksd-n-icon-column-3-outlined ksd-mr-8"></i>{{$t('expandAllTables')}}</el-dropdown-item>
          <el-dropdown-item command="collapseAllTables" :class="{'is-active': activeToolItem === 'collapseAllTables'}"><i class="el-ksd-n-icon-workflow-outlined ksd-mr-8"></i>{{$t('collapseAllTables')}}</el-dropdown-item>
          <el-dropdown-item command="showOnlyConnectedColumn" :class="{'is-active': activeToolItem === 'showOnlyConnectedColumn'}"><i class="el-ksd-n-icon-workspace-outlined ksd-mr-8"></i>{{$t('showOnlyConnectedColumn')}}</el-dropdown-item>
        </el-dropdown-menu>
      </el-dropdown>
      <span class="divide-line">|</span>
      <div class="zoom-tools"><span class="reduce-zoom el-ksd-n-icon-minus-outlined" @click="reduceZoom"></span><span class="zoom-num">{{zoom / 10 * 100}}%</span><span class="add-zoom el-ksd-n-icon-plus-outlined" @click="addZoom"></span></div>
    </div>
  </template>
  
  <script>
  import Vue from 'vue'
  import { Component } from 'vue-property-decorator'
  import { mapGetters, mapMutations } from 'vuex'
  @Component({
    props: {
      zoom: Number,
      showReset: {
        type: Boolean,
        default: false
      }
    },
    computed: {
      ...mapGetters([
        'isFullScreen'
      ])
    },
    methods: {
      ...mapMutations({
        toggleFullScreen: 'TOGGLE_SCREEN'
      })
    },
    locales: {
      'en': {
        reset: 'Reset',
        fullScreen: 'Full Screen',
        cancelFullScreen: 'Exit Full Screen',
        autoLayout: 'Auto-organize',
        expandAllTables: 'Expand All Tables',
        collapseAllTables: 'Collapse all tables',
        showOnlyConnectedColumn: 'Show only connected columns'
      },
      'zh-cn': {
        reset: '重置',
        fullScreen: '全屏',
        cancelFullScreen: '退出全屏',
        autoLayout: '自动整理',
        expandAllTables: '展开全部表',
        collapseAllTables: '收起全部表',
        showOnlyConnectedColumn: '仅显示连接列'
      }
    }
  })
  export default class ModelNavigationTools extends Vue {
    activeToolItem = ''
    collapseAllTables = false
    showOnlyConnectedColumn = false
    resetER () {
      this.activeToolItem = ''
      this.collapseAllTables = false
      this.showOnlyConnectedColumn = false
      this.$emit('reset')
    }
    // 放大视图
    addZoom () {
      this.$emit('addZoom')
    }
    // 缩小视图
    reduceZoom () {
      this.$emit('reduceZoom')
    }
    // 全屏
    fullScreen () {
      this.toggleFullScreen(!this.isFullScreen)
    }
    // 自动布局
    autoLayout () {
      this.$emit('autoLayout')
    }
    handleActionsCommand (command) {
      this.activeToolItem = command
      switch (command) {
        case 'collapseAllTables':
          this.collapseAllTables = true
          break
        case 'expandAllTables':
          this.collapseAllTables = false
          break
        case 'showOnlyConnectedColumn':
          if (!this.showOnlyConnectedColumn) {
            this.collapseAllTables = false
            this.showOnlyConnectedColumn = true
          } else {
            this.showOnlyConnectedColumn = false
          }
      }
      if (command === 'expandAllTables' && this.showOnlyConnectedColumn) {
        this.showOnlyConnectedColumn = false
        this.$emit('command', 'resetOnlyConnectedColumn', this.showOnlyConnectedColumn)
      } else {
        this.$emit('command', command, this.showOnlyConnectedColumn)
      }
    }
  }
  </script>
  
  <style lang="less">
  @import '../../../assets/styles/variables.less';
  .shortcuts-group {
    // top: 70%;
    background-color: @fff;
    border: 1px solid @ke-border-secondary;
    box-shadow: 6px 6px 14px rgba(0, 0, 0, 0.04);
    border-radius: 6px;
    padding: 7px;
    box-sizing: border-box;
    display: flex;
    // flex-direction: column;
    align-items: center;
    color: @text-normal-color;
    > div {
      margin-right: 8px;
      &:last-child {
        margin-right: 0;
      }
    }
    .resetER {
      position: absolute;
      left: -25px;
      top: 10px\0;
      cursor: pointer;
    }
    .zoom-tools {
      display: flex;
      // flex-direction: column;
      align-items: center;
      .reduce-zoom {
        cursor: pointer;
        font-size: 16px;
      }
      .zoom-num {
        margin-left: 8px;
        margin-right: 8px;
      }
      .add-zoom {
        cursor: pointer;
        font-size: 16px;
      }
    }
    .divide-line {
      border: 0;
      // border-left: 1px solid #E6EBF4;
      // margin: 4px 0 20px 0;
      color: @ke-border-secondary;
      width: 1px;
      height: 70%;
      margin-right: 8px;
    }
    .full-screen {
      font-size: 16px;
      cursor: pointer;
    }
    .auto-layout {
      font-size: 16px;
      cursor: pointer;
    }
    .action-dropdown {
      margin-top: -3px;
      .el-ksd-n-icon-column-3-outlined {
        font-size: 16px;
        color: @text-normal-color;
      }
      .el-ksd-n-icon-arrow-table-down-filled {
        font-size: 12px;
        color: @text-normal-color;
      }
    }
  }
  </style>
  