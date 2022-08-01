<template>
<div>
  <div class="ksd-title-label-small ksd-ml-20 ksd-mt-20">{{$t('dataSourceTypeCheckTip')}}</div>
  <div class="source-new">
    <ul>
      <el-tooltip :content="$t('disabledHiveOrKafkaTips', {jdbcName: jdbcSourceName})" placement="top" :disabled="!disabledSelectDataSource([sourceTypes.HIVE])">
        <li class="datasouce ksd-center" :class="getSourceClass([sourceTypes.HIVE])" @click="!disabledSelectDataSource([sourceTypes.HIVE]) && clickHandler(sourceTypes.HIVE)">
          <div class="datasource-icon" >
            <!-- <i class="el-icon-ksd-hive"></i> -->
            <img src="../../../../assets/img/Hive_logo.png" alt="">
          </div>
          <i class="el-ksd-icon-confirm_16 ksd-fs-12 checked-icon"></i>
          <div class="datasource-name">Hive</div>
        </li>
      </el-tooltip>
      <!-- <li class="datasouce ksd-center disabled"> -->
        <!-- 暂时屏蔽该功能 -->
        <!-- <li class="datasouce ksd-center disabled" :class="getSourceClass([sourceTypes.CSV])"> -->
        <!-- <div class="datasource-icon" @click="clickHandler(sourceTypes.CSV)"> -->
        <!--
        <div class="datasource-icon">
          <i class="el-icon-ksd-csv"></i>
        </div>
        <div class="datasource-name">CSV</div>
        <div class="status">
          <span>{{$t('upcoming')}}</span>
        </div>
        -->
      <!-- </li> -->

      <el-tooltip :content="$t('disabledHiveOrKafkaTips', {jdbcName: jdbcSourceName})" placement="top" :disabled="!disabledSelectDataSource([sourceTypes.KAFKA])">
        <li class="datasouce ksd-center" v-if="isStreamingEnabled" :class="getSourceClass([sourceTypes.KAFKA])" @click="!disabledSelectDataSource([sourceTypes.KAFKA]) && clickHandler(sourceTypes.KAFKA)">
          <div class="datasource-icon">
            <!-- <i class="el-icon-ksd-kafka"></i> -->
            <img src="../../../../assets/img/Kafka_logo.png" alt="">
          </div>
          <i class="el-ksd-icon-confirm_16 ksd-fs-12 checked-icon"></i>
          <div class="datasource-name">Kafka</div>
          <!-- <div class="status">
            <span>{{$t('upcoming')}}</span>
          </div> -->
        </li>
      </el-tooltip>

      <!-- jdbc 数据源的 sourceType 都为 8 -->
      <el-tooltip :content="$t('disabledJDBCTips', {jdbcName: jdbcSourceName})" placement="top" :disabled="!disabledSelectDataSource([8])">
        <li class="datasouce ksd-center" :class="getSourceClass([8])" @click="!disabledSelectDataSource([8]) && clickHandler(8)" v-if="showJDBCEnter">
          <div class="datasource-icon">
            <!-- <i class="el-icon-ksd-mysql"></i> -->
            <img src="../../../../assets/img/datasource.png" alt="">
          </div>
          <i class="el-ksd-icon-confirm_16 ksd-fs-12 checked-icon"></i>
          <div class="datasource-name">{{jdbcSourceName}}</div>
        </li>
      </el-tooltip>
    </ul>
    <!--
    <ul>
      <li class="datasouce disabled ksd-center">
        <div class="datasource-icon">
          <i class="el-icon-ksd-greenplum"></i>
        </div>
        <div class="datasource-name">Greenplum</div>
        <div class="status">
          <span>{{$t('upcoming')}}</span>
        </div>
      </li>
      <li class="datasouce disabled ksd-center">
        <div class="datasource-icon">
          <i class="el-icon-ksd-sqlserver"></i>
        </div>
        <div class="datasource-name">SQL Server</div>
        <div class="status">
          <span>{{$t('upcoming')}}</span>
        </div>
      </li>
    </ul>
    -->
  </div>
</div>
</template>
<script>
import Vue from 'vue'
import { mapGetters, mapState, mapMutations } from 'vuex'
import { types } from '../store'
import { Component } from 'vue-property-decorator'
import locales from './locales'
import * as config from '../../../../config'

@Component({
  props: [ 'sourceType' ],
  computed: {
    ...mapGetters([
      'globalDefaultDatasource',
      'isStreamingEnabled'
    ]),
    ...mapState({
      allProject: state => state.project.allProject,
      currentProject: state => state.project.selected_project,
      dataSource: state => state.datasource.dataSource
    })
  },
  methods: {
    ...mapMutations('DataSourceModal', {
      initForm: types.INIT_FORM
    })
  },
  locales
})
export default class SourceSelect extends Vue {
  sourceTypes = config.sourceTypes

  get isOpenJDBCSource () {
    if (this.dataSource[this.currentProject] && this.dataSource[this.currentProject].length) {
      return this.dataSource[this.currentProject].filter(it => it.source_type !== 1 && it.source_type !== 9).length > 0
    } else {
      return false
    }
  }

  get haveHiveDataSource () {
    if (this.dataSource[this.currentProject] && this.dataSource[this.currentProject].length) {
      return this.dataSource[this.currentProject].filter(it => it.source_type === 1 || it.source_type === 9).length > 0
    } else {
      return false
    }
  }

  get showJDBCEnter () {
    const [{override_kylin_properties}] = this.allProject.filter(it => it.name === this.currentProject)
    return override_kylin_properties['kylin.source.jdbc.source.name'] && override_kylin_properties['kylin.source.jdbc.source.enable'] === 'true'
  }

  get jdbcSourceName () {
    const [{override_kylin_properties}] = this.allProject.filter(it => it.name === this.currentProject)
    return override_kylin_properties['kylin.source.jdbc.source.name'] ? override_kylin_properties['kylin.source.jdbc.source.name'].replace(/^\w{1}/, ($1) => $1.toLocaleUpperCase()) : ''
  }

  disabledSelectDataSource (sourceTypes = []) {
    return sourceTypes.includes(9) || sourceTypes.includes(1) ? this.isOpenJDBCSource : this.haveHiveDataSource
  }

  getSourceClass (sourceTypes = []) {
    return {
      active: sourceTypes.includes(this.sourceType) && (this.sourceType === 8 ? !this.haveHiveDataSource : !this.isOpenJDBCSource),
      'is-disabled': this.disabledSelectDataSource(sourceTypes)
    }
  }

  clickHandler (value = '') {
    this.initForm()
    this.$emit('input', value)
  }

  mounted () {
    // 设置默认数据源
    // this.clickHandler(this.globalDefaultDatasource)
    // for newten 设置CSV为默认数据源
    document.activeElement && document.activeElement.blur()
    // this.clickHandler()
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.source-new {
  padding: 20px 0;
  margin: 0 auto;
  /* width: 472px; */
  width:100%;
  text-align: center;
  ul {
    margin-bottom: 35px;
    &:last-child {
      margin-bottom: 0px;
    }
  }
  .datasouce {
    display: inline-block;
    height: 112px;
    width: 120px;
    vertical-align: top;
    color: @text-secondary-color;
    margin-right: 50px;
    border: 1px solid transparent;
    border-radius: 4px;
    background: @ke-background-color-white;
    box-shadow: 0px 1px 4px rgba(63, 89, 128, 0.16);
    box-sizing: border-box;
    cursor: pointer;
    overflow: hidden;
    * {
      vertical-align: middle;
    }
    &:last-child {
      margin-right: 0;
    }
    .checked-icon {
      display: none;
    }
    .datasource-icon {
      img {
        width: 62px;
      }
    }
    &.is-disabled {
      opacity: 0.5;
      cursor: not-allowed;
      .datasource-icon {
        img {
          cursor: not-allowed;
        }
      }
    }
  }
  // .datasource-icon:hover {
  //   border-color: @base-color;
  // }
  .datasouce.active {
    position: relative;
    color: @text-normal-color;
    border: 1px solid @ke-color-primary;
    box-shadow: 0px 2px 8px rgba(50, 73, 107, 0.24);
    .datasource-name {
      color: @ke-color-primary;
    }
    .checked-icon {
      display: block;
      position: absolute;
      bottom: 3px;
      right: 5px;
      z-index: 10;
      color: @text-darkbg-color;
      font-weight: bold;
    }
    &::after {
      content: '';
      display: block;
      border-top: 24px solid transparent;
      border-left: 24px solid @ke-color-primary;
      border-bottom: 24px solid transparent;
      border-right: 24px solid transparent;
      /* border-color: #0875DA; */
      position: absolute;
      right: -24px;
      bottom: -24px;
      transform: rotate(45deg);
    }
  }
  .datasouce.disabled {
    .datasource-icon {
      color: @text-disabled-color;
      cursor: not-allowed;
      background: @background-disabled-color;
    }
    .datasource-name {
      color: @text-disabled-color;
      font-weight: normal;
    }
    &:hover {
      .datasource-icon {
        border-color: transparent;
      }
    }
    * {
      cursor: inherit;
    }
  }
  .datasource-icon {
    font-size: 65px;
    // height: 90px;
    line-height: 1.2;
    border-radius: 6px;
    // background: @grey-4;
    // margin-bottom: 10px;
    color: @base-color;
    cursor: pointer;
    border: 1px solid transparent;
  }
  .datasource-name {
    color: @text-normal-color;
    margin-top: 5px;
    font-size: 14px;
    font-weight: @font-medium;
  }
  .status {
    background: @background-disabled-color;
    border-radius: 12px;
    overflow: hidden;
    color: @text-disabled-color;
    font-size: 12px;
    width: 90px;
    display: inline-block;
    height: 22px;
    line-height: 22px;
  }
}
</style>
