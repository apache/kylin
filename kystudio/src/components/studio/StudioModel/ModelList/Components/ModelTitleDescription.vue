<template>
    <div class="model-alias-label" v-if="modelData">
      <div class="alias">
        <el-popover
          ref="statusPopover"
          popper-class="status-tooltip"
          placement="top-start"
          trigger="hover"
          :disabled="!modelData.status"
        >
          <span v-html="$t('modelStatus_c')" />
          <span>{{modelData.status}}</span>
          <div v-if="modelData.status === 'WARNING' && modelData.empty_indexes_count">{{$t('emptyIndexTips')}}</div>
          <div v-if="modelData.status === 'WARNING' && (modelData.segment_holes && modelData.segment_holes.length && modelData.model_type === 'BATCH') || (modelData.batch_segment_holes && modelData.batch_segment_holes.length && modelData.model_type === 'HYBRID')">
            <span>{{modelData.model_type === 'HYBRID' ? $t('modelSegmentHoleTips1') : $t('modelSegmentHoleTips')}}</span><span
              v-if="!['modelEdit'].includes(source)"
              style="color:#0988DE;cursor: pointer;"
              @click="autoFix(modelData.alias, modelData.model_type === 'HYBRID' ? modelData.batch_id : modelData.uuid, modelData.model_type === 'HYBRID' ? modelData.batch_segment_holes : modelData.segment_holes)">{{$t('seeDetail')}}</span>
          </div>
          <div v-if="modelData.status === 'WARNING' && (modelData.segment_holes && modelData.segment_holes.length && modelData.model_type !== 'BATCH')">
            <span>{{$t('modelSegmentHoleTips2')}}</span>
          </div>
          <div v-if="modelData.status === 'WARNING' && modelData.inconsistent_segment_count">
            <span>{{$t('modelMetadataChangedTips')}}</span><span
              v-if="!['modelEdit'].includes(source)"
              style="color:#0988DE;cursor: pointer;"
              @click="openComplementSegment(modelData, true)">{{$t('seeDetail')}}</span>
          </div>
          <div v-if="modelData.status === 'OFFLINE' && modelData.forbidden_online">
            <span>{{$t('SCD2ModalOfflineTip')}}</span>
          </div>
          <div v-if="modelData.status === 'OFFLINE' && !modelData.has_segments">
            <span>{{$t('noSegmentOnlineTip')}}</span>
          </div>
          <div v-if="modelData.status === 'OFFLINE' && !$store.state.project.multi_partition_enabled && modelData.multi_partition_desc">
            <span>{{$t('multilParTip')}}</span>
          </div>
        </el-popover>
        <el-popover
          ref="titlePopover"
          placement="top-start"
          width="250"
          trigger="hover"
          popper-class="title-popover-layout"
        >
          <div class="title-popover">
            <p class="title ksd-mb-20">{{modelData.alias || modelData.name}}</p>
            <div :class="['label', {'en': $lang === 'en'}]">
              <div class="group ksd-mb-8" v-if="!onlyShowModelName"><span class="title">{{$t('kylinLang.model.ownerGrid')}}</span><span class="item">{{modelData.owner}}</span></div>
              <div class="group"><span class="title">{{$t('description')}}</span><span class="item">{{modelData.description || '-'}}</span></div>
            </div>
          </div>
        </el-popover>
        <span :class="['filter-status', (modelData.status || 'OFFLINE')]" v-popover:statusPopover v-if="!onlyShowModelName"></span>
        <span class="model-alias-title" @mouseenter.prevent v-popover:titlePopover>{{modelData.alias || modelData.name}}</span>
      </div>
      <el-tooltip class="last-modified-tooltip" effect="dark" :content="`${$t('dataLoadTime')}${modelData.gmtTime}`" placement="bottom" :disabled="hideTimeTooltip" v-if="!onlyShowModelName">
        <span>{{getLastTime}}</span>
      </el-tooltip>
    </div>
  </template>
  
  <script>
  import Vue from 'vue'
  import { Component } from 'vue-property-decorator'
  import { transToGmtTime } from 'util/business'
  @Component({
    props: {
      modelData: {
        type: Object,
        default () {
          return null
        }
      },
      source: {
        type: String,
        default: ''
      },
      hideTimeTooltip: {
        type: Boolean,
        default: false
      },
      onlyShowModelName: {
        type: Boolean,
        default: false
      }
    },
    locales: {
      en: {
        modelStatus_c: 'Model Status:',
        emptyIndexTips: 'This model has unbuilt indexes. Please click the build index button to build the indexes.',
        modelSegmentHoleTips: 'This model\'s segment range has gaps in between. Empty results might be returned when querying those ranges. ',
        modelSegmentHoleTips1: 'This model\'s batch segment range has gaps in between. Empty results might be returned when querying those ranges. ',
        modelSegmentHoleTips2: 'This model\'s streaming data has gaps in between. Please contact technical support.',
        modelMetadataChangedTips: 'Data in the source table was changed. The query results might be inaccurate. ',
        seeDetail: 'View Details',
        description: 'Description',
        dataLoadTime: 'Last Updated Time: ',
        SCD2ModalOfflineTip: 'This model includes non-equal join conditions (≥, <), which are not supported at the moment. Please delete those join conditions, or turn on `Support History table` in project settings.',
        noSegmentOnlineTip: 'This model can\'t go online as it doesn\'t have segments. Models with no segment couldn\'t serve queries. Please add a segment.',
        multilParTip: 'This model used multilevel partitioning, which are not supported at the moment. Please set subpartition as \'None\' in model partition dialog, or turn on \'Multilevel Partitioning\' in project settings.',
        lastUpdate: 'Last Updated: '
      }
    }
  })
  export default class ModelTitleDescription extends Vue {
    transToGmtTime = transToGmtTime

    get getLastTime () {
      return this.source !== 'modelList' ? `${this.$t('lastUpdate')}${this.transToGmtTime(this.modelData.last_modified || Date.now())}` : this.transToGmtTime(this.modelData.last_modified)
    }
    openComplementSegment (model, status) {
      this.$emit('openSegment', model, status)
    }
    autoFix (...args) {
      this.$emit('autoFix', ...args)
    }
  }
  </script>
  
  <style lang="less">
  @import '../../../../../assets/styles/variables.less';
  .model-alias-label {
    cursor: default;
  }
  .alias {
    font-weight: bold;
    line-height: 20px;
    width: 100%;
    margin-top: 6px;
    cursor: default;
    .filter-status {
      position: relative;
      top: 6px;
      border-radius: 100%;
      width: 10px;
      height: 10px;
      display: inline-block;
      margin-right: 10px;
      cursor: default;
      &.ONLINE {
        background-color: @color-success;
      }
      &.OFFLINE {
        background-color: @ke-color-info-secondary;
      }
      &.BROKEN {
        background-color: @color-danger;
      }
      &.WARNING {
        background-color: @color-warning;
      }
    }
    .model-alias-title {
      max-width: 100%;
      display: inline-block;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      white-space: nowrap\0 !important;
    }
  }
  .status-tooltip {
    min-width: unset;
    transform: translate(-3px, 0);
    .popper__arrow {
      left: 8px !important;
    }
  }
  .title-popover-layout {
    font-size: 14px;
    word-break: break-all;
    .title {
      color: @text-title-color;
      font-weight: bold;
    }
    .label {
      display: inline-block;
      .group {
        color: @text-disabled-color;
        // text-align: right;
        margin-right: 15px;
        .title {
          color: @text-disabled-color;
          display: inline-block;
          min-width: 40px;
          word-break: break-all;
          text-align: right;
          margin-right: 10px;
        }
        .item {
          display: inline-block;
          max-width: 180px;
          word-break: break-all;
          vertical-align: top;
        }
      }
      &.en {
        .title {
          min-width: 80px;
        }
        .item {
          max-width: 140px;
        }
      }
    }
  }
  .last-modified-tooltip {
    min-width: unset;
    margin-top: 3px;
    margin-left: 15px;
    color: #8B99AE;
    font-size: 12px;
    line-height: 20px;
    display: block;
    cursor: default;
    .popper__arrow {
      left: 5px !important;
    }
  }
  </style>