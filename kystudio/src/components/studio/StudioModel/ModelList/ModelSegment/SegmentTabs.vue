<template>
<div class="segment-block">
  <div class="segment-actions clearfix">
    <el-popover
      ref="segmentGif"
      placement="left"
      width="500"
      trigger="hover">
      <div style="padding:10px">
        <div class="ksd-mb-10">{{$t('segmentSubTitle')}}</div>
        <div class="ksd-center">
          <img src="../../../../../assets/img/image-seg.png" width="400px" alt="">
        </div>
      </div>
      <!-- <i slot="reference" class="el-icon-question ksd-ml-2"></i> -->
    </el-popover>
    <div class="ksd-title-label-small ksd-mb-10">
      <div class="segment-header-title"><span class="ksd-title-page">{{$t('segmentList')}}</span><i v-popover:segmentGif class="el-icon-ksd-info ksd-ml-10"></i></div>
    </div>
  </div>
  <el-tabs class="segment_tabs" v-model="activeTab" v-if="model">
    <el-tab-pane :label="$t('batch')" name="batch">
      <BatchSegment
        :ref="'segmentComp' + model.alias"
        :isShowSegmentActions="isShowSegmentActions"
        v-if="activeTab === 'batch'"
        @purge-model="purgeModel(model)"
        @loadModels="loadModelsList"
        @refreshModel="refreshModel"
        @willAddIndex="willAddIndex"
        @auto-fix="autoFix(model.alias, model.uuid, model.batch_segment_holes)"
        :model="model" />
    </el-tab-pane>
    <el-tab-pane :label="$t('streaming')" name="streaming">
      <StreamingSegment v-if="activeTab === 'streaming'" :isShowSegmentActions="isShowSegmentActions" :model="model" @refreshModel="refreshModel" />
    </el-tab-pane>
  </el-tabs>
</div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import BatchSegment from './index.vue'
import StreamingSegment from './StreamingSegment/StreamingSegment.vue'

@Component({
  props: {
    model: {
      type: Object
    },
    isShowSegmentActions: {
      type: Boolean,
      default: true
    },
    isShowPageTitle: {
      type: Boolean,
      default: false
    }
  },
  components: {
    BatchSegment,
    StreamingSegment
  },
  locales
})
export default class SegmentTabs extends Vue {
  activeTab = 'batch'

  purgeModel (model) {
    this.$emit('purgeModel', model)
  }
  loadModelsList () {
    this.$emit('loadModels')
  }
  refreshModel () {
    this.$emit('refreshModel')
  }
  willAddIndex () {
    this.$emit('willAddIndex')
  }
  autoFix (alias, uuid, segment_holes) {
    const modelId = this.activeTab === 'batch' ? this.model.batch_id : uuid
    this.$emit('auto-fix', alias, modelId, segment_holes)
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
.segment-block {
  position: relative;
  height: 100%;
  .el-icon-question {
    position: absolute;
    color: @base-color;
    top: 8px;
    right: 35px;
    z-index: 2;
    font-size: 14px;
  }
}
.segment_tabs {
  height: 85%;
  .el-tabs__content {
    height: 97%;
    overflow: visible;
    .el-tab-pane {
      height: 100%;
    }
    .streaming-list {
      height: 100%;
      .segment-views {
        height: 92%;
        overflow: auto;
      }
      .segment-streaming-table {
        height: 85%;
        overflow: auto;
      }
    }
  }
  .model-segment {
    margin: 0;
    padding: 0;
    border: none;
    .ksd-title-label-small {
      display: none;
    }
  }
}
</style>
