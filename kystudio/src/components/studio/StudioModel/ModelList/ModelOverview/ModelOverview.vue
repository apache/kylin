<template>
  <el-tabs class="model-overview" v-model="activeTab" v-if="model">
    <el-tab-pane :label="$t('erDiagram')" name="erDiagram">
      <ModelERDiagram ref="$er-diagram" is-show-full-screen :model="model" />
    </el-tab-pane>
    <el-tab-pane :label="$t('dimension')" name="dimension">
      <ModelDimensionList :model="model" />
    </el-tab-pane>
    <el-tab-pane :label="$t('measure')" name="measure">
      <ModelMeasureList :model="model" />
    </el-tab-pane>
  </el-tabs>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'

import locales from './locales'
import { dataGenerator } from '../../../../../util'
import ModelERDiagram from '../../../../common/ModelERDiagram/ModelERDiagram.vue'
import ModelDimensionList from '../../../../common/ModelDimensionList/ModelDimensionList.vue'
import ModelMeasureList from '../../../../common/ModelMeasureList/ModelMeasureList.vue'

@Component({
  props: ['data'],
  components: {
    ModelERDiagram,
    ModelDimensionList,
    ModelMeasureList
  },
  locales
})
export default class ModelOverview extends Vue {
  activeTab = 'erDiagram'
  modelData = null

  // 此处有待调查：为何删除模型的瞬间，会造成模型object变化？
  // 这会导致computed model被重算，重算后，前端给table加的uuid被全部刷新了。
  // 临时解决方案：放一个modelData的缓存变量，判断模型的uuid不发生变化，则不更新modelData
  @Watch('data', { immediate: true })
  onDataChange (newVal, oldVal) {
    if (!oldVal || newVal.uuid !== oldVal.uuid) {
      this.modelData = newVal
    }
  }

  get model () {
    return this.modelData ? dataGenerator.generateModel(this.modelData) : null
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';

.model-overview {
  // height: 100%;
  height: calc(~'100% - 32px');
  // margin: 15px;
  box-shadow: none;
  // border:1px solid rgba(245,245,245,1);
  > .el-tabs__header,
  > .el-tabs__header .el-tabs__item {
    border: transparent;
    color: @text-normal-color;
    // &.is-active {
    //   font-weight: normal;
    // }
  }
  > .el-tabs__content {
    padding: 0;
  }
  > .el-tabs__content > .el-tab-pane {
    // padding: 20px;
    // height: 470px;
    box-sizing: border-box;
    overflow: auto;
  }
  .el-tabs__nav-scroll {
    background-color: @ke-background-color-white;
  }
  .model-er-diagram {
    // margin: -20px;
    // height: calc(~'100% + 40px');
    // width: calc(~'100% + 40px');
    height: 100%;
    width: 100%;
  }
}
</style>
