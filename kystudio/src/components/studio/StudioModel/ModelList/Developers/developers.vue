<template>
  <div class="developer-tabs">
     <el-tabs class="el-tabs--default model-developer" v-if="currentModelRow" v-model="step">
      <el-tab-pane label="SQL" name="sql">
        <ModelSql v-if="step === 'sql'" :model="currentModelRow.uuid"/>
      </el-tab-pane>
      <el-tab-pane label="JSON" name="json">
        <ModelJson v-if="step === 'json'" :model="currentModelRow.uuid"/>
      </el-tab-pane>
     </el-tabs>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import ModelJson from '../ModelJson/modelJson.vue'
import ModelSql from '../ModelSql/ModelSql.vue'

@Component({
  props: {
    currentModelRow: {
      type: Object,
      default: null
    }
  },
  components: {
    ModelJson,
    ModelSql
  }
})
export default class Developers extends Vue {
  step = 'sql'
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
.developer-tabs {
  height: 100%;
  .model-developer {
    height: 100%;
  }
  .el-tabs__nav-scroll {
    background-color: @ke-background-color-white;
  }
}
</style>

