<template>
  <div class="ksd-slider">
	<el-checkbox v-model="openCollectRange" v-if="!hideCheckbox" @change="changeCollectRange">
    <span v-if="label">{{label}}</span>
    <slot name="checkLabel" v-if="!label"></slot>
  </el-checkbox>
  <slot name="tipLabel"></slot>
  <div class="ksd-mt-10"><slot name="sliderLabel"></slot></div>
    <el-slider class="ksd-ml-6" :min="minConfig" :show-stops="showStop" :step="stepConfig" @change="changeBarVal" v-model="staticsRange" :max="maxConfig" :format-tooltip="formatTooltip" :show-tooltip="false"></el-slider>
    <span v-show="showRange">{{staticsRange}}%</span>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
@Component({
  props: ['label', 'step', 'showStops', 'max', 'min', 'show', 'hideCheckbox', 'range', 'showRange'],
  methods: {
    formatTooltip (value) {
      return value + '%'
    },
    changeBarVal (val) {
      this.$emit('changeBar', val)
      if (val > 0) {
        this.openCollectRange = true
        this.staticsRange = val
      }
    },
    reset () {
      this.openCollectRange = false
      this.staticsRange = this.range || 0
      this.$emit('changeBar', this.range || 0)
    },
    changeCollectRange () {
      if (this.openCollectRange) {
        this.staticsRange = 100
      } else {
        this.staticsRange = 0
      }
      this.$emit('changeBar', this.staticsRange)
    }
  },
  watch: {
    'show' () {
      this.reset()
    }
  }
})
export default class Pager extends Vue {
  data () {
    return {
      stepConfig: this.step || 20,
      openCollectRange: false,
      maxConfig: this.max || 100,
      showStop: this.showStops || true,
      minConfig: this.min || 0,
      staticsRange: this.range,
      currentPage: this.curPage || 1
    }
  }
}
</script>
<style lang="less">
</style>
