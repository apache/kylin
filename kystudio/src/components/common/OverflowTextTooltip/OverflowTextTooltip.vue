<template>
  <div class="overflow-text-tooltip">
    <el-tooltip
      :effect="effect"
      :content="content"
      :placement="placement"
      :disabled="!isShowTooltip">
      <div class="text-container" ref="$container">
        <span class="text-sample" ref="$sample">
          <slot></slot>
        </span>
      </div>
    </el-tooltip>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

@Component({
  props: {
    effect: {
      type: String
    },
    placement: {
      type: String,
      default: 'top'
    }
  }
})
export default class OverflowTextTooltip extends Vue {
  content = ''
  sampleWidth = 0
  containerWidth = 0

  mounted () {
    window.addEventListener('resize', this.handleResizeWindow)
    this.handleResizeWindow()
  }

  updated () {
    this.handleResizeWindow()
  }

  beforeDestroy () {
    window.removeEventListener('resize', this.handleResizeWindow)
  }

  get isShowTooltip () {
    const { sampleWidth, containerWidth } = this
    return sampleWidth > containerWidth
  }

  handleResizeWindow () {
    const { $container, $sample } = this.$refs

    this.content = $sample.innerText || $sample.textContent
    this.sampleWidth = $sample.getBoundingClientRect().width
    this.containerWidth = $container.clientWidth
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.overflow-text-tooltip {
  .text-container {
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .text-sample {
    white-space: nowrap;
  }
}
</style>
