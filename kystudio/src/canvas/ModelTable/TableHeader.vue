<template>
  <v-group ref="$group" :config="{ width: width }">
    <CanvasText
      color="#FFFFFF"
      text-overflow="ellipsis"
      :font-size="FONT_SIZE"
      :text="tableAlias"
      :width="width"
      :padding="`${realPadding.top} ${realPadding.left}`"
      :line-height="LINE_HEIGHT"
      :max-height="realPadding.top * 2 + FONT_SIZE * MAX_TEXT_LINES * LINE_HEIGHT"
      :background="isFactTable ? '#0A88DE' : '#25A79B'"
    />
  </v-group>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import CanvasText from '../CanvasText/CanvasText'

@Component({
  props: {
    tableAlias: {
      type: String,
      required: true
    },
    isFactTable: {
      type: Boolean,
      default: false
    },
    width: {
      type: Number,
      default: 0
    }
  },
  components: {
    CanvasText
  }
})
export default class ModelTable extends Vue {
  LINE_HEIGHT = 1.5
  MAX_TEXT_LINES = 5
  FONT_SIZE = 14
  PADDING = 10

  /**
   * 计算属性：真实内边距
   * @desc 由于设计稿视觉看上去内边距是10px，但实际上文字有LineHeight
   * 所以真正的 上下内边距 为 视觉边距 - 线高留白 / 2
   * 左右边距不受影响
   */
  get realPadding () {
    const { PADDING, LINE_HEIGHT, FONT_SIZE } = this
    const top = PADDING - (LINE_HEIGHT - 1) * FONT_SIZE / 2
    const left = PADDING
    return { top, left }
  }
}
</script>
