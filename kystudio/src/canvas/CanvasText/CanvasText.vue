<template>
  <v-group ref="$group" :config="groupStyle" @mousemove="handleMouseMove" @mouseleave="handleMouseLeave">
    <!-- 背景样式 -->
    <v-rect :config="backgroundStyle" />
    <!-- 采样文字 -->
    <v-text ref="$sampleText" :config="sampleStyle" />
    <!-- 真实渲染文字 -->
    <v-group :config="contentStyle">
      <v-text
        v-for="(displayText, lineIdx) in displayTexts"
        :ref="`${lineIdx}`"
        :key="displayText"
        :config="getMultipleLineStyle(displayText, lineIdx)"
      />
    </v-group>
  </v-group>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import { style } from '../utils'

@Component({
  props: {
    text: {
      type: String,
      required: true
    },
    x: {
      type: Number
    },
    y: {
      type: Number
    },
    fontSize: {
      type: Number,
      default: 12
    },
    fontStyle: {
      type: Number
    },
    color: {
      type: String
    },
    align: {
      type: String
    },
    verticalAlign: {
      type: String
    },
    padding: {
      type: [String, Number],
      default: 0
    },
    width: {
      type: Number,
      default: 0
    },
    minWidth: {
      type: Number,
      default: 0
    },
    maxWidth: {
      type: Number,
      default: Infinity
    },
    height: {
      type: Number,
      default: 0
    },
    minHeight: {
      type: Number,
      default: 0
    },
    maxHeight: {
      type: Number,
      default: Infinity
    },
    lineHeight: {
      type: Number,
      default: 1
    },
    background: {
      type: String
    },
    isVisible: {
      type: Boolean,
      default: true
    }
  }
})
export default class CanvasText extends Vue {
  sampleTexts = []

  mounted () {
    this.refreshSampleText()
  }

  updated () {
    this.refreshSampleText()
  }

  /**
   * 计算属性：文字是否被限制高度
   */
  get isLimitHeight () {
    const { height, maxHeight } = this
    // 有定高或者最大高度
    return height || maxHeight
  }

  /**
   * 计算属性：获取内边距尺寸
   */
  get paddings () {
    return style.getPaddings(this.padding)
  }

  /**
   * 计算属性：采样文字样式
   */
  get sampleStyle () {
    const { width, text, fontSize, paddings } = this
    const x = paddings.left
    const sampleWidth = width - paddings.left - paddings.right
    return { x, width: sampleWidth, text, fontSize, height: null, opacity: 0 }
  }

  /**
   * 计算属性：采样文本的高度
   */
  get sampleHeight () {
    const { sampleTexts, fontSize, lineHeight, paddings } = this
    return sampleTexts.length * fontSize * lineHeight + paddings.top + paddings.bottom
  }

  /**
   * 计算属性：Content尺寸
   */
  get contentSize () {
    const {
      height, minHeight, maxHeight, sampleHeight,
      width, minWidth, maxWidth,
      paddings
    } = this

    const fixSize = { width, height }
    const minSize = { width: minWidth, height: minHeight }
    const maxSize = { width: maxWidth, height: maxHeight }
    const sampleSize = { width, height: sampleHeight }

    return style.getContentSize(fixSize, minSize, maxSize, sampleSize, paddings)
  }

  /**
   * 计算属性：Box尺寸
   */
  get boxSize () {
    const { contentSize, paddings } = this
    return style.getBoxSize(contentSize, paddings)
  }

  /**
   * 计算属性：组件整体样式
   */
  get groupStyle () {
    const { x, y, isVisible: visible, boxSize: { width, height } } = this
    return { x, y, visible, width, height }
  }

  /**
   * 计算属性：背景样式
   */
  get backgroundStyle () {
    const { background: fill, boxSize: { width, height } } = this
    return { fill, width, height }
  }

  get contentStyle () {
    const { contentSize: { width, height }, paddings: { left: x, top: y } } = this
    return { x, y, width, height }
  }

  get displayTexts () {
    const { contentSize: { height }, fontSize, lineHeight, text, sampleTexts } = this
    const displayCount = Math.floor(height / (fontSize * lineHeight))

    let displayTexts = []
    let nextText = text

    for (let i = 0; i < displayCount; i += 1) {
      if (sampleTexts[i]) {
        if (i < displayCount - 1) {
          displayTexts.push(sampleTexts[i])
          nextText = nextText.replace(sampleTexts[i], '')
        } else {
          displayTexts.push(nextText)
        }
      }
    }
    return displayTexts
  }

  getMultipleLineStyle (text, lineIdx) {
    const { contentStyle, align, verticalAlign, fontSize, fontStyle, lineHeight, color: fill, displayTexts } = this
    const { width } = contentStyle
    const ellipsis = lineIdx === displayTexts.length - 1
    const wrap = 'none'
    const y = lineIdx * fontSize * lineHeight
    const height = fontSize * lineHeight

    return { y, text, width, height, align, verticalAlign, fontSize, fontStyle, fill, ellipsis, wrap, lineHeight }
  }

  isTextArrayNotSame (textArray = []) {
    const { sampleTexts } = this
    return !sampleTexts.length || sampleTexts.some((sampleText, idx) => sampleText !== textArray[idx])
  }

  refreshSampleText () {
    const $sampleText = this.$refs.$sampleText.getNode()
    const sampleTexts = $sampleText.textArr.map(({ text }) => text)

    if (this.isTextArrayNotSame(sampleTexts)) {
      this.sampleTexts = sampleTexts
    }

    this.$nextTick(() => {
      const $group = this.$refs.$group.getNode()
      $group.fire('text-redraw', null, true)
    })
  }

  handleMouseMove () {
    this.handleShowTooltip()
  }

  handleMouseLeave () {
    this.handleHideTooltip()
  }

  handleShowTooltip () {
    const { displayTexts, text } = this
    const $group = this.$refs.$group.getNode()
    const $layer = this.$refs.$group.getNode().getLayer()
    const [lastLine] = this.$refs[displayTexts.length - 1]
    const { text: lastLineText } = lastLine.getNode().textArr[0]

    if (/…$/.test(lastLineText)) {
      const { x, y } = $group.getClientRect({ relativeTo: $layer })
      $group.fire('show-tooltip', { x, y, text }, true)
    }
  }

  handleHideTooltip () {
    const { displayTexts } = this
    const $group = this.$refs.$group.getNode()
    const [lastLine] = this.$refs[displayTexts.length - 1]
    const { text: lastLineText } = lastLine.getNode().textArr[0]

    if (/…$/.test(lastLineText)) {
      $group.fire('hide-tooltip', null, true)
    }
  }
}
</script>
