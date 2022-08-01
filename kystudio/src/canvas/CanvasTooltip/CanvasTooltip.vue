<template>
  <v-label ref="$label" :config="labelStyle">
    <v-tag :config="tagStyle" />
    <v-text :config="textStyle" />
  </v-label>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import CanvasText from '../CanvasText/CanvasText'

@Component({
  components: {
    CanvasText
  }
})
export default class CanvasTooltip extends Vue {
  width = 300
  text = ''
  x = 0
  y = 0
  isVisible = false

  mounted () {
    this.addEventListener()
  }

  get labelStyle () {
    const { x, y, width, isVisible: visible } = this
    return { x, y, width, visible, listening: false }
  }

  get textStyle () {
    const { text, width } = this
    return {
      text,
      fontSize: 14,
      padding: 5,
      fill: 'white',
      width
    }
  }

  get tagStyle () {
    return {
      fill: '#5C5C5C',
      cornerRadius: 4,
      pointerDirection: 'down',
      pointerWidth: 10,
      pointerHeight: 10,
      lineJoin: 'round',
      shadowColor: 'black',
      shadowBlur: 5,
      shadowOffsetX: 5,
      shadowOffsetY: 5,
      shadowOpacity: 0.2
    }
  }

  addEventListener () {
    const $layer = this.$refs.$label.getNode().getLayer()
    $layer.on('show-tooltip', this.handleShowTooltip)
    $layer.on('hide-tooltip', this.handleHideTooltip)
  }

  reset () {
    this.x = 0
    this.y = 0
    this.text = ''
    this.isVisible = false
  }

  handleShowTooltip (event) {
    this.x = event.x
    this.y = event.y
    this.text = event.text
    this.isVisible = true
  }

  handleHideTooltip () {
    this.reset()
  }
}
</script>
